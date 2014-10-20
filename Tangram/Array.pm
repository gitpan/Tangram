use strict;

package Tangram::Array;

use Tangram::AbstractArray;
use base qw( Tangram::AbstractArray );

use Carp;

sub reschema
{
	my ($self, $members) = @_;

	foreach my $member (keys %$members)
	{
		my $def = $members->{$member};

		unless (ref($def))
		{
			$def = { class => $def };
			$members->{$member} = $def;
		}

		$def->{table} ||= $def->{class} . "_$member";
		$def->{coll} ||= 'coll';
		$def->{item} ||= 'item';
		$def->{slot} ||= 'slot';
	}
   
	return keys %$members;
}

sub get_save_closures
{
	my ($self, $storage, $obj, $def, $id) = @_;

	my ($table, $cc, $ic, $sc) = @{ $def }{ qw( table coll item slot ) };

	my $ne = sub { shift() != shift() };

	my $modify = sub
	{
		my ($slot, $item) = @_;

		my $item_id = $storage->id($item)
			|| croak "element at $slot has no id";
         
		$storage->sql_do(
            "UPDATE $table SET $ic = $item_id WHERE $cc = $id AND $sc = $slot");
	};

	my $add = sub
	{
		my ($slot, $item) = @_;
		my $item_id = $storage->id($item);
		$storage->sql_do(
		    "INSERT INTO $table ($cc, $ic, $sc) VALUES ($id, $item_id, $slot)");
	};

	my $remove = sub
	{
		my ($new_size) = @_;
		$storage->sql_do(
            "DELETE FROM $table WHERE $cc = $id AND $sc >= $new_size");
	};

	return ($ne, $modify, $add, $remove);
}

sub erase
{
	my ($self, $storage, $obj, $members, $coll_id) = @_;

	foreach my $member (keys %$members)
	{
		my $def = $members->{$member};
      
		my $table = $def->{table} || $def->{class} . "_$member";
		my $coll_col = $def->{coll} || 'coll';
     
		my $sql = "DELETE FROM $table WHERE $coll_col = $coll_id";
	  
		if ($def->{aggreg})
		{
			my @content = @{ $obj->{$member} };
			$storage->sql_do($sql);
			$storage->erase( @content ) ;
		}
		else
		{
			$storage->sql_do($sql);
		}
	}
}

sub cursor
{
	my ($self, $def, $storage, $obj, $member) = @_;

	my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

	my $coll_id = $storage->id($obj);
	my $coll_tid = $storage->alloc_table;
	my $table = $def->{table};
	my $item_tid = $cursor->{-stored}->root_table;
	my $coll_col = $def->{coll};
	my $item_col = $def->{item};
	my $slot_col = $def->{slot};
	$cursor->{-coll_tid} = $coll_tid;
	$cursor->{-coll_cols} = ", t$coll_tid.$slot_col";
	$cursor->{-coll_from} = ", $table t$coll_tid";
	$cursor->{-coll_where} = "t$coll_tid.$coll_col = $coll_id AND t$coll_tid.$item_col = t$item_tid.id";
   
	return $cursor;
}

sub query_expr
{
	my ($self, $obj, $members, $tid) = @_;
	map { Tangram::CollExpr->new($obj, $_); } values %$members;
}

sub prefetch
{
	my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;

	my $ritem = $storage->remote($def->{class});

	# first retrieve the collection-side ids of all objects satisfying $filter
	# empty the corresponding prefetch array

	my $ids = $storage->my_select_data( cols => [ $coll->{id} ], filter => $filter );
	my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {}; # weakref

	while (my ($id) = $ids->fetchrow)
	{
		$prefetch->{$id} = []
	}

	undef $ids;

	# now fetch the items

	my $cursor = Tangram::Cursor->new($storage, $ritem, $storage->{db});
	my $includes = $coll->{$member}->includes($ritem);

	# also retrieve collection-side id and index of elmt in sequence
	$cursor->retrieve($coll->{id},
        Tangram::Number->expr("t$includes->{link_tid}.$def->{slot}") );

	$cursor->select($filter ? $filter & $includes : $includes);
   
	while (my $item = $cursor->current)
	{
		my ($coll_id, $slot) = $cursor->residue;
		$prefetch->{$coll_id}[$slot] = $item;
		$cursor->next;
	}

	return $prefetch;
}

$Tangram::Schema::TYPES{array} = Tangram::Array->new;

1;

__END__


sub defered_save
{
	use integer;

	my ($self, $storage, $obj, $members, $coll_id) = @_;

	foreach my $member (keys %$members)
	{
		next if tied $obj->{$member}; # collection has not been loaded, thus not modified
		
		my $def = $members->{$member};
		my ($table, $coll_col, $item_col, $slot_col) = @{ $def }{ qw( table coll item slot ) };
		my $id = $storage->id($obj);

		my $new_state = $obj->{$member} || [];
		my $new_size = @$new_state;

		my $old_state = $self->get_load_state($storage, $obj, $member) || [];
		my $old_size = @$old_state;

		my ($common, $changed) = Tangram::Coll::array_diff($new_state, $old_state);
            
		for my $slot (@$changed)
		{
			my $item_id = $storage->id($new_state->[$slot])
				|| croak "member $new_state->[$slot] has no id";
         
			$storage->sql_do(
			    "UPDATE $table SET $item_col = $item_id WHERE $coll_col = $id AND $slot_col = $slot");
		}

		if ($old_size > $new_size)
		{
			$storage->sql_do(
                "DELETE FROM $table WHERE $coll_col = $id AND $slot_col >= $new_size");
		}

		for my $slot ($old_size .. ($new_size-1))
		{
			my $item_id = $storage->id($new_state->[$slot]);
			$storage->sql_do(
			    "INSERT INTO $table ($coll_col, $item_col, $slot_col) VALUES ($id, $item_id, $slot)");
		}

		$self->set_load_state($storage, $obj, $member, [ @$new_state ] );	
		$storage->tx_on_rollback(
            sub { $self->set_load_state($storage, $obj, $member, $old_state) } );
	}
}
