use strict;

package Tangram::FlatArray::Expr;

sub new
{
	my $pkg = shift;
	bless [ @_ ], $pkg;
}

sub includes
{
	my ($self, $item) = @_;
	my ($coll, $memdef) = @$self;

	$item = Tangram::String::quote($item)
		if $memdef->{string_type};

	my $coll_tid = 't' . $coll->root_table;
	my $data_tid = 't' . Tangram::Alias->new;

	return Tangram::Filter->new
		(
		 expr => "$data_tid.coll = $coll_tid.id AND $data_tid.v = $item",
		 tight => 100,      
		 objects => Set::Object->new($coll, Tangram::Table->new($memdef->{table}, $data_tid) ),
		 data_tid => $data_tid # for prefetch
		);
}

sub exists
{
	my ($self, $item) = @_;
	my ($coll, $memdef) = @$self;

	$item = Tangram::String::quote($item)
		if $memdef->{string_type};

	my $coll_tid = 't' . $coll->root_table;

	return Tangram::Filter->new
		(
		 expr => "EXISTS (SELECT * FROM $memdef->{table} WHERE coll = $coll_tid.id AND v = $item)",
		 objects => Set::Object->new($coll),
		);
}

package Tangram::FlatArray;

use base qw( Tangram::AbstractArray );
use Tangram::AbstractArray;

$Tangram::Schema::TYPES{flat_array} = Tangram::FlatArray->new;

sub reschema
{
    my ($self, $members, $class) = @_;
    
    for my $field (keys %$members)
    {
		my $def = $members->{$field};
		my $refdef = ref($def);

		unless ($refdef)
		{
			# not a reference: field => field
			$def = $members->{$field} = { type => 'string' };
		}

		$def->{table} ||= $class . "_$field";
		$def->{type} ||= 'string';
		$def->{string_type} = $def->{type} eq 'string';
		$def->{sql} ||= $def->{string_type} ? 'VARCHAR(255)' : uc($def->{type});
    }

    return keys %$members;
}

sub demand
{
	my ($self, $def, $storage, $obj, $member, $class) = @_;

	print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;
   
	my @coll;

	if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->id($obj)})
	{
		@coll = @$prefetch;
	}
	else
	{
		my $id = $storage->id($obj);

		my $sth = $storage->sql_prepare(
            "SELECT a.i, a.v FROM $def->{table} a WHERE coll = $id", $storage->{db});

		$sth->execute();
		
		for my $row (@{ $sth->fetchall_arrayref() })
		{
			my ($i, $v) = @$row;
			$coll[$i] = $v;
		}
	}

	$self->set_load_state($storage, $obj, $member, [ @coll ] );

	return \@coll;
}

sub save
{
	my ($self, $cols, $vals, $obj, $members, $storage, $table, $id) = @_;
	$storage->defer(sub { $self->defered_save(shift, $obj, $members, $id) } );
	return ();
}

my $no_ref = 'illegal reference in flat array';

sub get_save_closures
{
	my ($self, $storage, $obj, $def, $id) = @_;

	my $table = $def->{table};

	my ($ne, $quote);

	if ($def->{string_type})
	{
		$ne = sub { my ($a, $b) = @_; defined($a) != defined($b) || $a ne $b };
		$quote = sub { $storage->{db}->quote(shift()) };
	}
	else
	{
		$ne = sub { my ($a, $b) = @_; defined($a) != defined($b) || $a != $b };
		$quote = sub { shift() };
	}

	my $modify = sub
	{
		my ($i, $v) = @_;
		die $no_ref if ref($v);
		$v = $quote->($v);
		$storage->sql_do("UPDATE $table SET v = $v WHERE coll = $id AND i = $i");
	};

	my $add = sub
	{
		my ($i, $v) = @_;
		die $no_ref if ref($v);
		$v = $quote->($v);
		$storage->sql_do("INSERT INTO $table (coll, i, v) VALUES ($id, $i, $v)");
	};

	my $remove = sub
	{
		my ($new_size) = @_;
		$storage->sql_do("DELETE FROM $table WHERE coll = $id AND i >= $new_size");
	};

	return ($ne, $modify, $add, $remove);
}

sub erase
{
	my ($self, $storage, $obj, $members, $coll_id) = @_;

	foreach my $def (values %$members)
	{
		my $id = $storage->id($obj);
		$storage->sql_do("DELETE FROM $def->{table} WHERE coll = $id");
	}
}

sub coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
		$tables->{ $member->{table} }{COLS} =
		{
		 coll => $schema->{sql}{id},
		 i => 'INT',
		 v => $member->{sql}
		};
    }
}

sub query_expr
{
	my ($self, $obj, $members, $tid) = @_;
	map { Tangram::FlatArray::Expr->new($obj, $_); } values %$members;
}

sub prefetch
{
	my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;

	my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {};

	my $restrict = $filter ? ', ' . $filter->from() . ' WHERE ' . $filter->where() : '';

	my $sth = $storage->sql_prepare(
        "SELECT coll, i, v FROM $def->{table} $restrict", $storage->{db});
	$sth->execute();
		
	for my $row (@{ $sth->fetchall_arrayref() })
	{
		my ($id, $i, $v) = @$row;
		$prefetch->{$id}[$i] = $v;
	}

	return $prefetch;
}

1;