use strict;

use Tangram::Type;

package Tangram::Coll;

use base qw( Tangram::Type );

sub members
{
   my ($self, $members) = @_;
   keys %$members;
}

sub cols
{
   ()
}

sub read
{
   my ($self, $row, $obj, $members, $storage, $class) = @_;

   foreach my $member (keys %$members)
   {
      tie $obj->{$member}, 'Tangram::CollOnDemand',
			$self, $members->{$member}, $storage, $storage->id($obj), $member, $class;
   }
}

package Tangram::CollExpr;

sub new
{
   my $pkg = shift;
   bless [ @_ ], $pkg;
}

sub includes
{
   my ($self, $item) = @_;
   my ($coll, $memdef) = @$self;

   my $coll_tid = $coll->root_table;

   my $link_tid = Tangram::Alias->new;
   my $coll_col = $memdef->{coll};
   my $item_col = $memdef->{item};

   my $objects = Set::Object->new($coll, Tangram::LinkTable->new($memdef->{table}, $link_tid) ),
   my $target;

   if (ref $item)
   {
      if ($item->isa('Tangram::QueryObject'))
      {
         $target = 't' . $item->object->root_table . '.id';
         $objects->insert( $item->object );
      }
      else
      {
         $target = $coll->{storage}->id($item)
				or die "'$item' is not a persistent object";
      }
   }
   else
   {
      $target = $item;
   }

   Tangram::Filter->new
   (
      expr => "t$link_tid.$coll_col = t$coll_tid.id AND t$link_tid.$item_col = $target",
      tight => 100,      
      objects => $objects,
		link_tid => $link_tid # for Sequence prefetch
   )
}

package Tangram::IntrCollExpr;

sub new
{
   my $pkg = shift;
   bless [ @_ ], $pkg;
}

sub includes
{
   my ($self, $item) = @_;
   my ($coll, $memdef) = @$self;
   my $coll_tid = $coll->root_table;
	my $item_class = $memdef->{class};
	my $storage = $coll->{storage};

	my $item_id;

	if (ref($item))
	{
		if ($item->isa('Tangram::QueryObject'))
		{
		   my $item_tid = $item->object->table($item_class);

			return Tangram::Filter->new
			(
				expr => "t$item_tid.$memdef->{coll} = t$coll_tid.id",
				tight => 100,
				objects => Set::Object->new($coll, $item->object),
			)
		}

		$item_id = $storage->id($item);

	}
	else
	{
		$item_id = $item;
	}

	my $remote = $storage->remote($item_class);
	return $self->includes($remote) & $remote->{id} == $item_id;
}

package Tangram::LinkTable;
use Carp;

sub new
{
   my ($pkg, $name, $alias) = @_;
   bless [ $name, $alias ], $pkg;
}

sub from
{
   my ($name, $alias) = @{shift()};
   "$name t$alias"
}

sub where
{
   confess unless wantarray;
   ()
}
package Tangram::CollOnDemand;

sub TIESCALAR
{
   my $pkg = shift;
   return bless [ @_ ], $pkg; # [ $type, $storage, $id, $member, $class ]
}

sub FETCH
{
   my $self = shift;
   my ($type, $def, $storage, $id, $member, $class) = @$self;
   my $obj = $storage->{objects}{$id} or die;
   my $coll = $type->demand($def, $storage, $obj, $member, $class);
   untie $obj->{$member};
   $obj->{$member} = $coll;
}

sub STORE
{
   my ($self, $coll) = @_;
   my ($type, $def, $storage, $id, $member, $class) = @$self;

   my $obj = $storage->{objects}{$id} or die;
   $type->demand($def, $storage, $obj, $member, $class);

   untie $obj->{$member};

   $obj->{$member} = $coll;
}

package Tangram::CollCursor;

@Tangram::CollCursor::ISA = 'Tangram::Cursor';

sub build_select
{
   my ($self, $cols, $from, $where) = @_;

   if ($self->{-coll_where})
   {
      $where .= ' AND ' if $where;
      $where .= "$self->{-coll_where}" if $self->{-coll_where};
   }

   $where = $where && "WHERE $where";
   $cols .= $self->{-coll_cols} if exists $self->{-coll_cols};
	$from .= $self->{-coll_from} if exists $self->{-coll_from};
   "SELECT $cols\n\tFROM $from\n\t$where";
}

sub DESTROY
{
   my ($self) = @_;
   #print "@{[ keys %$self ]}\n";
   $self->{-storage}->free_table($self->{-coll_tid});
}


1;