use strict;

use Tangram::Coll;

package Tangram::AbstractSet;

use base qw( Tangram::Coll );

use Carp;

sub save
{
   my ($self, $obj, $members, $storage, $table, $id) = @_;

   foreach my $coll (keys %$members)
   {
      next if tied $obj->{$coll};
      next unless defined $obj->{$coll};

      foreach my $item ($obj->{$coll}->members)
      {
         $storage->insert($item) unless $storage->id($item);
      }
   }

   $storage->defer(sub { $self->defered_save(shift, $obj, $members, $id) } );

   return ();
}

sub update
{
   my ($self, $storage, $obj, $member, $insert, $remove) = @_;

   return unless defined $obj->{$member};

   my $coll_id = $storage->id($obj);
   my $old_states = $storage->{scratch}{ref($self)}{$coll_id};
   my $old_state = $old_states->{$member};
   my %new_state = ();

   foreach my $item ($obj->{$member}->members)
   {
      my $item_id = $storage->id($item) || croak "member $item has no id";
      
      unless (exists $old_state->{$item_id})
      {
         &$insert($item_id);
      }

      $new_state{$item_id} = 1;
   }

   foreach my $del (keys %$old_state)
   {
      next if $new_state{$del};
      &$remove($del);
   }

   $old_states->{$member} = \%new_state;
   $storage->tx_on_rollback( sub { $old_states->{$member} = $old_state } );
}

sub remember_state
{
   my ($self, $def, $storage, $obj, $member, $set) = @_;

   my %new_state;
   @new_state{ map { $storage->id($_) } $set->members } = 1 x $set->size;
   $storage->{scratch}{ref($self)}{$storage->id($obj)}{$member} = \%new_state;
}

sub content
{
   shift;
   shift->members;
}

package Tangram::Set;

use base qw( Tangram::AbstractSet );

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
   }
   
   return keys %$members;
}

sub defered_save
{
   my ($self, $storage, $obj, $members, $coll_id) = @_;

   foreach my $member (keys %$members)
   {
      next if tied $obj->{$member};

      my $def = $members->{$member};
      
      my $table = $def->{table} || $def->{class} . "_$member";
      my $coll_col = $def->{coll} || 'coll';
      my $item_col = $def->{item} || 'item';
      
      $self->update($storage, $obj, $member,
         sub
         {
            my $sql = "INSERT INTO $table ($coll_col, $item_col) VALUES ($coll_id, @_)";
            $storage->sql_do($sql);
         },

         sub
         {
            my $sql = "DELETE FROM $table WHERE $coll_col = $coll_id AND $item_col = @_";
            $storage->sql_do($sql);
         } );
   }
}

sub prefetch
{
   my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;
   
   my $ritem = $storage->remote($def->{class});

   my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {}; # weakref

   my $ids = $storage->my_select_data( cols => [ $storage->object($class)->{id} ] );

   while (my $id = $ids->fetchrow)
   {
      $prefetch->{$id} = []
   }

	my $includes = $coll->{$member}->includes($ritem);
   $includes &= $filter if $filter;

   my $cursor = $storage->my_cursor( $ritem, filter => $includes, retrieve => [ $coll->{id} ] );
   
   while (my $item = $cursor->current)
   {
      my ($coll_id) = $cursor->residue;
      push @{ $prefetch->{$coll_id} }, $item;
      $cursor->next;
   }

   return $prefetch;
}

sub demand
{
   my ($self, $def, $storage, $obj, $member, $class) = @_;

   print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;

   my $set = Set::Object->new;

   if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->id($obj)})
   {
      $set->insert(@$prefetch);
   }
   else
   {
      my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

      my $coll_id = $storage->id($obj);
      my $coll_tid = $storage->alloc_table;
      my $table = $def->{table};
      my $item_tid = $cursor->{-stored}->root_table;
      my $coll_col = $def->{coll} || 'coll';
      my $item_col = $def->{item} || 'item';
      $cursor->{-coll_tid} = $coll_tid;
      $cursor->{-coll_from} = ", $table t$coll_tid";
      $cursor->{-coll_where} = "t$coll_tid.$coll_col = $coll_id AND t$coll_tid.$item_col = t$item_tid.id";

      $set->insert($cursor->select);
   }

   $self->remember_state($def, $storage, $obj, $member, $set);

   $set;
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
      $storage->sql_do($sql);
   }
}

sub query_expr
{
   my ($self, $obj, $members, $tid) = @_;
   map { Tangram::CollExpr->new($obj, $_); } values %$members;
}

package Tangram::Alias;

my $top = 1_000;

sub new
{
   ++$top
}

$Tangram::Schema::TYPES{set} = Tangram::Set->new;

package Tangram::IntrSet;

use base qw( Tangram::AbstractSet );

use Carp;

sub reschema
{
   my ($self, $members, $class, $schema) = @_;

   foreach my $member (keys %$members)
   {
      my $def = $members->{$member};

      unless (ref($def))
      {
         $def = { class => $def };
         $members->{$member} = $def;
      }

      $def->{coll} ||= $class . "_$member";

      $schema->{classes}{$def->{class}}{stateless} = 0;
   }
   
   return keys %$members;
}

sub defered_save
{
   my ($self, $storage, $obj, $members, $coll_id) = @_;

	my $classes = $storage->{schema}{classes};

   foreach my $member (keys %$members)
   {
      next if tied $obj->{$member};

      my $def = $members->{$member};
      
      my $item_classdef = $classes->{$def->{class}};
      my $table = $item_classdef->{table};
      my $item_col = $def->{coll};
      
      $self->update($storage, $obj, $member,
         sub
         {
            my $sql = "UPDATE $table SET $item_col = $coll_id WHERE id = @_";
            $storage->sql_do($sql);
         },

         sub
         {
            my $sql = "UPDATE $table SET $item_col = NULL WHERE id = @_ AND $item_col = $coll_id";
            $storage->sql_do($sql);
         } );
   }
}

sub demand
{
   my ($self, $def, $storage, $obj, $member, $class) = @_;

   print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;

   my $set = Set::Object->new();

   if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->id($obj)})
   {
      $set->insert(@$prefetch);
   }
   else
   {
      my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

      my $coll_id = $storage->id($obj);
      my $tid = $cursor->{-stored}->leaf_table;
      $cursor->{-coll_where} = "t$tid.$def->{coll} = $coll_id";
   
      $set->insert($cursor->select);
   }

   $self->remember_state($def, $storage, $obj, $member, $set);

   return $set;
}

sub erase
{
   my ($self, $storage, $obj, $members, $coll_id) = @_;

   foreach my $member (keys %$members)
   {
      next if tied $obj->{$member};

      my $def = $members->{$member};
      my $item_classdef = $storage->{schema}{$def->{class}};
      my $table = $item_classdef->{table} || $def->{class};
      my $item_col = $def->{coll};
      
      my $sql = "UPDATE $table SET $item_col = NULL WHERE $item_col = $coll_id";
      $storage->sql_do($sql);
   }
}

sub query_expr
{
   my ($self, $obj, $members, $tid) = @_;
   map { Tangram::IntrCollExpr->new($obj, $_); } values %$members;
}

sub prefetch
{
   my ($self, $storage, $def, $coll, $class, $member, $filter) = @_;
   
   my $ritem = $storage->remote($def->{class});

   my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {}; # weakref

	my $includes = $coll->{$member}->includes($ritem);
   $includes &= $filter if $filter;

   my $cursor = $storage->my_cursor( $ritem, filter => $includes, retrieve => [ $coll->{id} ] );

   while (my $item = $cursor->current)
   {
      my ($coll_id) = $cursor->residue;
      push @{ $prefetch->{$coll_id} }, $item;
      $cursor->next;
   }

   return $prefetch;
}

$Tangram::Schema::TYPES{iset} = Tangram::IntrSet->new;

1;