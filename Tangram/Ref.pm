use strict;

use Tangram::Scalar;

package Tangram::Ref;

use base qw( Tangram::Scalar );

$Tangram::Schema::TYPES{ref} = Tangram::Ref->new;

sub save
{
   my ($self, $obj, $members, $storage, $table, $id) = @_;
   map { tied($obj->{$_}) ? tied($obj->{$_})->id
      : $storage->auto_insert($obj->{$_}, $table, $_, $id) } keys %$members;
}

sub read
{
   my ($self, $row, $obj, $members, $storage) = @_;
   
   my $id = $storage->id($obj);

   foreach my $r (keys %$members)
   {
      my $rid = shift @$row;

      if ($rid)
      {
         tie $obj->{$r}, 'Tangram::RefOnDemand', $storage, $id, $r, $rid;
      }
      else
      {
         $obj->{$r} = undef;
      }
   }
}

sub query_expr
{
   my ($self, $obj, $memdefs, $tid) = @_;

   map
   {
      Tangram::Expr->new("t$tid.$_", $self, $obj);
   } keys %$memdefs;
}

1;