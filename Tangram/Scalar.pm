use strict;

use Tangram::Type;

package Tangram::Scalar;

use base qw( Tangram::Type );

sub reschema
{
   my ($self, $members) = @_;
   keys %$members;
}

sub query_expr
{
   my ($self, $obj, $memdefs, $tid) = @_;

   map
   {
      Tangram::Expr->new("t$tid.$_", $self, $obj);
   } keys %$memdefs;
}

sub cols
{
   my ($self, $members) = @_;
   values %$members;
}

sub read
{
   my ($self, $row, $obj, $members) = @_;
   @$obj{keys %$members} = splice @$row, 0, keys %$members;
}

sub literal
{
   my ($self, $lit) = @_;
   return $lit;
}

sub content
{
   shift;
   shift;
}

package Tangram::Number;

use base qw( Tangram::Scalar );

sub save
{
   my ($self, $obj, $members) = @_;
   map { defined($_) ? 0 + $_ : 'NULL' } @$obj{keys %$members};
}

package Tangram::Integer;

use base qw( Tangram::Number );

$Tangram::Schema::TYPES{int} = Tangram::Integer->new;

package Tangram::Real;

use base qw( Tangram::Number );

$Tangram::Schema::TYPES{real} = Tangram::Real->new;

package Tangram::String;

use base qw( Tangram::Scalar );

$Tangram::Schema::TYPES{string} = Tangram::String->new;

sub save
{
   my ($self, $obj, $members) = @_;
   return map { if (defined($_)) { s/'/''/g; "'$_'" } else { 'NULL' } } @$obj{keys %$members};
}

sub literal
{
   my ($self, $lit) = @_;
   return "'$lit'";
}

package Tangram::RefOnDemand;

sub TIESCALAR
{
   my $pkg = shift;
   return bless [ @_ ], $pkg;
}

sub FETCH
{
   my $self = shift;
   my ($storage, $id, $member, $refid) = @$self;
   my $obj = $storage->{objects}{$id};
   my $refobj = $storage->load($refid);
   untie $obj->{$member};
   $obj->{$member} = $refobj;
   return $refobj;
}

sub STORE
{
   my ($self, $val) = @_;
   my ($storage, $id, $member, $refid) = @$self;
   my $obj = $storage->{objects}{$id};
   untie $obj->{$member};
   return $obj->{$member} = $val;
}

sub id
{
   my ($storage, $id, $member, $refid) = @{shift()};
   $refid
}


1;