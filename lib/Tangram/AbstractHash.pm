# (c) Sound Object Logic 2000-2001

use strict;

package Tangram::AbstractHash;

use Tangram::Coll;
use vars qw(@ISA);
 @ISA = qw( Tangram::Coll );

use Carp;

sub content
{
    shift;
    @{shift()};
}

sub demand
{
    my ($self, $def, $storage, $obj, $member, $class) = @_;

    my %coll;

    if (my $prefetch = $storage->{PREFETCH}{$class}{$member}{$storage->export_object($obj)})
    {
	    print $Tangram::TRACE "demanding ".$storage->id($obj)
		.".$member from prefetch\n" if $Tangram::TRACE;
		%coll = %$prefetch;
    }
    else
    {
	    print $Tangram::TRACE "demanding ".$storage->id($obj)
		.".$member from storage\n" if $Tangram::TRACE;
		my $cursor = $self->cursor($def, $storage, $obj, $member);

		my @lost;
		for (my $item = $cursor->select; $item; $item = $cursor->next)
		{
			my $slot = shift @{ $cursor->{-residue} };
			if (!defined($slot)) {
			    warn "object ".$storage->id($item)." has no slot in hash ".$storage->id($obj)."/$member!";
			    push @lost, $item;
			} else {
			    $coll{$slot} = $item;
			}
		}
		# Try to DTRT when you've got NULL slots, though this
		# isn't much of a RT to D.
		while (@lost) {
		    my $c = 0;
		    while (!exists $coll{$c++}) { }
		    $coll{$c} = shift @lost;
		}
    }

	$self->set_load_state($storage, $obj, $member, map { ($_ ? ($_ => ($coll{$_} && $storage->id( $coll{$_} ) ) ) : ()) } keys %coll );

    return \%coll;
}

sub save_content
  {
	my ($obj, $field, $context) = @_;

	# has collection been loaded? if not, then it hasn't been modified
	return if tied $obj->{$field};
	return unless exists $obj->{$field} && defined $obj->{$field};
	
	my $storage = $context->{storage};

	foreach my $item (values %{ $obj->{$field} }) {
	  $storage->insert($item)
		unless $storage->id($item);
	}
  }

sub get_exporter
  {
	my ($self, $context) = @_;
	my $field = $self->{name};

	return sub {
	  my ($obj, $context) = @_;

	  return if tied $obj->{$field};
	  return unless exists $obj->{$field} && defined $obj->{$field};
	
	  my $storage = $context->{storage};

	  foreach my $item (values %{ $obj->{$field} }) {
		$storage->insert($item)
		  unless $storage->id($item);
	  }

	  $context->{storage}->defer(sub { $self->defered_save($obj, $field, $storage) } );
	  ();
	}
  }


1;
