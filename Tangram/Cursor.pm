use strict;

package Tangram::Cursor;

use vars qw( $stored %done );
use Carp;

sub new
{
	my ($pkg, $storage, $target, $conn) = @_;

	confess unless $conn;

	my $implicit = ref $target ? $target->object : $storage->cursor_object($target);
	$target = Tangram::CursorObject->copy($implicit);

	my $self = {};

	$self->{-storage} = $storage;
	$self->{-target} = $target->{class};
	$self->{-stored} = $target;
	$self->{-implicit} = $implicit;
	$self->{-selects} = [];
	$self->{-conn} = $conn;

	bless $self, $pkg;
}

sub select
{
	my $self = shift;

	my %args;

	if (@_ > 1)
	{
		%args = @_;
	}
	else
	{
		$args{filter} = shift;
	}

	$self->{-order} = $args{order};
	$self->{-desc} = $args{desc};
	$self->{-distinct} = $args{distinct};

	$self->retrieve( @{ $args{retrieve} } ) if exists $args{retrieve};

	local $stored = $self->{-stored};
   
	local %done = map { $_ => 1 } $stored->parts();
	delete $done{ $stored->class };

	my $filter = Tangram::Filter->new( tight => 100, objects => Set::Object->new($stored) );

	if (my $user_filter = $args{filter})
	{
		$filter->{expr} = $user_filter->{expr};
		$filter->{objects}->insert($user_filter->{objects}->members);
		$filter->{objects}->remove($self->{-implicit});
	}

	$self->_select($self->{-target}, $filter);

	return undef unless @{$self->{-selects}}; 

	my $select = shift @{$self->{-selects}};
	return undef unless $select;

	my ($sql, @parts) = @$select;
	$self->{parts} = \@parts;
	$self->{-cursor} = $self->{-storage}->sql_cursor($sql, $self->{-conn});

	return $self->next;
}

sub _select
{
	my ($self, $class, $filter) = @_;

	return if exists $done{$class};

	$done{$class} = 1;

	my $storage = $self->{-storage};
	my $schema = $storage->{schema};
	my $classes = $schema->{classes};
	my $classdef = $classes->class($class);
	my $class2id = $storage->{class2id};
	my $stored = $self->{-stored};

	if ($classdef->{specs} && @{$classdef->{specs}} || $classdef->{stateless})
	{
		my @shared = ( ($classdef->{abstract} ? () : $class), # concat lists
					   map { _select_shared($_, $classes) } @{$classdef->{specs}} );

		if (@shared)
		{
			my $cols = $stored->cols;
			my $from = $filter->from;
			my $cid = $stored->class_id_col;

			my $where = join ' AND ',
				"$cid IN (" . join(', ', map { $storage->class_id($_) } @shared) . ')',
					$filter->where;
         
			push @{$self->{-selects}}, [ $self->build_select($cols, $from, $where), $stored->parts ];
		}

		foreach my $spec (@{$classdef->{specs}})
		{
			$self->_select_unshared($spec, $filter);
		}
	}

	elsif (!$classdef->{abstract})
	{
		my $cols = $stored->cols;
		my $from = $filter->from;
		my $where = $filter->where;
		push @{$self->{-selects}}, [ $self->build_select($cols, $from, $where), $stored->parts ];
	}
}

sub _select_shared
{
	my ($class, $classes) = @_;

	return () if $done{$class};

	my $classdef = $classes->class($class);
	return () unless $classdef->{stateless} && @{ $classdef->{bases} } <= 1;

	$done{$class} = 1;

	( ($classdef->{abstract} ? () : $class), # concat lists
	  map { _select_shared($_, $classes) } @{$classdef->{specs}} );
}

sub _select_unshared
{
	my ($self, $class, $filter) = @_;

	my $classes = $self->{-storage}{schema}{classes};
	my $classdef = $classes->{$class};

	if ($classdef->{stateless} && @{ $classdef->{bases} } <= 1)
	{
		foreach my $spec (@{$classdef->{specs}})
		{
			$self->_select_unshared($spec, $filter);
		}
	}
	else
	{
		my $mark = $stored->mark();
      
		$stored->push_spec($class) unless $classdef->{stateless};

		my $bases = $classdef->{bases};

		if (@$bases > 1)
		{
			my $schema = $self->{-storage}{schema};

			foreach my $base ( @$bases )
			{
				next if $done{$base};

				$schema->visit_up( $base,
								   sub
								   {
									   my $base = shift;
									   $stored->push_spec( $base ) unless $classes->{$base}{stateless} || $done{$base};
									   $done{$base} = 1;
								   } );
			}
		}

		$self->_select($class, $filter);
      
		$stored->pop_spec($mark);
	}
}

sub build_select
{
	my ($self, $cols, $from, $where) = @_;

	if (my $retrieve = $self->{-retrieve})
	{
		$cols = join ', ', $cols, map { $_->{expr} } @$retrieve;
	}

	my $select = "SELECT";

	$select .= ' DISTINCT' if $self->{-distinct};

	$select .= " $cols\n\tFROM $from\n\t";
	$select .= "WHERE $where" if $where;

	if (my $order = $self->{-order})
	{
		$select .= "\n\tORDER BY " . join ', ', map { $_->{expr} } @$order;
	}

	if ($self->{-desc})
	{
		$select .= ' DESC';
	}

	return $select;
}

sub _next
{
	my ($self) = @_;

	$self->{-current} = undef;
	my @row;

	while (1)
	{
		@row = $self->{-cursor}->fetchrow;
		last if @row;

		my $select = shift @{$self->{-selects}};
      
		unless ($select)
		{
			$self->{-cursor}->close();
			return undef;
		}

		$self->{-cursor}{statement}->finish;

		my ($sql, @parts) = @$select;
		$self->{parts} = \@parts;
		$self->{-cursor} = $self->{-storage}->sql_cursor($sql, $self->{-conn});
	}

	my $id = shift @row;
	my $storage = $self->{-storage};

	my $classId = shift @row;
	my $class = $storage->{id2class}{$classId} or die "unknown class id $classId";

	# even if object is already loaded we must read it so that @rpw only contains residue
	my $obj = $storage->read_object($id, $class, \@row, @{ $self->{parts} } );

	$self->{-residue} = exists $self->{-retrieve}
		? [ map { ref $_ ? $_->{type}->read_data(\@row) : shift @row } @{$self->{-retrieve}} ]
			: \@row;

	# if object is already loaded return previous copy
	$obj = $storage->{objects}{$id} if exists $storage->{objects}{$id};
   
	$self->{-current} = $obj;

	return $obj;
}

sub next
{
	my ($self) = @_;

	return $self->_next unless wantarray;

	my ($obj, @results);

	while (defined($obj = $self->_next))
	{
		push @results, $obj;
	}

	return @results;
}

sub current
{
	my ($self) = @_;
	$self->{-current}
}

sub retrieve
{
	my $self = shift;
	push @{$self->{-retrieve}}, @_;
}

sub residue
{
	@{shift->{-residue}};
}

sub object
{
	my ($self) = @_;
	return $self->{object};
}

sub close
{
	my ($self) = @_;
	$self->{-cursor}->close();
}

package Tangram::DataCursor;

use Carp;

sub open
{
	my ($type, $storage, $select, $conn) = @_;
   
	confess unless $conn;
	
	bless
	{
	 select => $select,
	 cursor => $storage->sql_cursor(substr($select->{expr}, 1, -1), $conn),
	}, $type;
}

sub fetchrow
{
	my $self = shift;
	my @row = $self->{cursor}->fetchrow;
	return () unless @row;
	map { $_->{type}->read_data(\@row) } @{$self->{select}{cols}};
}

sub fetchall_arrayref
{
	my $self = shift;
	my @results;

	while (my @row = $self->fetchrow)
	{
		push @results, [ @row ];
	}

	return \@results;
}

sub new
{
	my $pkg = shift;
	return bless [ @_ ] , $pkg;
}

1;
