

use strict;

package Tangram::Cursor;

use vars qw( $stored %done );
use Carp;

sub new
{
	my ($pkg, $storage, $remote, $conn) = @_;

	confess unless $conn;

	$remote = $storage->remote($remote)
	    unless ref $remote or not defined $remote;

	my $self = {};

	$self->{TARGET} = $remote;
	$self->{STORAGE} = $storage;
	$self->{SELECTS} = [];
	$self->{CONNECTION} = $conn;
	$self->{OWN_CONNECTION} = $conn != $storage->{db};

	bless $self, $pkg;
}

sub DESTROY
  {
	my $self = shift;
	$self->close();
  }

sub close
  {
	my $self = shift;

	if ($self->{SELECTS}) {
	  for my $select ( @{ $self->{SELECTS} } ) {
		my $sth = $select->[1] or next;
		$sth->finish() if $sth->{Active};
	  }
	}

	$self->{CONNECTION}->disconnect()
	  if $self->{OWN_CONNECTION};
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
	$self->{-group} = $args{group};
	$self->{-desc} = $args{desc};
	$self->{-distinct} = $args{distinct};
	$self->{-limit} = $args{limit};
	$self->{-noexec} = $args{noexec};

	# with outer queries, each remote object is either inside or
	# outside the query.
	my ($inner_objects, $outer_objects)
	    = (Set::Object->new(), Set::Object->new());

	if (exists $args{retrieve}) {
	    $self->retrieve( @{ $args{retrieve} } );
	    # assume that objects are outside the query until joined
	    $outer_objects->insert
		( map { $_->{objects}->members }
		  @{ $args{retrieve} } );
	}

	my $target = $self->{TARGET};
	my (@filter_from, @filter_where);
	$inner_objects->insert($target->object) if $target;

	my $filter = Tangram::Filter->new( tight => 100,
					   objects => $inner_objects );
	my ($seen_inner, $outer);

	# anything mentioned in the `filter' is part of the inner query
	if (my $user_filter = $args{filter}) {
	    $seen_inner = 1;
	    $filter->{expr} = $user_filter->{expr};
	    $inner_objects->insert($user_filter->{objects}->members);
	}

	$outer_objects->remove($inner_objects->members);

	# anything mentioned in the `outer_filter' is part of the
	# outer query
	if (my $outer_filter = $args{outer_filter}) {
	    #kill 2, $$;
	    if ( my $forced_outer = $args{force_outer} ) {
		$inner_objects->remove(map { $_->object }
				       @$forced_outer);
	    }

	    $outer = Tangram::Filter->new( tight => 100,
					   objects => $outer_objects );
	    $outer->{expr} = $outer_filter->{expr};
	    $outer->{objects}->insert($outer_filter->{objects}->members);
	    $outer->{objects}->remove($inner_objects->members);

	} elsif ( $outer_objects->size ) {

	    # If there is no outer query, then we must add the
	    # selected tables to the inner query part.

	    # this follows old behaviour, but may result in cartesian
	    # products.
	    $inner_objects->insert($outer_objects->members);
	}

	# insert all inner tables to the inner filter
	$filter->{objects}->insert($inner_objects->members);
	$filter->{objects}->remove($target->object) if $target;

	my @polysel = 
	     $self->{STORAGE}->get_polymorphic_select
	     ( $target
	       ? ($target->class||confess("argh!"))
	       : "");

	$self->{SELECTS} =
	    [
	     map {
		 [ $self->build_select( $_,
					[],
					[ $filter->from ],
					[ $filter->where ],
					( $outer
					  ? ([ $outer->from ],
					     [ $outer->where ],
					    ) : () )
				      ),
		   undef, $_ ]
	     } @polysel
	    ];

	$self->{position} = -1;

	return $self->execute() unless delete $self->{-noexec};
}

sub execute
  {
	my ($self) = @_;
	return $self->{-current} if $self->{position} == 0;
	$self->{cur_select} = [ @{ $self->{SELECTS} } ];
	$self->prepare_next_statement() && $self->next();
  }

sub sql_string
  {
      my $self = shift;

      if ( $self->{_last_sql} ) {
	  print STDERR "RETURNING FROM _last_sql\n";
	  return $self->{_last_sql};
      }
      elsif ( $self->{ACTIVE} ) {
	  print STDERR "RETURNING FROM ACTIVE\n";
	  return $self->{ACTIVE}[0];
      }
      elsif ( $self->{cur_select} and @{$self->{cur_select}} ) {
	  print STDERR "RETURNING FROM CUR_SELECT\n";
	  return $self->{cur_select}[0][0];
      }
      elsif ( $self->{SELECTS} ) {
	  print STDERR "RETURNING FROM SELECTS\n";
	  return $self->{SELECTS}[0][0];
      }

  }

sub prepare_next_statement
  {
	my ($self) = @_;

	my $select = $self->{ACTIVE} = shift @{ $self->{cur_select} }
	    or do { 
		#print $Tangram::TRACE "Cursor - no active selects?\n"
		    #if $Tangram::TRACE;
		return undef;
	    };
	my ($sql, $sth, $template) = @$select;

	$self->{sth}->finish() if $self->{sth};

	$sth = $select->[1] = $self->{STORAGE}->sql_prepare($sql, $self->{CONNECTION})
	  unless $sth;

	$self->{sth} = $sth;


	$sth->execute() or croak "Execute failed; $DBI::errstr";

	return $sth;
  }

sub build_select
{
	my ($self, $template, $cols, $from, $where, $ofrom, $owhere)
	    = @_;

	if (my $retrieve = $self->{-retrieve})
	{
		@$cols = map { $_->{expr} } @$retrieve;
	}

	my @options;
	# this needs a hack to get right...
	if ( $self->{-limit} ) {
	    @options = $self->{STORAGE}->limit_sql($self->{-limit});
	}

	my $select = $template->instantiate
	    ( $self->{TARGET}, $cols, $from, $where,
	      ( $self->{-group} ? (group => $self->{-group}) : () ),
	      ( $self->{-order} ? (order => $self->{-order}) : () ),
	      ( $self->{-distinct} ? (distinct => $self->{-distinct}) : () ),
	      ( $self->{-desc} ? (desc => $self->{-desc}) : () ),
	      ( $ofrom ? ( ofrom => $ofrom ) : () ),
	      ( $owhere ? ( owhere => $owhere ) : () ),
	      @options,
	    );
	

	return $select;
}

sub _next
{
	my ($self) = @_;

	$self->{-current} = undef;
	++$self->{position};

	my $sth = $self->{sth}
	    or confess "no sth";

	my @row;

	while (1)
	{
		@row = $sth->fetchrow();
		last if @row;
		$sth = $self->prepare_next_statement() or return undef;
	}

	my $storage = $self->{STORAGE};

	if ($self->{TARGET}) {
	    my ($id, $classId, $state) = $self->{ACTIVE}[-1]->extract(\@row);

	    $id = $storage->{import_id}->($id, $classId);

	    my $class = $storage->{id2class}{$classId} or die "unknown class id $classId";

	    # even if object is already loaded we must read it so that @rpw only contains residue
	    my $obj = $storage->read_object($id, $class, $state);

	    # if object is already loaded return previous copy
	    $obj = $storage->{objects}{$id} if exists $storage->{objects}{$id};

	    $self->{-current} = $obj;

	} else {
	    $self->{-current} = undef;
	}

	$self->{-residue} = exists $self->{-retrieve}
	    ? [ map { ref $_ ? $_->{type}->read_data(\@row) : shift @row } @{$self->{-retrieve}} ]
		: \@row;

	$self->{-current} ||=
	    (@{$self->{-residue}} > 1
	     ? $self->{-residue}
	     : $self->{-residue}[0]);

	return $self->{-current};
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
	return $self->{_object};
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
	 storage => $storage,
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

sub DESTROY
  {
	my $self = shift;
	$self->close();
  }

sub close
  {
	my $self = shift;
	$self->{cursor}{connection}->disconnect()
	  unless $self->{cursor}{connection} == $self->{storage}{db};
  }

1;
