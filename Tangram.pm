package Tangram;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

require Exporter;

@ISA = qw(Exporter AutoLoader);
# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
@EXPORT = qw(
	
);
$VERSION = '0.08';


# Preloaded methods go here.

use strict;
use Set::Object;
use Carp;

my $skip;

package Tangram::ClassHash;
use Carp;

sub class
{
   my ($self, $class) = @_;
   $self->{$class} or croak "unknown class '$class'";
}

package Tangram::Class;

sub members
{
   my ($self, $type) = @_;
   return @{$self->{$type}};
}

package Tangram::Schema;
use Carp;

sub new
{
   my $pkg = shift;
   my $self = bless { @_ }, $pkg;

   my $types = $self->{types} ||= {};
   $types->{int} ||= new Tangram::Integer;
   $types->{real} ||= new Tangram::Real;
   $types->{string} ||= new Tangram::String;
   $types->{ref} ||= new Tangram::Ref;
   $types->{set} ||= new Tangram::Set;
   $types->{iset} ||= new Tangram::IntrSet;
   $types->{array} ||= new Tangram::Sequence;
   $types->{iarray} ||= new Tangram::IntrSequence;

   my $classes = $self->{'classes'};
   bless $classes, 'Tangram::ClassHash';

   while (my ($class, $def) = each %$classes)
   {
      my $classdef = $classes->{$class};

      bless $classdef, 'Tangram::Class';

      $classdef->{table} ||= $class;

      my $cols = 0;

      foreach my $typetag (keys %{$classdef->{members}})
      {
         my $memdefs = $classdef->{members}{$typetag};
         $memdefs = $classdef->{members}{$typetag} = { map { $_, $_ } @$memdefs } if (ref $memdefs eq 'ARRAY');
         my $type = $self->{types}{$typetag};
         my @members = $types->{$typetag}->reschema($memdefs, $class, $self) if $memdefs;
         @{$classdef->{member_type}}{@members} = ($type) x @members;
         
         @{$classdef->{MEMDEFS}}{keys %$memdefs} = values %$memdefs;
			
         local $^W = undef;
         $cols += scalar($type->cols($memdefs));
      }

      $classdef->{stateless} = !$cols && (!exists $classdef->{stateless} || $classdef->{stateless});

      foreach my $base (@{$classdef->{bases}})
      {
         push @{$classes->{$base}{specs}}, $class;
      }
   }

   while (my ($class, $classdef) = each %$classes)
   {
      my $root = $class;
      
      while (@{$classes->{$root}{bases}})
      {
         $root = @{$classes->{$root}{bases}}[0];
      }

      $classdef->{root} = $classes->{$root};
      delete $classdef->{stateless} if $root eq $class;
   }

   return $self;
}

sub check_class
{
   my ($self, $class) = @_;
   confess "unknown class '$class'" unless exists $self->{classes}{$class};
}

sub classdef
{
   my ($self, $class) = @_;
   return $self->{classes}{$class} or confess "unknown class '$class'";
}

sub classes
{
   my ($self) = @_;
   return keys %{$self->{'classes'}};
}

sub direct_members
{
   my ($self, $class) = @_;
   return $self->{'classes'}{$class}{member_type};
}

sub all_members
{
   my ($self, $class) = @_;
   my $classes = $self->{'classes'};
	my $members = {};
   
	$self->visit_up($class, sub
	{
		my $direct_members = $classes->{shift()}{member_type};
		@$members{keys %$direct_members} = values %$direct_members;
	} );

	$members;
}

sub all_bases
{
   my ($self, $class) = @_;
   my $classes = $self->{'classes'};
	$self->visit_down($class, sub { @{ $classes->{shift()}{bases} } } );
}

sub find_member
{
   my ($self, $class, $member) = @_;
   my $classes = $self->{'classes'};
   my $result;
   local $@;

   eval
   {
      $self->visit_down($class, sub {
         die if $result = $classes->{shift()}{member_type}{$member}
         })
   };

   $result;
}

sub visit_up
{
   my ($self, $class, $fun) = @_;
   _visit_up($self, $class, $fun, { });
}

sub _visit_up
{
   my ($self, $class, $fun, $done) = @_;
   
   return if $done->{$class};

   my @results = ();

   foreach my $base (@{$self->{'classes'}{$class}{bases}})
   {
      push @results, _visit_up($self, $base, $fun, $done);
   }

   $done->{$class} = 1;

   return @results, &$fun($class);
}

sub visit_down
{
   my ($self, $class, $fun) = @_;
   _visit_down($self, $class, $fun, { });
}

sub _visit_down
{
   my ($self, $class, $fun, $done) = @_;
   
   return if $done->{$class};

   my @results = &$fun($class);

   foreach my $base (@{$self->{'classes'}{$class}{bases}})
   {
      push @results, _visit_down($self, $base, $fun, $done);
   }

   $done->{$class} = 1;

   @results
}

sub for_each_spec
{
   my ($self, $class, $fun) = @_;
   my $done = {};

   foreach my $spec (@{$self->{'classes'}{$class}{specs}})
   {
      _for_each_spec($self, $spec, $fun, $done);
   }
}

sub _for_each_spec
{
   my ($self, $class, $fun, $done) = @_;
   
   return if $done->{$class};

   &$fun($class);
   $done->{$class} = 1;

   foreach my $spec (@{$self->{'classes'}{$class}{specs}})
   {
      _for_each_spec($self, $spec, $fun, $done);
   }

}

sub declare_classes
{
   my ($self, $root) = @_;
   
   foreach my $class ($self->classes)
   {
		my $decl = "package $class;";

      my $bases = @{$self->{classes}{$class}{bases}}
         ? (join ' ', @{$self->{'classes'}{$class}{bases}})
         : $root;

		$decl .= "\@$class\:\:ISA = qw( $bases );" if $bases;

      eval $decl;
   }
}

$skip = <<'SKIP';

package Tangram::Object;

sub new
{
   my $class = shift;
   my $self = bless { @_ }, $class;
   return $self;
}

sub id
{
   shift->{'id'}
}

sub is_stored
{
   shift->{'id'}
}

SKIP

package Tangram::Cursor;
package Tangram::RDBObject;
package Tangram::QueryObject;
package Tangram::Expr;
package Tangram::DataCursor;

sub new
{
   my $pkg = shift;
   return bless [ @_ ] , $pkg;
}

sub execute
{
   my ($self, $storage) = @_;
   my ($obj, $table, $col, $id) = @$self;
	my $obj_id = $storage->id($obj);
   my $sql = "UPDATE $table SET $col = $obj_id WHERE $table.id = $id";
   $storage->sql_do($sql);
}

package Tangram::Transaction;

sub new
{
   my ($pkg, $storage) = @_;
}

package Tangram::AbstractStorage;

use Carp;
use vars qw( %done );

sub new
{
   my $pkg = shift;
   return bless { @_ }, $pkg;
}

sub schema
{
   shift->{schema}
}

sub _open
{   
   my ($self, $schema) = @_;

   $self->{table_top} = 0;
   $self->{free_tables} = [];
   
   $self->{tx} = [];

   $self->{schema} = $schema;

   my $cursor = $self->sql_cursor('SELECT classId, className FROM OpalClass', $self->{db});

   my $classes = $schema->{classes};
   my %table2class = map { $classes->{$_}{table}, $_ } keys %$classes;
   
   my $id2class = {};
   my $class2id = {};

   my ($classId, $className);

   while (($classId, $className) = $cursor->fetchrow())
   {
      if ($className = $table2class{$className})
      {
         $id2class->{$classId} = $className;
         $class2id->{$className} = $classId;
      }
   }

   $cursor->close();

   $self->{id2class} = $id2class;
   $self->{class2id} = $class2id;

   foreach my $class (keys %$classes)
   {
      warn "no class id for '$class'\n"
			if $classes->{$class}{concrete} && !exists $self->{class2id}{$class}
   }

   $self->{set_id} = $schema->{set_id} ||
      sub
      {
         my ($obj, $id) = @_;
         
         if ($id)
         {
            $self->{ids}{0 + $obj} = $id;
         }
         else
         {
            delete $self->{ids}{0 + $obj};
         }
      };

   $self->{get_id} = $schema->{get_id}
      || sub { $self->{ids}{0 + shift()} };

   return $self;
}

sub alloc_table
{
   my ($self) = @_;
   return @{$self->{free_tables}} > 0
      ? pop @{$self->{free_tables}}
      : ++$self->{table_top};
}

sub free_table
{
   my $self = shift;
   push @{$self->{free_tables}}, grep { $_ } @_;
}

sub open_connection
{
   # private - open a new connection to DB for read

   my $self = shift;
   DBI->connect($self->{-cs}, $self->{-user}, $self->{-pw}) or die;
}

sub close_connection
{
   # private - close read connection to DB unless it's the default one

   my ($self, $conn) = @_;
   confess unless $conn;

   if ($conn == $self->{db})
   {
      $conn->commit unless $self->{no_tx} || @{ $self->{tx} };
   }
   else
   {
      $conn->disconnect;
   }
}

sub cursor
{
   my ($self, $class, @args) = @_;
   my $cursor = Tangram::Cursor->new($self, $class, $self->open_connection());
	$cursor->select(@args);
	return $cursor;
}

sub my_cursor
{
   my ($self, $class, @args) = @_;
   my $cursor = Tangram::Cursor->new($self, $class, $self->{db});
	$cursor->select(@args);
	return $cursor;
}

sub select_data
{
   my $self = shift;
   Tangram::Select->new(@_)->execute($self, $self->open_connection());
}


sub my_select_data
{
   my $self = shift;
   Tangram::Select->new(@_)->execute($self, $self->{db});
}

sub make_id
{
   my ($self, $class_id) = @_;

   my $sql = "UPDATE OpalClass SET lastObjectId = lastObjectId + 1 WHERE classId = $class_id";
   $self->sql_do($sql);
   my $cursor = $self->sql_cursor("SELECT lastObjectId from OpalClass WHERE classId = $class_id", $self->{db});
   sprintf '%d%04d', $cursor->fetchrow(), $class_id;
}

sub unknown_classid
{
   my $class = shift;
   confess "class '$class' doesn't exist in this storage"
}

sub class_id
{
   my ($self, $class) = @_;
	$self->{class2id}{$class} or unknown_classid $class;
}

#############################################################################
# Transaction

my $error_no_transaction = 'no transaction is currently active';

sub tx_start
{
   my $self = shift;
   push @{ $self->{tx} }, [];
}

sub tx_commit
{
   # public - commit current transaction

   my $self = shift;

   carp $error_no_transaction unless @{ $self->{tx} };

   $self->{db}->commit unless $self->{no_tx}
		|| @{ $self->{tx} } > 1; # don't commit db if nested tx

   pop @{ $self->{tx} }; # drop rollback subs
}

sub tx_rollback
{
   # public - rollback current transaction

   my $self = shift;

   carp $error_no_transaction unless @{ $self->{tx} };

	if ($self->{no_tx})
	{
		pop @{ $self->{tx} };
	}
	else
	{
		$self->{db}->rollback if @{ $self->{tx} } == 1; # don't rollback db if nested tx

		# execute rollback subs in reverse order

		foreach my $rollback ( @{ pop @{ $self->{tx} } } )
		{
			$rollback->($self);
		}
	}
}

sub tx_do
{
   # public - execute closure inside tx

   my ($self, $sub) = @_;

   $self->tx_start();

   my ($results, @results);
   my $wantarray = wantarray();

   eval
   {
      if ($wantarray)
      {
         @results = $sub->()
      }
      else
      {
         $results = $sub->()
      }
   };

   if ($@)
   {
      $self->tx_rollback();
      die $@;
   }
   else
   {
      $self->tx_commit();
   }

   return wantarray ? @results : $results;
}

sub tx_on_rollback
{
   # private - register a sub that will be called if/when the tx is rolled back

   my ($self, $rollback) = @_;
   carp $error_no_transaction if $^W && !@{ $self->{tx} };
   unshift @{ $self->{tx}[0] }, $rollback; # rollback subs are executed in reverse order
}

#############################################################################
# insertion

sub insert
{
   # public - insert objects into storage; return their assigned ids

   my ($self, @objs) = @_;

   my @ids = $self->tx_do(
      sub
      {
         map
         {
            local %done = ();
            local $self->{defered} = [];
            my $id = $self->_insert($_);
            $self->do_defered;
            $id;
         } @objs;
      } );

   return wantarray ? @ids : shift @ids;
}

sub _insert
{
   my ($self, $obj) = @_;
   my $schema = $self->{schema};

   confess "$obj is already persistent, id = ", $self->id($obj) if $self->id($obj);

   $done{$obj} = 1;

   my $class = ref $obj;
   my $classId = $self->{class2id}{$class} or unknown_classid $class;

   my $id = $self->make_id($classId);

   $self->tx_on_rollback( sub { $self->{set_id}->($obj, undef) } );

   $schema->visit_up($class,
      sub
      {
         my ($class) = @_;
         
         my $classdef = $schema->classdef($class);

         my $table = $classdef->{table};
         my (@cols, @vals);

         if (!@{$classdef->{bases}})
         {
            push @cols, 'classId';
            push @vals, $classId;
         }

         foreach my $typetag (keys %{$classdef->{members}})
         {
            my $members = $classdef->{members}{$typetag};
            my $type = $schema->{types}{$typetag};
            push @cols, $type->cols($members);
            push @vals, $type->save($obj, $members, $self, $table, $id);
         }

         if (@cols)
         {
            my $cols = join ', ', 'id', @cols;
            my $vals = join ', ', $id, @vals;
            my $insert = "INSERT INTO $table ($cols) VALUES ($vals)";
            $self->sql_do($insert);
         }
      } );

   $self->{objects}{$id} = $obj;
	$self->{set_id}->($obj, $id);

   return $id;
}

sub auto_insert
{
   # private - convenience sub for Refs, will be moved there someday

   my ($self, $obj, $table, $col, $id) = @_;

   return 'NULL' unless $obj;

	if (exists $done{$obj})
	{
		# object is being saved already: we have a cycle

		$self->defer( sub
		{
			# now that the object has been saved, we have an id for it
			my $obj_id = $self->id($obj);

			# patch the column in the referant
         $self->sql_do( "UPDATE $table SET $col = $obj_id WHERE id = $id" );
		} );

		return 'NULL';
	}

   return $self->id($obj) # already persistent
		|| $self->_insert($obj); # autosave
}

#############################################################################
# update

sub update
{
   # public - write objects to storage

	my ($self, @objs) = @_;

   $self->tx_do(
      sub
      {
	      foreach my $obj (@objs)
	      {
		      my $id = $self->id($obj) or confess "$obj must be persistent";
   
		      local %done = ();
		      local $self->{defered} = [];

		      my $class = ref $obj;
		      my $schema = $self->{schema};

		      $schema->visit_up($class,
			      sub
			      {
				      my ($class) = @_;

				      my $classdef = $schema->classdef($class);

				      my $table = $classdef->{table};
				      my @cols = ();
				      my @vals = ();

				      foreach my $typetag (keys %{$classdef->{members}})
				      {
					      my $members = $classdef->{members}{$typetag};
					      my $type = $schema->{types}{$typetag};
					      push @cols, $type->cols($members);
					      push @vals, $type->save($obj, $members, $self, $table, $id);
				      }

				      if (@cols)
				      {
					      my $assigns = join ', ', map { "$_ = " . shift @vals } @cols;
					      my $update = "UPDATE $table SET $assigns WHERE id = $id";
					      $self->sql_do($update);
				      }
			      } );

		      $self->do_defered;
	      }
      } );

   @objs = (); # MM
}

#############################################################################
# save

sub save
{
	my $self = shift;

	foreach my $obj (@_)
	{
		if ($self->id($obj))
		{
			$self->update($obj)
		}
		else
		{
			$self->insert($obj)
		}
	}
}

#############################################################################
# erase

sub erase
{
   my $self = shift;
   my $schema = $self->{schema};
   my $classes = $self->{schema}{classes};

   foreach my $obj (@_)
   {
      my $id = $self->id($obj) or confess "object $obj is not persistent";

      local $self->{defered} = [];
      
      $schema->visit_down(ref($obj), sub
      {
         my $class = shift;
         my $classdef = $classes->{$class};

         foreach my $typetag (keys %{$classdef->{members}})
         {
            my $members = $classdef->{members}{$typetag};
            my $type = $schema->{types}{$typetag};
            $type->erase($self, $obj, $members, $id);
         }
      } );
      
      $schema->visit_down(ref($obj), sub
      {
         my $class = shift;
         my $classdef = $classes->{$class};

         my $sql = "DELETE FROM $classdef->{table} WHERE id = $id";
         $self->sql_do($sql);
      } );

      $self->do_defered;
   }

}

sub do_defered
{
   my ($self) = @_;

   foreach my $defered (@{$self->{defered}})
   {
      $defered->($self);
   }

   $self->{defered} = [];
}

sub defer
{
   my ($self, $action) = @_;
   push @{$self->{defered}}, $action;
}

sub load
{
	my $self = shift;

	return map { scalar $self->load( $_ ) } @_ if wantarray;

	my $id = shift;
	die if @_;

   return $self->{objects}{$id} if exists $self->{objects}{$id};

   my $classId = int substr $id, -4;
   my $class = $self->{id2class}{$classId};
   my $alias = Tangram::CursorObject->new($self, $class);
   my $select = $alias->cols;
   my $from = $alias->from;
   my $where = join ' AND ', $alias->where, " t" . $alias->root_table . ".id = $id";
   my $sql = "SELECT $select FROM $from WHERE $where";
   
   my $cursor = $self->sql_cursor($sql, $self->{db});
   my @row = $cursor->fetchrow();
   $cursor->close();
   
   splice @row, 0, 2; # id and classId

   # in load   
   my $obj = $self->read_object($id, $class, \@row, $alias->parts);
   
   $self->{-residue} = \@row;
   
   return $obj;
}

sub read_object
{
   my ($self, $id, $class, $row, @parts) = @_;

   #print "read_object $class: ", (join ' ', map { defined($_) ? $_ : 'undef' } @$row), "\n";

   my $schema = $self->{schema};

   my $obj = $class->new;

   unless (exists	$self->{objects}{$id})
   {
      # do this only if object is not loaded yet
      # otherwise we're just skipping columns in $row
	   $self->{set_id}->($obj, $id);
      $self->{objects}{$id} = $obj;
   }
   
   my $types = $schema->{types};

   foreach my $class (@parts)
   {
      my $classdef = $schema->classdef($class);

      foreach my $typetag (keys %{$classdef->{members}})
      {
         my $members = $classdef->{members}{$typetag};
         $types->{$typetag}->read($row, $obj, $members, $self, $class);
      }
   }

   $schema->visit_up($class,
      sub
      {
         my ($class) = @_;
         my $classdef = $schema->classdef($class);

         if ($classdef->{stateless})
         {
			   my $types = $schema->{types};

            foreach my $typetag (keys %{$classdef->{members}})
            {
               my $members = $classdef->{members}{$typetag};
               $types->{$typetag}->read($row, $obj, $members, $self, $class);
            }
         }
      } );

   return $obj;
}

sub select
{
   carp "valid only in list context" unless wantarray;
   my ($self, $class, @args) = @_;
   my $cursor = Tangram::Cursor->new($self, $class, $self->{db});
	$cursor->select(@args);
}

sub cursor_object
{
   my ($self, $class) = @_;
   $self->{IMPLICIT}{$class} ||= Tangram::RDBObject->new($self, $class)
}

sub query_objects
{
   my ($self, @classes) = @_;
	map { Tangram::QueryObject->new(Tangram::RDBObject->new($self, $_)) } @classes;
}

sub remote
{
	my ($self, @classes) = @_;
   wantarray ? $self->query_objects(@classes) : (&remote)[0]
}

sub object
{
	carp "cannot be called in list context; use objects instead" if wantarray;
   my $self = shift;
   my ($obj) = $self->query_objects(@_);
   $obj;
}

sub count
{
	my ($self, $filter) = @_;

   my $where = $filter->where;
   my $from = $filter->from;
   my $sql = "SELECT COUNT(*) \nFROM $from\n WHERE $where";

   print $Tangram::TRACE "$sql\n" if $Tangram::TRACE;

	return ($self->{db}->selectrow_array($sql))[0];
}

sub id
{
   my ($self, $obj) = @_;
   return $self->{get_id}->($obj);
}

sub disconnect
{
   my ($self) = @_;

	unless ($self->{no_tx})
	{   
		if (@{ $self->{tx} })
		{
			$self->{db}->rollback;
		}
		else
		{
			$self->{db}->commit;
		}
	}
   
   $self->{db}->disconnect;

   %$self = ();
}

sub DESTROY
{
   my ($self) = @_;
   carp "Tangram::Storage '$self' destroyed without explicit disconnect" if keys %$self;
}

sub prefetch
{
   my ($self, $class, $member, $filter) = @_;
   my $classdef = $self->{schema}{classes}{$class} or confess;
   my $type = $classdef->{member_type}{$member} or confess "$class has no member '$member'";
   my $memdef = $classdef->{MEMDEFS}{$member} or confess;
   $type->prefetch($self, $memdef, $class, $member, $filter);
}

package Tangram::CursorObject;
use Carp;

sub new
{
   my ($pkg, $storage, $class) = @_;

   my $schema = $storage->{schema};
   my $classes = $schema->{classes};
	$schema->check_class($class);

   my @tables;
	my $table_hash = { };
   my $self = bless { storage => $storage, tables => \@tables, class => $class,
		table_hash => $table_hash }, $pkg;

   $storage->{schema}->visit_up($class,
      sub
      {
         my $class = shift;
			
			unless ($classes->{$class}{stateless})
			{
				my $id = $storage->alloc_table;
				push @tables, [ $class, $id ];
				$table_hash->{$class} = $id;
			}
      } );

   return $self;
}

sub copy
{
   my ($pkg, $other) = @_;

	my $self = { %$other };
	$self->{tables} = [ @{ $self->{tables} } ];

	bless $self, $pkg;
}

sub storage
{
   shift->{storage}
}

sub table
{
	my ($self, $class) = @_;
	$self->{table_hash}{$class} or confess "no table for $class in stored '$self->{class}'";
}

sub tables
{
   shift->{tables}
}

sub class
{
	shift->{class}
   #my ($self) = @_;
   #my $tables = $self->{tables};
   #return $tables->[$#$tables][0];
}

sub parts
{
   return map { $_->[0] } @{ shift->{tables} };
}

sub root_table
{
   my ($self) = @_;
   return $self->{tables}[0][1];
}

sub class_id_col
{
   my ($self) = @_;
   return "t$self->{tables}[0][1].classId";
}

sub leaf_table
{
   my ($self) = @_;
   return $self->{tables}[-1][1];
}

sub from
{
   return join ', ', &from unless wantarray;

   my ($self) = @_;
   my $schema = $self->storage->{schema};
   my $classes = $schema->{classes};
   my $tables = $self->{tables};
   map { "$classes->{$_->[0]}{table} t$_->[1]" } @$tables;
}

sub where
{
   return join ' AND ', &where unless wantarray;

   my ($self) = @_;
   
	my $tables = $self->{tables};
   my $root = $tables->[0][1];

   map { "t@{$_}[1].id = t$root.id" } @$tables[1..$#$tables];
}

sub cols
{
   return join ', ', &cols unless wantarray;

   my ($self) = @_;

   my $tables = $self->tables;
   my $root = $tables->[0][1];
   my $schema = $self->storage->{schema};

   my $cols = "t$root.id, t$root.classId";

   foreach my $table (@$tables)
   {
      my ($class, $id) = @$table;
      my $classdef = $schema->classdef($class);

      foreach my $typetag (keys %{$classdef->{members}})
      {
         my $members = $classdef->{members}{$typetag};

         foreach my $col ($schema->{types}{$typetag}->cols($members))
         {
            $cols .= ", t$table->[1].$col";
         }
      }
   }

   return $cols;
}

sub mark
{
   return @{ shift->{tables} };
}

sub push_spec
{
   my ($self, $spec) = @_;
   my $tables = $self->tables;
   push @$tables, [ $spec, $self->storage->alloc_table ];
}

sub pop_spec
{
   my ($self, $mark) = @_;
   my $tables = $self->{tables};
   $self->storage->free_table( map { $_->[1] } splice @$tables, $mark, @$tables - $mark );
}

sub expr_hash
{
   my ($self) = @_;
   my $storage = $self->{storage};
   my $schema = $storage->{schema};
   my $classes = $schema->{classes};
	my @tables = @{$self->{tables}};
   my $root_tid = $tables[0][1];
   
   my %hash =
   (
      object => $self, 
      id => Tangram::Expr->new("t$root_tid.id", 'Tangram::Number', $self)
   );

   $schema->visit_up($self->{class},
      sub
      {
			my $classdef = $classes->{shift()};

			my $tid = (shift @tables)->[1] unless $classdef->{stateless};

			foreach my $typetag (keys %{$classdef->{members}})
			{
				my $type = $schema->{types}{$typetag};
				my $memdefs = $classdef->{members}{$typetag};
				@hash{$type->members($memdefs)} =
					$type->query_expr($self, $memdefs, $tid);
			}
      } );

   return \%hash;
}

package Tangram::RDBObject;

@Tangram::RDBObject::ISA = qw( Tangram::CursorObject );

sub where
{
   return join ' AND ', &where unless wantarray;

   my ($self) = @_;
   
	my $storage = $self->{storage};
   my $schema = $storage->{schema};
   my $classes = $schema->{classes};
	my $tables = $self->{tables};
   my $root = $tables->[0][1];
	my $class = $self->{class};

	my @where_class_id;

	if ($classes->{$class}{stateless})
	{
		my @class_ids;

		push @class_ids, $storage->class_id($class) unless $classes->{$class}{abstract};

		$schema->for_each_spec($class,
			sub { my $spec = shift; push @class_ids, $storage->class_id($spec) unless $classes->{$spec}{abstract} } );

		@where_class_id = "t$root.classId IN (" . join(', ', @class_ids) . ')';
	}

   return (@where_class_id, map { "t@{$_}[1].id = t$root.id" } @$tables[1..$#$tables]);
}

package Tangram::Filter;
use Carp;

sub new
{
   my $pkg = shift;
   my $self = bless { @_ }, $pkg;
   $self->{objects} ||= Set::Object->new;
   $self;
}

sub and
{
   my ($self, $other, $reversed) = @_;
   return op($self, 'AND', 10, $other);
}

sub or
{
   my ($self, $other, $reversed) = @_;
   return op($self, 'OR', 9, $other);
}

sub as_string
{
   my $self = shift;
   return ref($self) . "($self->{expr})";
}

use overload "&" => \&and, "|" => \&or, '""' => \&as_string, fallback => 1;

sub op
{
   my ($left, $op, $tight, $right) = @_;

   confess "undefined operand(s) for $op" unless $left && $right;

   my $lexpr = $tight > $left->{tight} ? "($left->{expr})" : $left->{expr};
   my $rexpr = $tight > $right->{tight} ? "($right->{expr})" : $right->{expr};

   return new Tangram::Filter(
      expr => "$lexpr $op $rexpr",
      tight => $tight,
      objects => Set::Object->new(
         $left->{objects}->members, $right->{objects}->members ) );
}

sub from
{
   return join ', ', &from unless wantarray;
   map { $_->from } shift->objects;
}

sub where
{
   return join ' AND ', &where unless wantarray;

   my ($self) = @_;
   my @expr = "($self->{expr})" if exists $self->{expr};
   (@expr, map { $_->where } $self->objects);
}

sub where_objects
{
   return join ' AND ', &where_objects unless wantarray;
   my ($self, $object) = @_;
   map { $_ == $object ? () : $_->where } $self->objects;
}

sub objects
{
   shift->{objects}->members;
}

package Tangram::Expr;

sub new
{
   my ($pkg, $expr, $type, @objects) = @_;
   return bless { expr => $expr, type => $type,
      objects => Set::Object->new(@objects),
		storage => $objects[0]->{storage} }, $pkg;
}

sub objects
{
   return shift->{objects}->members;
}

sub eq
{
   my ($self, $arg, $reversed) = @_;
   return $self->binop('=', $arg, $reversed);
}

sub ne
{
   my ($self, $arg, $reversed) = @_;
   return $self->binop('<>', $arg, $reversed);
}

sub lt
{
   my ($self, $arg, $reversed) = @_;
   return $self->binop('<', $arg, $reversed);
}

sub le
{
   my ($self, $arg, $reversed) = @_;
   return $self->binop('<=', $arg, $reversed);
}

sub gt
{
   my ($self, $arg, $reversed) = @_;
   return $self->binop('>', $arg, $reversed);
}

sub ge
{
   my ($self, $arg, $reversed) = @_;
   return $self->binop('>=', $arg, $reversed);
}

sub binop
{
   my ($self, $op, $arg, $reversed) = @_;

   my @objects = $self->objects;
   my $objects = Set::Object->new(@objects);

   if ($arg)
   {
	   if (my $type = ref($arg))
      {
         if ($arg->isa('Tangram::Expr'))
         {
            $objects->insert($arg->objects);
            $arg = $arg->{expr};
         }
   
         elsif ($arg->isa('Tangram::QueryObject'))
         {
            $objects->insert($arg->object);
            $arg = $arg->{id}->{expr};
         }
   
         elsif (exists $self->{storage}{schema}{classes}{$type})
         {
            $arg = $self->{storage}->id($arg) or Carp::confess "$arg is not persistent";
         }

		   else
		   {
			    $arg = $self->{type}->literal($arg);
		   }
      }
	   else
      {
          $arg = $self->{type}->literal($arg);
      }
   }
   else
   {
      $op = $op eq '=' ? 'IS' : $op eq '<>' ? 'IS NOT' : Carp::confess;
      $arg = 'NULL';
   }

   return new Tangram::Filter(expr => "$self->{expr} $op $arg", tight => 100,
      objects => $objects );
}

sub like
{
	my ($self, $val) = @_;
   return new Tangram::Filter(expr => "$self->{expr} like '$val'", tight => 100,
      objects => Set::Object->new($self->objects) );
}

sub count
{
	my ($self, $val) = @_;
   Tangram::Expr->new( "COUNT($self->{expr})", 'Tangram::Integer', $self->objects );
}

sub as_string
{
   my $self = shift;
   return ref($self) . "($self->{expr})";
}

use overload
   "==" => \&eq,
   "eq" => \&eq,
   "!=" => \&ne,
   "ne" => \&ne,
   "<" => \&lt,
   "lt" => \&lt,
   "<=" => \&le,
   "le" => \&le,
   ">" => \&gt,
   "gt" => \&gt,
   ">=" => \&ge,
   "ge" => \&ge,
   '""' => \&as_string,
	fallback => 1;

package Tangram::QueryObject;

use Carp;

sub new
{
   my ($pkg, $obj) = @_;
   bless $obj->expr_hash(), $pkg;
}

sub object
{
   shift->{object}
}

sub class
{
	shift->{object}{class}
}

sub eq
{
	my ($self, $other, $swapped) = @_;
	
	($self, $other) = ($other, $self) if $swapped;

	if ($other->isa('Tangram::QueryObject'))
	{
		$self->{id} == $other->{id}
	}
	else
	{
		my $other_id = $self->{object}{storage}->id($other)
			or confess "'$other' is not a persistent object";
		$self->{id} == $self->{object}{storage}->id($other)
	}
}

use overload "==" => \&eq, "!=" => \&ne, fallback => 1;

#############################################################################
# Cursor

package Tangram::Cursor;

use vars qw($stored %done);
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

   my $select = "SELECT $cols\n\tFROM $from\n\t" . ($where && "WHERE $where");

   if (my $order = $self->{-order})
   {
      $select .= "\n\tORDER BY " . join ', ', map { $_->{expr} } @$order;
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
      $self->{-cursor}->close();

      my $select = shift @{$self->{-selects}};
      return undef unless $select;

      my ($sql, @parts) = @$select;
      $self->{parts} = \@parts;
      $self->{-cursor} = $self->{-storage}->sql_cursor($sql, $self->{-conn});
   }

   my $id = shift @row;
   my $storage = $self->{-storage};

   my $classId = shift @row;
   my $class = $storage->{id2class}{$classId};

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

package Tangram::Select;

use Carp;

@Tangram::Select::ISA = 'Tangram::Expr';

sub new
{
   my ($type, %args) = @_;

   my $cols = join ', ', map
   {
      confess "column specification must be a Tangram::Expr" unless $_->isa('Tangram::Expr');
      $_->{expr};
   } @{$args{cols}};

   my $filter = exists $args{where} ? $args{where} : Tangram::Filter->new;
   
   my $from = $filter->from;
   my $where;

	my @objects;

	if (exists $args{from})
	{
		$from = join ', ', map { $_->object->from } @{ $args{from} };
	   $where = join ' AND ', $filter->{expr}, map { $_->object->where } @{ $args{from} };
	}
	else
	{
		@objects = exists $args{where} ? $filter->objects : (map { $_->objects } @{$args{cols}});
		$from = join ', ', map { $_->from } @objects;
		my @filter = "($filter->{expr})" if $filter->{expr};
	   $where = join ' AND ', @filter, map { $_->where } @objects;
	}

   my $sql = "SELECT $cols\nFROM $from";
   $sql .= "\nWHERE $where" if $where;

   if (exists $args{order})
   {
      $sql .= "\nORDER BY " . join ', ', map { $_->{expr} } @{$args{order}};
   }

   my $self = $type->SUPER::new("($sql)", 'Tangram::Integer', @objects);
	
	$self->{cols} = $args{cols};

	return $self;
}

sub from
{
	my ($self) = @_;
	my $from = $self->{from};
	return $from ? $from->members : $self->SUPER::from;
}

sub execute
{
   my ($self, $storage, $conn) = @_;
   return Tangram::DataCursor->open($storage, $self, $conn);
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

package Tangram::Storage::Statement;

sub new
{
   my $class = shift;
   bless { @_ }, $class;
}

sub fetchrow
{
   my $self = shift;
   return $self->{statement}->fetchrow;
   
   my @row = $self->{statement}->fetchrow;
   print '*** ', join(' ', @row), "\n";
   @row;
}

sub close
{
   my $self = shift;

   if ($self->{storage})
   {
      $self->{statement}->finish;
      $self->{storage}->close_connection($self->{connection});
      %$self = ();
   }
}

sub DESTROY
{
   shift->close();
}

package Tangram::Storage;

use DBI;
use Carp;

@Tangram::Storage::ISA = qw(Tangram::AbstractStorage);

sub connect
{
   my ($pkg, $schema, $cs, $user, $pw) = @_;
   my $self = $pkg->new;
	my $db = DBI->connect($cs, $user, $pw);

	eval { $db->{AutoCommit} = 0 };

	$self->{no_tx} = $db->{AutoCommit};

	$self->{db} = $db;

   @$self{ -cs, -user, -pw } = ($cs, $user, $pw);
   $self->_open($schema);
   return $self;
}

sub sql_do
{
   my ($self, $sql) = @_;
   print $Tangram::TRACE "$sql\n" if $Tangram::TRACE;
   $self->{db}->do($sql) or croak $DBI::errstr;
}

sub sql_cursor
{
   my ($self, $sql, $connection) = @_;

   confess unless $connection;

   print $Tangram::TRACE "$sql\n" if $Tangram::TRACE;

   my $sth = $connection->prepare($sql) or die;
   $sth->execute() or die;

   new Tangram::Storage::Statement( statement => $sth, storage => $self,
      connection => $connection );
}

sub DESTROY
{
   my $self = shift;
   $self->{db}->disconnect if $self->{db};
}

package Tangram::Type;

my %instances;

sub instance
{
	my $pkg = shift;
	Carp::confess "no arguments '@_' allowed" if @_;
	return $instances{$pkg} ||= bless { }, $pkg;
}

*new = \&instance;

sub reschema
{
}

sub members
{
   my ($self, $members) = @_;
   keys %$members;
}

sub query_expr
{
}

sub erase
{
}

sub read_data
{
	my ($self, $row) = @_;
	shift @$row;
}

sub read
{
   my ($self, $row, $obj, $members) = @_;
	
	foreach my $key (keys %$members)
	{
		$obj->{$key} = $self->read_data($row)
	}
}

sub prefetch
{
}

package Tangram::ScalarType;

@Tangram::ScalarType::ISA = 'Tangram::Type';

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

@Tangram::Number::ISA = 'Tangram::ScalarType';

sub save
{
   my ($self, $obj, $members) = @_;
   map { defined($_) ? 0 + $_ : 'NULL' } @$obj{keys %$members};
}

package Tangram::Integer;
@Tangram::Integer::ISA = 'Tangram::Number';

package Tangram::Real;
@Tangram::Real::ISA = 'Tangram::Number';

package Tangram::String;

@Tangram::String::ISA = 'Tangram::ScalarType';

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

package Tangram::Relation;

package Tangram::Ref;

@Tangram::Ref::ISA = qw(Tangram::ScalarType Tangram::Relation);

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

package Tangram::Coll;

@Tangram::Coll::ISA = qw(Tangram::Type Tangram::Relation);

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

package Tangram::AbstractSet;

@Tangram::AbstractSet::ISA = 'Tangram::Coll';

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

@Tangram::Set::ISA = 'Tangram::AbstractSet';

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
   my ($self, $storage, $def, $class, $member, $filter) = @_;
   
   my ($coll, $ritem) = $storage->remote($class, $def->{class});

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
      #print $Tangram::TRACE "prefetched $coll_id\n";
      $cursor->next;
   }
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

package Tangram::IntrSet;

@Tangram::IntrSet::ISA = 'Tangram::AbstractSet';

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
   my ($self, $def, $storage, $obj, $member) = @_;

   print $Tangram::TRACE "loading $member\n" if $Tangram::TRACE;

   my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

   my $coll_id = $storage->id($obj);
   my $tid = $cursor->{-stored}->leaf_table;
   $cursor->{-coll_where} = "t$tid.$def->{coll} = $coll_id";
   
   my $set = Set::Object->new($cursor->select);

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

package Tangram::AbstractSequence;

@Tangram::AbstractSequence::ISA = 'Tangram::Coll';

sub content
{
   shift;
   @{shift()};
}

use Carp;

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
		my $cursor = $self->cursor($def, $storage, $obj, $member);

		for (my $item = $cursor->select; $item; $item = $cursor->next)
		{
			my $slot = shift @{ $cursor->{-residue} };
			$coll[$slot] = $item;
		}
	}

	$storage->{scratch}{ref($self)}{$storage->id($obj)}{$member} = [ map { $_ && $storage->id($_) } @coll ];

   return \@coll;
}

sub save
{
   my ($self, $obj, $members, $storage, $table, $id) = @_;

   foreach my $coll (keys %$members)
   {
      next if tied $obj->{$coll};

      foreach my $item (@{$obj->{$coll}})
      {
         $storage->insert($item) unless $storage->id($item);
      }
   }

   $storage->defer(sub { $self->defered_save(shift, $obj, $members, $id) } );

   return ();
}

package Tangram::Sequence;

@Tangram::Sequence::ISA = 'Tangram::AbstractSequence';

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

sub defered_save
{
   use integer;

   my ($self, $storage, $obj, $members, $coll_id) = @_;

	my $old_states = $storage->{scratch}{ref($self)}{$coll_id};

   foreach my $member (keys %$members)
   {
      next if tied $obj->{$member}; # collection has not been loaded, thus not modified

      my $def = $members->{$member};
      my ($table, $coll_col, $item_col, $slot_col) = @{ $def }{ qw( table coll item slot ) };
      
		my $coll = $obj->{$member};
		my $coll_size = @$coll;

      my $old_state = $old_states->{$member};
      my $old_size = $old_state ? @$old_state : 0;

      my $common_size = $coll_size < $old_size ? $coll_size : $old_size;

		my @new_state = ();
		my $slot = 0;

		while ($slot < $common_size)
		{
			my $item_id = $storage->id($coll->[$slot]) || croak "member $coll->[$slot] has no id";
         my $old_id = $old_state->[$slot];
         
         unless ($item_id == $old_id)
         {
            # array entry has changed value
            my $sql = "UPDATE $table SET $item_col = $item_id WHERE $coll_col = $coll_id AND $slot_col = $slot AND $item_col = $old_id";
            $storage->sql_do($sql);
         }
			
         push @new_state, $item_id;
			++$slot;
		}

      if ($old_size > $coll_size)
		{
         # array shrinks
			my $sql = "DELETE FROM $table WHERE $coll_col = $coll_id AND $slot_col >= $slot";
			$storage->sql_do($sql);
		}

		while ($slot < $coll_size)
		{
         # array grows
			my $item_id = $storage->id($coll->[$slot]) || croak "member $coll->[$slot] has no id";
         my $sql = "INSERT INTO $table ($coll_col, $item_col, $slot_col) VALUES ($coll_id, $item_id, $slot)";
         $storage->sql_do($sql);
			push @new_state, $item_id;
			++$slot;
		}

      $old_states->{$member} = \@new_state;
      $storage->tx_on_rollback( sub { $old_states->{$member} = $old_state } );
   }
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
   my ($self, $storage, $def, $class, $member, $filter) = @_;

   my ($coll, $ritem) = $storage->remote($class, $def->{class});

	# first retrieve the collection-side ids of all objects satisfying $filter
	# empty the corresponding prefetch array

   my $ids = $storage->my_select_data( cols => [ $coll->{id} ], filter => $filter );
   my $prefetch = $storage->{PREFETCH}{$class}{$member} ||= {}; # weakref

   while (my $id = $ids->fetchrow)
   {
      $prefetch->{$id} = []
   }

   undef $ids;

	# now fetch the items

   my $cursor = Tangram::Cursor->new($storage, $ritem, $storage->{db});
	my $includes = $coll->{$member}->includes($ritem);

	# also retrieve collection-side id and index of elmt in sequence
   $cursor->retrieve($coll->{id},
		Tangram::Expr->new("t$includes->{link_tid}.$def->{slot}", 'Tangram::Number') );

   $cursor->select($filter ? $filter & $includes : $includes);
   
   while (my $item = $cursor->current)
   {
      my ($coll_id, $slot) = $cursor->residue;
      $prefetch->{$coll_id}[$slot] = $item;
      $cursor->next;
   }
}

package Tangram::IntrSequence;

@Tangram::IntrSequence::ISA = 'Tangram::AbstractSequence';

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
      $def->{slot} ||= $class . "_$member" . "_slot";
   
      $schema->{classes}{$def->{class}}{stateless} = 0;
   }

   return keys %$members;
}

sub defered_save
{
   use integer;

   my ($self, $storage, $obj, $members, $coll_id) = @_;

	my $classes = $storage->{schema}{classes};
	my $old_states = $storage->{scratch}{ref($self)}{$coll_id};

   foreach my $member (keys %$members)
   {
      next if tied $obj->{$member};

      my $def = $members->{$member};
      my $item_classdef = $classes->{$def->{class}};
      my $table = $item_classdef->{table} or die;
      my $item_col = $def->{coll};
      my $slot_col = $def->{slot};

		my $coll_id = $storage->id($obj);
		my $coll = $obj->{$member};
		my $coll_size = @$coll;
		
		my @new_state = ();
		
		my $old_state = $old_states->{$member};
      my $old_size = $old_state ? @$old_state : 0;

		my %removed;
		@removed{ @$old_state } = () if $old_state;

		my $slot = 0;

		while ($slot < $coll_size)
		{
			my $item_id = $storage->id( $coll->[$slot] ) || die;

			$storage->sql_do("UPDATE $table SET $item_col = $coll_id, $slot_col = $slot WHERE id = $item_id")
				unless $slot < $old_size && $item_id eq $old_state->[$slot];

			push @new_state, $item_id;
			delete $removed{$item_id};
			++$slot;
		}

		if (keys %removed)
		{
			my $removed = join(' ', keys %removed);
			$storage->sql_do("UPDATE $table SET $item_col = NULL, $slot_col = NULL WHERE id IN ($removed)");
		}

      $old_states->{$member} = \@new_state;

      $storage->tx_on_rollback( sub { $old_states->{$member} = $old_state } );
   }
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
      my $slot_col = $def->{slot};
      
      my $sql = "UPDATE $table SET $item_col = NULL, $slot_col = NULL WHERE $item_col = $coll_id";
      $storage->sql_do($sql);
   }
}

sub cursor
{
   my ($self, $def, $storage, $obj, $member) = @_;

   my $cursor = Tangram::CollCursor->new($storage, $def->{class}, $storage->{db});

   my $item_col = $def->{coll};
   my $slot_col = $def->{slot};

   my $coll_id = $storage->id($obj);
   my $tid = $cursor->{-stored}->leaf_table;
   $cursor->{-coll_cols} = ", t$tid.$slot_col";
   $cursor->{-coll_where} = "t$tid.$item_col = $coll_id";

   return $cursor;
}

sub query_expr
{
   my ($self, $obj, $members, $tid) = @_;
   map { Tangram::IntrCollExpr->new($obj, $_); } values %$members;
}

package Tangram;

1;

__END__
