# (c) Sound Object Logic 2000-2001

use strict;
use Tangram::Schema;

package Tangram::Relational::TableSet;

use constant TABLES => 0;
use constant SORTED_TABLES => 1;
use constant KEY => 2;

sub new
  {
	my $class = shift;
	my %seen;
	my @tables = grep { !$seen{$_}++ } @_;
	my @sorted_tables = sort @tables;

	return bless [ \@tables, \@sorted_tables, "@sorted_tables" ], $class;
  }

sub key
  {
	return shift->[KEY];
  }

sub tables
  {
	@{ shift->[TABLES] }
  }

sub is_improper_superset
  {
	my ($self, $other) = @_;
	my %other_tables = map { $_ => 1 } $other->tables();

	for my $table ($self->tables()) {
	  delete $other_tables{$table};
	  return 1 if keys(%other_tables) == 0;
	}

	return 0;
  }

#=====================================================================
#  Tangram::Relational::Engine
#
# Broom, broom!
#
# Each Class has an Engine, which generates closures to do certain
# operations.  This engine is generated from the Tangram Schema (?)
#
# The operations are:
#
#   - get_heterogeniety() - the total number of subclasses of a given
#                           class, I think
#
# A whole load forwarded to the Tangram::Relational::Engine::Class
# object:
#
#   - get_instance_select
#   - get_insert_statements
#   - get_insert_fields
#   - get_update_statements
#   - get_update_fields
#   - get_deletes
#   - get_polymorphic_select
#   - get_table_set
#
#   - get_save_cache (?)
#   - qualify
#
# Generated;
#
#   - get_exporter
#   - get_importer
#=====================================================================
package Tangram::Relational::Engine;

sub new {
    my ($class, $schema, %opts) = @_;

    my $heterogeneity = { };
    my $engine = bless { SCHEMA => $schema,
			 HETEROGENEITY => $heterogeneity }, $class;

    if ($opts{layout1}) {
	$engine->{layout1} = 1;
	$engine->{TYPE_COL} = $schema->{sql}{class_col} || 'classId';
    } else {
	$engine->{TYPE_COL} = $schema->{sql}{class_col} || 'type';
    }

    for my $class ($schema->all_classes) {
	$engine->{ROOT_TABLES}{$class->{table}} = 1
	    if $class->is_root();
    }

    for my $class ($schema->all_classes) {

	$engine->{ROOT_TABLES}{$class->{table}} = 1
	    if $class->is_root();

	next if $class->{abstract};

	my $table_set = $engine->get_table_set($class);
	my $key = $table_set->key();

	for my $other ($schema->all_classes) {
	    ++$heterogeneity->{$key}
		if my $ss = ($engine->get_table_set($other)
			     ->is_improper_superset($table_set));
	    my $other_key = $engine->get_table_set($other)->key;
	}
    }

    # use Data::Dumper; print Dumper $heterogeneity;

    return $engine;
}

sub get_heterogeneity {
    my ($self, $table_set) = @_;
    my $key = $table_set->key();

    return $self->{HETEROGENEITY}{$key} ||= do {

	# XXX - this code path never reached in the test suite - is it
	# required?
	my $heterogeneity = 0;

	for my $class (values %{ $self->{CLASS} }) {
	    ++$heterogeneity
		if (!$class->{abstract} &&
		    ($class->get_table_set($self)
		     ->is_improper_superset($table_set)));
	}

	$heterogeneity;
    }
}

sub get_parts
  {
	my ($self, $class) = @_;

	@{ $self->{CLASSES}{$class->{name}}{PARTS} ||= do {
	  my %seen;
	  [ grep { !$seen{ $_->{name} }++ }
		(map { $self->get_parts($_) } $class->direct_bases()),
		$class
	  ]
	} }
  }

sub deploy
{
	my ($self, $out) = @_;
    $self->relational_schema()->deploy($out);
}

sub retreat
{
	my ($self, $out) = @_;
    $self->relational_schema()->retreat($out);
}

sub get_deploy_info
  {
	my ($self) = @_;
	return { LAYOUT => 2, ENGINE => ref($self), ENGINE_LAYOUT => 1 };
  }

sub relational_schema
  {
    my ($self) = @_;

    my $schema = $self->{SCHEMA};
    my $classes = $schema->{classes};
    my $tables = {};

    foreach my $class (keys %{$schema->{classes}}) {

	my $classdef = $classes->{$class};

	my $tabledef = $tables->{ $classdef->{table} } ||= {};
	my $cols = $tabledef->{COLS} ||= {};
	$tabledef->{TYPE} = $classdef->{table_type};

	$cols->{ $schema->{sql}{id_col} } = $schema->{sql}{id};

	$cols->{ $schema->{sql}{class_col} || 'type' }
	    = $schema->{sql}{cid}
		if $self->{ROOT_TABLES}{$classdef->{table}};

	foreach my $typetag (keys %{$classdef->{members}})
	{
	    my $members = $classdef->{members}{$typetag};
	    my $type = $schema->{types}{$typetag};

	    $type->coldefs($tabledef->{COLS}, $members, $schema,
			   $class, $tables);
	}
    }

    delete @$tables{
		    grep { 1 == keys %{ $tables->{$_}{COLS} } }
		    keys %$tables
		   };

    return bless [ $tables, $self ], 'Tangram::RelationalSchema';
}

#---------------------------------------------------------------------
#  Tangram::Scalar->_coldefs($cols, $members, $sql, $schema)
#
# Adds entries to the current table mapping for the columns for a
# single class of a given type.  Inheritance is not in the picture
# yet.
#
# $cols is the columns definition for the current table mapping
# $members is the `members' property of the current class (ie, the
#          members for a particular data type, eg string => $members)
# $sql is the SQL type to default columns to
# $schema is the Tangram::Schema object
#---------------------------------------------------------------------
sub Tangram::Scalar::_coldefs
{
    my ($self, $cols, $members, $sql, $schema) = @_;

    for my $def (values %$members)
	{
	    $cols->{ $def->{col} } =
		(
		 $def->{sql} ||
		 "$sql " . ($schema->{sql}{default_null} || "")
		);
	}
}

#---------------------------------------------------------------------
#  Tangram::Integer->coldefs($cols, $members, $schema)
#  Tangram::Real->coldefs($cols, $members, $schema)
#  Tangram::String->coldefs($cols, $members, $schema)
#
# Setup column defines for the root column types
#---------------------------------------------------------------------
sub Tangram::Integer::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, 'INT', $schema);
}

sub Tangram::Real::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, 'REAL', $schema);
}

sub Tangram::String::coldefs
{
    my ($self, $cols, $members, $schema) = @_;
    $self->_coldefs($cols, $members, 'VARCHAR(255)', $schema);
}

#---------------------------------------------------------------------
#  Tangram::Set->coldefs($cols, $members, $schema, $class, $tables)
#
#  Setup column mappings for many to many unordered mappings (link
#  table)
#---------------------------------------------------------------------
sub Tangram::Set::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	my $COLS = $tables->{ $member->{table} }{COLS} ||= { };

	$COLS->{$member->{coll}} = $schema->{sql}{id};
	$COLS->{$member->{item}} = $schema->{sql}{id};
    }
}

#---------------------------------------------------------------------
#  Tangram::IntrSet->coldefs($cols, $members, $schema, $class,
#                            $tables)
#
#  Setup column mappings for one to many unordered mappings (foreign
#  key)
#---------------------------------------------------------------------
sub Tangram::IntrSet::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	my $table =
	    $tables->{ $schema->{classes}{$member->{class}}{table} }
		||= {};
	$table->{COLS}{$member->{coll}}
	    = "$schema->{sql}{id} $schema->{sql}{default_null}";
    }
}

#---------------------------------------------------------------------
#  Tangram::Array->coldefs($cols, $members, $schema, $class, $tables)
#
#  Setup column mappings for many to many unordered mappings (link
#  table with integer category)
#---------------------------------------------------------------------
sub Tangram::Array::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	my $COLS = $tables->{ $member->{table} }{COLS} ||= { };

	$COLS->{$member->{coll}} = $schema->{sql}{id};
	$COLS->{$member->{item}} = $schema->{sql}{id};
	$COLS->{$member->{slot}} = "INT $schema->{sql}{default_null}";
    }
}

#---------------------------------------------------------------------
#  Tangram::IntrArray->coldefs($cols, $members, $schema, $class,
#                              $tables)
#
#  Setup column mappings for one to many ordered mappings (foreign
#  key with associated integer category/column)
#---------------------------------------------------------------------
sub Tangram::IntrArray::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members) {
	my $table =
	    $tables->{ $schema->{classes}{$member->{class}}{table} }
		||= {};
	$table->{COLS}{$member->{coll}}
	    = "$schema->{sql}{id} $schema->{sql}{default_null}";
	$table->{COLS}{$member->{slot}}
	    = "INT $schema->{sql}{default_null}";
    }
}

#---------------------------------------------------------------------
#  Tangram::Hash->coldefs($cols, $members, $schema, $class, $tables)
#
#  Setup column mappings for many to many indexed mappings (link
#  table with string category)
#---------------------------------------------------------------------
sub Tangram::Hash::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	$tables->{ $member->{table} }{COLS} =
	    {
	     $member->{coll} => $schema->{sql}{id},
	     $member->{item} => $schema->{sql}{id},
	     # XXX - hardcoded slot type
	     $member->{slot} => "VARCHAR(255) $schema->{sql}{default_null}"
	    };
    }
}

#---------------------------------------------------------------------
#  Tangram::IntrHash->coldefs($cols, $members, $schema, $class,
#                             $tables)
#
#  Setup column mappings for one to many indexed mappings (foreign
#  key with string category)
#---------------------------------------------------------------------
sub Tangram::IntrHash::coldefs
{
    my ($self, $cols, $members, $schema, $class, $tables) = @_;

    foreach my $member (values %$members)
    {
	my $table =
	    $tables->{ $schema->{classes}{$member->{class}}{table} }
		||= {};
	$table->{COLS}{$member->{coll}} =
	    "$schema->{sql}{id} $schema->{sql}{default_null}";
	$table->{COLS}{$member->{slot}} =
	    "VARCHAR(255) $schema->{sql}{default_null}";
    }
}

#---------------------------------------------------------------------
#  Tangram::BackRef->coldefs(...)
#
#  BackRefs do not set up any columns by default.
#---------------------------------------------------------------------
sub Tangram::BackRef::coldefs
{
    return ();
}

package Tangram::RelationalSchema;

sub _deploy_do
{
    my $output = shift;

    return ref($output) && eval { $output->isa('DBI::db') }
		? sub { print $Tangram::TRACE @_, "\n" if $Tangram::TRACE;
			$output->do( join '', @_ ); }
		: sub { print $output @_, ";\n\n" };
}

sub deploy
{
    my ($self, $output) = @_;
    my ($tables, $engine) = @$self;
	my $schema = $engine->{SCHEMA};

    $output ||= \*STDOUT;

    my $do = _deploy_do($output);

    foreach my $table (sort keys %$tables)
    {
	my $def = $tables->{$table};
	my $cols = $def->{COLS};

	my @base_cols;

	my $type = $def->{TYPE} || $schema->{sql}{table_type};

	my $id_col = $schema->{sql}{id_col};
	my $class_col = $schema->{sql}{class_col} || 'type';
	my $timestamp_col = $schema->{sql}{timestamp_col} || '__ts';
	my $timestamp_type = $schema->{sql}{timestamp} || 'TIMESTAMP';
	my $timestamp = $schema->{sql}{timestamp_all_tables};

	push @base_cols,("$id_col $schema->{sql}{id} NOT NULL,\n"
			 ."PRIMARY KEY( $id_col )")
	    if exists $cols->{$id_col};
	push @base_cols, "$class_col $schema->{sql}{cid} NOT NULL"
	    if exists $cols->{$class_col};

	push @base_cols, "$timestamp_col $timestamp_type NOT NULL"
            if $timestamp;

	delete @$cols{$id_col};
	delete @$cols{$class_col};

	$do->("CREATE TABLE $table\n(\n  ",
	      join( ",\n  ", (@base_cols,
			      map { "$_ $cols->{$_}" } keys %$cols) ),
	      "\n) ".($type?" TYPE=$type":""));
    }

    my $control = $schema->{control};
    my $table_type = $schema->{sql}{table_type};

    $do->( <<SQL . ($table_type?" TYPE=$table_type":"") );
CREATE TABLE $control
(
layout INTEGER NOT NULL,
engine VARCHAR(255),
engine_layout INTEGER,
mark INTEGER NOT NULL
)
SQL

    my $info = $engine->get_deploy_info();
    my ($l) = split '\.', $Tangram::VERSION;

    # Prevent additional records on redeploy.
    #  -- ks.perl@kurtstephens.com 2004/04/29
    $do->("CREATE UNIQUE INDEX ${control}_Guard ON $control (layout, engine, engine_layout)");

    $do->("INSERT INTO $control (layout, engine, engine_layout, mark)"
	  ." VALUES ($info->{LAYOUT}, '$info->{ENGINE}', "
	  ."$info->{ENGINE_LAYOUT}, 0)");
}

sub retreat
{
    my ($self, $output) = @_;
    my ($tables, $engine) = @$self;
	my $schema = $engine->{SCHEMA};

    $output ||= \*STDOUT;

    my $do = _deploy_do($output);

    for my $table (sort keys %$tables, $schema->{control})
    {
		$do->( "DROP TABLE $table" );
    }
}

# XXX - never reached in the test suite; debugging function?
sub classids
{
    my ($self) = @_;
    my ($tables, $schema) = @$self;
	my $classes = $schema->{classes};
	# use Data::Dumper;
	return { map { $_ => $classes->{$_}{id} } keys %$classes };
}

package Tangram::Relational::PolySelectTemplate;

sub new
  {
	my $class = shift;
	bless [ @_ ], $class;
  }

sub instantiate {

    my ($self, $remote, $xcols, $xfrom, $xwhere) = @_;
    my ($expand, $cols, $from, $where) = @$self;

    $xcols ||= [];
    $xfrom ||= [];

    my @xwhere;

    if (@$xwhere) {
	$xwhere[0] = join ' AND ', @$xwhere;
	$xwhere[0] =~ s[%][%%]g;
    }

    my @tables = $remote->table_ids();

    my $select = sprintf("SELECT %s\n  FROM %s",
			 join(', ', @$cols, @$xcols),
			 join(', ', @$from, @$xfrom)
			);

    $select = sprintf("%s\n  WHERE %s",
		      $select,
		      join(' AND ', @$where, @xwhere)
		     )
	if @$where || @$xwhere;

    sprintf $select, map { $tables[$_] } @$expand;
}

sub extract {

    my ($self, $row) = @_;
    my $id = shift @$row;
    my $class_id = shift @$row;

    my $slice = $self->[-1]{$class_id}
	or Carp::croak("unexpected class id '$class_id' (OK: "
		       .(join(",",keys %{$self->[-1]})).")");

    my $state = [ @$row[ @$slice ] ];

    splice @$row, 0, @{ $self->[1] } - 2;

    return ($id, $class_id, $state);
}

1;

#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################

package Tangram::Relational::Engine;

#---------------------------------------------------------------------
#  $engine->get_class_engine($ClassDef)
#
#  Returns the Engine for a particular Class - the class definition is
#  passed rather than the name.
#
#  Returns a Tangram::Relational::Engine::Class object.
#---------------------------------------------------------------------
sub get_class_engine {
  my ($engine, $class) = @_;

  my $class_engine;

  unless ($class_engine = $engine->{CLASS}{$class->{name}}) {

      $class_engine = $engine->{CLASS}{$class->{name}}
	  = $engine->make_class_engine($class);

      $class_engine->initialize($engine, $class, $class);
  }

  return $class_engine;
}

#---------------------------------------------------------------------
#  $engine->make_class_engine($ClassDef)
#---------------------------------------------------------------------
sub make_class_engine {
    my ($self, $class) = @_;
    return Tangram::Relational::Engine::Class->new();
}

# forward some methods to class engine

for my $method (qw( get_instance_select
		    get_insert_statements get_insert_fields
		    get_update_statements get_update_fields
		    get_deletes
		    get_polymorphic_select get_table_set
		  )) {
    eval qq{
	sub $method {
	    my (\$self, \$class, \@args) = \@_;
	    return \$self->get_class_engine(\$class)->$method(\$self, \@args);
	}
    }
}

#---------------------------------------------------------------------
#  $engine->get_exporter($ClassDef)
#
# Returns a closure that will `export' an object to the DB
# XXX - never reached in the test suite
#---------------------------------------------------------------------
sub get_exporter {
    my ($self, $class) = @_;
    return $self->get_class_engine($class)->get_exporter
	( { layout1 => $self->{layout1} } );
}

#---------------------------------------------------------------------
#  $engine->get_importer($ClassDef)
#
# Returns a closure that will `import' an object from the DB
# XXX - never reached in the test suite
#---------------------------------------------------------------------
sub get_importer {
    my ($self, $class) = @_;
    return $self->get_class_engine($class)->get_importer
	( { layout1 => $self->{layout1} } );
}

# Looks like a Catch 22 destructor, but test suite says otherwise :)
sub DESTROY {
    my ($self) = @_;

    for my $class (values %{ $self->{CLASS} }) {
	$class->fracture()
	    if $class;
    }
}

package Tangram::Relational::Engine::Class;

use vars qw(@ISA);
@ISA = qw( Tangram::Node );

sub new {
    bless { }, shift;
}

sub fracture {
    my ($self) = @_;
    delete $self->{BASES};
    delete $self->{SPECS};
}

sub initialize {
    my ($self, $engine, $class, $mapping) = @_;
    $self->{CLASS} = $class;
    $self->{MAPPING} = $mapping;
    $self->{BASES} = [
		      map { $engine->get_class_engine($_) }
		      $class->get_bases()
		     ];
    $self->{SPECS} = [
		      map { $engine->get_class_engine($_) }
		      $class->get_specs()
		     ];
    $self->{ID_COL} = $engine->{SCHEMA}{sql}{id_col};
}

sub get_instance_select {
    my ($self, $engine) = @_;

    return $self->{INSTANCE_SELECT} ||= do {

	my $schema = $engine->{SCHEMA};
	my $id_col = $schema->{sql}{id_col};

	my $context = {
		       engine => $engine,
		       schema => $schema,
		       layout1 => $engine->{layout1}
		      };

	my (@tables, %seen, @cols, $root);

	$self->for_composing
	    (
	     sub {
		 my ($part) = @_;
		 $root ||= $part;
		 $context->{class} = $part->{CLASS};
		 push @cols,
		     (
		      map {
			  my ($table, $col) = @$_;
			  push @tables, $table unless $seen{$table}++;
			  "$table.$col"
		      }
		      $part->{MAPPING}->get_import_cols($context)
		     );
		 }
	    );

	unless (@tables) {
	    # in case the class has absolutely no state at all...
	    @cols = $id_col;
	    @tables = $root->{MAPPING}->get_table;
	}

	my $first_table = shift @tables;

	sprintf("SELECT %s FROM %s WHERE %s",
		join(', ', @cols),
		join(', ', $first_table, @tables),
		join(' AND ', "$first_table.$id_col = ?",
		     (map { "$first_table.$id_col = $_.$id_col" }
		      @tables)
		    )
	       );
  };
}

sub get_insert_statements {
  my ($self, $engine) = @_;
  return @{ $self->get_save_cache($engine)->{INSERTS} };
}

sub get_insert_fields {
  my ($self, $engine) = @_;
  return @{ $self->get_save_cache($engine)->{INSERT_FIELDS} };
}

sub get_update_statements {
  my ($self, $engine) = @_;
  return @{ $self->get_save_cache($engine)->{UPDATES} };
}

sub get_update_fields {
  my ($self, $engine) = @_;
  return @{ $self->get_save_cache($engine)->{UPDATE_FIELDS} };
}

sub get_save_cache {

    my ($class, $engine) = @_;

    return $class->{SAVE} ||= do {

	my $schema = $engine->{SCHEMA};
	my $id_col = $schema->{sql}{id_col};
	my $type_col = $engine->{TYPE_COL};

	my (%tables, @tables);
	my (@export_sources, @export_closures);

	my $context = { layout1 => $engine->{layout1} };

	my $field_index = 2;

	$class->for_composing
	    (sub {
		 my ($part) = @_;

		 my $table_name =  $part->{MAPPING}{table};
		 my $table = $tables{$table_name}
		     ||= do {
			 push @tables,
			     my $table = [ $table_name, [], [] ];
			 $table
		     };

		 $context->{class} = $part;

		 for my $field ($part->{MAPPING}->get_direct_fields())
		 {
		     my @export_cols =
			 $field->get_export_cols($context);

		     push @{ $table->[1] }, @export_cols;
		     push @{ $table->[2] },
			 $field_index..($field_index + $#export_cols);
		     $field_index += @export_cols;
		 }
	     });

	my (@inserts, @updates, @insert_fields, @update_fields);

	for my $table (@tables) {
	    my ($table_name, $cols, $fields) = @$table;
	    my @meta = ( $id_col );
	    my @meta_fields = ( 0 );

	    if ($engine->{ROOT_TABLES}{$table_name}) {
		push @meta, $type_col;
		push @meta_fields, 1;
	    }

	    next unless @meta > 1 || @$cols;

	    push @inserts, sprintf('INSERT INTO %s (%s) VALUES (%s)',
				   $table_name,
				   join(', ', @meta, @$cols),
				   join(', ', ('?') x (@meta + @$cols)));
	    push @insert_fields, [ @meta_fields, @$fields ];

	    if (@$cols) {
		push @updates, sprintf('UPDATE %s SET %s WHERE %s = ?',
				       $table_name,
				       join(', ', map { "$_ = ?" } @$cols),
				       $id_col);
		push @update_fields, [ @$fields, 0 ];
	    }
	}

	{
	    INSERT_FIELDS => \@insert_fields, INSERTS => \@inserts,
	    UPDATE_FIELDS => \@update_fields, UPDATES => \@updates,
	}
    };
}

sub get_deletes {

    my ($self, $engine) = @_;

    return @{ $self->{DELETE} ||= do {
	my $schema = $engine->{SCHEMA};
	my $context = {
		       engine => $engine,
		       schema => $schema,
		       layout1 => $engine->{layout1}
		      };
	my (@tables, %seen);

	$self->for_composing
	    (sub {
		 my ($part) = @_;
		 my $mapping = $part->{MAPPING};

		 my $home_table = $mapping->{table};
		 push @tables, $home_table
		     if $mapping->is_root() && !$seen{$home_table}++;

		 $context->{class} = $part->{CLASS};

		 for my $qcol ($mapping->get_export_cols($context)) {
		     my ($table) = @$qcol;
		     push @tables, $table unless $seen{$table}++;
		 }
	     });

	  my $id_col = $engine->{SCHEMA}{sql}{id_col};

	  [ map { "DELETE FROM $_ WHERE $id_col = ?" } @tables ]
      } };
}

sub get_table_set {
    my ($self, $engine) = @_;

    # return the TableSet on which the object's state resides
    # it doesn't include tables resulting solely from an intrusion
    # tabled that carry only meta-information are also included

    return $self->{TABLE_SET} ||= do {

	my $mapping = $self->{MAPPING};
	my $home_table = $mapping->{table};
	my $context = {
		       layout1 => $engine->{layout1},
		       class => $self->{CLASS}
		      };

	my @table = map { $_->[0] }
	    $mapping->get_export_cols($context);

	push @table, $home_table
	    if $engine->{ROOT_TABLES}{$home_table};

	Tangram::Relational::TableSet
		->new((map { $_->get_table_set($engine)->tables }
		       $self->direct_bases()), @table );
    };
}

sub get_polymorphic_select {
    my ($self, $engine, $storage) = @_;

    my $selects = $self->{POLYMORPHIC_SELECT} ||= do {

	my $schema = $engine->{SCHEMA};
	my $id_col = $schema->{sql}{id_col};
	my $type_col = $engine->{TYPE_COL};
	my $context = {
		       engine => $engine,
		       schema => $schema,
		       layout1 => $engine->{layout1}
		      };

	my $table_set = $self->get_table_set($engine);
	my %base_tables = do {
	    my $ph = 0; map { $_ => $ph++ } $table_set->tables()
	};

	my %partition;

	$self->for_conforming
	    (sub {
		 my $conforming = shift;
		 my $key = $conforming->get_table_set($engine)->key;
		 push @{ $partition{ $key } }, $conforming
		     unless $conforming->{CLASS}{abstract};
	     });

	my @selects;

	for my $table_set_key (keys %partition) {

	    my $mates = $partition{$table_set_key};
	    my $table_set = $mates->[0]->get_table_set($engine);
	    my @tables = $table_set->tables();

	    my %slice;
	    my %col_index;
	    my $col_mark = 0;
	    my (@cols, @expand);

	    my $root_table = $tables[0];

	    push @cols, qualify($id_col, $root_table,
				\%base_tables, \@expand);
	    push @cols, qualify($type_col, $root_table,
				\%base_tables, \@expand);

	    my %used;
	    $used{$root_table} += 2;

	    for my $mate (@$mates) {
		my @slice;

		$mate->for_composing
		    (sub {
			 my ($composing) = @_;
			 my $table = $composing->{MAPPING}{table};
			 $context->{class} = $composing;
			 my @direct_fields =
			     $composing->{MAPPING}->get_direct_fields();
			 for my $field (@direct_fields) {
			     my @import_cols =
				 $field->get_import_cols($context);

			     $used{$table} += @import_cols;

			     for my $col (@import_cols) {
				 my $qualified_col = "$table.$col";
				 unless (exists $col_index{$qualified_col}) {
				     push @cols, qualify($col, $table,
							 \%base_tables,
							 \@expand);
				     $col_index{$qualified_col} = $col_mark++;
				 }

				 push @slice, $col_index{$qualified_col};
			     }
			 }
		     });

		$slice{ $storage->{class2id}{$mate->{CLASS}{name}}
			|| $mate->{MAPPING}{id} }
		    = \@slice; # should be $mate->{id} (compat)
	    }

	    my @from;

	    for my $table (@tables) {
		next unless $used{$table};
		if (exists $base_tables{$table}) {
		    push @expand, $base_tables{$table};
		    push @from, "$table t%d";
		} else {
		    push @from, $table;
		}
	    }

	    my @where =
		(map {
		    (qualify($id_col, $root_table, \%base_tables,
			     \@expand)
		     . ' = '
		     . qualify($id_col, $_, \%base_tables, \@expand) )
		}
		 grep { $used{$_} }
		 @tables[1..$#tables]
		);

	    unless (@$mates == $engine->get_heterogeneity($table_set))
	    {
		push @where,
		    (sprintf("%s IN (%s)",
			     qualify($type_col, $root_table, \%base_tables,
				     \@expand),
			     join ', ', map {
				# try $storage first for compatibility
				# with layout1
				 $storage->{class2id}{$_->{CLASS}{name}}
				     or $_->{MAPPING}{id}
			     } @$mates)
		    );
	    }

	    push @selects,
		Tangram::Relational::PolySelectTemplate
			->new(\@expand, \@cols, \@from, \@where,
			      \%slice);
	  }

	\@selects;
    };

    return @$selects;
}

sub qualify {
    my ($col, $table, $ph, $expand) = @_;

    if (exists $ph->{$table}) {
	push @$expand, $ph->{$table};
	return "t%d.$col";
    } else {
	return "$table.$col";
    }
}

# XXX - never reached (?)
sub get_exporter {
    my ($self, $context) = @_;

    return $self->{EXPORTER} ||= do {

	my (@export_sources, @export_closures);

	$self->for_composing
	    (sub {
		 my ($composing) = @_;

		 my $class = $composing->{CLASS};
		 $context->{class} = $class;

		 for my $field ($composing->{MAPPING}->get_direct_fields()) {
		     if (my $exporter = $field->get_exporter($context)) {
			 if (ref $exporter) {
			     push @export_closures, $exporter;
			     push @export_sources,
				 'shift(@closures)->($obj, $context)';
			 } else {
			     push @export_sources, $exporter;
			 }
		     }
		 }
	     });

	my $export_source = join ",\n", @export_sources;
	my $copy_closures =
	    ( @export_closures ? ' my @closures = @export_closures;' : '' );

	$export_source = ("sub { my (\$obj, \$context) = \@_;"
			  ."$copy_closures\n$export_source }");

	print $Tangram::TRACE ("Compiling exporter for $self->{name}..."
			       ."\n$export_source\n")
	    if $Tangram::TRACE;

	eval $export_source or die;
    }
}

# XXX - never reached (?)
sub get_importer {
  my ($self, $context) = @_;

  return $self->{IMPORTER} ||= do {
	my (@import_sources, @import_closures);

	$self->for_composing
	    (
	     sub {
		 my ($composing) = @_;

		 my $class = $composing->{CLASS};
		 $context->{class} = $class;

		 for my $field ($composing->{MAPPING}->get_direct_fields()) {

		     my $importer = $field->get_importer($context)
			 or next;

		     if (ref $importer) {
			 push @import_closures, $importer;
			 push @import_sources,
			     'shift(@closures)->($obj, $row, $context)';
		     } else {
			 push @import_sources, $importer;
		     }
		 }
	     } );

	my $import_source = join ";\n", @import_sources;
	my $copy_closures = 
	    ( @import_closures
	      ? ' my @closures = @import_closures;'
	      : '' );

	# $Tangram::TRACE = \*STDOUT;

	$import_source = ("sub { my (\$obj, \$row, \$context) = \@_;"
			  ."$copy_closures\n$import_source }");

	print $Tangram::TRACE ("Compiling importer for $self->{name}:"
			       ."\n$import_source\n")
	  if $Tangram::TRACE;

	# use Data::Dumper; print Dumper \@cols;
	eval $import_source or die;
  };
}

1;
