use strict;
use Carp;

package Tangram::Schema;

my $id_type = 'numeric(15, 0)';
my $oid_type = 'numeric(10, 0)';
my $cid_type = 'numeric(5,0)';
my $classname_type = 'varchar(128)';

sub tabledefs
{
	my ($self, $file) = @_;

	my $classes = $self->{classes};
	my $tables = {};

   foreach my $class (keys %{$self->{classes}})
	{
		my $classdef = $classes->{$class};
		my $tabledef = $tables->{$class} ||= {};
		my $cols = $tabledef->{COLS} ||= {};

		$cols->{id} = $id_type;
      $cols->{classId} = $cid_type if $classdef->{root} == $classdef;

      foreach my $typetag (keys %{$classdef->{members}})
      {
         my $members = $classdef->{members}{$typetag};
         my $type = $self->{types}{$typetag};
			@{$tabledef->{COLS}}{ $type->cols($members) } = $type->coldefs($members, $self, $class, $tables);
      }
	}

	delete @$tables{ grep { 1 == keys %{ $tables->{$_}{COLS} } } keys %$tables };

	return $tables;
}

sub Tangram::Integer::coldefs
{
	my ($self, $members) = @_;
	map { 'INT NULL' } keys %$members;
}

sub Tangram::Real::coldefs
{
	my ($self, $members) = @_;
	map { 'REAL NULL' } keys %$members;
}

sub Tangram::Scalar::coldefs
{
	my ($self, $members) = @_;
	map { 'VARCHAR(128) NULL' } keys %$members;
}

sub Tangram::Ref::coldefs
{
	my ($self, $members) = @_;
	map { "$id_type NULL" } keys %$members;
}

sub Tangram::String::coldefs
{
	my ($self, $members) = @_;
	map { 'VARCHAR(128) NULL' } keys %$members;
}

sub Tangram::Set::coldefs
{
	my ($self, $members, $schema, $class, $tables) = @_;

	foreach my $member (keys %$members)
	{
		$tables->{ $members->{$member}{table} }{COLS} =
			{ coll => $id_type, item => $id_type };
	}
}

sub Tangram::IntrSet::coldefs
{
	my ($self, $members, $schema, $class, $tables) = @_;

	foreach my $member (values %$members)
	{
		my $table = $tables->{ $schema->{classes}{$member->{class}}{table} } ||= {};
		$table->{COLS}{$member->{coll}} = "$id_type NULL";
	}
}

sub Tangram::Array::coldefs
{
	my ($self, $members, $schema, $class, $tables) = @_;

	foreach my $member (keys %$members)
	{
		$tables->{ $members->{$member}{table} }{COLS} =
			{ coll => $id_type, item => $id_type, slot => 'INT NULL' };
	}
}

sub Tangram::Hash::coldefs
{
	my ($self, $members, $schema, $class, $tables) = @_;

	foreach my $member (keys %$members)
	{
		$tables->{ $members->{$member}{table} }{COLS} =
			{ coll => $id_type, item => $id_type, slot => 'VARCHAR(128)' };
	}
}

sub Tangram::IntrArray::coldefs
{
	my ($self, $members, $schema, $class, $tables) = @_;

	foreach my $member (values %$members)
	{
		my $table = $tables->{ $schema->{classes}{$member->{class}}{table} } ||= {};
		$table->{COLS}{$member->{coll}} = "$id_type NULL";
		$table->{COLS}{$member->{slot}} = 'INT NULL';
	}
}

sub Tangram::HashRef::coldefs
{
	#later
}

sub Tangram::Schema::deploy_classids
{
	my ($self) = @_;

	my $classes = $self->{classes};
   my $classids = {};
   my $classid = 1;

   foreach my $class (keys %{$self->{classes}})
	{
      $classids->{$class} = $classid++ unless $classes->{$class}{abstract};
   }

   return $classids;
}

sub Tangram::Schema::deploy
{
	my ($self, $file) = @_;

	my $tables = $self->tabledefs;

	foreach my $table (sort keys %$tables)
	{
		my $def = $tables->{$table};
		print $file "CREATE TABLE $table\n(";
		my $cols = $def->{COLS};

      my @base_cols;

      push @base_cols, "id $id_type NOT NULL,\n  PRIMARY KEY( id )" if exists $cols->{id};
		push @base_cols, "classId $cid_type NOT NULL" if exists $cols->{classId};

		delete @$cols{qw( id classId )};

		print $file "\n  ", join( ",\n  ", @base_cols, map { "$_ $cols->{$_}" } keys %$cols );

		print $file "\n)\n\n";
	}

   print $file <<SQL;
CREATE TABLE OpalClass
(
        classId $cid_type NOT NULL,
        className $classname_type,
        lastObjectId $oid_type,
        PRIMARY KEY ( classId )
)

SQL

   my $classids = $self->deploy_classids;
   print $file map { "INSERT INTO OpalClass(classId, className, lastObjectId) VALUES ($classids->{$_}, '$_', 0)\n\n" }
      keys %$classids;

}

1;