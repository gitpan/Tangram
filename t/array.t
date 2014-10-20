# -*- perl -*-
# (c) Sound Object Logic 2000-2001

use strict;
use Test::More tests => 50;
# for emacs debugger
#use lib "../blib/lib";
#use lib ".";
use lib "t";
use Springfield;

# This is set to 1 by iarray.t
use vars qw( $intrusive );

#$intrusive = 1;
#$Tangram::TRACE = \*STDOUT;

my $children = $intrusive ? 'ia_children' : 'children';

my %id;
my @kids = qw( Bart Lisa Maggie );

sub NaturalPerson::children
{
    my ($self) = @_;
    join(' ', map { $_->{firstName} || '' } @{ $self->{$children} } )
}

sub marge_test
{
    my $storage = shift;
    SKIP:
    {
	skip("one to many - marge skipped", 1);
	is( $storage->load( $id{Marge} )->children,
	    'Bart Lisa Maggie',
	    "Marge's children all found" );
    }
}

sub stdpop
{
    my $storage = Springfield::connect_empty;

    my @children = (map { NaturalPerson->new( firstName => $_ ) }
		    @kids);
    @id{ @kids } = $storage->insert( @children );
    like("@id{ @kids }", qr/^\d+ \d+ \d+$/, "Got ids back OK");

    my $homer = NaturalPerson->new( firstName => 'Homer',
				    $children => [ @children ] );
    $id{Homer} = $storage->insert($homer);
    isnt($id{Homer}, 0, "Homer inserted OK");

    my $marge = NaturalPerson->new( firstName => 'Marge' );
    # cannot have >1 parent with a one to many relationship!
    $marge->{$children} = [ @children ] unless $intrusive;
    $id{Marge} = $storage->insert($marge);
    isnt($id{Marge}, 0, "Marge inserted OK");

    $storage->disconnect;
}

#=====================================================================
#  TESTING BEGINS
#=====================================================================

# insert the test data
stdpop();

is(leaked, 0, "Nothing leaked yet!");

# Test that updates notice changes to collections
{
    my $storage = Springfield::connect;
    my $homer = $storage->load( $id{Homer} );
    ok($homer, "Homer still exists!");

    is($homer->children, 'Bart Lisa Maggie', "array auto-vivify 1" );
    marge_test( $storage );

    @{ $homer->{$children} }[0, 2] = @{ $homer->{$children} }[2, 0];
    $storage->update( $homer );

    $storage->disconnect;
}

is(leaked, 0, "leaktest");

{
    my $storage = Springfield::connect;
    my $homer = $storage->load( $id{Homer} );

    is($homer->children, 'Maggie Lisa Bart', "array update test 1");
    marge_test( $storage );

    pop @{ $homer->{$children} };
    $storage->update( $homer );

    $storage->disconnect;
}

###############################################
# insert

{
    my $storage = Springfield::connect;
    my $homer = $storage->load($id{Homer}) or die;

    is( $homer->children, 'Maggie Lisa',
	"array update test 2 (pop)" );

    shift @{ $homer->{$children} };
    $storage->update($homer);

    $storage->disconnect;
}

is(leaked, 0, "leaktest");

{
    my $storage = Springfield::connect;
    my $homer = $storage->load($id{Homer}) or die;
    is( $homer->children, 'Lisa',
	"array update test 2 (shift)" );
    $storage->disconnect;
}

is(leaked, 0, "leaktest");

{
    my $storage = Springfield::connect;
    my $homer = $storage->load($id{Homer}) or die;
    shift @{ $homer->{$children} };
    $storage->update($homer);
    $storage->disconnect;
}

is(leaked, 0, "leaktest");

{
    my $storage = Springfield::connect;
    my $homer = $storage->load($id{Homer}) or die;

    is( $homer->children, "", "array update test 3 (all gone)");

    push @{ $homer->{$children} }, $storage->load( $id{Bart} );
    $storage->update($homer);

    $storage->disconnect;
}

is(leaked, 0, "leaktest");

{
    my $storage = Springfield::connect;
    my $homer = $storage->load($id{Homer}) or die;

    is( $homer->children, 'Bart', "array insert test 1"  );

    push ( @{ $homer->{$children} },
	   $storage->load( @id{qw(Lisa Maggie)} ) );
    $storage->update($homer);

    $storage->disconnect;
}

is(leaked, 0, "leaktest");

{
    my $storage = Springfield::connect;
    my $homer = $storage->load( $id{Homer} );

    is( $homer->children, 'Bart Lisa Maggie', "array insert test 2" );
    marge_test( $storage );

    $storage->disconnect;
}

is(leaked, 0, "leaktest");

{
    my $storage = Springfield::connect;
    my $homer = $storage->load( $id{Homer} );

    is( $homer->children, 'Bart Lisa Maggie', "still there" );
    marge_test( $storage );

    $storage->unload();
    undef $homer;

    is(leaked, 0, "leaktest (unload)");

    $storage->disconnect;
}

###########
# back-refs
SKIP:
{
    skip("No backref support without Intr types", 2)
	unless $intrusive;

    my $storage = Springfield::connect;
    my $bart = $storage->load( $id{Bart} );

    is($bart->{ia_parent}{firstName}, 'Homer', "array back-refs" );
    marge_test( $storage );

    $storage->disconnect;
}

is(leaked, 0, "leaktest");

##########
# prefetch
# FIXME - add documentation to Tangram::Storage for prefetch
{
    my $storage = Springfield::connect;

    my @prefetch = $storage->prefetch( 'NaturalPerson', $children );

    my $homer = $storage->load( $id{Homer} );

    is( $homer->children, 'Bart Lisa Maggie',
	"prefetch test returned same results");

    marge_test( $storage );

    $storage->disconnect();
}

is(leaked, 0, "leaktest");

{
    my $storage = Springfield::connect;

    my $person = $storage->remote('NaturalPerson');
    my @prefetch = $storage->prefetch( 'NaturalPerson', $children );

    my $homer = $storage->load( $id{Homer} );

    is( $homer->children, 'Bart Lisa Maggie',
	"prefetch test returned same results");
    marge_test( $storage );

    $storage->disconnect();
}

is(leaked, 0, "leaktest");

#########
# queries

my $parents = $intrusive ? 'Homer' : 'Homer Marge';
    #'Homer Marge';

{
    my $storage = Springfield::connect;
    my ($parent, $child)
	= $storage->remote(qw( NaturalPerson NaturalPerson ));

    my @results = $storage->select
	(
	 $parent,
	 $parent->{$children}->includes( $child )
	 & $child->{firstName} eq 'Bart'
	);

    is(join( ' ', sort map { $_->{firstName} } @results ),
       $parents, "Query (array->includes(t2) & t2->{foo} eq Bar)" );

    $storage->disconnect();
}

is(leaked, 0, "leaktest");

{
    my $storage = Springfield::connect;
    my $parent = $storage->remote( 'NaturalPerson' );
    my $bart = $storage->load( $id{Bart} );

    my @results = $storage->select
	(
	 $parent,
	 $parent->{$children}->includes( $bart )
	);

    is(join( ' ', sort map { $_->{firstName} } @results ),
       $parents, 'Query (array->includes($dbobj))' );
    $storage->disconnect();
}

is(leaked, 0, "leaktest");

#############
# aggreg => 1
{
    my $storage = Springfield::connect_empty;

    my @children = (map { NaturalPerson->new( firstName => $_ ) }
		    @kids);

    my $homer = NaturalPerson->new
	(
	 firstName => 'Homer',
	 $children => [ map { NaturalPerson->new( firstName => $_ ) }
			@kids ]
	);

    my $abe = NaturalPerson->new( firstName => 'Abe',
				  $children => [ $homer ] );

    $id{Abe} = $storage->insert($abe);

    $storage->disconnect();
}

is(leaked, 0, "leaktest");

SKIP:
{
    my $storage = Springfield::connect;

    $storage->erase( $storage->load( $id{Abe} ) );

    my @pop = $storage->select('NaturalPerson');
    is(@pop, 0, "aggreg deletes children via arrays");

    skip( "No link table with Intr Types", 1 ) if $intrusive;

    is($storage->connection()->selectall_arrayref
       ("SELECT COUNT(*) FROM a_children")->[0][0],
       0, "Link table cleared successfully after remove");

    $storage->disconnect();
}

is(leaked, 0, "leaktest");


#############################################################################
# Tx

SKIP:
{
    skip "No transactions configured/supported", 8
	if $Springfield::no_tx;

    stdpop();

    # check rollback of DB tx
    is(leaked, 0, "leaktest");

    {
	my $storage = Springfield::connect;
	my $homer = $storage->load( $id{Homer} );

	$storage->tx_start();

	shift @{ $homer->{$children} };
	$storage->update( $homer );

	$storage->tx_rollback();

	$storage->disconnect;
    }

    is(leaked, 0, "leaktest");


    # storage should still contain 3 children

    {
	my $storage = Springfield::connect;
	my $homer = $storage->load( $id{Homer} );

	is( $homer->children, 'Bart Lisa Maggie', "rollback 1" );
	marge_test( $storage );

	$storage->disconnect;
    }

    is(leaked, 0, "leaktest");


    # check that DB and collection state remain in synch in case of rollback
    {
	my $storage = Springfield::connect;
	my $homer = $storage->load( $id{Homer} );

	$storage->tx_start();

	shift @{ $homer->{$children} };
	$storage->update( $homer );

	$storage->tx_rollback();

	$storage->update( $homer );

	$storage->disconnect;
    }

    # Bart should no longer be Homer's child
    {
	my $storage = Springfield::connect;
	my $homer = $storage->load( $id{Homer} );

	is( $homer->children, 'Lisa Maggie',
	    "auto-commit on disconnect" );
	marge_test( $storage );

	$storage->disconnect;
    }

    is(leaked, 0, "leaktest");

}

1;
