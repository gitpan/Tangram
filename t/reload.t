# (c) Sound Object Logic 2000-2001

use strict;
use lib 't';
use Springfield;

# $Tangram::TRACE = \*STDOUT;   

Springfield::begin_tests(5);

{
	my $storage = Springfield::connect_empty;
	$storage->insert( NaturalPerson->new( firstName => 'Marge', name => 'Bouvier' ) );
	$storage->disconnect;
}

Springfield::leaktest;

{
	my $storage = Springfield::connect;
	my ($marge) = $storage->select('NaturalPerson');

	testcase($marge->{name} eq 'Bouvier');

	$marge->{name} = 'Simpson';
	$marge->{children} = [ NaturalPerson->new( firstName => 'Bart', name => 'Simpson' ) ];
	$storage->update($marge);

	$storage->reload($marge);

	testcase($marge->{name} eq 'Simpson');
	testcase(@{ $marge->{children} } == 1);

	$storage->disconnect;
}

Springfield::leaktest;

1;
