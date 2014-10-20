#!/usr/bin/perl -w

use strict;

use lib "t";
use lib "t/musicstore";
use Prerequisites;

use Test::More tests => 2;

BEGIN {
    use_ok "MusicStore";
};

local $/;

SKIP:
{
    my $dbh = DBI->connect( $DBConfig::cs,
			    $DBConfig::user,
			    $DBConfig::passwd )
	or skip "could not connect to database", 1;

    do {
	local $dbh->{PrintError};
	local $dbh->{RaiseError};
	$DBConfig::dialect->retreat(MusicStore->schema, $dbh);
	$DBConfig::dialect->retreat(MusicStore->new_schema, $dbh);
    };

    $dbh->{RaiseError} = 1;

    $DBConfig::dialect->deploy(MusicStore->schema, $dbh);

    pass("deploy completed without raising errors");

    $dbh->disconnect;
}
