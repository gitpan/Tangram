use DBI;
use Set::Object;

my $configured;

if (-e 't/CONFIG')
{
   print <<'TXT';
It looks like there is a 't/CONFIG' file already. It probably contains
connection information from a previous installation.
Should I use it? (Y/n)
TXT
   $configured = <STDIN> =~ /^(Y|)$/i;
}

unless ($configured)
{
   print <<'MSG';
I need an *EMPTY* database to perform the regression tests. I must be able to
create and drop tables in that database. Please give me the following information:
MSG

   print "1) DBI connect string (you can omit the \'DBI:\' part): ";
   my $cs = <STDIN>;
   chop $cs;

   $cs = "DBI:$cs" unless $cs =~ /^DBI\:/i;
   
   print "2) Login: ";
   my $user = <STDIN>;
   chop $user;
   
   print "3) Password: ";
   my $passwd = <STDIN>;
   chop $passwd;

   print <<'MSG';

Thank you. I am going to save this information to 't/CONFIG'.
If you have given me sensitive information, make sure to destroy
the file when the tests have been completed.
MSG

   open CONFIG, '>t/CONFIG' or die "Cannot create 't/CONFIG', reason: $!";
   print CONFIG "$cs\n$user\n$passwd\n";
}

{
   print "\nNow I will attempt to connect and prepare the database...";

   local $/;

   open CONFIG, 't/CONFIG' or die "Cannot open 't/CONFIG', reason: $!";
   my ($cs, $user, $passwd) = split "\n", <CONFIG>;

   if (my $conn = DBI->connect( $cs, $user, $passwd ))
   {
      print "it worked!\n\n";

      local $/ = '';

      open DEPLOY, 't/deploy.sql' or die "Cannot open 'deploy.sql', reason: $!";

      while (my $command = <DEPLOY>)
      {
         next if $command =~ /^\s*$/s;
         eval { $conn->do("DROP TABLE $1") } if $command =~ /CREATE TABLE (\w+)/;
         $conn->do($command) or die;
      }

      $conn->disconnect;
   }

}

use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'	=> 'Tangram',
    'VERSION_FROM' => 'Tangram.pm', # finds $VERSION
);
