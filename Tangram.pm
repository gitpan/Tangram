# (c) Sound Object Logic 2000-2001

package Tangram;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

require Exporter;

@ISA = qw(Exporter AutoLoader);
# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
@EXPORT = qw(
	
);
$VERSION = '2.05';


# Preloaded methods go here.

use Tangram::Core;

use Tangram::Set;
use Tangram::IntrSet;

use Tangram::Array;
use Tangram::IntrArray;

use Tangram::Hash;

sub connect
  {
	shift;
	Tangram::Storage->connect( @_ );
  }

1;

__END__
