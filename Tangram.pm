package Tangram;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

require Exporter;

@ISA = qw(Exporter AutoLoader);
# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
@EXPORT = qw(
	
);
$VERSION = '0.10';


# Preloaded methods go here.

use Set::Object;

use Tangram::Scalar;
use Tangram::Ref;
use Tangram::Set;
use Tangram::Array;
use Tangram::Hash;
use Tangram::Schema;
use Tangram::Cursor;
use Tangram::Storage;
use Tangram::Expr;

1;

__END__
