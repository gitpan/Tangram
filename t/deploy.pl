use t::Springfield;
use Tangram::Deploy;

open DEPLOY, '>k:/perl/Tangram/t/DEPLOY.sql' or die $!;

$Springfield::schema->deploy(\*DEPLOY);
