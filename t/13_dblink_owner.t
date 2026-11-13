use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

# read_dblink_from_file() must record the schema as the owner of the link
# (the key was misspelled "shema", so the owner was always undefined).
my ($fh, $file) = tempfile(SUFFIX => '.sql', UNLINK => 1);
print $fh "CREATE DATABASE LINK RDB1 CONNECT TO REMUSR IDENTIFIED BY pw USING 'SRCDB';\n";
print $fh "CREATE PUBLIC DATABASE LINK OTHERDB USING 'otherdb';\n";
close $fh;

my $o = bless { input_file => $file, schema => 'APP' }, 'Ora2Pg';
$o->read_dblink_from_file();

is($o->{dblink}{RDB1}{owner},    'APP',   'CONNECT TO link: owner is the schema');
is($o->{dblink}{RDB1}{host},     'SRCDB', 'CONNECT TO link: host parsed');
is($o->{dblink}{OTHERDB}{owner}, 'APP',   'USING-only link: owner is the schema');

done_testing();
