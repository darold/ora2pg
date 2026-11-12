use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);

use Ora2Pg;

# USE_ORAFCE 1 must leave decode() to orafce instead of rewriting it as CASE
for my $orafce (0, 1)
{
	my $o = bless { type => 'FUNCTION', use_orafce => $orafce, plsql_pgsql => 1,
			text_values => {}, function_metadata => {} }, 'Ora2Pg';
	my $out = Ora2Pg::PLSQL::convert_plsql_code($o, 'x := decode(a, 1, 2, 3);');
	if ($orafce) {
		like($out, qr/decode\(a, 1, 2, 3\)/, 'USE_ORAFCE 1 keeps decode()');
	} else {
		like($out, qr/CASE WHEN a=1 THEN\s+2\s+ELSE 3 END/, 'USE_ORAFCE 0 rewrites decode() to CASE');
	}
}

# Foreign server names for database links
my @names = (
	[ 'RDB1',             undef,     'rdb1',             'plain link' ],
	[ 'APPQ.WORD',    undef,     'appq',         'global name domain is dropped' ],
	[ 'SRCDB@XX_TESTUSER', undef,     'srcdb_xx_testuser', 'connection qualifier kept as suffix' ],
	[ 'LINKB@REMUSR',      undef,     'linkb_remusr',      'qualifier distinguishes links' ],
	[ 'APP.RDB1',     'APP', 'rdb1',             'owner prefix from _get_dblink is removed' ],
	[ 'APP.RDB1',     'SW',     'app',          'only the real owner is stripped' ],
	[ '"DBB_BAK"',      undef,     'dbb_bak',        'quotes removed' ],
	[ 'DBB-X',          undef,     '"dbb-x"',        'non identifier name is quoted' ],
);
is(Ora2Pg::PLSQL::dblink_server_name($_->[0], $_->[1]), $_->[2], $_->[3]) for @names;

# read_dblink_from_file must record the schema as the link owner
my ($fh, $file) = tempfile(SUFFIX => '.sql', UNLINK => 1);
print $fh "CREATE DATABASE LINK RDB1 CONNECT TO REMUSR IDENTIFIED BY secret USING 'SRCDB';\n";
print $fh "CREATE PUBLIC DATABASE LINK OTHERDB USING 'otherdb';\n";
close $fh;
my $o = bless { input_file => $file, schema => 'APP' }, 'Ora2Pg';
$o->read_dblink_from_file();
is($o->{dblink}{RDB1}{owner}, 'APP', 'CONNECT TO link: owner is the schema');
is($o->{dblink}{RDB1}{host},  'SRCDB',    'CONNECT TO link: host parsed');
is($o->{dblink}{OTHERDB}{owner}, 'APP', 'USING-only link: owner is the schema');

done_testing();
