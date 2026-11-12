use utf8;
use strict;
use warnings;
use Test::More;
use File::Temp ();
use Ora2Pg;
use Ora2Pg::DbLink qw(server_for server_name_from_def);

# server = <database>_<login>
is(server_name_from_def('dbb', 'SW'), 'dbb_sw', 'database alias + login');
is(server_name_from_def('DBB', 'APP'), 'dbb_app', 'case does not matter');
is(server_name_from_def('dbb_bak', 'SW'), 'dbb_bak_sw', 'another database, same login');
is(server_name_from_def('SRCDB', 'REMUSR'), 'srcdb_remusr', 'RDB1');
is(server_name_from_def('EXT4', '"app"'), 'ext4_app', 'quoted login');
is(server_name_from_def('(DESCRIPTION=(ADDRESS=(HOST=h.x.loc)(PORT=1521))(CONNECT_DATA=(SID=srcsid)))', 'APP'), 'srcsid_app', 'descriptor with SID');
is(server_name_from_def('(DESCRIPTION=(ADDRESS=(HOST=h.x.loc)(PORT=1521)))', 'APP'), undef, 'descriptor without database');
is(server_name_from_def('dbb', undef), undef, 'no login');

my %defs = (
	'SW'      => { owner => 'APP', user => 'APP', host => 'dbb' },
	'SW_PROD' => { owner => 'APP', user => 'APP', host => 'DBB' },
	'DBB'    => { owner => 'APP', user => 'SW',     host => 'dbb' },
	'DBB_BAK'=> { owner => 'APP', user => 'SW',     host => 'dbb_bak' },
	'APP.RDB1' => { owner => 'APP', user => 'REMUSR', host => 'SRCDB' },
);
my $o = bless { dblink => \%defs, dblink_server => {} }, 'Ora2Pg';
is(server_for($o, 'SW'), 'dbb_app', 'link SW');
is(server_for($o, 'SW_PROD'), 'dbb_app', 'same database and login: same server');
is(server_for($o, 'DBB'), 'dbb_sw', 'link DBB');
is(server_for($o, 'DBB_BAK'), 'dbb_bak_sw', 'copy of the database: own server');
is(server_for($o, 'RDB1'), 'srcdb_remusr', 'definition keyed with the owner');
is(server_for($o, 'nowhere'), 'nowhere', 'no definition: old name from the link');
$o->{dblink_server} = { DBB => 'custom' };
is(server_for($o, 'DBB'), 'custom', 'DBLINK_SERVER wins');

# live dictionary on demand; no query when the input is a file
package Stub { sub new { bless { n => 0 }, shift } sub selectall_arrayref { $_[0]{n}++; return [['REMUSR', 'SRCDB']] } sub errstr { '' } }
package main;
my $dbh = Stub->new;
my $l = bless { dbh => $dbh, dblink_server => {} }, 'Ora2Pg';
is(server_for($l, 'RDB1'), 'srcdb_remusr', 'definition from ALL_DB_LINKS');
server_for($l, 'RDB1');
is($dbh->{n}, 1, 'cached');
my $f = bless { dbh => $dbh, input_file => 'x.sql', dblink_server => {} }, 'Ora2Pg';
is(server_for($f, 'RDB1'), 'rdb1', 'input file: no dictionary, old name');
is($dbh->{n}, 1, 'no query');

# export_dblink: one CREATE SERVER per database and login
my $e = bless { type => 'DBLINK', dblink => { %defs }, dblink_mode => 'fdw', dblink_server => {}, quiet => 1, no_header => 1,
	output_dir => File::Temp::tempdir(CLEANUP => 1), schema => 'APP' }, 'Ora2Pg';
for (values %{$e->{dblink}}) { $_->{username} = $_->{user}; $_->{password} = 'NONE'; }
my $out = '';
{
	no warnings 'redefine';
	local *Ora2Pg::dump = sub { $out .= $_[1] };
	local *Ora2Pg::_set_file_header = sub { '' };
	local *Ora2Pg::save_filetoupdate_list = sub { };
	$e->export_dblink();
}
my @srv = $out =~ /^CREATE SERVER (\S+)/mg;
is_deeply([sort @srv], [qw(dbb_app dbb_bak_sw dbb_sw srcdb_remusr)], 'SW and SW_PROD share one server') or diag $out;

done_testing;
