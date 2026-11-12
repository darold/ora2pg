use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use File::Temp ();
use lib "$Bin/lib";
use DblinkTest qw(slurp norm);

binmode(Test::More->builder->$_, ':utf8') for qw(output failure_output todo_output);


# export_view on an object built by hand (as ora2pg would fill it from Oracle)
sub export
{
	my ($name, $body, %v) = @_;
	my $o = bless { type => 'VIEW', use_orafce => 0, plsql_pgsql => 1, text_values => {}, comment_values => {},
		text_values_pos => 0, function_metadata => {}, dblink_mode => 'fdw', dblink_server => {}, dblink_exclude => {},
		data_type => { %Ora2Pg::Oracle::SQL_TYPE }, pg_integer_type => 1, pg_numeric_type => 1, default_numeric => 'bigint',
		dblink_schema_prefix => 'fdw_', output_dir => File::Temp::tempdir(CLEANUP => 1), quiet => 1, no_header => 1, idxcomment => 0, create_or_replace => ' OR REPLACE',
		views => { $name => { text => $body, alias => [], owner => 'APP', %v } } }, 'Ora2Pg';
	my $out = '';
	no warnings 'redefine';
	local *Ora2Pg::dump = sub { $out .= $_[1] };
	local *Ora2Pg::_set_file_header = sub { '' };
	local *Ora2Pg::save_filetoupdate_list = sub { };
	$o->export_view();
	return (norm($out), $o);
}

my $vw = slurp("$Bin/fixtures/ddl/v_sales_total.vw");
my ($body) = $vw =~ /\bAS\s+(select.*?);\s*$/is or die 'fixture';
my @cols = ({ name => 'SM', type => 'NUMBER' }, { name => 'DFROM', type => 'DATE' });

my ($out) = export('APP.V_SALES_TOTAL', $body, columns => \@cols);
like($out, qr/^CREATE FOREIGN TABLE app\.v_sales_total \( sm numeric, dfrom timestamp\(0\) \) SERVER dbb_bak OPTIONS \(table \$ora\$\(select /, 'foreign table header');
unlike($out, qr/CREATE (?:OR REPLACE )?VIEW|TODO/, 'no CREATE VIEW, no leftover foreign tables of the discarded text');
my ($q) = $out =~ /\$ora\$(.*)\$ora\$/;
ok(defined $q && $q !~ /\@/, 'no @ in query');
like($q, qr/\Q'%ПРОДАЖ%ИТОГ%'\E/, 'literal kept');
like($q, qr/decode\(d\.k_vf, 1, d\.unit_price, d\.unit_price\/\(sw\.rate\(d\.k_vf, d\.dfrom,1\)\) \)\)/, 'decode untouched');
like($q, qr{/\*\+ ordered use_nl\(tp\)\*/}, 'hint kept');
like($q, qr/sw\.doctype tp,sw\.doc d where/, 'tables without link');

# one local table and one remote: normal path
($out) = export('APP.V_MIX', 'select a.x from app.loc a, sw.doc@dbb_bak d where a.id = d.id', columns => [{ name => 'X', type => 'NUMBER' }]);
like($out, qr/^CREATE OR REPLACE VIEW app\.v_mix AS /, 'mixed: normal view');
unlike($out, qr/FOREIGN TABLE/, 'mixed: no foreign table');

# JOIN with local table, subquery with local table
($out) = export('APP.V_J', 'select 1 x from sw.doc@dbb_bak d join app.loc l on l.id = d.id', columns => [{ name => 'X', type => 'NUMBER' }]);
unlike($out, qr/FOREIGN TABLE/, 'join with local: normal');
($out) = export('APP.V_S', 'select x from sw.doc@dbb_bak d where d.id in (select id from app.loc)', columns => [{ name => 'X', type => 'NUMBER' }]);
unlike($out, qr/FOREIGN TABLE/, 'subquery with local: normal');

# JOIN of two remote tables on one link: foreign table
($out) = export('APP.V_J2', 'select d.x from sw.doc@dbb_bak d left join sw.doctype@dbb_bak t on t.k_c = d.tp', columns => [{ name => 'X', type => 'NUMBER' }]);
like($out, qr/FOREIGN TABLE app\.v_j2/, 'two remote tables with join');

# two links, excluded link: normal
($out) = export('APP.V_2L', 'select 1 x from sw.doc@dbb_bak d, sw.t@rdb1 o', columns => [{ name => 'X', type => 'NUMBER' }]);
unlike($out, qr/FOREIGN TABLE/, 'two links: normal');
($out) = export('APP.V_EX', 'select 1 x from sw.doc@dbb_bak d', columns => [{ name => 'X', type => 'NUMBER' }], );
like($out, qr/FOREIGN TABLE/, 'control');

# no column types (-i mode): normal path + TODO
($out) = export('APP.V_NC', 'select 1 x from sw.doc@dbb_bak d');
like($out, qr/^-- TODO dblink: view on one link, no column types CREATE OR REPLACE VIEW/, 'no types: TODO + normal');

# ---- jobs -> pg_cron: all 17 scheduler jobs ----
use Ora2Pg::DbLink qw(cron_expr job_sql);
is(cron_expr('FREQ=MINUTELY;INTERVAL=30'), '*/30 * * * *', 'minutely');
is(cron_expr('freq=hourly;'), '0 */1 * * *', 'hourly');
is(cron_expr('FREQ=HOURLY;INTERVAL=2;BYMINUTE=15'), '15 */2 * * *', 'hourly byminute');
is(cron_expr('Freq=daily;ByHour=0,14,15;ByMinute=0;BySecond=0'), '0 0,14,15 * * *', 'daily list');
is(cron_expr('FREQ=MONTHLY; BYMONTHDAY=1;BYHOUR=1;BYMINUTE=00'), '0 1 1 * *', 'monthly');
is(cron_expr('FREQ=WEEKLY;BYDAY=MON'), undef, 'weekly unsupported');
is(cron_expr('sysdate+1/24'), undef, 'old DBA_JOBS interval unsupported');

my $jobs = 0;
my (@todo, @done);
for my $f (sort glob("$Bin/fixtures/jobs/*.sql")) {
	my $t = slurp($f);
	my ($iv) = $t =~ /repeat_interval\s*=>\s*'([^']*)'/i or next;
	my ($act) = $t =~ /job_action\s*=>\s*'((?:[^']|'')*)'/is;
	$act =~ s/''/'/g;
	(my $n = $f) =~ s{.*/}{};
	my ($jn) = $t =~ /job_name\s*=>\s*'([^']*)'/i;
	$jobs++;
	my $o = bless { type => 'JOB', use_orafce => 0, plsql_pgsql => 1, text_values => {}, comment_values => {}, idxcomment => 0,
		function_metadata => {}, dblink_mode => 'fdw', dblink_server => {}, dblink_exclude => {},
		data_type => { %Ora2Pg::Oracle::SQL_TYPE }, dblink_schema_prefix => 'fdw_' }, 'Ora2Pg';
	my $sql = job_sql($o, $jn, $iv, $act);
	if ($sql =~ /^-- TODO cron/) { push @todo, $n } else {
		push @done, $n;
		like($sql, qr/^SELECT cron\.schedule\('\Q@{[ lc($jn) =~ s{"}{}gr ]}\E', '[\d,*\/ ]+', \$cron\$DO \$do\$.*\$do\$;\$cron\$\);\n\z/s, "job $n");
	}
}
is($jobs, 3, '3 jobs');
is(scalar(@done) + scalar(@todo), 3, 'every job is converted or TODO');
is_deeply(\@todo, ['app.demo_weekly_job.sql'], 'weekly -> TODO');
diag('TODO jobs: ' . (join(', ', @todo) || 'none'));

done_testing;
