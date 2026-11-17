use utf8;
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

# SYSDATE inside parentheses of a parameter default (TRUNC (SYSDATE)) must be converted too
{
	my $o = bless { type => 'PACKAGE', use_orafce => 1, plsql_pgsql => 1, text_values => {}, comment_values => {},
			function_metadata => {}, preserve_case => 0, pg_supports_procedure => 1, pg_supports_outparam => 1,
			package_as_schema => 0, data_type => { %Ora2Pg::Oracle::SQL_TYPE }, pg_integer_type => 1, pg_numeric_type => 1,
			default_numeric => 'bigint' }, 'Ora2Pg';
	my $f = "FUNCTION F1 (a IN NUMBER, d IN DATE DEFAULT TRUNC (SYSDATE), n IN NUMBER(10) DEFAULT NULL) RETURN VARCHAR2 IS\nBEGIN\n RETURN 'x';\nEND F1;\n";
	my $out = $o->_convert_function('APP', $f, 'PKG');
	unlike($out, qr/SYSDATE/i, 'SYSDATE in parameter default converted');
	like($out, qr/DEFAULT TRUNC\(clock_timestamp\(\)\)/, 'default converted');
	like($out, qr/n bigint DEFAULT NULL/, 'other parameter kept');
}

# a package variable registered twice (spec and body) keeps a single default
{
	my $o = bless { type => 'PACKAGE', use_orafce => 1, plsql_pgsql => 1, text_values => {}, comment_values => {},
			function_metadata => {}, global_variables => {}, data_type => { %Ora2Pg::Oracle::SQL_TYPE } }, 'Ora2Pg';
	for my $call (1, 2) {
		my $decl = "VAL_ CONSTANT VARCHAR2(3) := 'RUR';";
		$o->_remove_text_constant_part(\$decl);
		$o->register_global_variable('pk', $decl);
	}
	is($o->{global_variables}{'pk.val_'}{default}, 'RUR', 'global variable default not doubled');
}

# an index named like a table gets a suffix (one relation namespace in PostgreSQL)
{
	my $o = bless { indexes_renaming => 0, indexes_suffix => '', tables => { 'APP.CURRENCY' => {}, 'APP.ORDERS' => {} },
			preserve_case => 0, type => 'TABLE' }, 'Ora2Pg';
	is($o->get_indexname('APP.ORDERS', 'APP.CURRENCY', 'CURRENCY'), 'currency_idx', 'index named like a table is renamed');
	is($o->get_indexname('APP.ORDERS', 'APP.CURRENCY_FACT', 'X'), 'currency_fact', 'other index names untouched');
}

# outer join with a constant and a trailing comment: "col(+) = 1 -- text"
{
	use FindBin qw($Bin);
	use lib "$Bin/lib";
	require DblinkTest;
	my ($out) = DblinkTest::convert("SELECT u.a FROM usr u, addr ad, terr tr WHERE ad.cd_u(+) = u.cd_u\n  AND ad.code_type(+) = 1   -- legal\n  AND tr.code(+) = ad.code",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/ON \(u\.cd_u = ad\.cd_u AND 1 = ad\.code_type\)/, 'constant condition stays in ON');
	unlike($out, qr/AND\s+=|\bWHERE\b/, 'no empty operand, no dangling WHERE');
	like($out, qr/-- legal/, 'comment kept');
}

# a TODO marker before a table of the FROM list survives the outer join rewrite
{
	my ($out) = DblinkTest::convert('SELECT a.x FROM app.t a, s.u@ext1 b, c WHERE b.k(+) = a.k AND c.z(+) = a.z',
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_exclude => { EXT1 => 1 });
	like($out, qr{LEFT OUTER JOIN /\* TODO dblink EXT1 \*/ s\.u\@ext1 b ON \(a\.k = b\.k\)}, 'TODO marker kept with its table');
	unlike($out, qr{/\* todo,}, 'marker not taken for a table name');
}

# Cyrillic tbl.col%TYPE is lowercased (PG folds only ASCII), in code and in arguments
{
	my ($out) = DblinkTest::convert("DECLARE v Payment.Заявка.Валюта_Фикс%TYPE; w ЗАЯВКА.ЗАЯВКА%TYPE; BEGIN NULL; END;", type => 'FUNCTION', use_orafce => 1, idxcomment => 0);
	like($out, qr/v payment\.заявка\.валюта_фикс%TYPE/, 'schema.table.col%TYPE lowercased');
	like($out, qr/w заявка\.заявка%TYPE/, 'table.col%TYPE lowercased');
	is(Ora2Pg::PLSQL::lc_type_refs('( z Payment.Заявка.Заявка%TYPE, s int)'), '( z payment.заявка.заявка%TYPE, s int)', 'arguments');
}

# pack.func() in a view is rewritten to the flat function name
{
	my $o = bless { package_functions => { 'pack_бюджет' => { val => { name => 'pack_бюджет_val', package => 'pack_бюджет' } } } }, 'Ora2Pg';
	my $v = 'SELECT Rate2(d, c, pack_бюджет.VAL()) x FROM t';
	$o->requalify_function_call(\$v);
	is($v, 'SELECT Rate2(d, c, pack_бюджет_val()) x FROM t', 'package call in view');
}

# PG: OUT after DEFAULT — strip DEFAULT so CREATE PROCEDURE succeeds
{
	my $in = "(opl_num bigint, p_rz_type text default 'C', p_summ numeric DEFAULT NULL, p_res_ OUT numeric)";
	my $out = Ora2Pg::Oracle::_pg_strip_default_before_out($in);
	unlike($out, qr/DEFAULT/i, 'DEFAULT stripped when OUT follows');
	like($out, qr/p_res_\s+OUT\s+numeric/, 'OUT parameter kept');
	like($out, qr/p_rz_type text,\s*p_summ numeric,\s*p_res_/, 'parameter order preserved');

	my $ok = '(a int DEFAULT 1, b int DEFAULT 2)';
	is(Ora2Pg::Oracle::_pg_strip_default_before_out($ok), $ok, 'no OUT: unchanged');

	my $out_first = '(r OUT numeric, n int DEFAULT 1)';
	is(Ora2Pg::Oracle::_pg_strip_default_before_out($out_first), $out_first, 'OUT before DEFAULT: unchanged');
}

# schema-qualified FROM table referenced without schema in (+) conditions is not duplicated
{
	my ($out) = DblinkTest::convert("select s.a from app.zz, app.op, app.st\n where zz.z = op.z and op.s(+) = st.a", type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	is(scalar(() = $out =~ /\bapp\.st\b/g), 1, 'table listed once');
	is(scalar(() = $out =~ /\bapp\.op\b/g), 1, 'outer joined table listed once');
}

# (+) conditions written as schema.table.col
{
	for my $w ('app.op.s(+) = app.st.a', 'app.op.s(+) = st.a', 'op.s(+) = app.st.a') {
		my ($out) = DblinkTest::convert("select 1 from app.zz, app.op, app.st\n where zz.z = op.z and $w", type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
		like($out, qr/FROM app\.zz zz, app\.st st\nLEFT OUTER JOIN app\.op op ON \(st\.a = op\.s\)\nWHERE zz\.z = op\.z/, "no duplicates: $w");
	}
}

# mixed comma-FROM + JOIN: schema.table.col -> alias.col (inet_detail class)
{
	my ($out) = DblinkTest::convert(
		"SELECT a.x, app.tab1.col_y AS y FROM app.tab1 tab1, app.tab2 tab2\nLEFT OUTER JOIN app.tab3 tab3 ON (a.k = tab3.k)\nWHERE tab2.k = tab1.k",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	unlike($out, qr/app\.tab1\.col_y/, 'qualified table ref rewritten to alias in SELECT');
	like($out, qr/tab1\.col_y/, 'alias used in SELECT');
}

# CONNECT BY LEVEL <= N from DUAL -> generate_series
{
	my ($out) = DblinkTest::convert(
		"SELECT to_char(EXTRACT(YEAR FROM statement_timestamp()) - LEVEL + 1) yr FROM DUAL CONNECT BY LEVEL <= 3",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/generate_series\(1,\s*3\)/, 'CONNECT BY LEVEL counter -> generate_series');
	unlike($out, qr/\bCONNECT\s+BY\b/i, 'no leftover CONNECT BY');
	unlike($out, qr/JOIN cte c ON \(\)/, 'no broken recursive stub');
}

# TO_CHAR with nls_date_language third arg (xcal class)
{
	my ($out) = DblinkTest::convert(
		"SELECT TO_CHAR(dt, 'Month', 'nls_date_language=RUSSIAN') AS m FROM app.cal",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	unlike($out, qr/nls_date_language/i, 'NLS third arg stripped');
	like($out, qr/TO_CHAR\s*\(\s*dt\s*,\s*'TMMonth'/i, 'Month -> TMMonth with Russian NLS');
}

# schema.package.function in view -> flat name
{
	my $o = bless { package_functions => { 'pay_pack' => { 'get_val' => { name => 'pay_pack_get_val', package => 'pay_pack' } } } }, 'Ora2Pg';
	my $v = 'SELECT app.pay_pack.get_val() FROM app.tbl';
	$o->requalify_function_call(\$v);
	is($v, 'SELECT app.pay_pack_get_val() FROM app.tbl', 'schema-qualified package call flattened');
}

done_testing();
