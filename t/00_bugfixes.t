use utf8;
use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);

use Ora2Pg;

# DECODE always -> CASE (even with USE_ORAFCE): orafce decode(anyelement,...)
# rejects decode(smallint, integer, text, ...) which is common after NUMBER(p)->smallint.
for my $orafce (0, 1)
{
	my $o = bless { type => 'FUNCTION', use_orafce => $orafce, plsql_pgsql => 1,
			text_values => {}, function_metadata => {} }, 'Ora2Pg';
	my $out = Ora2Pg::PLSQL::convert_plsql_code($o, 'x := decode(a, 1, 2, 3);');
	like($out, qr/CASE WHEN a IS NOT DISTINCT FROM 1 THEN\s+2\s+ELSE 3 END/,
		"USE_ORAFCE $orafce rewrites decode() to CASE");
	unlike($out, qr/\bdecode\s*\(/i, "USE_ORAFCE $orafce: no leftover decode()");
}

# Mixed numeric search + text results (orafce would fail: smallint vs integer literals)
{
	my $o = bless { type => 'VIEW', use_orafce => 1, plsql_pgsql => 1,
			text_values => {}, function_metadata => {} }, 'Ora2Pg';
	my $out = Ora2Pg::PLSQL::convert_plsql_code($o,
		q{select decode(d.k_vf,1,'RUR',2,'US$',10,'EUR') as cd_vf from sw.doc d});
	like($out, qr/CASE WHEN d\.k_vf IS NOT DISTINCT FROM 1 THEN\s+'RUR'/i,
		'decode(col,1,text,...) -> CASE with first WHEN');
	like($out, qr/WHEN d\.k_vf IS NOT DISTINCT FROM 10 THEN\s+'EUR'/i,
		'decode multi-branch keeps last WHEN');
	unlike($out, qr/\bdecode\s*\(/i, 'no orafce decode left for typed mismatch pattern');
}

# NULL search branch must stay NULL-safe (Oracle DECODE matches NULL to NULL)
{
	my $o = bless { type => 'VIEW', use_orafce => 1, plsql_pgsql => 1,
			text_values => {}, function_metadata => {} }, 'Ora2Pg';
	my $out = Ora2Pg::PLSQL::convert_plsql_code($o,
		q{select decode(p.status, null, 'open', 'done') s from app.p});
	like($out, qr/CASE WHEN p\.status IS NOT DISTINCT FROM null THEN\s+'open'\s+ELSE 'done' END/i,
		'decode(..., null, ...) uses IS NOT DISTINCT FROM');
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

# (+) with a function on the side without (+): ec.col(+) = TO_CHAR(x.other_col)
{
	use FindBin qw($Bin);
	use lib "$Bin/lib";
	require DblinkTest;
	my ($out) = DblinkTest::convert(
		"SELECT x.v_name FROM apps.xx x, ESS.COUNTRIES ec, ESS.TERRITORIES et\n WHERE ec.alfa2(+) = TO_CHAR (x.oebs_country)\n   AND et.code_terr(+) = x.oebs_code_terr",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	unlike($out, qr/,\s*to_char\w*\s*\n/i, 'no phantom FROM table from to_char()');
	like($out, qr/LEFT OUTER JOIN ess\.countries ec ON \(TO_CHAR\(x\.oebs_country\) = ec\.alfa2\)/i,
		'function stays in ON predicate');
	like($out, qr/LEFT OUTER JOIN ess\.territories et ON \(x\.oebs_code_terr = et\.code_terr\)/i, 'second (+) join ok');
}

# coalesce(...) on the non-(+) side of (+)
{
	use FindBin qw($Bin);
	use lib "$Bin/lib";
	require DblinkTest;
	my ($out) = DblinkTest::convert(
		"SELECT 1 FROM a.b ab, ec.t ec, et.t et WHERE ec.c(+) = to_char(ab.c)\n   AND et.c(+) = coalesce(ab.a, ab.b)",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	unlike($out, qr/,\s*to_char\w*|,\s*coalesce\w*/i, 'no phantom FROM from coalesce/to_char');
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

# UNION ALL: alias rewrite must run per SELECT (v_budget_bdr class)
{
	my ($out) = DblinkTest::convert(
		"CREATE OR REPLACE VIEW v_x AS\n".
		"SELECT a.k FROM app.t1 a, app.t2 b WHERE b.k = a.k\n".
		"UNION ALL\n".
		"SELECT a.k FROM app.t1 a, app.t2 b\n".
		"LEFT OUTER JOIN app.t3 c ON (a.k = c.k)\n".
		"WHERE app.t2.k = app.t3.k",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	unlike($out, qr/app\.t2\.k/, 'UNION branch: schema.table dropped in WHERE');
	like($out, qr/WHERE\s+b\.k\s*=\s*c\.k/is, 'UNION branch: aliases in second WHERE');
}

# Cyrillic schema.table + comma-FROM + LEFT JOIN (inet_detail / inet_payment class)
{
	my ($out) = DblinkTest::convert(
		"SELECT app.таб_а.к1 AS c1, app.таб_б.к2, таб_в.к3\n".
		"FROM app.таб_а таб_а, app.таб_б таб_б\n".
		"LEFT OUTER JOIN app.таб_в таб_в ON (app.таб_а.к0 = таб_в.к0)\n".
		"WHERE app.таб_б.к2 = таб_а.к1",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	unlike($out, qr/app\.таб_[аб]\./, 'Cyrillic schema.table dropped in SELECT/WHERE');
	like($out, qr/таб_а\.к1/, 'Cyrillic alias in SELECT');
	like($out, qr/ON \(таб_а\.к0 = таб_в\.к0\)/, 'Cyrillic schema.table rewritten in JOIN ON');
	unlike($out, qr/app\.таб_а\.к0/, 'no schema.table left in ON');
}

# CONNECT BY PRIOR: lowercase prior must not leak into JOIN cte c ON (c.prior col ...)
{
	my ($out) = DblinkTest::convert(
		"SELECT n.id, n.name FROM app.dim_tree n START WITH n.root_id = 0 CONNECT BY prior n.id = n.parent_id",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/JOIN cte c ON \(c\.id = n\.parent_id\)/, 'CONNECT BY prior: cte join uses c.column');
	unlike($out, qr/c\.prior\s/i, 'no c.prior keyword left in join');
}

# (+) -> LEFT JOIN without alias: JOIN keyword ON must not become table alias (емельянов_бр class)
{
	my ($out) = DblinkTest::convert(
		"SELECT app.reg.pay, app.supp.name AS sn, app.item.name AS iname, s1.name AS s1n, app.acc.bank\n".
		"FROM app.acc s1, app.acc, app.supp, app.reg, app.item\n".
		"WHERE app.reg.typ <> 'X'\n".
		"AND app.reg.acc = app.acc.acc (+)\n".
		"AND app.reg.supp = app.supp.supp (+)\n".
		"AND app.reg.item = app.item.item (+)\n".
		"AND app.acc.acc (+) = app.reg.acc_in",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	unlike($out, qr/(?<![\w"])\bon\./i, 'JOIN ON not mistaken for alias on.');
	unlike($out, qr/"on"\./i, 'no quoted on. alias either');
	like($out, qr/\bsupp\.name\b/, 'unaliased joined table kept as table.col');
	like($out, qr/\bitem\.name\b/, 'second unaliased join kept as table.col');
	like($out, qr/\bs1\.name\b/, 'explicit alias s1 preserved');
	# same table twice: identity occurrence must keep acc.col, not become s1.col in ON
	unlike($out, qr/JOIN\s+app\.acc\s+ON\s*\([^)]*\bs1\./i, 'unaliased acc ON does not use s1');
	like($out, qr/\bacc\.bank\b/, 'unaliased acc.bank kept');
}

# CONNECT BY LEVEL as alias: recursive part must use cte column (c.уровень+1), not c.level
{
	my ($out) = DblinkTest::convert(
		"SELECT level as уровень, s.id FROM app.node s START WITH s.parent IS NULL CONNECT BY PRIOR s.id = s.parent",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/\(c\.уровень\+1\)/, 'CONNECT BY LEVEL alias uses cte column in recursive arm');
	unlike($out, qr/\(c\.level\+1\)\s+As\s+уровень/i, 'no hardcoded c.level for LEVEL as уровень');
}

# ORDER SIBLINGS BY: outer SELECT ORDER BY hierarchy, no empty WHERE (v_cfo_tree class)
{
	my ($out) = DblinkTest::convert(
		"SELECT level as lvl, d.id, d.name FROM app.dept d, app.item i\n".
		"WHERE d.id = i.dept_id(+)\n".
		"START WITH d.parent_id IS NULL CONNECT BY PRIOR d.id = d.parent_id\n".
		"ORDER SIBLINGS BY d.sort_k, i.name",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/SELECT \* FROM cte ORDER BY hierarchy/is, 'ORDER SIBLINGS: final ORDER BY hierarchy');
	unlike($out, qr/FROM cte WHERE\s+ORDER BY/i, 'ORDER SIBLINGS: no empty WHERE before ORDER BY');
	like($out, qr/JOIN cte c ON \(c\.id = d\.parent_id\)/, 'ORDER SIBLINGS: prior join keeps child alias');
	unlike($out, qr/JOIN cte c ON \([^)]*f\.d\./, 'prior join: no bogus first-FROM alias on rhs');
}

# CONNECT BY LEVEL <= N from DUAL -> generate_series
{
	my ($out) = DblinkTest::convert(
		"SELECT to_char(EXTRACT(YEAR FROM statement_timestamp()) - LEVEL + 1) yr FROM DUAL CONNECT BY LEVEL <= 3",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/generate_series\(1,\s*3\)/, 'CONNECT BY LEVEL counter -> generate_series');
	like($out, qr/-\s*gs\s*\+\s*1/, 'CONNECT BY LEVEL counter -> gs in SELECT list');
	unlike($out, qr/\bLEVEL\b/i, 'no orphan LEVEL after generate_series');
	unlike($out, qr/\bCONNECT\s+BY\b/i, 'no leftover CONNECT BY');
	unlike($out, qr/JOIN cte c ON \(\)/, 'no broken recursive stub');
}

# CONNECT BY LEVEL counter inside WITH (v_subrep_year pattern)
{
	my ($out) = DblinkTest::convert(
		"WITH v AS (SELECT to_char(EXTRACT(YEAR FROM statement_timestamp()) - LEVEL + 1) yr ".
		"FROM DUAL CONNECT BY LEVEL <= 5) SELECT v.yr FROM v",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/generate_series\(1,\s*5\)/, 'WITH: LEVEL counter -> generate_series');
	unlike($out, qr/\bLEVEL\b/i, 'WITH: no orphan LEVEL');
}

# CONNECT BY LEVEL in nested FROM subquery (v_subrep_year inner arm)
{
	my ($out) = DblinkTest::convert(
		"SELECT v.yr FROM (SELECT to_char(EXTRACT(YEAR FROM statement_timestamp()) - LEVEL + 1) yr ".
		"FROM DUAL CONNECT BY LEVEL <= 5) v",
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/-\s*gs\s*\+\s*1/, 'nested FROM: LEVEL -> gs in SELECT');
	unlike($out, qr/\bLEVEL\b/i, 'nested FROM: no orphan LEVEL');
}

# Quoted Oracle table in WHERE vs unquoted Cyrillic FROM (v_week_rep_padding)
{
	my ($out) = DblinkTest::convert(
		q{SELECT t.type, счет.валюта FROM счет, (SELECT '1' type FROM DUAL) t WHERE "СЧЕТ"."В_РАБОТЕ" = -1},
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/WHERE\s+счет\."В_РАБОТЕ"\s*=/i,
		'quoted FROM table in WHERE mapped to unquoted FROM name');
	unlike($out, qr/"СЧЕТ"\s*\.\s*"/, 'no quoted uppercase table ref in WHERE');
}

# Bare Cyrillic table (no alias) in comma-FROM + quoted name in WHERE
{
	my ($out) = DblinkTest::convert(
		q{SELECT t1.type, счет.валюта FROM счет, (SELECT 'x' type FROM DUAL) t1 WHERE "СЧЕТ"."В_РАБОТЕ" = -1},
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/WHERE\s+счет\."В_РАБОТЕ"\s*=/i, 'bare FROM table: quoted WHERE -> счет.');
}

# v_subrep_year YEAR alias + nested WITH fragment
{
	my ($out) = DblinkTest::convert(
		q{WITH v AS (SELECT to_char(EXTRACT(YEAR FROM statement_timestamp()) - LEVEL + 1) "YEAR" }.
		q{FROM DUAL CONNECT BY LEVEL <= 5) SELECT v."YEAR" FROM v},
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/-\s*gs\s*\+\s*1/, 'WITH YEAR alias: LEVEL -> gs');
	unlike($out, qr/\bLEVEL\b/i, 'WITH YEAR alias: no orphan LEVEL');
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

# standalone schema function in view -> schema.func; SYSDATE cast for dlook
{
	my $o = bless {
		schema => 'payment',
		functions => { dlook => { text => 'x' } },
		package_functions => {},
	}, 'Ora2Pg';
	my $v = 'SELECT DLook(a, statement_timestamp()) / payment.DLook(b, statement_timestamp()) FROM t';
	$o->requalify_function_call(\$v);
	like($v, qr/payment\.dlook\(a, statement_timestamp\(\)::timestamp\(0\)\)/, 'unqualified DLook qualified + cast');
	like($v, qr/payment\.dlook\(b, statement_timestamp\(\)::timestamp\(0\)\)/, 'qualified DLook cast');
}

# Reserved column alias ORDER (unquoted in Oracle) -> AS "order"
{
	my ($out) = DblinkTest::convert(
		q{SELECT z.a||'/'||o.p ORDER, o.dt FROM app.z z, app.o o},
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/\s+AS\s+"order"\s*,/i, 'unquoted ORDER alias quoted for PG');
	unlike($out, qr/\|\|\s*o\.p\s+ORDER\s*,/, 'no bare ORDER as alias');
}

# Quoted alias "FROM" must not break rewrite_from_table_aliases (v_cash_c_depo_list)
{
	my ($out) = DblinkTest::convert(
		q{SELECT a.x AS "FROM", b.y AS "TO" FROM app.a a, app.b b WHERE a.id = b.id},
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/AS\s+"from"\s*,.*AS\s+"to"\s*,/is, 'quoted FROM/TO aliases preserved');
	unlike($out, qr/AS\s+FROM"\s*,/, 'no torn AS FROM" alias');
}

# Cyrillic table.col IS NOT NULL (null_equal_empty): must not leave table(.col
{
	my ($out) = DblinkTest::convert(
		q{SELECT x FROM dual WHERE оплата.SUN_BU_BR IS NOT NULL},
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off', null_equal_empty => 1);
	like($out, qr/оплата\.SUN_BU_BR IS NOT NULL AND оплата\.SUN_BU_BR::text/,
		'Unicode qualified column kept for IS NOT NULL rewrite');
	unlike($out, qr/оплата\(\.SUN_BU_BR/, 'no torn table(.col from ASCII-only ident regex');
}

# Unquoted DAY as subquery column alias -> AS day (v_week_rep_padding)
{
	my ($out) = DblinkTest::convert(
		q{(SELECT -1 DAY FROM dual)},
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/SELECT -1 AS day FROM/i, 'numeric DAY alias quoted for PG');
	unlike($out, qr/SELECT -1 DAY\s+FROM/i, 'no bare DAY before FROM');
}

# hp200_abc server: fdw schema must not double remote owner (abc.usr@hp200)
{
	my ($out) = DblinkTest::convert(
		'SELECT cd_u FROM abc.usr@hp200',
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'fdw',
		dblink_server => { HP200 => 'hp200_abc' });
	like($out, qr/\bfrom\s+fdw_hp200_abc\.usr\b/i, 'dblink table -> fdw_hp200_abc owner');
	unlike($out, qr/fdw_hp200_abc_abc/, 'no duplicate _abc in fdw schema');
}

# quote_object_name: dotted display name is one quoted id, not schema.column
{
	my $o = bless({ preserve_case => 0, use_reserved_words => 0 }, 'Ora2Pg');
	is($o->quote_object_name('Доп.Кол-во_Б/Д'), '"доп.кол-во_б/д"', 'dotted slash column quoted as one id');
}

# FDW pass-through view: Oracle quoted uppercase columns -> lowercase (wabc_agent)
{
	my ($out) = DblinkTest::convert(
		q{SELECT "CD_A","FULL_NAME" FROM abc.agent@HP200},
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'fdw',
		dblink_server => { HP200 => 'hp200_abc' });
	like($out, qr/select\s+cd_a\s*,\s*full_name\s+from\s+fdw_hp200_abc\.agent/i,
		'quoted Oracle cols lowered for fdw table');
	unlike($out, qr/"CD_A"/, 'no quoted uppercase on fdw pass-through');
}

# Same via export_view path (_format_view), not only convert_plsql_code
{
	my $o = bless {
		type => 'VIEW', plsql_pgsql => 1, use_orafce => 1, preserve_case => 0,
		schema => 'payment', dblink_mode => 'fdw', dblink_server => { HP200 => 'hp200_abc' },
		dblink_exclude => {}, text_values => {}, comment_values => {}, text_values_pos => 0,
		function_metadata => {}, idxcomment => 0, replaced_cols => {},
	}, 'Ora2Pg';
	my $text = q{select "CD_A","FULL_NAME" from abc.agent@hp200};
	$o->_remove_comments(\$text);
	my $out = $o->_format_view('wabc_agent', $text);
	like($out, qr/select\s+cd_a\s*,\s*full_name\s+from\s+fdw_hp200_abc\.agent/i,
		'_format_view: fdw pass-through cols lowered');
	unlike($out, qr/"CD_A"/, '_format_view: no quoted uppercase on fdw');
}

# Dotted SELECT alias with slash: AS доп.кол-во_б/д (v_bo_doc_post_b4)
{
	my ($out) = DblinkTest::convert(
		q{SELECT 1 AS доп.кол-во_б/д, 2 AS x FROM dual},
		type => 'VIEW', use_orafce => 1, idxcomment => 0, dblink_mode => 'off');
	like($out, qr/AS\s+"доп\.кол-во_б\/д"\s*,/i, 'dotted slash alias quoted after AS');
	unlike($out, qr/AS\s+доп\.кол-во_б\/д\s*,/, 'no unquoted dotted slash alias');
}

# PACKAGE_AS_SCHEMA: payment.Pay_Pack.get_x -> pay_pack.get_x()
{
	my $o = bless({
		package_as_schema => 1, schema => 'payment',
		package_functions => { pay_pack => { get_foo => { name => 'pay_pack_get_foo' } } },
	}, 'Ora2Pg');
	my $v = 'SELECT payment.Pay_Pack.get_foo FROM t';
	$o->requalify_function_call(\$v);
	is($v, 'SELECT pay_pack.get_foo() FROM t', 'Pay_Pack call -> schema function with ()');
}

done_testing();
