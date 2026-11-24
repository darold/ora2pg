use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use DblinkTest qw(convert norm slurp);
use Ora2Pg::PLSQL ();
use Ora2Pg::DbLink qw(meta write_tables_file);

binmode(Test::More->builder->$_, ':utf8') for qw(output failure_output todo_output);

my %o = (dblink_signatures => "$Bin/fixtures/dblink_wrappers_sig.txt", pg_supports_named_operator => 1);
my ($out, $c);
sub ddl { my ($c, $k) = @_; return norm($c->{dblink_funcs}{$k} // '') }

# 1. positional call, literal '/' restored, scalar body
($out, $c) = convert("BEGIN\n" . slurp("$Bin/fixtures/ddl/cast_call.sql") . " END;", %o);
like(norm($out), qr{\Qfdw_rdb1_rsch_rem_reserve_pkg.verify_item_exist(numb_||'/'||r_cur.товар)\E}, 'call site');
my $w = ddl($c, 'fdw_rdb1_rsch_rem_reserve_pkg.verify_item_exist/1');
like($w, qr/CREATE SCHEMA IF NOT EXISTS fdw_rdb1_rsch_rem_reserve_pkg; CREATE OR REPLACE FUNCTION fdw_rdb1_rsch_rem_reserve_pkg\.verify_item_exist\(p_x text\) RETURNS numeric LANGUAGE sql AS/, 'header') or diag($w);
like($w, qr/oracle_scalar\('rdb1', \$ora\$SELECT RSCH\.REM_RESERVE_PKG\.VERIFY_ITEM_EXIST\(:1\) FROM dual\$ora\$, ARRAY\[\$1::text\]\)::numeric/, 'oracle_scalar body') or diag($w);
like($w, qr/REVOKE ALL ON FUNCTION fdw_rdb1_rsch_rem_reserve_pkg\.verify_item_exist\(text\) FROM PUBLIC;/, 'revoke');
unlike($w, qr/STRICT|STABLE|IMMUTABLE|SECURITY/, 'no STRICT, volatility or definer by default');
unlike(norm($out), qr/oracle_call|TODO/, 'no T5 output');

# SECURITY DEFINER from the directives
(undef, $c) = convert("BEGIN x := rsch.rem_iface_pkg.ref_id\@rdb1; END;", %o, dblink_wrapper_definer => 1, dblink_ext_schema => 'ext');
like(ddl($c, 'fdw_rdb1_rsch_rem_iface_pkg.ref_id/0'), qr/LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, ext AS/, 'definer + search_path');

# 2. named call
($out, $c) = convert("BEGIN\n" . slurp("$Bin/fixtures/ddl/named_call.sql") . "END;", %o);
like(norm($out), qr/l_state := fdw_rdb1_rsch_rem_iface_pkg\.verify_state\(p_ref => l_ref_no\);/, 'named call') or diag($out);
unlike($out, qr/oracle_exec/, 'no oracle_exec in the code');

# 3. no parentheses
($out, $c) = convert("BEGIN\n" . slurp("$Bin/fixtures/ddl/expr_call.sql") . "END;", %o);
like(norm($out), qr/l_ref_id := fdw_rdb1_rsch_rem_iface_pkg\.ref_id\(\);/, 'expr -> ()');
$w = ddl($c, 'fdw_rdb1_rsch_rem_iface_pkg.ref_id/0');
like($w, qr/ref_id\(\) RETURNS numeric/, 'no parameters');
like($w, qr/oracle_scalar\('rdb1', \$ora\$SELECT rsch\.rem_iface_pkg\.ref_id FROM dual\$ora\$\)::numeric/i, 'no ARRAY') or diag($w);
unlike($w, qr/ARRAY/, 'no ARRAY');

# 4. DEFAULT parameters: one overload per argument count, DATE
($out, $c) = convert('BEGIN x := sw.rate@dbb(2,d.dfrom,d.k_vf,1) + sw.rate@dbb(d.k_vf, d.dfrom, 1); END;', %o);
like(norm($out), qr/fdw_dbb_sw\.rate\(2, d\.dfrom, d\.k_vf, 1\) \+ fdw_dbb_sw\.rate\(d\.k_vf, d\.dfrom, 1\)/, 'call sites');
is(join(',', sort keys %{$c->{dblink_funcs}}), 'fdw_dbb_sw.rate/3,fdw_dbb_sw.rate/4', 'two overloads');
$w = ddl($c, 'fdw_dbb_sw.rate/4');
like($w, qr/rate\(cd_1 numeric, fdate timestamp\(0\), cd_2 numeric, rclc numeric\) RETURNS numeric/, '4 parameters');
like($w, qr/SELECT sw\.rate\(:1, TO_DATE\(:2, 'YYYY-MM-DD HH24:MI:SS'\), :3, :4\) FROM dual/, 'TO_DATE bind');
like($w, qr/ARRAY\[\$1::text, to_char\(\$2, 'YYYY-MM-DD HH24:MI:SS'\), \$3::text, \$4::text\]\)::numeric/, 'to_char in the array');
like(ddl($c, 'fdw_dbb_sw.rate/3'), qr/rate\(cd_1 numeric, fdate timestamp\(0\), cd_2 numeric\)/, '3 parameters');
($out) = convert('BEGIN x := sw.rate@dbb(1); END;', %o);
like(norm($out), qr/oracle_call\('dbb', 'sw\.rate', \(1\)::text\)/, 'required argument missing -> T5');

# named arguments: 1..max must all be named; mixed -> T5
($out, $c) = convert('BEGIN x := sw.rate@dbb(fdate => d, cd_1 => 1); END;', %o);
like(norm($out), qr/fdw_dbb_sw\.rate\(fdate => d, cd_1 => 1\)/, 'named, any order');
is(join(',', keys %{$c->{dblink_funcs}}), 'fdw_dbb_sw.rate/2', 'wrapper with the 2 named parameters');
($out) = convert('BEGIN x := sw.rate@dbb(cd_1 => 1, cd_2 => 2); END;', %o);
like(norm($out), qr/oracle_exec\('dbb'/, 'gap in named parameters -> T5');
($out) = convert('BEGIN x := sw.rate@dbb(1, fdate => d); END;', %o);
like(norm($out), qr/oracle_exec\('dbb'/, 'mixed -> T5');

# 5. MODIFIES -> oracle_exec, DATE argument
(undef, $c) = convert('BEGIN x := rsch.rem_pkg.post_item@rdb1(i, d); END;', %o);
$w = ddl($c, 'fdw_rdb1_rsch_rem_pkg.post_item/2');
like($w, qr/SELECT oracle_exec\('rdb1', \$ora\$BEGIN :p0 := rsch\.rem_pkg\.post_item\(:p1, TO_DATE\(:p2, 'YYYY-MM-DD HH24:MI:SS'\)\); END;\$ora\$, \$1::text, to_char\(\$2, 'YYYY-MM-DD HH24:MI:SS'\)\)::numeric/, 'oracle_exec body') or diag($w);
unlike($w, qr/oracle_scalar/, 'no oracle_scalar');

# DATE result
(undef, $c) = convert('BEGIN x := rsch.rem_pkg.today@rdb1(1); y := rsch.rem_pkg.next_day@rdb1(1); END;', %o);
like(ddl($c, 'fdw_rdb1_rsch_rem_pkg.today/1'), qr/RETURNS timestamp\(0\).*SELECT TO_CHAR\(rsch\.rem_pkg\.today\(:1\), 'YYYY-MM-DD HH24:MI:SS'\) FROM dual.*\)::timestamp\(0\)/, 'DATE result, scalar');
like(ddl($c, 'fdw_rdb1_rsch_rem_pkg.next_day/1'), qr/:p0 := TO_CHAR\(rsch\.rem_pkg\.next_day\(:p1\), 'YYYY-MM-DD HH24:MI:SS'\); END;.*\)::timestamp\(0\)/, 'DATE result, exec');

# 6. the same call twice -> one wrapper
(undef, $c) = convert('BEGIN x := rsch.rem_iface_pkg.verify_state@rdb1(a); y := rsch.rem_iface_pkg.verify_state@rdb1(b); END;', %o);
is(scalar(keys %{$c->{dblink_funcs}}), 1, 'one wrapper');

# 7. %TYPE, BOOLEAN -> T5; unknown type
for my $call ('rsch.rem_pkg.by_col@rdb1(1)', 'rsch.rem_pkg.is_ok@rdb1(1)') {
	($out, $c) = convert("BEGIN x := $call; END;", %o);
	like(norm($out), qr/oracle_call\('rdb1', 'rsch\.rem_pkg\.\w+', \(1\)::text\)/, "$call -> T5");
	ok(!%{$c->{dblink_funcs} || {}}, 'no wrapper registered');
}

# 8. no signature -> T5
($out) = convert('BEGIN x := rsch.rem_pkg.nosig@rdb1(1); END;', %o);
like(norm($out), qr/oracle_call\('rdb1', 'rsch\.rem_pkg\.nosig', \(1\)::text\)/, 'no signature -> T5');

# 9. the real signature file: no warnings, DEFAULT and flags
{
	package Log; sub logit { push @{$_[0]{log}}, $_[1] }
}
# flags: MODIFIES, unknown flag -> WARNING, line skipped
my $l = bless { dblink_signatures => $o{dblink_signatures} }, 'Log';
ok(meta($l, 'RDB1', 'ROUTINE', 'rsch.rem_pkg.post_item')->{modifies}, 'MODIFIES flag');
ok(!meta($l, 'RDB1', 'ROUTINE', 'rsch.rem_pkg.bad_flag'), 'unknown flag: line skipped');
like(join('', @{$l->{log}}), qr/line 12: unknown flag 'NOSUCH', skipped/, 'unknown flag: WARNING with the line number');

# live dictionary: DEFAULT from ALL_ARGUMENTS.DEFAULTED
{
	package DbhS;
	sub execute {
		my ($self) = @_;
		$self->{rows} = $self->{dbh}->selectall_arrayref($self->{sql});
		return 1;
	}
	sub fetchall_arrayref { return $_[0]{rows} }
	sub err { 0 }
	sub errstr { '' }
	package DbhStub;
	sub prepare { bless { dbh => $_[0], sql => $_[1] }, 'DbhS' }
	sub selectall_arrayref { return [ [undef, 0, 'OUT', 'NUMBER', undef, undef], ['A', 1, 'IN', 'NUMBER', undef, 'N'], ['B', 2, 'IN', 'NUMBER', undef, 'Y'] ] }
	sub errstr { 'stub' }
}
my $live = bless { dbh => bless({}, 'DbhStub'), dblink_signatures => '' }, 'Log';
is_deeply(meta($live, 'RDB1', 'ROUTINE', 'rsch.pkg.f')->{args}, [ { name => 'A', mode => 'IN', type => 'NUMBER' }, { name => 'B', mode => 'IN', type => 'NUMBER', default => 1 } ], 'live dictionary: DEFAULTED');

# 10. only function calls, no @link tables: the wrapper file exists, the tables file does not
{
	package FileStub;
	our (%file, $cur);
	sub open_export_file { $cur = $_[1]; $file{$cur} = ''; return 1 }
	sub set_binmode { }
	sub dump { $file{$cur} .= $_[1] }
	sub close_export_file { }
}
(undef, $c) = convert('BEGIN x := rsch.rem_iface_pkg.verify_state@rdb1(a); y := sw.rate@dbb(1, d, 2); END;', %o);
{
	no warnings 'redefine';
	local (*Ora2Pg::open_export_file, *Ora2Pg::set_binmode, *Ora2Pg::dump, *Ora2Pg::close_export_file) =
		(\&FileStub::open_export_file, \&FileStub::set_binmode, \&FileStub::dump, \&FileStub::close_export_file);
	write_tables_file($c);
}
is_deeply([ sort keys %FileStub::file ], ['dblink_functions_function.sql'], 'only the functions file');
my $f = norm($FileStub::file{'dblink_functions_function.sql'});
like($f, qr/rate\(.*verify_state\(|verify_state\(.*rate\(/, 'both wrappers in the file');
ok(index($f, 'fdw_dbb_sw.rate') < index($f, 'fdw_rdb1_rsch_rem_iface_pkg.verify_state'), 'keys in order');

done_testing;
