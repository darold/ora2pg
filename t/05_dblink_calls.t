use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use DblinkTest qw(convert norm lines slurp);

binmode(Test::More->builder->$_, ':utf8') for qw(output failure_output todo_output);

my $sig = "$Bin/fixtures/dblink_calls_sig.txt";
my ($out, $x);

# 1. named argument -> oracle_exec, assignment without cast
my $exp = q{l_state := oracle_exec('rdb1', $ora$BEGIN :p0 := rsch.rem_iface_pkg.verify_state(p_ref => :p1); END;$ora$, (l_ref_no)::text);};
($out) = convert("BEGIN\n" . slurp("$Bin/fixtures/ddl/named_call.sql") . "END;");
like(norm($out), qr/\Q$exp\E/, 'named -> oracle_exec');

# 2. cast from the signature / TODO without it
my $src = slurp("$Bin/fixtures/ddl/cast_call.sql");
my $call = q{oracle_call('rdb1', 'RSCH.REM_RESERVE_PKG.VERIFY_ITEM_EXIST', (numb_||'/'||r_cur.товар)::text)};
($out) = convert("BEGIN\n$src END;", dblink_signatures => $sig);
like(norm($out), qr{\Qfdw_rdb1_rsch_rem_reserve_pkg.verify_item_exist(numb_||'/'||r_cur.товар)\E}, 'signature -> typed wrapper (T9)');
($out) = convert("BEGIN\n$src END;");
like(norm($out), qr/\Q$call\E \/\* TODO dblink type \*\//, 'no signature -> TODO type');

# 3. view, positional, DATE bind
$src = 'select 1 from t d where sw.rate@dbb_bak(d.k_vf, d.dfrom,1)<>0';
($out) = convert($src);
like(norm($out), qr/\Qoracle_call('dbb_bak', 'sw.rate', (d.k_vf)::text, (d.dfrom)::text, (1)::text)\E/, 'view call');
# 2 of 3 required arguments: no wrapper, T5 output with the DATE bind
($out) = convert('select 1 from t d where sw.rate@dbb_bak(d.k_vf, d.dfrom)<>0', dblink_signatures => $sig);
like(norm($out), qr/\Qoracle_call('dbb_bak', 'sw.rate', (d.k_vf)::text, to_char((d.dfrom), 'YYYY-MM-DD HH24:MI:SS'))\E/, 'DATE bind');
($out) = convert($src, dblink_signatures => $sig);
like(norm($out), qr/\Qfdw_dbb_bak_sw.rate(d.k_vf, d.dfrom, 1)\E/, 'complete call -> wrapper');

# 3a. comment between the comma and the next remote table in FROM
($out) = convert("select 1 from rsch.rem_accounts\@rdb1 a,\n -- note\n rsch.rem_orders\@rdb1 b where a.id = b.id");
unlike(norm($out), qr/\@rdb1/, 'comment after comma in FROM: both tables rewritten');

# 4. nested
($out) = convert("BEGIN x := a.f\@rdb1(a.g\@rdb1(1)); END;");
$out = norm($out);
like($out, qr/\Qx := oracle_call('rdb1', 'a.f', (oracle_call('rdb1', 'a.g', (1)::text)\E/, 'nested');
unlike($out, qr/\@/, 'nested: no @');

# 5. statement call: handled in T6 (t/06_dblink_procs.t)
($out) = convert("BEGIN\napps.pkg.p\@rdb1(1);\nEND;");
like(norm($out), qr/PERFORM oracle_exec\('rdb1'/, 'statement call: oracle_exec (T6)');

# 6. @ in a literal
($out) = convert("BEGIN x := 'a\@b'; END;");
like(norm($out), qr/x := 'a\@b';/, 'literal untouched');

# 7. no args, expr without parens
($out) = convert("BEGIN x := rsch.pkg.f\@rdb1(); END;");
like(norm($out), qr/x := oracle_call\('rdb1', 'rsch\.pkg\.f'\);/, 'no args');
($out) = convert("BEGIN\n" . slurp("$Bin/fixtures/ddl/expr_call.sql") . "END;");
like(norm($out), qr/l_ref_id := oracle_call\('rdb1', 'rsch\.rem_iface_pkg\.ref_id'\);/, 'expr -> oracle_call');

# 8. SELECT INTO: call in SQL expression -> typed wrapper (GET_SUMM-style)
($out) = convert(<<"PL", dblink_signatures => $sig);
BEGIN
  SELECT x + rsch.pkg.get_summ\@rdb1(z) INTO x FROM dual;
END;
PL
like(norm($out), qr/SELECT x \+ fdw_rdb1_rsch_pkg\.get_summ\(z\) INTO STRICT x ?;/, 'SELECT INTO + call -> function wrapper');

done_testing;
