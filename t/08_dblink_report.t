use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use DblinkTest qw(convert norm);
use Ora2Pg::DbLink qw(report_item);

# 3 refs on 2 links; ZZZ is excluded -> 1 TODO in the converted output
my $src = <<'SQL';
BEGIN
  UPDATE sw.doc@dbb_bak SET x = 1 WHERE id = 2;
  SELECT t.a INTO v FROM sw.tab2@dbb_bak t WHERE t.id = 1;
  SELECT z.a INTO v FROM sw.tab3@zzz z;
END;
SQL
my ($out, $o) = convert($src, dblink_exclude => { ZZZ => 1 });
my ($cost_in,  %in)  = Ora2Pg::PLSQL::estimate_cost($o, $src, 'FUNCTION');
my ($cost_out, %out) = Ora2Pg::PLSQL::estimate_cost($o, $out, 'FUNCTION');

is($in{DBLINK}, 3, 'source: 3 refs counted by scan_refs');
is($in{DBLINK_TODO}, 0, 'source: no TODO yet');
is($out{DBLINK_TODO}, 1, 'output: 1 TODO (excluded link)');
is($Ora2Pg::PLSQL::UNCOVERED_SCORE{DBLINK_TODO}, 3, 'weight of a TODO');
my ($c1) = Ora2Pg::PLSQL::estimate_cost($o, "BEGIN null; /* TODO dblink X */ END;", 'FUNCTION');
my ($c0) = Ora2Pg::PLSQL::estimate_cost($o, "BEGIN null; /* todo dblink X */ END;", 'FUNCTION');
is($c1 - $c0, 3, 'one TODO costs 3');

is(report_item('DBLINK', \%in), "\tDBLINK: 3 refs, 0 TODO", 'report line');
is(report_item('TRUNC', { TRUNC => 2 }), "\tTRUNC => 2", 'other keys unchanged');

# mysql/mssql paths and DBLINK-free text: no dblink noise
my ($c, %d) = Ora2Pg::PLSQL::estimate_cost($o, "BEGIN\n null;\nEND;", 'FUNCTION');
is($d{DBLINK}, 0, 'no refs, no cost');

done_testing;
