use strict;
use warnings;
use utf8;
use Test::More;
use File::Temp qw(tempdir);
use FindBin qw($Bin);
use lib "$Bin/lib";
use DblinkTest qw(slurp norm);

chdir "$Bin/.." or die;
my $dir = tempdir(CLEANUP => 1);

# Same script as the CLI, but DBI->connect dies: any Oracle connection fails the run.
my $wrap = 'require DBI; no warnings "redefine"; *DBI::connect = sub { die "no Oracle in e2e\n" };'
	. ' do "./scripts/ora2pg" or die $@ || $!;';
my @cmd = ($^X, '-Ilib', '-e', $wrap, '--', '-c', 't/ora2pg_test.conf', '-t', 'PACKAGE',
	'-i', 't/fixtures/pkg_dblink.sql', '-o', 'out.sql', '-b', $dir);
open(my $olderr, '>&', \*STDERR); open(STDERR, '>', '/dev/null');
my $rc = system(@cmd);
open(STDERR, '>&', $olderr);
is($rc, 0, 'ora2pg exit code 0, DBI::connect never reached');

my $out = -f "$dir/out.sql" ? slurp("$dir/out.sql") : '';
my $tab = -f "$dir/dblink_tables_package.sql" ? slurp("$dir/dblink_tables_package.sql") : '';
ok(length $out, 'out.sql produced');
ok(length $tab, 'dblink_tables_package.sql produced');

like($out, qr/oracle_exec/, 'oracle_exec');
like($out, qr/oracle_call/, 'oracle_call');
like($out, qr/oracle_scalar/, 'oracle_scalar');
like($out, qr/fdw_rdb1_/, 'fdw_rdb1_ schema');

# (a)
unlike($out, qr/PERFORM\s+PERFORM|CALL\s+oracle_exec/, 'no PERFORM PERFORM / CALL oracle_exec');

# no @link left outside comments, except on lines carrying a TODO dblink
(my $nc = $out) =~ s{/\*(?!\s*TODO dblink).*?\*/}{ my $x = $&; $x =~ s/[^\n]//g; $x }gse;
$nc =~ s/--[^\n]*//g;
my @left = grep { /\w\s*@\s*\w/ && !/TODO dblink/ } split /\n/, $nc;
is(scalar(@left), 0, 'no @link outside comments/TODO') or diag(join "\n", @left);

# (b) (c) every _dbl_rN / _ora2pg_r used is declared in the same function
my $bad = 0;
for my $f (split /(?=^CREATE OR REPLACE (?:FUNCTION|PROCEDURE) )/m, $out) {
	for my $v ($f =~ /\b(_dbl_r\d+|_ora2pg_r)\b/g) {
		$bad++ unless $f =~ /\b\Q$v\E\s+(?:text|RECORD)\b/i;
	}
}
is($bad, 0, '_dbl_rN / _ora2pg_r declared in same function');

# (d)
like(norm($out), qr/SELECT oracle_scalar\('rdb1', \$ora\$SELECT rsch\.seq_item_s\.NEXTVAL FROM DUAL\$ora\$\)::numeric INTO STRICT new_item_id/,
	'sequence via oracle_scalar ::numeric INTO STRICT');

# regressions: Cyrillic routine name is kept; package END is stripped despite the trailing "/"
like($out, qr/CREATE OR REPLACE FUNCTION "demo_pkg_найти_товар"/, 'routine with a Cyrillic name is not dropped');
unlike($out, qr/END;\s*END;/, 'no duplicate END');

# reference output
my $exp = "t/expected/pkg_dblink";
is(norm($out), norm(slurp("$exp/out.sql")), 'out.sql == expected');
is(norm($tab), norm(slurp("$exp/dblink_tables_package.sql")), 'dblink_tables_package.sql == expected');

done_testing;
