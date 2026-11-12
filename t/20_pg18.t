use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

my $dsn = $ENV{PGTEST_DSN};
plan skip_all => 'PGTEST_DSN not set' unless $dsn;
plan skip_all => 'psql not found' unless system('psql --version >/dev/null 2>&1') == 0;
chdir "$Bin/.." or die;

sub psql { my $sql = shift; my $o = `psql "\$PGTEST_DSN" -X -At -v ON_ERROR_STOP=1 -c \Q$sql\E 2>&1`; return ($? == 0, $o); }
sub psqlf { my $f = shift; my $o = `psql "\$PGTEST_DSN" -X -At -f \Q$f\E 2>&1`; return ($? == 0, $o); }

for my $e (qw(oracle_fdw orafce plpgsql_check)) {
	my ($ok, $o) = psql("CREATE EXTENSION IF NOT EXISTS $e");
	ok($ok, "extension $e") or diag($o);
}
my ($ok, $o) = psql("DROP SERVER IF EXISTS rdb1 CASCADE; CREATE SERVER rdb1 FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '//127.0.0.1:1/none')");
ok($ok, 'server rdb1') or diag($o);

# The fixture is ora2pg output without its environment: local tables used in %TYPE
# (tables referenced via %TYPE, e.g. v_rdb1_export_list) do not exist here, so "does not exist" errors
# are expected. Anything else (syntax, bad function call) is a real failure.
# Load without stopping at the first error (strip the fixture's own ON_ERROR_STOP).
# Scratch schema, dropped at the end; the name is not tied to any customer schema.
my $sch = $ENV{PGTEST_SCHEMA} || 'ora2pg_t_dblink';
($ok, $o) = psql("DROP SCHEMA IF EXISTS $sch CASCADE; CREATE SCHEMA $sch");
ok($ok, "schema $sch") or diag($o);
# Local tables the way ora2pg exports them: lower-cased and quoted (quote_object_name). Bodies must find them.
($ok, $o) = psql(qq{SET search_path = $sch; CREATE TABLE "товар" ("товар" bigint, "партия" bigint); CREATE TABLE "группа_товаров" ("группа" text, "название" text)});
ok($ok, 'local tables as exported by ora2pg') or diag($o);
for my $f ('t/expected/pkg_dblink/dblink_tables_package.sql', 't/expected/pkg_dblink/out.sql') {
	my $out = `grep -v ON_ERROR_STOP \Q$f\E | psql "\$PGTEST_DSN" -X -At -v ON_ERROR_STOP=0 -c "SET search_path = $sch, public" -f - 2>&1`;
	my @bad = grep { /ERROR/ && !/does not exist/ } split /\n/, $out;
	is(scalar @bad, 0, "no unexpected errors loading $f") or diag(join "\n", @bad);
}

($ok, $o) = psql(qq{SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='$sch'});
cmp_ok($o, '>', 0, "functions created in schema $sch") or diag($o);

($ok, $o) = psql(qq{SET search_path = $sch, public; SELECT p.oid::regprocedure || E'\t' || c.message FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace, LATERAL plpgsql_check_function_tb(p.oid::regprocedure, fatal_errors => false) c WHERE p.prolang=(SELECT oid FROM pg_language WHERE lanname='plpgsql') AND n.nspname='$sch' AND c.level='error'});
utf8::decode($o);
diag("plpgsql_check errors (missing local objects and TODO dblink expected):\n$o");
ok($ok, 'plpgsql_check ran');
unlike($o, qr/relation "(?:товар|ТОВАР|группа_товаров|ГРУППА_ТОВАРОВ)" does not exist/, 'Cyrillic table names in bodies match the exported tables');

psql("DROP SCHEMA IF EXISTS $sch CASCADE");
done_testing;
