use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

# With USE_ORAFCE 1 decode() must be left to orafce. convert_plsql_code()
# tested an undeclared $self instead of $class, so decode() was always
# rewritten as CASE.
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

done_testing();
