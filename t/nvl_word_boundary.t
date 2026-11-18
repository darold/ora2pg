use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

# Minimal object, no Oracle connection: same mask -> convert -> restore pipeline as ora2pg.
sub convert
{
	my ($code, %opts) = @_;
	my $o = bless { type => 'FUNCTION', use_orafce => 1, plsql_pgsql => 1, text_values => {},
		comment_values => {}, text_values_pos => 0, function_metadata => {}, data_type => { %Ora2Pg::Oracle::SQL_TYPE },
		pg_integer_type => 1, pg_numeric_type => 1, default_numeric => 'bigint', %opts }, 'Ora2Pg';
	$o->_remove_text_constant_part(\$code);
	$o->_remove_comments(\$code);
	my $out = Ora2Pg::PLSQL::convert_plsql_code($o, $code);
	$o->_restore_text_constant_part(\$out);
	$o->_restore_comments(\$out);
	$out =~ s/\s+/ /g;
	return $out;
}

# NVL() -> coalesce() must not touch longer names that end with NVL, such as Oracle's LNNVL().
for my $orafce (0, 1) {
	my %o = (use_orafce => $orafce);
	like(convert('IF LNNVL(a > 1) THEN x := 1; END IF;', %o), qr/\bLNNVL\(a > 1\)/, "LNNVL is kept (orafce=$orafce)");
	unlike(convert('IF LNNVL(a > 1) THEN x := 1; END IF;', %o), qr/LNcoalesce/, "no LNcoalesce (orafce=$orafce)");
	like(convert('x := NVL(a, b);', %o), qr/coalesce\(a, ?b\)/, "NVL still becomes coalesce (orafce=$orafce)");
	like(convert('x := nvl (a, b);', %o), qr/coalesce\(a, ?b\)/, "lower case with a space (orafce=$orafce)");
	unlike(convert('x := my_nvl(a, b);', %o), qr/coalesce/, "my_nvl() is left alone (orafce=$orafce)");
}

done_testing();
