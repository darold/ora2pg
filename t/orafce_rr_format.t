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

# RR / RRRR are Oracle-only: with USE_ORAFCE the format was passed through unchanged
# (PostgreSQL to_char prints "RR" literally, to_date misparses the year).
like(convert(q{x := TO_CHAR(d, 'dd.mm.rr');}), qr/TO_CHAR\(d, 'dd\.mm\.YY'\)/, 'to_char: rr -> YY');
like(convert(q{x := TO_CHAR(d, 'DD.MM.RRRR');}), qr/'DD\.MM\.YYYY'/, 'to_char: RRRR -> YYYY');
like(convert(q{x := TO_DATE(s, 'dd.mm.rr');}), qr/to_date\(s, ?'dd\.mm\.YY'\)/i, 'to_date: rr -> YY');
like(convert(q{x := TO_DATE(s, 'DD.MM.RRRR HH24:MI');}), qr/'DD\.MM\.YYYY HH24:MI'/, 'to_date: RRRR with time');
like(convert(q{x := TO_CHAR(SYSDATE + 1, 'dd.mm.rr');}), qr/'dd\.mm\.YY'/, 'expression as first argument');
like(convert(q{x := TO_CHAR(d, 'DD.MM.YYYY');}), qr/TO_CHAR\(d, 'DD\.MM\.YYYY'\)/, 'plain format untouched');
like(convert(q{x := TO_CHAR(d, 'DD "ERROR" YYYY');}), qr/'DD "ERROR" YYYY'/, 'RR inside a word untouched');
like(convert(q{x := TO_CHAR(d, 'dd.mm.rr'), y := other(s, 'rr');}), qr/other\(s, 'rr'\)/, 'other functions untouched');

done_testing();
