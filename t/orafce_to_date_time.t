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

# pg_catalog.to_date() returns a date: the time part of the format is lost. With orafce use oracle.to_date().
like(convert(q{x := TO_DATE(s, 'DD.MM.YYYY HH24:MI:SS');}), qr/oracle\.to_date\(s, ?'DD\.MM\.YYYY HH24:MI:SS'\)/, 'HH24:MI:SS');
like(convert(q{x := to_date(s, 'dd.mm.yyyy hh24:mi');}), qr/oracle\.to_date\(s, ?'dd\.mm\.yyyy hh24:mi'\)/, 'lower case HH24:MI');
like(convert(q{x := TO_DATE(s, 'DD.MM.YYYY HH:MI:SS AM');}), qr/oracle\.to_date/, '12 hour format');
like(convert(q{x := TO_DATE(lower(s), 'YYYY-MM-DD HH24:MI:SS');}), qr/oracle\.to_date\(lower\(s\), ?'YYYY-MM-DD HH24:MI:SS'\)/, 'function call as first argument');
unlike(convert(q{x := TO_DATE(s, 'DD.MM.YYYY');}), qr/oracle\./, 'date only: plain to_date');
unlike(convert(q{x := TO_DATE(s, 'DD-MON-YYYY');}), qr/oracle\./, 'MON is not MI');
unlike(convert(q{x := TO_DATE(s, 'DD.MM.YYYY HH24:MI:SS');}, use_orafce => 0), qr/oracle\./, 'no prefix without USE_ORAFCE');
unlike(convert(q{x := my_to_date(s, 'DD.MM.YYYY HH24:MI:SS');}), qr/oracle\./, 'other function with a similar name untouched');

done_testing();
