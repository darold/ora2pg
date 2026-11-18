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

# With USE_ORAFCE the call must be schema-qualified: the unqualified GREATEST/LEAST is the
# PostgreSQL SQL construct, which ignores NULL arguments while Oracle (and oracle.greatest) return NULL.
like(convert('x := GREATEST(a, b);'), qr/\boracle\.GREATEST\(a, b\)/, 'GREATEST is prefixed');
like(convert('x := least(a, b, c);'), qr/\boracle\.least\(a, b, c\)/, 'LEAST is prefixed');
like(convert('SELECT GREATEST (a, b) FROM t'), qr/oracle\.GREATEST\(a, b\)/, 'space before the parenthesis');
unlike(convert('x := oracle.greatest(a, b);'), qr/oracle\.oracle/, 'already qualified: no second prefix');
unlike(convert('x := my_greatest(a, b);'), qr/oracle\./, 'identifier that ends with greatest is left alone');
unlike(convert('x := GREATEST(a, b);', use_orafce => 0), qr/oracle\./, 'without USE_ORAFCE no prefix');

done_testing();
