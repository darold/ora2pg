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

# Parameterless orafce functions: "NEW.cs := dbms_utility.format_call_stack;" fails in PL/pgSQL
# (missing FROM-clause entry for table "dbms_utility").
like(convert('NEW.cs := dbms_utility.format_call_stack;'), qr/format_call_stack\(\)/i, 'format_call_stack()');
like(convert('t := DBMS_UTILITY.GET_TIME;'), qr/GET_TIME\(\)/, 'get_time()');
like(convert('r := dbms_random.value;'), qr/dbms_random\.value\(\)/i, 'dbms_random.value without arguments');
like(convert('r := dbms_random.value(1, 10);'), qr/dbms_random\.value\(1, 10\)/i, 'with arguments: untouched');
unlike(convert('s := dbms_utility.format_call_stack();'), qr/\(\)\s*\(\)/, 'already has parentheses');
unlike(convert('t := DBMS_UTILITY.GET_TIME;', use_orafce => 0), qr/GET_TIME\(\)/, 'no change without USE_ORAFCE');

# orafce has dbms_output.put_line(text) only
like(convert('dbms_output.put_line(n);'), qr/PERFORM dbms_output\.put_line\(\(n\)::text\)/i, 'variable cast to text');
like(convert('dbms_output.put_line(f(a, b));'), qr/put_line\(\(f\(a, b\)\)::text\)/i, 'function call cast to text');
like(convert(q{dbms_output.put_line('plain');}), qr/put_line\('plain'\)/i, 'literal not cast');
like(convert(q{dbms_output.put_line('a' || n);}), qr/put_line\('a' \|\| n\)/i, 'concatenation not cast');
like(convert('dbms_output.put_line(to_char(a || b));'), qr/put_line\(\(to_char\(a \|\| b\)\)::text\)/i, 'concatenation inside a call does not hide a non-text result');
like(convert('dbms_output.put_line(n::text);'), qr/put_line\(n::text\)/i, 'already text');

done_testing();
