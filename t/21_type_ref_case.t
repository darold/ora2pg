use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

binmode(Test::More->builder->$_, ':utf8') for qw(output failure_output todo_output);

# tbl.col%TYPE with non-ASCII names is lower-cased like the created objects (PG folds only ASCII)
is(Ora2Pg::PLSQL::lc_type_refs('( z Pay.Заявка.Валюта%TYPE, s int)'), '( z pay.заявка.валюта%TYPE, s int)', 'schema.table.col%TYPE');
is(Ora2Pg::PLSQL::lc_type_refs('v ЗАЯВКА.Сумма %TYPE;'), 'v заявка.сумма %TYPE;', 'table.col %TYPE');

my $o = bless { type => 'FUNCTION', use_orafce => 0, plsql_pgsql => 1, text_values => {},
	comment_values => {}, text_values_pos => 0, function_metadata => {}, data_type => { %Ora2Pg::Oracle::SQL_TYPE },
	pg_integer_type => 1, pg_numeric_type => 1, default_numeric => 'bigint' }, 'Ora2Pg';
my $out = Ora2Pg::PLSQL::convert_plsql_code($o, "DECLARE v Pay.Заявка.Валюта%TYPE; BEGIN NULL; END;");
like($out, qr/v pay\.заявка\.валюта%TYPE/, 'in code');

$o->{preserve_case} = 1;
$out = Ora2Pg::PLSQL::convert_plsql_code($o, "DECLARE v Pay.Заявка.Валюта%TYPE; BEGIN NULL; END;");
like($out, qr/v Pay\.Заявка\.Валюта%TYPE/, 'PRESERVE_CASE keeps the case');

done_testing;
