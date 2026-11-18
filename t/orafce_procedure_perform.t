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

# With USE_ORAFCE only DBMS_OUTPUT got PERFORM; a procedure of another orafce package used as a
# statement (dbms_pipe.pack_message(...);) is a syntax error in PL/pgSQL.
like(convert("BEGIN dbms_pipe.pack_message(x); END;"), qr/PERFORM dbms_pipe\.pack_message\(x\);/i, 'pack_message');
like(convert("BEGIN\n  dbms_pipe.purge(c);\nEND;"), qr/PERFORM dbms_pipe\.purge\(c\);/i, 'purge at the line start');
like(convert("BEGIN a := 1; DBMS_ALERT.SIGNAL('a', 'b'); END;"), qr/PERFORM DBMS_ALERT\.SIGNAL\(/, 'alert signal after a statement');
like(convert("BEGIN utl_file.put_line(f, 'x'); utl_file.fclose(f); END;"), qr/PERFORM utl_file\.put_line\(.*PERFORM utl_file\.fclose\(f\)/, 'utl_file procedures');
like(convert("IF ok THEN dbms_random.seed(1); END IF;"), qr/THEN PERFORM dbms_random\.seed\(1\)/i, 'after THEN');
# functions in expressions are left alone
unlike(convert("IF dbms_pipe.send_message(c, 3) > 0 THEN null; END IF;"), qr/PERFORM/, 'function in a condition');
unlike(convert("s := dbms_pipe.receive_message(c, 5);"), qr/PERFORM/, 'function in an assignment');
unlike(convert("f := utl_file.fopen(d, n, 'w');"), qr/PERFORM/, 'fopen in an assignment');
# unchanged behaviors
like(convert("BEGIN dbms_output.put_line('x'); END;"), qr/PERFORM dbms_output\.put_line/i, 'dbms_output still gets PERFORM');
unlike(convert("BEGIN dbms_pipe.pack_message(x); END;", use_orafce => 0), qr/PERFORM/, 'no PERFORM without USE_ORAFCE');

done_testing();
