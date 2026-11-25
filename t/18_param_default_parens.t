use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

# A parameter default with parenthesis, e.g. DEFAULT TRUNC (SYSDATE), must be converted like any other expression.
my $o = bless { type => 'FUNCTION', use_orafce => 0, plsql_pgsql => 1, text_values => {}, comment_values => {},
	function_metadata => {}, preserve_case => 0, pg_supports_procedure => 1, pg_supports_outparam => 1,
	package_as_schema => 0, data_type => { %Ora2Pg::Oracle::SQL_TYPE }, pg_integer_type => 1, pg_numeric_type => 1,
	default_numeric => 'bigint' }, 'Ora2Pg';
my $src = "FUNCTION f1 (a IN NUMBER, d IN DATE DEFAULT TRUNC (SYSDATE), n IN NUMBER(10) DEFAULT NULL) RETURN VARCHAR2 IS\nBEGIN\n RETURN 'x';\nEND f1;\n";
my $out = $o->_convert_function('APP', $src, 'PKG');
unlike($out, qr/SYSDATE/i, 'SYSDATE in a parameter default is converted');
like($out, qr/DEFAULT date_trunc\('day', clock_timestamp\(\)\)/, 'converted default');
like($out, qr/n bigint DEFAULT NULL/, 'other parameters unchanged');

done_testing;
