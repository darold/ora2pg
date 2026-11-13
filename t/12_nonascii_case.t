use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

binmode(Test::More->builder->$_, ':utf8') for qw(output failure_output todo_output);

# Minimal object, no Oracle connection: same mask -> convert -> restore pipeline as ora2pg.
sub convert
{
	my ($code, %opts) = @_;
	my $o = bless { type => 'FUNCTION', use_orafce => 0, plsql_pgsql => 1, text_values => {},
		comment_values => {}, text_values_pos => 0, function_metadata => {}, data_type => { %Ora2Pg::Oracle::SQL_TYPE },
		pg_integer_type => 1, pg_numeric_type => 1, default_numeric => 'bigint', %opts }, 'Ora2Pg';
	$o->_remove_text_constant_part(\$code);
	$o->_remove_comments(\$code);
	my $out = Ora2Pg::PLSQL::convert_plsql_code($o, $code);
	$o->_restore_text_constant_part(\$out);
	$o->_restore_comments(\$out);
	return $out;
}
sub norm { my $s = shift; $s =~ s/\s+/ /g; $s =~ s/^ | $//g; return $s; }

# Unquoted non-ASCII identifiers are lower-cased (PG folds only ASCII), like the exported names.
my $out = norm(convert("BEGIN\nSELECT MAX(op.ТОВАР), Количество INTO x, y FROM ТОВАР op, Склад_ERP s WHERE op.партия = s.ПАРТИЯ AND op.Id = 1;\nEND;"));
like($out, qr/SELECT MAX\(op\.товар\), количество INTO STRICT x, y FROM товар op, склад_erp s WHERE op\.партия = s\.партия AND op\.Id = 1/, 'unquoted: Cyrillic lower, ASCII untouched');

# literals, comments and quoted names stay as they are
$out = norm(convert("BEGIN\n-- ТОВАР в комментарии\nSELECT 'ТОВАР' , \"ТОВАР\" FROM Т;\nEND;"));
like($out, qr/-- ТОВАР в комментарии/, 'comment kept');
like($out, qr/'ТОВАР' , "ТОВАР" FROM т;/, 'literal and quoted name kept');

# PRESERVE_CASE: nothing is changed
$out = norm(convert("BEGIN\nSELECT ТОВАР INTO STRICT x FROM ТОВАР;\nEND;", preserve_case => 1));
like($out, qr/SELECT ТОВАР INTO STRICT x FROM ТОВАР;/, 'preserve_case: unchanged');

done_testing;
