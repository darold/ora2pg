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

# String literals are masked (?TEXTVALUEn?) before replace_sys_context() runs, so the
# SYS_CONTEXT('USERENV', ...) conversions never matched.
for my $orafce (0, 1) {
	my %o = (use_orafce => $orafce);
	like(convert(q{x := SYS_CONTEXT('USERENV','CURRENT_SCHEMA');}, %o), qr/x := current_schema\b(?!\()/, "CURRENT_SCHEMA (orafce=$orafce)");
	like(convert(q{x := SYS_CONTEXT('USERENV', 'SESSION_USER');}, %o), qr/x := session_user\b/, "SESSION_USER (orafce=$orafce)");
	like(convert(q{x := SYS_CONTEXT('USERENV','IP_ADDRESS');}, %o), qr/x := inet_client_addr\(\)/, "IP_ADDRESS (orafce=$orafce)");
	like(convert(q{x := SYS_CONTEXT('USERENV','DB_NAME');}, %o), qr/x := current_database\(\)/, "DB_NAME needs () (orafce=$orafce)");
	like(convert(q{x := SYS_CONTEXT('userenv','module');}, %o), qr/current_setting\('application_name'\)/, "lower case (orafce=$orafce)");
}
# every call in a statement, not only the first one
like(convert(q{x := lower(SYS_CONTEXT('USERENV','SESSION_USER')) || lower(SYS_CONTEXT('USERENV','CURRENT_SCHEMA'));}),
	qr/lower\(session_user\) \|\| lower\(current_schema\)/, 'two calls in one statement');
# anything else is left as it is
like(convert(q{x := SYS_CONTEXT('MYCTX','PARAM');}), qr/SYS_CONTEXT\('MYCTX','PARAM'\)/, 'other namespace untouched');
like(convert(q{x := SYS_CONTEXT('USERENV','SID');}), qr/SYS_CONTEXT\('USERENV','SID'\)/, 'unknown USERENV parameter untouched');

done_testing();
