use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

sub convert
{
	my ($code) = @_;
	my $o = bless { type => 'VIEW', use_orafce => 0, plsql_pgsql => 1, text_values => {}, comment_values => {},
		text_values_pos => 0, idxcomment => 0, function_metadata => {} }, 'Ora2Pg';
	$o->_remove_text_constant_part(\$code);
	$o->_remove_comments(\$code);
	my $out = Ora2Pg::PLSQL::convert_plsql_code($o, $code);
	$o->_restore_text_constant_part(\$out);
	$o->_restore_comments(\$out);
	return $out;
}

# a schema-qualified FROM table referenced without schema in the conditions is listed once
my $out = convert("select s.a from app.zz, app.op, app.st\n where zz.z = op.z and op.s(+) = st.a");
is(scalar(() = $out =~ /\bapp\.st\b/g), 1, 'table listed once');
is(scalar(() = $out =~ /\bapp\.op\b/g), 1, 'outer joined table listed once');

# (+) conditions written with the schema are rewritten to the table alias
for my $w ('app.op.s(+) = app.st.a', 'app.op.s(+) = st.a', 'op.s(+) = app.st.a')
{
	$out = convert("select 1 from app.zz, app.op, app.st\n where zz.z = op.z and $w");
	like($out, qr/FROM app\.zz zz, app\.st st\nLEFT OUTER JOIN app\.op op ON \(st\.a = op\.s\)\nWHERE zz\.z = op\.z/, "no duplicates: $w");
}

done_testing;
