use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

sub convert
{
	my ($code, %opts) = @_;
	my $o = bless { type => 'VIEW', use_orafce => 0, plsql_pgsql => 1, text_values => {}, comment_values => {},
		text_values_pos => 0, idxcomment => 0, function_metadata => {}, %opts }, 'Ora2Pg';
	$o->_remove_text_constant_part(\$code);
	$o->_remove_comments(\$code);
	my $out = Ora2Pg::PLSQL::convert_plsql_code($o, $code);
	$o->_restore_text_constant_part(\$out);
	$o->_restore_comments(\$out);
	return $out;
}

# "col(+) = constant" followed by a comment: the comment must not be glued to the constant
my $out = convert("SELECT u.a FROM usr u, addr ad, terr tr WHERE ad.id(+) = u.id\n  AND ad.kind(+) = 1   -- legal\n  AND tr.code(+) = ad.code");
like($out, qr/ON \(u\.id = ad\.id AND 1 = ad\.kind\)/, 'constant condition is moved into ON');
unlike($out, qr/AND\s+=/, 'no empty operand');
unlike($out, qr/\bWHERE\b/, 'no WHERE left when every condition is a join');
like($out, qr/-- legal/, 'comment is kept');

# without the comment, as before
$out = convert("SELECT u.a FROM usr u, addr ad WHERE ad.id(+) = u.id AND ad.kind(+) = 1");
like($out, qr/ON \(u\.id = ad\.id AND 1 = ad\.kind\)/, 'no comment: same result');

done_testing;
