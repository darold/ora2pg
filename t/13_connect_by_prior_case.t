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
	my $o = bless { type => 'VIEW', use_orafce => 1, plsql_pgsql => 1, text_values => {},
		comment_values => {}, text_values_pos => 0, function_metadata => {}, data_type => { %Ora2Pg::Oracle::SQL_TYPE },
		pg_integer_type => 1, pg_numeric_type => 1, default_numeric => 'bigint', %opts }, 'Ora2Pg';
	$o->_remove_text_constant_part(\$code);
	$o->_remove_comments(\$code);
	my $out = Ora2Pg::PLSQL::convert_plsql_code($o, $code);
	$o->_restore_text_constant_part(\$out);
	$o->_restore_comments(\$out);
	return $out;
}

# Oracle keywords are case-insensitive: "prior" must be handled like "PRIOR".
for my $kw ('PRIOR', 'prior', 'Prior') {
	my $out = convert("SELECT n.id, n.name FROM app.dim_tree n START WITH n.root_id = 0 CONNECT BY $kw n.id = n.parent_id");
	like($out, qr/JOIN cte c ON \(c\.id = n\.parent_id\)/, "CONNECT BY $kw: join on c.id");
	unlike($out, qr/c\.prior/i, "CONNECT BY $kw: no prior keyword left in the join");
}

done_testing();
