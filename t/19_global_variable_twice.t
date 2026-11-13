use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

# A package variable registered twice must keep one default value, not "RUR\'RUR\".
my $o = bless { type => 'PACKAGE', use_orafce => 0, plsql_pgsql => 1, text_values => {}, text_values_pos => 0, comment_values => {},
	function_metadata => {}, global_variables => {}, data_type => { %Ora2Pg::Oracle::SQL_TYPE } }, 'Ora2Pg';
for my $call (1, 2)
{
	my $decl = "VAL_ CONSTANT VARCHAR2(3) := 'RUR';";
	$o->_remove_text_constant_part(\$decl);
	$o->register_global_variable('pk', $decl);
	is($o->{global_variables}{'pk.val_'}{default}, 'RUR', "default after registration $call");
}

done_testing;
