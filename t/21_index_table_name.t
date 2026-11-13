use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

# An index named like a table cannot be created in PostgreSQL (one namespace for relations).
my $o = bless { indexes_renaming => 0, indexes_suffix => '', tables => { 'APP.CURRENCY' => {}, 'APP.ORDERS' => {} },
	preserve_case => 0, type => 'TABLE' }, 'Ora2Pg';
is($o->get_indexname('APP.ORDERS', 'APP.CURRENCY', 'CURRENCY'), 'currency_idx', 'index named like a table gets a suffix');
is($o->get_indexname('APP.ORDERS', 'APP.CURRENCY_FACT', 'X'), 'currency_fact', 'other index names are unchanged');

done_testing;
