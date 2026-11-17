use strict;
use warnings;
use Test::More;
use Ora2Pg::Oracle;

# DEFAULT_NUMERIC numeric is for PL/SQL; table NUMBER without precision stays bigint
my $self = bless {
	pg_integer_type => 1,
	default_numeric => 'numeric',
	type => 'TABLE',
	data_type => { %{Ora2Pg::Oracle::SQL_TYPE} },
}, 'Ora2Pg';

is(
	Ora2Pg::Oracle::_sql_type($self, 'NUMBER', 22, '', 0),
	'bigint',
	'unconstrained NUMBER column is bigint even with DEFAULT_NUMERIC numeric'
);
is(
	Ora2Pg::Oracle::_sql_type($self, 'NUMBER', 22, 11, 0),
	'bigint',
	'NUMBER(11) column is bigint'
);

done_testing;