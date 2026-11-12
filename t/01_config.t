use strict;
use warnings;
use utf8;
use Test::More;

use Ora2Pg;

# _init cannot run without Oracle, so copy %AConfig into the object the same way
# _init does, then run the DBLINK_* normalization it calls.
sub from_config
{
	my @lines = @_;
	%Ora2Pg::AConfig = ();
	my $parser = bless { debug => 0 }, 'Ora2Pg';
	$parser->parse_config($_) for @lines;
	my $self = bless { debug => 0 }, 'Ora2Pg';
	$self->{lc($_)} = $Ora2Pg::AConfig{$_} for keys %Ora2Pg::AConfig;
	$self->_dblink_settings();
	return $self;
}

my $o = from_config('DBLINK_SERVER SRCDB:rdb1 dbb_bak:dbsrv2');
is_deeply($o->{dblink_server}, { SRCDB => 'rdb1', DBB_BAK => 'dbsrv2' }, 'DBLINK_SERVER keys are upper case, server names kept');

$o = from_config('DBLINK_EXCLUDE ext1 EXT2');
is_deeply($o->{dblink_exclude}, { EXT1 => 1, EXT2 => 1 }, 'DBLINK_EXCLUDE keys are upper case');

$o = from_config('DBLINK_MODE FDW');
is($o->{dblink_mode}, 'fdw', 'DBLINK_MODE is lower cased');

$o = from_config();
is($o->{dblink_mode}, 'off', 'DBLINK_MODE defaults to off');
is($o->{dblink_schema_prefix}, 'fdw_', 'DBLINK_SCHEMA_PREFIX defaults to fdw_');
is_deeply($o->{dblink_server}, {}, 'DBLINK_SERVER defaults to empty hash');
is_deeply($o->{dblink_exclude}, {}, 'DBLINK_EXCLUDE defaults to empty hash');
is($o->{dblink_signatures}, '', 'DBLINK_SIGNATURES defaults to empty string');

$o = from_config('DBLINK_SCHEMA_PREFIX x_', 'DBLINK_SIGNATURES sig.txt');
is($o->{dblink_schema_prefix}, 'x_', 'DBLINK_SCHEMA_PREFIX is taken from config');
is($o->{dblink_signatures}, 'sig.txt', 'DBLINK_SIGNATURES is taken from config');

# An unknown DBLINK_MODE is fatal: logit() prints the message and dies
my $log = '';
{
	open(my $fh, '>', \$log) or die $!;
	local *STDOUT = $fh;
	eval { from_config('DBLINK_MODE bogus') };
}
like($@, qr/Aborting export/, 'unknown DBLINK_MODE aborts');
like($log, qr/FATAL: DBLINK_MODE must be off or fdw, not 'bogus'/, 'unknown DBLINK_MODE reports FATAL');

done_testing();
