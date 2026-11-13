use utf8;
use strict;
use warnings;
use Test::More;
use File::Temp qw(tempdir);
use Encode qw(decode);
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

binmode(Test::More->builder->$_, ':utf8') for qw(output failure_output todo_output);

my $dir = tempdir(CLEANUP => 1);

# 1. LOGFILE: the log handle needs a UTF-8 layer, otherwise logging a
# non-ASCII object name warns "Wide character in print".
open(my $sql, '>:utf8', "$dir/in.sql") or die $!;
print $sql "CREATE OR REPLACE FUNCTION НАЙТИ_ТЕСТ (p NUMBER) RETURN NUMBER IS\nBEGIN\n  RETURN p + 1;\nEND;\n/\n";
close $sql;
# Count the "Wide character" warnings of one export, with and without LOGFILE: the
# log handle must not add any (other handles are covered by their own tests).
sub wide_warnings
{
	my ($logfile) = @_;
	open(my $cfg, '>', "$dir/ora2pg.conf") or die $!;
	print $cfg "ORACLE_DSN dbi:Oracle:host=x;sid=y\nSCHEMA DEMO\n";
	print $cfg "LOGFILE $logfile\n" if $logfile;
	close $cfg;
	my $err = qx{cd "$Bin/.." && "$^X" -Ilib scripts/ora2pg -c "$dir/ora2pg.conf" -t FUNCTION -i "$dir/in.sql" -d -o out.sql -b "$dir" 2>&1 >/dev/null};
	return scalar(() = $err =~ /Wide character/g);
}

my $without = wide_warnings('');
is(wide_warnings("$dir/ora2pg.log"), $without, 'LOGFILE: no extra Wide character warning');
open(my $in, '<:raw', "$dir/ora2pg.log") or die "log: $!";
my $log = do { local $/; <$in> };
close $in;
like(decode('UTF-8', $log), qr/НАЙТИ_ТЕСТ/, 'LOGFILE: object name logged as UTF-8');

# 2. Report file named by DUMP_AS_FILE_PREFIX goes through the same fhlog handle.
my @warn;
local $SIG{__WARN__} = sub { push @warn, @_ };
my $o = bless { dump_as_file_prefix => "$dir/report", binmode => '', nls_lang => '' }, 'Ora2Pg';
$o->_select_output_file_suffix('html');
$o->{fhlog}->print("пакет_тест\n");
$o->{fhlog}->close;
is(scalar(grep { /Wide character/ } @warn), 0, 'report file: no Wide character warning');

done_testing();
