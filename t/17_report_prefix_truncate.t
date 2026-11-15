use strict;
use warnings;
use Test::More;
use File::Temp qw(tempdir);
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Ora2Pg;

# With DUMP_AS_FILE_PREFIX each export must overwrite <prefix>.<ext>.
# It was opened for append, so every new run stacked a complete report
# on top of the previous one.
my $dir = tempdir(CLEANUP => 1);
my $o = bless { dump_as_file_prefix => "$dir/report", binmode => '', nls_lang => '' }, 'Ora2Pg';

for my $run (1 .. 2)
{
	$o->_select_output_file_suffix('html');
	$o->{fhlog}->print("report\n");
	$o->{fhlog}->close;
}

open(my $in, '<', "$dir/report.html") or die $!;
my @lines = <$in>;
close $in;
is(scalar(@lines), 1, 'second run replaces the report instead of appending');

done_testing();
