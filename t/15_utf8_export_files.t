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

# Files opened by open_export_file() and append_export_file() must be UTF-8
# encoded, as the .gz and .bz2 branches of open_export_file() already are.
# Without it, writing a non-ASCII object name warns "Wide character in print".
my $dir  = tempdir(CLEANUP => 1);
my $text = "пакет_тест:пакет_тест_найти\n";
my $o    = bless { binmode => '', nls_lang => '' }, 'Ora2Pg';

for my $method (qw(open_export_file append_export_file))
{
	my $file = "$dir/$method.dat";
	my @warn;
	local $SIG{__WARN__} = sub { push @warn, @_ };

	my $fh = $o->$method($file, 1);
	$fh->print($text);
	$fh->close;

	is(scalar(grep { /Wide character/ } @warn), 0, "$method: no Wide character warning");
	open(my $in, '<:raw', $file) or die "$file: $!";
	my $raw = do { local $/; <$in> };
	close $in;
	is(decode('UTF-8', $raw, Encode::FB_CROAK | Encode::LEAVE_SRC), $text, "$method: content is UTF-8");
}

done_testing();
