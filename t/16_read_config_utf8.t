use strict;
use warnings;
use utf8;
use Test::More;
use File::Temp qw(tempfile);

use Ora2Pg;

# read_config must decode the file as UTF-8, otherwise non-ASCII keys of
# MODIFY_TYPE never match the (decoded) names from the Oracle dictionary.
my ($fh, $file) = tempfile(UNLINK => 1);
binmode($fh, ':encoding(UTF-8)');
print $fh "MODIFY_TYPE заявка:ндс10:numeric,t1:col:text\n";
close $fh;

%Ora2Pg::AConfig = ();
my $o = bless { debug => 0 }, 'Ora2Pg';
$o->read_config($file);

is_deeply($Ora2Pg::AConfig{MODIFY_TYPE}, { 'заявка' => { 'ндс10' => 'numeric' }, t1 => { col => 'text' } },
	'non-ASCII MODIFY_TYPE keys are decoded characters');

done_testing;
