use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use DblinkTest qw(convert norm);

binmode(Test::More->builder->$_, ':utf8') for qw(output failure_output todo_output);

# The Oracle side of a dblink call is hidden from the case folding; only the local part is lower-cased.
my ($out) = convert("BEGIN\nSELECT ЦЕНА INTO x FROM rsch.rem_prices\@rdb1 WHERE ГРУППА = 1;\nEND;", dblink_signatures => "$Bin/fixtures/dblink_signatures.txt");
like(norm($out), qr/INTO STRICT x FROM fdw_rdb1_rsch\.rem_prices WHERE группа = 1/i, 'local part lower-cased');

done_testing;
