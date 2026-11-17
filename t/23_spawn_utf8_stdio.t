use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);

# A forked export child writes non-ASCII object names to STDERR/STDOUT: the
# child must have a UTF-8 layer on both, or Perl warns "Wide character".
my $code = q{use utf8; use Ora2Pg; Ora2Pg::spawn(sub { print STDERR "НАЙТИ\n"; print STDOUT "ТЕСТ\n"; return 0; }); 1 while wait() > 0;};
my $out = qx{"$^X" -I"$Bin/../lib" -e '$code' 2>&1};
unlike($out, qr/Wide character/, 'no Wide character warning in the child');
like($out, qr/\xd0\x9d\xd0\x90\xd0\x99\xd0\xa2\xd0\x98/, 'STDERR is UTF-8 encoded');

done_testing;
