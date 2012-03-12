use ExtUtils::MakeMaker qw(prompt WriteMakefile);

my @ALLOWED_ARGS = ('DESTDIR','INSTALLDIRS');

# Parse command line arguments and store them as environment variables
while ($_ = shift) {
	my ($k,$v) = split(/=/, $_, 2);
	if (grep(/^$k$/, @ALLOWED_ARGS)) {
		$ENV{$k} = $v;
	}
}
$ENV{DESTDIR} =~ s/\/$//;

# Default install path
my $DESTDIR = $ENV{DESTDIR} || '';
$ENV{INSTALLDIRS} ||= 'vendor';


WriteMakefile(
    'NAME'         => 'Ora2Pg::PLSQL',
    'VERSION_FROM' => 'PLSQL.pm',
    'LICENSE'      => 'GPLv3',
    'AUTHOR'       => 'Gilles Darold (gilles _AT_ darold _DOT_ net)',
    'DESTDIR'      => $ENV{DESTDIR},
    'INSTALLDIRS'  => $ENV{INSTALLDIRS},
);

