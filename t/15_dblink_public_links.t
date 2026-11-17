use strict;
use warnings;
use Test::More;
use Ora2Pg;

# Schema export must include PUBLIC database links (MARX, PROD, …)
package DblinkStub::Sth {
	sub execute { 1 }
	sub fetch {
		my $self = shift;
		return undef if $self->{done}++;
		return @{ $self->{rows}[$self->{i}++] };
	}
	sub errstr { '' }
}
package DblinkStub::Dbh {
	sub new { bless { n_prepare => 0, sql => [] }, shift }
	sub prepare {
		my ($self, $sql) = @_;
		$self->{n_prepare}++;
		push @{ $self->{sql} }, $sql;
		my $rows = [];
		if ($sql =~ /OWNER IN \('PAYMENT', 'PUBLIC'\)/) {
			$rows = [ [ 'PUBLIC', 'MARX', 'ESS', 'marx' ], [ 'PAYMENT', 'HP200', 'ABC', 'hp200_bak' ] ];
		}
		elsif ($sql =~ /OWNER = 'PAYMENT'/) {
			$rows = [ [ 'PAYMENT', 'HP200', 'ABC', 'hp200_bak' ] ];
		}
		return bless { i => 0, done => 0, rows => $rows }, 'DblinkStub::Sth';
	}
	sub errstr { '' }
}

my $dbh = DblinkStub::Dbh->new;
my $o = bless {
	dbh => $dbh, schema => 'PAYMENT', prefix => 'DBA', export_schema => 0,
	query_bind_params => [], sysusers => ['SYS'],
}, 'Ora2Pg';

my %d = $o->_get_dblink();
ok(exists $d{MARX}, 'PUBLIC link MARX in dictionary when SCHEMA PAYMENT');
is($d{MARX}{owner}, 'PUBLIC', 'MARX owner');
is($d{MARX}{user}, 'ESS', 'MARX login');
like($dbh->{sql}[0], qr/OWNER IN \('PAYMENT', 'PUBLIC'\)/, 'SQL includes PUBLIC');

done_testing;