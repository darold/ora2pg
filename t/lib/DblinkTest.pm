package DblinkTest;
use strict;
use warnings;
use Ora2Pg;
use Exporter 'import';
our @EXPORT_OK = qw(convert norm slurp lines);

sub slurp { my $f = shift; open(my $h, '<:encoding(UTF-8)', $f) or die "$f: $!"; local $/; return <$h>; }

# lines($file, $from, $to) of a fixture
sub lines { my ($f, $a, $b) = @_; my @l = split /\n/, slurp($f); return join("\n", @l[$a - 1 .. $b - 1]) . "\n"; }

# Minimal object: no Oracle connection; $opts override/extend the fields.
sub convert
{
	my ($code, %opts) = @_;
	my $o = bless { type => 'FUNCTION', use_orafce => 0, plsql_pgsql => 1, text_values => {},
			comment_values => {}, text_values_pos => 0, function_metadata => {}, dblink_mode => 'fdw', dblink_server => {},
			dblink_exclude => {}, data_type => { %Ora2Pg::Oracle::SQL_TYPE },
			pg_integer_type => 1, pg_numeric_type => 1, default_numeric => 'bigint', dblink_schema_prefix => 'fdw_', %opts }, 'Ora2Pg';
	$o->_remove_text_constant_part(\$code);
	$o->_remove_comments(\$code);
	my $out = Ora2Pg::PLSQL::convert_plsql_code($o, $code);
	$o->_restore_text_constant_part(\$out);
	$o->_restore_comments(\$out);
	return ($out, $o);
}

sub norm { my $s = shift; $s =~ s/\s+/ /g; $s =~ s/^ | $//g; return $s; }

1;
