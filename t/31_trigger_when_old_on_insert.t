use utf8;
use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);

use Ora2Pg;

is(Ora2Pg::_pg_trigger_event_includes_insert('UPDATE OR INSERT'), 1,
	'INSERT OR UPDATE event');
is(Ora2Pg::_pg_trigger_event_includes_insert('UPDATE'), 0,
	'UPDATE-only event');
is(Ora2Pg::_pg_trigger_when_references_old('new.key_id!=old.key_id'), 1,
	'WHEN compares NEW and OLD');
is(Ora2Pg::_pg_trigger_when_references_old('new.key_id IS NOT NULL'), 0,
	'WHEN without OLD');

{
	my ($fh, $file) = tempfile(SUFFIX => '.sql', UNLINK => 1);
	print $fh <<'SQL';
CREATE OR REPLACE TRIGGER sync_trg
  AFTER UPDATE OR INSERT ON app.row_dim
  FOR EACH ROW
WHEN (NEW.key_id != OLD.key_id)
BEGIN
  NULL;
END sync_trg;
/
SQL
	close $fh;

	my $o = bless { input_file => $file }, 'Ora2Pg';
	$o->read_trigger_from_file();
	ok($o->{triggers} && @{ $o->{triggers} }, 'trigger DDL parsed from file');
	my $trig = $o->{triggers}[0];
	is($trig->[0], 'sync_trg', 'trigger name');
	like($trig->[2], qr/INSERT/i, 'triggering event includes INSERT');
	ok(
		Ora2Pg::_pg_trigger_event_includes_insert($trig->[2])
			&& Ora2Pg::_pg_trigger_when_references_old($trig->[5]),
		'WHEN on INSERT OR UPDATE referencing OLD must be omitted in PG export'
	);
}

done_testing();