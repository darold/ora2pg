package Ora2Pg::DbLink;

# Pure helpers to locate Oracle database-link references (ident.ident@link)
# in PL/SQL text already masked by ora2pg (?TEXTVALUEn?, %ORA2PG_COMMENTn%).

use utf8;
use strict;
use warnings;

use Exporter 'import';
use Scalar::Util ();
our @EXPORT_OK = qw(scan_refs balanced_args bind_text meta rewrite oracle_text server_for server_name_from_def foreign_tables_sql write_tables_file _dbl_lit view_ddl cron_expr job_sql report_item);

my $ID   = qr/(?:[\w\$#]+|"[^"]*")/;
my $PATH = qr/$ID(?:\.$ID)*/;
my $LINK = qr/[\w\$#]+(?:\.[\w\$#]+)*(?:\@[\w\$#]+)?/;
my $MASK = qr/\?TEXTVALUE\d+\?|%ORA2PG_COMMENT\d+%/;
my %CLAUSE = map { $_ => 1 } qw(FROM WHERE SELECT SET GROUP ORDER HAVING VALUES ON JOIN);

sub scan_refs
{
	my ($str) = @_;
	my (@refs, @stack, $prev);
	$stack[0] = '';
	pos($str) = 0;
	while (pos($str) < length($str)) {
		if ($str =~ /\G($MASK)/gc) {
			$prev = 'TEXT' unless $1 =~ /^%/;  # a masked comment is transparent
		} elsif ($str =~ /\G($PATH)\@($LINK)/gc) {
			my ($path, $link) = ($1, $2);
			my $end = pos($str);
			my $start = $end - length($path) - length($link) - 1;
			$link =~ s/"//g;
			my %r = (start => $start, end => $end, path => $path, link => uc($link));
			if ($path =~ s/\.(NEXTVAL|CURRVAL)$//i) {
				@r{qw(kind seq_op path)} = ('seq', uc($1), $path);
			} elsif ($str =~ /\G%(TYPE|ROWTYPE)\b/gci) {
				$r{kind} = lc($1);
				$r{end} = pos($str);
			} elsif (defined $prev && ($prev =~ /^(?:FROM|JOIN|INTO|UPDATE|TABLE)$/
				|| ($prev eq ',' && $stack[-1] =~ /^(?:FROM|JOIN)$/))) {
				$r{kind} = 'table';
			} else {
				pos($str) = $end;
				$str =~ /\G(?:\s+|$MASK)*/gc;
				my $q = pos($str);
				if (substr($str, $q, 1) eq '(') {
					$r{kind} = 'call';
					$r{args_start} = $q;
					(undef, $r{args_end}) = balanced_args($str, $q);
				} else {
					$r{kind} = 'expr';
				}
			}
			push @refs, \%r;
			pos($str) = $r{end};
			$prev = 'REF';
		} elsif ($str =~ /\G($ID)/gc) {
			$prev = uc($1);
			$stack[-1] = $prev if $CLAUSE{$prev};
		} elsif ($str =~ /\G\(/gc) {
			push @stack, '';
			$prev = '(';
		} elsif ($str =~ /\G\)/gc) {
			pop @stack if @stack > 1;
			$prev = ')';
		} elsif ($str =~ /\G;/gc) {
			$stack[-1] = '';
			$prev = ';';
		} elsif ($str =~ /\G(?:\s+|%ORA2PG_COMMENT\d+%)/gc) {
			# whitespace and masked comments do not change $prev
		} else {
			$str =~ /\G(.)/gcs;
			$prev = $1;
		}
	}
	return @refs;
}

# $pos points at '('; returns (\@args, offset after the matching ')')
sub balanced_args
{
	my ($str, $pos) = @_;
	my (@parts, $depth, $quote, $from);
	$depth = 0;
	for (my $i = $pos; $i < length($str); $i++) {
		my $c = substr($str, $i, 1);
		if ($quote) { $quote = 0 if $c eq $quote; next; }
		if ($c eq "'" || $c eq '"') { $quote = $c; }
		elsif ($c eq '(') { $from = $i + 1 if ++$depth == 1; }
		elsif ($c eq ')' || ($c eq ',' && $depth == 1)) {
			$depth-- if $c eq ')';
			if ($c eq ',' || $depth == 0) {
				push @parts, substr($str, $from, $i - $from);
				$from = $i + 1;
			}
			if ($depth == 0) {
				@parts = () if @parts == 1 && $parts[0] =~ /^\s*$/;
				my @args;
				for my $p (@parts) {
					$p =~ s/^\s+|\s+$//g;
					if ($p =~ /^($ID)\s*=>\s*(.*)$/s) {
						my ($n, $e) = ($1, $2);
						$n =~ s/"//g;
						push @args, { name => $n, expr => $e };
					} else {
						push @args, { expr => $p };
					}
				}
				return (\@args, $i + 1);
			}
		}
	}
	return ([], undef);
}

sub bind_text
{
	my ($expr, $ora_type) = @_;
	return "to_char(($expr), 'YYYY-MM-DD HH24:MI:SS')" if defined $ora_type && uc($ora_type) eq 'DATE';
	return "($expr)::text";
}

# ---- remote metadata: signatures file first, then the live dictionary ----

sub _warn
{
	my ($class, $msg) = @_;
	if (Scalar::Util::blessed($class) && $class->can('logit')) { $class->logit("WARNING: $msg\n", 0); }
	else { warn "WARNING: $msg\n"; }
}

# split on top-level commas (NUMBER(12,2) stays whole)
sub _split_top
{
	my ($s) = @_;
	my @o; my ($d, $from) = (0, 0);
	for my $i (0 .. length($s) - 1) {
		my $c = substr($s, $i, 1);
		if ($c eq '(') { $d++ } elsif ($c eq ')') { $d-- }
		elsif ($c eq ',' && !$d) { push @o, substr($s, $from, $i - $from); $from = $i + 1; }
	}
	push @o, substr($s, $from);
	return grep { /\S/ } map { s/^\s+|\s+$//gr } @o;
}

# 'NUMBER(12,2)' -> { type len precision scale }; undef when malformed
sub _parse_type
{
	my ($t) = @_;
	$t =~ /^\s*([A-Za-z][\w\$#]*(?:\s+[A-Za-z]+)?)\s*(?:\(\s*(\d+)\s*(?:,\s*(-?\d+)\s*)?\))?\s*$/ or return undef;
	my ($n, $a, $b) = (uc $1, $2, $3);
	my %r = (type => $n, len => undef, precision => undef, scale => undef);
	if ($n eq 'NUMBER') { $r{len} = 22; @r{qw(precision scale)} = (defined $a ? ($a, $b // 0) : (undef, undef)); }
	else { $r{len} = $a; }
	return \%r;
}

sub _parse_spec
{
	my ($kind, $spec) = @_;
	if ($kind eq 'COLUMN') { return _parse_type($spec); }
	if ($kind eq 'TABLE') {
		my ($cols, $pk) = split /;/, $spec, 2;
		my @c;
		for my $d (_split_top($cols // '')) {
			$d =~ s/\s+NOT\s+NULL\s*$//i and my $nn = 1;
			$d =~ /^("?[\w\$#]+"?)\s+(.+)$/ or return undef;
			my ($n, $t) = ($1, _parse_type($2));
			$t or return undef;
			$n =~ s/"//g;
			push @c, { name => uc $n, %$t, nullable => $nn ? 'N' : 'Y' };
		}
		@c or return undef;
		my @pk;
		if (defined $pk) {
			$pk =~ s/^\s*PK\s+//i or return undef;
			@pk = map { uc } _split_top($pk);
		}
		return { columns => \@c, pk => \@pk };
	}
	# FUNCTION / PROCEDURE
	my ($ret, $args) = ($spec, undef);
	my %r = (return => undef, args => []);
	if ($kind eq 'FUNCTION') {
		($ret, $args) = split /;/, $spec, 2;
		$ret =~ s/^\s*RETURN\s+//i or return undef;
		$ret =~ s/\s+$//;
		$r{return} = uc $ret;
	} else { $args = $spec; }
	for my $a (_split_top($args // '')) {
		$a =~ /^([\w\$#]+)\s+(IN\s+OUT|IN|OUT)\s+(.+)$/i or return undef;
		my ($n, $m, $t) = ($1, uc $2, $3);
		$m =~ s/\s+/\//;
		my $def = $t =~ s/\s+DEFAULT\s*$//i;
		$t =~ s/\s*\(.*\)\s*$//;
		push @{$r{args}}, { name => $n, mode => $m, type => uc $t, ($def ? (default => 1) : ()) };
	}
	return \%r;
}

sub _load_signatures
{
	my ($class) = @_;
	my $h = $class->{dblink_sigfile_cache} = {};
	my $f = $class->{dblink_signatures};
	return $h unless defined $f && length $f;
	my $fh;
	unless (open($fh, '<:encoding(UTF-8)', $f)) { _warn($class, "DBLINK_SIGNATURES: can not read $f: $!"); return $h; }
	while (my $l = <$fh>) {
		next if $l =~ /^\s*(?:#|$)/;
		$l =~ s/\s+$//;
		my ($link, $kind, $path, $spec, $flags) = split /\|/, $l, 5;
		$kind = uc($kind // '');
		my $r = (defined $spec && $kind =~ /^(?:COLUMN|TABLE|FUNCTION|PROCEDURE)$/ && length $path)
			? _parse_spec($kind, $spec) : undef;
		unless ($r) { _warn($class, "DBLINK_SIGNATURES $f line $.: bad format, skipped"); next; }
		my @bad = grep { $_ ne 'MODIFIES' } map { s/^\s+|\s+$//gr } map { uc } split /,/, $flags // '';
		if (@bad) { _warn($class, "DBLINK_SIGNATURES $f line $.: unknown flag '$bad[0]', skipped"); next; }
		$r->{modifies} = 1 if ($flags // '') =~ /\S/;
		if ($kind eq 'TABLE') {
			my @p = split /\./, $path;
			@$r{qw(owner name)} = (@p > 1 ? uc $p[-2] : undef, uc $p[-1]);
		} elsif ($kind ne 'COLUMN') { $r->{type} = $kind; $kind = 'ROUTINE'; }
		$h->{uc($link) . "|$kind|" . lc $path} = $r;
	}
	close $fh;
	return $h;
}

sub _sql_one_line
{
	my ($sql) = @_;
	$sql =~ s/\s+/ /g;
	return length($sql) > 200 ? substr($sql, 0, 197) . '...' : $sql;
}

# Collect everything DBD::Oracle / DBI expose on a handle (sth before dbh).
sub _h_err_parts
{
	my ($h) = @_;
	return () unless $h && ref $h && $h->can('errstr');
	my @p;
	my $s = $h->errstr;
	push @p, $s if defined $s && length $s;
	if ($h->can('err')) {
		my $c = $h->err;
		push @p, "DBI err=$c" if defined $c && $c ne '';
	}
	if ($h->can('state')) {
		my $st = $h->state;
		push @p, "SQLSTATE=$st" if defined $st && length $st;
	}
	for my $k (qw(ora_errno ora_errstr)) {
		my $v = eval { $h->{$k} };
		push @p, "$k=$v" if defined $v && length "$v";
	}
	return @p;
}

sub _db_err
{
	my ($dbh, $sth, $sql, $stage) = @_;
	$stage //= 'query';
	my %seen;
	my @parts;
	for my $h ($sth, $dbh) {
		for my $p (_h_err_parts($h)) {
			my $k = lc $p;
			next if $seen{$k}++;
			push @parts, $p;
		}
	}
	if (defined $DBI::errstr && length $DBI::errstr && !$seen{lc $DBI::errstr}) {
		push @parts, $DBI::errstr;
	}
	if (defined $DBI::err && $DBI::err ne '' && !$seen{"dbi err=$DBI::err"}) {
		push @parts, "DBI err=$DBI::err";
	}
	if ($dbh && ref $dbh && $dbh->can('func')) {
		for my $name (qw(ora_error OraError)) {
			my $oci = eval { $dbh->func(undef, $name) };
			next if $@ || !defined $oci;
			my $t = ref($oci) eq 'ARRAY' ? join(' ', grep { defined && length } @$oci)
				: (ref($oci) ? '' : "$oci");
			next unless length $t;
			next if $seen{lc $t}++;
			push @parts, $t;
			last;
		}
	}
	my $e = @parts ? join('; ', @parts) : 'unknown error';
	if ($e !~ /ORA-\d{5}/i) {
		my $act = eval { $dbh->{Active} } if $dbh;
		$act //= eval { defined $dbh->ping ? ($dbh->ping ? 'ping=1' : 'ping=0') : undef } if $dbh && $dbh->can('ping');
		$e .= "; dbh_active=$act" if defined $act;
	}
	return "$e [$stage; " . _sql_one_line($sql) . ']';
}

# Dictionary binds are owner/package/name identifiers; inline them in remote SQL so DBD::Oracle
# does not ship bind metadata across a database link (sqlplus-style literals, same as the probe).
sub _sql_inline_dict_binds
{
	my ($sql, @b) = @_;
	return ($sql, @b) unless $sql =~ /@/ && @b;
	return ($sql, @b) if grep { !defined $_ || $_ !~ /^[\w\$#-]+$/i } @b;
	for my $v (@b) {
		my $lit = $v =~ /^-/i ? "'-'" : "'" . uc($v =~ s/'/''/gr) . "'";
		$sql =~ s/\?/$lit/ or die "bind placeholder mismatch\n";
	}
	return ($sql);
}

sub _reconnect_oracle
{
	my ($class) = @_;
	return unless $class->can('_db_connection');
	eval { $class->{dbh}->disconnect() if $class->{dbh} };
	my $dbh = eval { $class->_db_connection() };
	if ($@ || !$dbh) {
		my $why = $@ ? $@ : 'no handle';
		$why =~ s/\s+$//;
		_warn($class, "Oracle reconnect failed: $why");
	}
	$class->{dbh} = $dbh;
}

sub _q
{
	my ($class, $sql, @b) = @_;
	my $dbh = $class->{dbh} or die "no database connection\n";
	($sql, @b) = _sql_inline_dict_binds($sql, @b);
	my $remote = $sql =~ /@/;

	my $run = sub {
		my $dbh = $class->{dbh} or die "no database connection\n";
		local $dbh->{RaiseError} = 0;
		local $dbh->{PrintError} = 0;
		my $sth = $dbh->prepare($sql);
		die _db_err($dbh, $sth, $sql, 'prepare') . "\n" if !$sth;
		die _db_err($dbh, $sth, $sql, 'execute') . "\n" unless $sth->execute(@b);
		return $sth->fetchall_arrayref();
	};

	my @attempt_err;
	for my $try (1 .. 2) {
		my $rows = eval { $run->() };
		return $rows unless $@;
		my $err = $@;
		$err =~ s/\s+$//;
		push @attempt_err, "try $try: $err";
		# ora2pg may have closed its connection before the conversion phase: local dictionary queries need the reconnect too
		if ($try == 1 && $class->can('_db_connection')) {
			_reconnect_oracle($class);
			next;
		}
		_reconnect_oracle($class) if $remote && $class->can('_db_connection');
		die join(' | ', @attempt_err) . "\n";
	}
	die join(' | ', @attempt_err) . "\n" if @attempt_err;
	die "dictionary query failed\n";
}

sub _ident { my $s = shift; return $s =~ /^"(.*)"$/ ? $1 : uc $s; }

sub _table_cols
{
	my ($class, $L, $owner, $name) = @_;
	my $rows = _q($class, "SELECT column_name, data_type, data_length, data_precision, data_scale, nullable FROM all_tab_columns\@$L WHERE owner = ? AND table_name = ? ORDER BY column_id", $owner, $name);
	return [ map { { name => $_->[0], type => $_->[1], len => $_->[2], precision => $_->[3], scale => $_->[4], nullable => $_->[5] } } @$rows ];
}

# Default owner for path without schema prefix: CONNECT TO user from the link definition (not USER@link).
sub _default_owner
{
	my ($class, $L) = @_;
	return $class->{dblink_meta}{$L}{'_DEFAULT_OWNER'}
		if exists $class->{dblink_meta}{$L}{'_DEFAULT_OWNER'};
	my $d = _link_def($class, $L);
	my $u = ($d && defined $d->{user} && $d->{user} ne '') ? uc($d->{user}) : undef;
	$class->{dblink_meta}{$L}{'_DEFAULT_OWNER'} = $u if defined $u;
	return $u;
}

# segs: (name) or (owner, name); returns TABLE hashref or undef
sub _live_table
{
	my ($class, $L, @segs) = @_;
	my ($owner, $name) = @segs > 1 ? @segs : do {
		my $o = _default_owner($class, $L);
		return undef unless defined $o;
		($o, $segs[0]);
	};
	my $cols = _table_cols($class, $L, $owner, $name);
	unless (@$cols) {
		my $syn = _q($class, "SELECT table_owner, table_name, db_link, owner FROM all_synonyms\@$L WHERE owner IN (?, 'PUBLIC') AND synonym_name = ?", $owner, $name);
		my ($s) = sort { ($a->[3] eq 'PUBLIC') <=> ($b->[3] eq 'PUBLIC') } @$syn;	# own schema before PUBLIC
		return undef unless $s;
		if (defined $s->[2]) { _warn($class, "$L: synonym $owner.$name points to another database link ($s->[2])"); return undef; }
		($owner, $name) = @$s[0, 1];
		$cols = _table_cols($class, $L, $owner, $name);
	}
	return undef unless @$cols;
	my $pk = _q($class, "SELECT cc.column_name FROM all_constraints\@$L c JOIN all_cons_columns\@$L cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name WHERE c.constraint_type = 'P' AND c.owner = ? AND c.table_name = ? ORDER BY cc.position", $owner, $name);
	return { owner => $owner, name => $name, columns => $cols, pk => [ map { $_->[0] } @$pk ] };
}

sub _rows_to_routine
{
	my ($rows) = @_;
	return undef unless @$rows;
	my %r = (type => 'PROCEDURE', return => undef, args => []);
	for my $row (@$rows) {
		my ($n, $pos, $io, $dt, undef, $defd) = @$row;
		if ($pos == 0) { @r{qw(type return)} = ('FUNCTION', $dt); next; }
		next unless defined $dt;	# procedure without arguments
		push @{$r{args}}, { name => $n, mode => $io, type => $dt, (($defd // '') eq 'Y' ? (default => 1) : ()) };
	}
	return \%r;
}

sub _routine_arg_ok
{
	my ($sig, $argc) = @_;
	return 1 unless defined $argc;
	my @in = grep { ($_->{mode} // 'IN') =~ /^IN/ } @{$sig->{args} // []};
	my $req = scalar grep { !$_->{default} } @in;
	return 0 if $argc < $req || $argc > @in;
	return 1;
}

# Picks one overload group from ALL_ARGUMENTS when several share object_name.
sub _pick_overload
{
	my ($class, $L, $path_lbl, $by_ov, $hint) = @_;
	my @ovs = sort { $a <=> $b } keys %$by_ov;
	return $ovs[0] if @ovs <= 1;
	$hint ||= {};
	my $argc      = $hint->{argc};
	my $want_fn   = $hint->{want_function};
	my @cand;
	for my $ov (@ovs) {
		my $sig = _rows_to_routine($by_ov->{$ov});
		next unless $sig;
		next if defined $want_fn && $want_fn && $sig->{type} ne 'FUNCTION';
		next if defined $want_fn && !$want_fn && $sig->{type} eq 'FUNCTION';
		next unless _routine_arg_ok($sig, $argc);
		push @cand, $ov;
	}
	return $cand[0] if @cand == 1;
	if (@cand > 1) {
		_warn($class, "$L: overloaded routine $path_lbl is not supported");
	} elsif (!@cand) {
		my $wf = defined $want_fn ? ($want_fn ? 'function' : 'procedure') : 'any';
		my $ac = defined $argc ? $argc : '?';
		_warn($class, "$L: overloaded routine $path_lbl: no overload matches hint (argc=$ac, want=$wf)");
	}
	return undef;
}

sub _live_routine
{
	my ($class, $L, $hint, @segs) = @_;
	my @try;
	if (@segs == 3) {
		@try = ([ @segs ]);
	} elsif (@segs == 2) {
		@try = ([ $segs[0], '-', $segs[1] ]);
	} elsif (@segs == 1) {
		my $o = _default_owner($class, $L);
		return undef unless defined $o;
		@try = ([ $o, '-', $segs[0] ]);
	} else {
		return undef;
	}
	for my $t (@try) {
		my $rows = _q($class, "SELECT argument_name, position, in_out, data_type, overload, defaulted FROM all_arguments\@$L WHERE owner = ? AND NVL(package_name, '-') = ? AND object_name = ? AND data_level = 0 ORDER BY overload, position", @$t);
		next unless @$rows;
		my %by_ov;
		for my $row (@$rows) {
			my $ov = defined $row->[4] ? $row->[4] : 0;
			push @{$by_ov{$ov}}, $row;
		}
		my $path_lbl = join('.', @$t);
		my $ov = _pick_overload($class, $L, $path_lbl, \%by_ov, $hint);
		next unless defined $ov;
		return _rows_to_routine($by_ov{$ov});
	}
	return undef;
}

sub _live
{
	my ($class, $L, $kind, $path, $hint) = @_;
	my @s = map { _ident($_) } $path =~ /("[^"]*"|[^.]+)/g;
	return undef unless @s;
	if ($kind eq 'ROUTINE') { return @s > 3 ? undef : _live_routine($class, $L, $hint, @s); }
	my $col = $kind eq 'COLUMN' ? pop @s : undef;
	return undef if !@s || @s > 2;
	my $t = _live_table($class, $L, @s) or return undef;
	return $t unless defined $col;
	my ($c) = grep { $_->{name} eq $col } @{$t->{columns}};
	return $c ? { map { $_ => $c->{$_} } qw(type len precision scale) } : undef;
}

sub meta
{
	my ($class, $link, $kind, $path, $hint) = @_;
	$kind = uc $kind;
	my $c = $class->{dblink_meta}{$link}{$kind} ||= {};
	my $hk = ($kind eq 'ROUTINE' && ref($hint) eq 'HASH')
		? '|' . join(',', map { $_ // '' } @$hint{qw(argc want_function)}) : '';
	my $k = lc($path) . $hk;
	return $c->{$k} if exists $c->{$k};
	my $sig = $class->{dblink_sigfile_cache} || _load_signatures($class);
	# links mapped to one server (DBLINK_SERVER) share their signatures
	my $r = $sig->{"$link|$kind|" . lc($path)} // $sig->{uc(server_for($class, $link)) . "|$kind|" . lc($path)};
	if (!$r && $class->{dbh} && !$class->{input_file}) {
		if ($link =~ /^[A-Z0-9_\$#]+(\.[A-Z0-9_\$#]+)*(\@[A-Z0-9_\$#]+)?$/) {
			$r = eval { _live($class, $link, $kind, $path, $hint) };
			_warn($class, "$link $kind $path: dictionary query failed: " . ($@ =~ s/\s+$//r)) if $@;
		} else { _warn($class, "invalid database link name '$link'"); }
	}
	return $c->{$k} = $r;
}

sub _routine_hint
{
	my ($class, $r, $args, $assign) = @_;
	my $want_fn = $assign ? 1 : ($r->{kind} eq 'call' || $r->{kind} eq 'expr' ? 1 : 0);
	return {
		argc           => scalar(@$args),
		want_function  => $want_fn,
	};
}

# ---- rewrite: references to @link -> oracle_fdw constructs ----

# server name = <database>_<login>: the database alias of the link and the remote user,
# lower case. undef when the link definition has no usable database or user.
sub server_name_from_def
{
	my ($host, $user) = @_;
	my $db = $host // '';
	$db = $1 if $db =~ /\b(?:SERVICE_NAME|SID)\s*=\s*([^\s)]+)/i;	# full descriptor
	$db =~ s/['"\s]//g;
	$db =~ s/\..*$//;	# drop the domain
	my $u = $user // '';
	$u =~ s/"//g;
	return undef unless length $db && length $u && $db !~ /[()=]/;
	my $n = lc("${db}_$u");
	$n =~ s/[^a-z0-9_]/_/g;
	return $n;
}

# definition { user host } of a link from $class->{dblink} (export_dblink, DDL files) or,
# on a live run, from the Oracle dictionary; cached
sub _link_def
{
	my ($class, $link) = @_;
	my $name = uc($link // '');
	$name =~ s/"//g;
	my $idx = $class->{dblink_defs} ||= {};
	if (%{$class->{dblink} || {}} && !$class->{dblink_defs_loaded}++) {
		for my $k (keys %{$class->{dblink}}) {
			my $d = $class->{dblink}{$k};
			my $n = uc($k);
			$n =~ s/"//g;
			$idx->{$n} = $d;
			$n =~ s/^\Q$d->{owner}\E\.//i if $d->{owner};
			$idx->{$n} = $d;
		}
	}
	return $idx->{$name} if exists $idx->{$name};
	my $d;
	if ($class->{dbh} && !$class->{input_file} && $name =~ /^[A-Z0-9_\$#]+(\.[A-Z0-9_\$#]+)*$/) {
		my $r = eval { _q($class, "SELECT USERNAME, HOST FROM ALL_DB_LINKS WHERE UPPER(DB_LINK) = ? OR UPPER(DB_LINK) LIKE ? ORDER BY OWNER", $name, "$name.%") };
		_warn($class, "link $name: dictionary query failed: " . ($@ =~ s/\s+$//r)) if $@;
		$d = { user => $r->[0][0], host => $r->[0][1] } if $r && $r->[0];
	}
	return $idx->{$name} = $d;
}

# server of a link: DBLINK_SERVER wins, then <database>_<login> from the link definition,
# last the old name derived from the link name (tests and offline runs without definitions)
sub server_for
{
	my ($class, $link) = @_;
	return $class->{dblink_server}{$link} if $class->{dblink_server}{$link};
	my $d = _link_def($class, $link);
	return (server_name_from_def($d->{host}, $d->{user}) // Ora2Pg::PLSQL::dblink_server_name($link)) if $d;
	return Ora2Pg::PLSQL::dblink_server_name($link);
}

# a literal generated by the patch, hidden from the rest of the converter
sub _dbl_lit
{
	my ($class, $text) = @_;
	$class->{text_values_pos} //= 1 + (sort { $b <=> $a } keys %{$class->{text_values} || {}}, -1)[0];
	my $n = $class->{text_values_pos}++;
	$class->{text_values}{$n} = $text;
	return "?TEXTVALUE$n?";
}

# masked PL/SQL -> text that Oracle understands
sub oracle_text
{
	my ($class, $str) = @_;
	for my $r (reverse scan_refs($str)) {
		my $raw = substr($str, $r->{start}, $r->{end} - $r->{start});
		$raw =~ s/^($PATH)\@$LINK/$1/;
		substr($str, $r->{start}, $r->{end} - $r->{start}) = $raw;
	}
	# optimizer hints are meaningful for Oracle, other comments are not
	$str =~ s{(%ORA2PG_COMMENT\d+%)}{ my $c = $class->{comment_values}{$1} // ''; $c =~ m!^/\*\+! ? $c : ' ' }ge;
	$str =~ s/%OUTERJOIN\d+%/(+)/g;
	$str =~ s/\?TEXTVALUE(\d+)\?/$class->{text_values}{$1}/g;
	# '' and \' were masked by _remove_text_constant_part; the caller's restore does not see this string
	$str =~ s/ORA2PG_ESCAPE2_QUOTE/''/g;
	$str =~ s/ORA2PG_ESCAPE1_QUOTE'/\\'/g;
	return $str;
}

# fdw schema for a table reference: lc(prefix . server . '_' . owner)
sub _fdw_schema
{
	my ($class, $server, $owner) = @_;
	# Server name is <database>_<login> (e.g. rdb_own); remote Oracle owner own is
	# already implied — do not build fdw_rdb_own_own (foreign tables use fdw_rdb_own).
	my $own = defined($owner) ? lc($owner) : undef;
	if (defined $own && $server =~ /_\Q$own\E\z/) {
		$own = undef;
	}
	my $s = lc(($class->{dblink_schema_prefix} // 'fdw_') . $server . (defined $own ? "_$own" : ''));
	$s =~ s/[^a-z0-9_]/_/g;
	return $s;
}

# column name for a foreign table: lower case, quoted when it is a reserved word (freeze, user, ...) or not a plain identifier
sub _col_ident
{
	my ($name) = @_;
	my $n = lc $name;
	return $n if $n =~ /^[\w\$#]+$/ && $n !~ /^\d/ && !grep { $_ eq uc $n } @Ora2Pg::KEYWORDS, @Ora2Pg::SYSTEM_FIELDS;
	return qq("$n");
}

sub _pg_type
{
	my ($class, $m) = @_;
	# NUMBER without precision may hold fractions: never the integer default
	return 'numeric' if uc($m->{type} // '') eq 'NUMBER' && !length($m->{precision} // '');
	return $class->_sql_type($m->{type}, map { $_ // '' } @$m{qw(len precision scale)});
}

my %ORA_OUT = (NUMBER => 'NUMBER', INTEGER => 'NUMBER', FLOAT => 'NUMBER', DATE => 'DATE',
	map { $_ => 'VARCHAR2(32767)' } qw(VARCHAR2 CHAR NVARCHAR2 NCHAR));

sub _simple_var
{
	my $e = shift;
	return $e =~ /^[\w\$#"]+(?:\.[\w\$#"]+)*$/ && $e !~ /^\d/ && uc($e) ne 'NULL';
}

# statement call p@l(...); -> PERFORM oracle_exec / OUT values unpacked from :p0
sub _stmt_call
{
	my ($class, $sref, $r, $args, $end) = @_;
	my ($link, $path) = @$r{qw(link path)};
	$r->{_end} = $end;
	my $todo = sub { return "/* TODO dblink $link: $_[0] */ " . substr($$sref, $r->{start}, $end - $r->{start}); };
	my $sig = meta($class, $link, 'ROUTINE', $path, { argc => scalar(@$args), want_function => 0 });
	return $todo->("no signature for $path") if !$sig && grep { _simple_var($_->{expr}) } @$args;
	my ($i, $k, @call, @bind, @dec, @pack, @outs) = (0, 0);
	for my $a (@$args) {
		$i++;
		my ($mode, $type) = ('IN', undef);
		if ($sig) {
			my ($sa) = defined $a->{name} ? grep({ uc $_->{name} eq uc $a->{name} } @{$sig->{args}}) : $sig->{args}[$i - 1];
			return $todo->("argument $i is not in the signature of $path") unless $sa;
			($mode, $type) = (uc($sa->{mode} // ''), uc($sa->{type} // ''));
		}
		my $pre = defined $a->{name} ? "$a->{name} => " : '';
		if ($mode eq 'IN') {
			push @call, "$pre:p" . ++$k;
			push @bind, bind_text($a->{expr}, $type);
			next;
		}
		return $todo->("unknown mode '$mode' of argument $i") unless $mode eq 'OUT' || $mode eq 'IN/OUT';
		my $ot = $ORA_OUT{$type // ''} or return $todo->("unsupported OUT type " . ($type || '?') . " of argument $i");
		return $todo->("OUT argument $i is not a variable") unless _simple_var($a->{expr});
		my $init = '';
		if ($mode eq 'IN/OUT') {
			push @bind, bind_text($a->{expr}, $type);
			$init = ' := ' . ($ot eq 'DATE' ? "TO_DATE(:p" . ++$k . ", 'YYYY-MM-DD HH24:MI:SS')" : ':p' . ++$k);
		}
		push @dec, "o$i $ot$init;";
		push @call, "${pre}o$i";
		push @pack, $ot eq 'NUMBER' ? "NVL(TO_CHAR(o$i), chr(31))"
			: $ot eq 'DATE' ? "NVL(TO_CHAR(o$i, 'YYYY-MM-DD HH24:MI:SS'), chr(31))" : "NVL(o$i, chr(31))";
		push @outs, $a->{expr};
	}
	my $block = (@dec ? 'DECLARE ' . join(' ', @dec) . ' ' : '') . "BEGIN $path" . (@call ? '(' . join(', ', @call) . ')' : '')
		. '; :p0 := ' . (@pack ? join(' || chr(30) || ', @pack) : 'NULL') . '; END;';
	my $new = 'oracle_exec(' . _dbl_lit($class, "'" . server_for($class, $link) . "'") . ', '
		. _dbl_lit($class, "\$ora\$$block\$ora\$") . join('', map { ", $_" } @bind) . ')';
	return "PERFORM $new" unless @outs;
	my $v = '_dbl_r' . ++$class->{dblink_rn};
	my $j = 0;
	return "$v := $new;\n" . join(";\n", map { $_ . ' := nullif(split_part(' . "$v, chr(30), " . ++$j . '), chr(31))' } @outs);
}

# Oracle types a typed wrapper can carry -> PG type; undef = no wrapper
sub _wrap_type
{
	my ($class, $t) = @_;
	$t = uc($t // '');
	return 'integer' if $t =~ /^(?:INTEGER|BINARY_INTEGER|PLS_INTEGER)$/;
	return 'text' if $t =~ /^(?:VARCHAR2|CHAR|NVARCHAR2|NCHAR)$/;
	return 'timestamp(0)' if $t eq 'DATE';
	return _pg_type($class, { type => $t }) if $t eq 'NUMBER' || $t eq 'FLOAT';
	return undef;
}

# typed PG wrapper for a remote function (T9): registers its DDL, returns the call text or undef
# shortcut: parameter names are not checked against PG reserved words, rename in the signature file if CREATE fails
sub _wrapper
{
	my ($class, $link, $path, $sig, $args) = @_;
	return undef unless $sig && ($sig->{type} // '') eq 'FUNCTION' && $sig->{return};
	return undef if $path =~ /"/;
	my @segs = split /\./, $path;
	return undef if @segs < 1 || @segs > 3;
	my $ret = _wrap_type($class, $sig->{return}) // return undef;
	my @sa = @{$sig->{args}};
	my $named = grep { defined $_->{name} } @$args;
	my $n;
	if ($named) {
		return undef if $named != @$args;	# mixed positional and named
		my %pos = map { uc($sa[$_]{name} // '') => $_ + 1 } 0 .. $#sa;
		my %seen;
		for my $a (@$args) { $seen{ $pos{uc $a->{name}} // return undef } = 1; }
		$n = (sort { $b <=> $a } keys %seen)[0];
		return undef if keys %seen != $n || @$args != $n;	# every parameter 1..n named exactly once
	} else {
		$n = @$args;
		return undef if $n > @sa || grep { !$_->{default} } @sa[$n .. $#sa];
	}
	my @p = @sa[0 .. $n - 1];
	my (@decl, @types);
	for my $p (@p) {
		my $t = ($p->{mode} // '') eq 'IN' ? _wrap_type($class, $p->{type}) : undef;
		my $nm = lc($p->{name} // '');
		return undef unless defined $t && $nm =~ /^[a-z_][a-z0-9_]*$/;
		push @decl, "$nm $t";
		push @types, $t;
	}
	my $name = lc $segs[-1];
	return undef unless $name =~ /^[a-z_][a-z0-9_\$]*$/;
	my $server = server_for($class, $link);
	my $sch = _fdw_schema($class, $server, @segs > 1 ? join('_', @segs[0 .. $#segs - 1]) : undef);
	{ use bytes; return undef if length($sch) > 63; }
	my $fmt = "'YYYY-MM-DD HH24:MI:SS'";
	my (@bind, @ora);
	for my $i (1 .. $n) {
		my $d = $types[$i - 1] eq 'timestamp(0)';
		my $b = $sig->{modifies} ? ":p$i" : ":$i";
		push @ora, $d ? "TO_DATE($b, $fmt)" : $b;
		push @bind, $d ? "to_char(\$$i, $fmt)" : "\$$i\::text";
	}
	my $call = $path . (@ora ? '(' . join(', ', @ora) . ')' : '');
	$call = "TO_CHAR($call, $fmt)" if $ret eq 'timestamp(0)';
	my $body = $sig->{modifies}
		? "SELECT oracle_exec('$server', \$ora\$BEGIN :p0 := $call; END;\$ora\$" . join('', map { ", $_" } @bind) . ")::$ret"
		: "SELECT oracle_scalar('$server', \$ora\$SELECT $call FROM dual\$ora\$"
			. (@bind ? ",\n    ARRAY[" . join(', ', @bind) . ']' : '') . ")::$ret";
	my $sec = $class->{dblink_wrapper_definer}
		? ' SECURITY DEFINER SET search_path = pg_catalog, ' . ($class->{dblink_ext_schema} || 'public') : '';
	my $fn = "$sch.$name";
	$class->{dblink_funcs}{"$fn/$n"} //= "CREATE SCHEMA IF NOT EXISTS $sch;\n"
		. "CREATE OR REPLACE FUNCTION $fn(" . join(', ', @decl) . ")\n"
		. "RETURNS $ret LANGUAGE sql$sec AS \$fn\$\n  $body\n\$fn\$;\n"
		. "REVOKE ALL ON FUNCTION $fn(" . join(', ', @types) . ") FROM PUBLIC;\n";
	return "$fn(" . join(', ', map { (defined $_->{name} ? lc($_->{name}) . ' => ' : '') . $_->{expr} } @$args) . ')';
}

# value call -> oracle_call/oracle_exec; undef = leave to the caller (statement call, odd expr)
sub _value_call
{
	my ($class, $sref, $r) = @_;
	my ($link, $path) = @$r{qw(link path)};
	my $before = substr($$sref, 0, $r->{start});
	$before =~ s/(?:\s|%ORA2PG_COMMENT\d+%)+$//;
	my (@args, $end);
	if ($r->{kind} eq 'call') {	# args re-read: nested calls were already replaced
		my $a;
		($a, $end) = balanced_args($$sref, $r->{args_start});
		return undef unless defined $end;
		@args = @$a;
	} else { $end = $r->{end}; }
	my $after = substr($$sref, $end);
	$after =~ s/^(?:\s|%ORA2PG_COMMENT\d+%)+//;
	my $assign = $before =~ /:=$/ && $after =~ /^;/;
	return undef if $r->{kind} eq 'expr' && !$assign;
	return _stmt_call($class, $sref, $r, \@args, $end)
		if $r->{kind} eq 'call' && $after =~ /^;/ && ($before eq '' || $before =~ /(?:;|\b(?:BEGIN|THEN|ELSE|LOOP|DECLARE))$/i);
	my $sig = meta($class, $link, 'ROUTINE', $path, _routine_hint($class, $r, \@args, $assign));
	if (my $w = _wrapper($class, $link, $path, $sig, \@args)) { $r->{_end} = $end; return $w; }
	my $i = 0;
	for my $a (@args) {
		my ($sa) = $sig ? (defined $a->{name} ? grep({ uc $_->{name} eq uc $a->{name} } @{$sig->{args}}) : $sig->{args}[$i]) : ();
		$a->{bind} = bind_text($a->{expr}, $sa && $sa->{type});
		$i++;
	}
	my $srv = _dbl_lit($class, "'" . server_for($class, $link) . "'");
	my @segs = $path =~ /("[^"]*"|[^.]+)/g;
	my $new;
	if (grep({ defined $_->{name} } @args) || @segs > 3 || $path =~ /"/) {
		my $k = 0;
		my $call = $path . (@args ? '(' . join(', ', map { (defined $_->{name} ? "$_->{name} => " : '') . ':p' . ++$k } @args) . ')' : '');
		$new = "oracle_exec($srv, " . _dbl_lit($class, "\$ora\$BEGIN :p0 := $call; END;\$ora\$") . join('', map { ", $_->{bind}" } @args) . ')';
	} else {
		$new = "oracle_call($srv, " . _dbl_lit($class, "'$path'") . join('', map { ", $_->{bind}" } @args) . ')';
	}
	if (!$assign) {
		if ($sig && $sig->{return}) { $new .= '::' . _pg_type($class, { type => $sig->{return} }); }
		else { $new .= ' /* TODO dblink type */'; }
	}
	$r->{_end} = $end;
	return $new;
}

# Writes to an excluded (MS SQL) link: the whole statement becomes a "--" comment
# The text is hidden as an ordinary comment placeholder; a separate "NULL;" line is added after it so an
# enclosing block never ends up empty (the original code is only commented, never replaced). Reads (SELECT, INSERT ... SELECT) are left alone.
sub comment_writes
{
	my ($class, $str) = @_;
	my $ex = $class->{dblink_exclude} or return $str;
	return $str unless %$ex;
	my $re = qr/(?:\A|;|\b(?:THEN|ELSE|BEGIN|LOOP|AS|IS)\b)\s*\K(?:UPDATE|DELETE(?:\s+FROM)?|INSERT\s+INTO|MERGE\s+INTO)\s+$PATH\@([\w\$#]+)/i;
	my $from = 0;
	while (substr($str, $from) =~ /$re/) {
		my $link = uc $1;
		my $start = $from + $-[0];
		$from += $+[0];
		next unless $ex->{$link};
		# statement ends at the first ';' outside parentheses (text and comments are masked)
		my ($depth, $end) = (0, length $str);
		pos($str) = $from;
		while ($str =~ /\G(?:$MASK|[^();]+|(\()|(\))|(;))/gc) {
			if    ($1) { $depth++ }
			elsif ($2) { $depth-- }
			elsif ($3 && $depth <= 0) { $end = pos($str); last }
		}
		my $stmt = substr($str, $start, $end - $start);
		1 while $stmt =~ s/\?TEXTVALUE(\d+)\?/$class->{text_values}{$1}/e
			 || $stmt =~ s/(%ORA2PG_COMMENT\d+%)/$class->{comment_values}{$1}/e;
		$stmt =~ s/^/-- /mg;
		my $n = 0;
		$n++ while exists $class->{comment_values}{"%ORA2PG_COMMENT9$n%"};
		my $id = "%ORA2PG_COMMENT9$n%";
		$class->{comment_values}{$id} = "-- TODO MS SQL-write: write path not selected\n$stmt";
		my $new = "$id\nNULL;";
		substr($str, $start, $end - $start) = $new;
		$from = $start + length $new;
	}
	return $str;
}

sub rewrite
{
	my ($class, $str) = @_;
	$str = comment_writes($class, $str);
	my %alias;	# alias -> { COLUMN => type class } of the remote table (undef: ambiguous)
	for my $r (reverse scan_refs($str)) {
		my $len  = $r->{end} - $r->{start};
		my $orig = substr($str, $r->{start}, $len);
		my ($link, $kind) = @$r{qw(link kind)};
		my $new;
		if ($class->{dblink_exclude}{$link}) {
			$new = "/* TODO dblink $link */ $orig";
		} elsif ($kind eq 'table') {
			my @p = map { s/"//gr } $r->{path} =~ /("[^"]*"|[^.]+)/g;
			if (@p > 2) { $new = "/* TODO dblink $link: table */ $orig"; }
			else {
				my $server = server_for($class, $link);
				my ($owner, $tab) = @p > 1 ? @p : (undef, $p[0]);
				$class->{dblink_used}{$server}{uc(defined $owner ? "$owner.$tab" : $tab)} = 1;
				$class->{dblink_link}{$server} = $link;
				$new = _fdw_schema($class, $server, $owner) . '.' . lc($tab);
				if (substr($str, $r->{end}) =~ /^\s+(?:AS\s+)?([\w\$#]+)/i && uc($1) !~ /^(?:WHERE|ON|USING|LEFT|RIGHT|FULL|INNER|CROSS|NATURAL|JOIN|GROUP|ORDER|HAVING|UNION|MINUS|INTERSECT|CONNECT|START|FOR|SET|VALUES)$/) {
					my ($a, $m) = (uc $1, meta($class, $link, 'TABLE', uc(defined $owner ? "$owner.$tab" : $tab)));
					my %c = map { uc($_->{name}) => _type_class($_->{type}) } @{$m->{columns} || []} if $m;
					_merge_alias(\%alias, $a, %c ? \%c : undef);
				}
			}
		} elsif ($kind eq 'type') {
			my $m = meta($class, $link, 'COLUMN', $r->{path});
			$new = $m ? _pg_type($class, $m) : "text /* TODO dblink $link: $r->{path}%TYPE */";
		} elsif ($kind eq 'seq') {
			my $q = _dbl_lit($class, '$ora$SELECT ' . $r->{path} . ".$r->{seq_op} FROM DUAL\$ora\$");
			$new = 'oracle_scalar(' . _dbl_lit($class, "'" . server_for($class, $link) . "'") . ", $q)::numeric";
		} elsif ($kind eq 'call' || $kind eq 'expr') {
			$new = _value_call($class, \$str, $r) // "/* TODO dblink $link: $kind */ $orig";
			$len = $r->{_end} - $r->{start} if defined $r->{_end};
		} else {	# rowtype
			$new = "/* TODO dblink $link: $kind */ $orig";
		}
		substr($str, $r->{start}, $len) = $new;
	}
	_local_aliases($class, $str, \%alias) if $str =~ /\b(?:coalesce|nvl)\b|[=<>]/i;
	$str = _align_unions($class, $str, \%alias) if $str =~ /\b(?:UNION|MINUS|INTERSECT)\b/i && $str =~ /\bNULL\b/i;
	$str = _align_coalesce($str, \%alias) if grep { defined } values %alias;
	# refs were handled right to left: number _dbl_rN left to right
	my (%m, $n);
	$str =~ s/\b_dbl_r(\d+)\b/'_dbl_r' . ($m{$1} \/\/= ++$n)/ge;
	$class->{dblink_declare} .= "_dbl_r$_ text;\n" for 1 .. ($n // 0);
	return $str;
}

# Oracle converts the later NVL/COALESCE arguments to the type of the first one;
# PG refuses to mix types. Remote columns have known types: cast to the first one's class.
# shortcut: only qualified remote columns alias.col (and nested COALESCE/NVL of them in comparisons) are aligned, anything else is left alone
sub _type_class
{
	my $t = uc($_[0] // '');
	return 'numeric'   if $t =~ /^(?:NUMBER|INTEGER|INT|SMALLINT|FLOAT|DECIMAL|NUMERIC|REAL|BINARY_(?:FLOAT|DOUBLE)|DOUBLE PRECISION)$/;
	return 'text'      if $t =~ /^(?:VARCHAR2?|CHAR|NCHAR|NVARCHAR2|CLOB|NCLOB|LONG)$/;
	return 'timestamp' if $t =~ /^(?:DATE|TIMESTAMP.*)$/;
	return undef;
}

# one alias may name different tables in the branches of a UNION or in subqueries: a column keeps its class only if every table agrees
# shortcut: no per-subquery scopes; a column present in two tables with different classes is left alone
sub _merge_alias
{
	my ($al, $a, $m) = @_;
	if (!exists $al->{$a}) { $al->{$a} = $m ? { %$m } : undef; return }
	my $o = $al->{$a} or return;
	$m or do { $al->{$a} = undef; return };
	for my $c (keys %$m) { $o->{$c} = exists $o->{$c} && ($o->{$c} // '') ne ($m->{$c} // '') ? undef : $m->{$c} }
}

# alias -> { COLUMN => type class } for local tables/views in FROM/JOIN lists (dictionary of the export connection)
my $KWS = 'WHERE|ON|USING|LEFT|RIGHT|FULL|INNER|CROSS|NATURAL|JOIN|GROUP|ORDER|HAVING|UNION|MINUS|INTERSECT|CONNECT|START|FOR|SET|VALUES|MODEL|PIVOT|UNPIVOT|OUTER|SELECT';

sub _local_cols
{
	my ($class, $owner, $name) = @_;
	return undef unless $class->{dbh} && !$class->{input_file} && defined $owner;
	my $c = $class->{dblink_localcols} ||= {};
	my $k = uc("$owner.$name");
	return $c->{$k} if exists $c->{$k};
	my $rows = eval { _q($class, "SELECT column_name, data_type FROM all_tab_columns WHERE owner = ? AND table_name = ?", uc $owner, uc $name) };
	_warn($class, "local $owner.$name: dictionary query failed: " . ($@ =~ s/\s+$//r)) if $@;
	my %m = map { uc($_->[0]) => _type_class($_->[1]) } @{$rows || []};
	return $c->{$k} = (%m ? \%m : undef);
}

sub _local_aliases
{
	my ($class, $str, $al) = @_;
	return unless $class->{dbh} && !$class->{input_file};
	my $ID = qr/"[^"]+"|[\w\$#]+/;
	my $pfx = lc($class->{dblink_schema_prefix} // 'fdw_');
	my $s = $str;
	while ($s =~ /\b(?:FROM|JOIN)\s+/gi) {
		while ($s =~ /\G($ID(?:\.$ID)?)(?:\s+(?:AS\s+)?(?!(?:$KWS)\b)([\w\$#]+))?/gci) {
			my ($t, $a) = ($1, $2);
			my @p = map { s/"//gr } $t =~ /($ID)/g;
			my ($owner, $name) = @p > 1 ? @p : ($class->{schema}, $p[0]);
			unless (!defined $owner || lc($owner) =~ /^\Q$pfx\E/ || $name =~ /^(?:DUAL|TABLE)$/i) {
				$a = uc($a // $name);
				my $m = _local_cols($class, $owner, $name);
				_merge_alias($al, $a, $m);
			}
			last unless $s =~ /\G\s*,\s*/gc;
		}
	}
}

# UNION branches: a bare NULL gets the type of the same column in another branch (PG resolves NULL, NULL to text before it sees the typed branch)
# shortcut: only top-level branches that start with SELECT; types come from alias.col, literals, to_date/to_char/||; anything else is left alone
sub _align_unions
{
	my ($class, $str, $remote) = @_;
	my (@cut, $d);
	while ($str =~ /([()])|\b(UNION(?:\s+ALL)?|MINUS|INTERSECT)\b/gi) {
		if (defined $1) { $d += $1 eq '(' ? 1 : -1 } elsif (!$d) { push @cut, [$-[0], $+[0]] }
	}
	return $str unless @cut;
	my @seg;	# [start, end] of every branch
	my $p = 0;
	for my $c (@cut, [length $str, length $str]) { push @seg, [$p, $c->[0]]; $p = $c->[1] }
	my $strip = sub { my $e = shift; $e =~ s/%ORA2PG_COMMENT\d+%//g; $e =~ s/^\s+|\s+$//g; $e };
	my @br;
	for my $sg (@seg) {
		my $t = substr($str, $sg->[0], $sg->[1] - $sg->[0]);
		$t =~ /^\s*(?:%ORA2PG_COMMENT\d+%\s*)*SELECT\b\s*(?:(?:DISTINCT|ALL|UNIQUE)\b\s*)?/i or return $str;
		my ($ls, $le, $dd) = ($+[0], length $t, 0);
		while ($t =~ /([()])|\bFROM\b/gi) {
			next if $-[0] < $ls;
			if (defined $1) { $dd += $1 eq '(' ? 1 : -1 } elsif (!$dd) { $le = $-[0]; last }
		}
		my %al;
		_merge_alias(\%al, $_, $remote->{$_}) for keys %$remote;
		_local_aliases($class, $t, \%al);
		push @br, { off => $sg->[0] + $ls, len => $le - $ls, items => [ _split_args(substr($t, $ls, $le - $ls)) ], al => \%al };
	}
	my $n = @{$br[0]{items}};
	return $str if grep { @{$_->{items}} != $n } @br;
	my $ecls; $ecls = sub {
		my ($e, $al) = @_;
		$e = $strip->($e);
		$e =~ s/^(.*?[\w\$#)"?])\s+(?:AS\s+)?(?:"[^"]+"|[\w\$#]+)$/$1/is unless $e =~ /^[\w\$#]+\.[\w\$#]+$/ || $e =~ /^[\w\$#]+$/;
		return 'numeric'   if $e =~ /^-?\d+(?:\.\d+)?$/;
		return 'text'      if $e =~ /^\?TEXTVALUE\d+\?$/ || $e =~ /\|\|/ || $e =~ /^to_char\s*\(/i;
		return 'numeric'   if $e =~ /^to_number\s*\(/i;
		return 'timestamp' if $e =~ /^(?:to_date|to_timestamp)\s*\(/i || $e =~ /^(?:sysdate|current_date|statement_timestamp\(\))$/i;
		return $ecls->((_split_args($1))[0], $al) if $e =~ /^(?:coalesce|nvl)\s*\((.*)\)$/is;
		if ($e =~ /^([\w\$#]+)\.([\w\$#]+)$/) { return $al->{uc $1} ? $al->{uc $1}{uc $2} : undef }
		return undef;
	};
	my %fix;	# branch index -> list of rewritten items
	for my $i (0 .. $n - 1) {
		my ($class_i) = grep { defined } map { $ecls->($_->{items}[$i], $_->{al}) } @br;
		next unless $class_i;
		for my $b (@br) {
			my $it = $b->{items}[$i];
			next unless $strip->($it) =~ /^NULL(?:\s+(?:AS\s+)?(?:"[^"]+"|[\w\$#]+))?$/i;
			$it =~ s/\bNULL\b/NULL::$class_i/i;
			$b->{items}[$i] = $it;
			$b->{changed} = 1;
		}
	}
	for my $b (reverse grep { $_->{changed} } @br) { substr($str, $b->{off}, $b->{len}) = join(',', @{$b->{items}}) }
	return $str;
}

# top-level comma split of a call body
sub _split_args
{
	my ($in) = @_;
	my @a;
	my ($d, $cur) = (0, '');
	for my $c (split //, $in) {
		$d++ if $c eq '(';
		$d-- if $c eq ')';
		if (!$d && $c eq ',') { push @a, $cur; $cur = ''; next }
		$cur .= $c;
	}
	return (@a, $cur);
}

sub _align_coalesce
{
	my ($str, $al) = @_;
	my $Q = qr/[\w\$#]+\.[\w\$#]+/;
	my $F = qr/\b(?:coalesce|nvl)\s*(?<p>\((?:[^()]++|(?&p))*\))/i;
	my $OJ = qr/(?:%OUTERJOIN\d+%)?/;	# (+) is still masked here; it must not hide the operand
	my $E = qr/(?:$F|$Q$OJ)/;
	# class of an expression: a column alias.col, or a (nested) COALESCE/NVL = class of its first argument
	my $cls; $cls = sub {
		my $e = shift =~ s/^\s+|\s+$//gr;
		$e =~ s/%OUTERJOIN\d+%$//;
		if ($e =~ /^(?:coalesce|nvl)\s*\((.*)\)$/is) { return $cls->((_split_args($1))[0]) }
		return 'numeric' if $e =~ /^-?\d+(?:\.\d+)?$/;
		return undef unless $e =~ /^$Q$/;
		my ($t, $col) = split /\./, $e;
		return $al->{uc $t} ? $al->{uc $t}{uc $col} : undef;
	};
	my $cast = sub { my ($e, $c) = @_; my $oj = $e =~ s/(%OUTERJOIN\d+%)$// ? $1 : ''; ($e =~ /^(?:$Q|-?\d+(?:\.\d+)?)$/ ? $e : "($e)") . "::$c$oj" };
	# Oracle converts the later NVL/COALESCE arguments to the type of the first one; PG refuses to mix types
	my $fix; $fix = sub {
		my $s = shift;
		$s =~ s{\b(coalesce|nvl)(\s*)\(((?:[^()]++|(?<p>\((?:[^()]++|(?&p))*\)))*)\)}{
			my ($f, $o, $in) = ($1, $2, $3);
			my $body = $fix->($in);
			my @a = _split_args($body);
			my $first = $cls->($a[0]);
			for my $i (1 .. $#a) {
				my $c = $cls->($a[$i]);
				next unless $first && $c && $c ne $first;
				my ($lead, $e, $trail) = $a[$i] =~ /^(\s*)(.*?)(\s*)$/s;
				$a[$i] = $lead . $cast->($e, $first) . $trail;
			}
			"$f$o(" . join(',', @a) . ')';
		}gie;
		return $s;
	};
	$str = $fix->($str);
	# numeric functions get a character argument (e.g. the result of NVL(varchar, 0)): Oracle converts it to a number
	$str =~ s{\b(sign|abs|ceil|floor|round|trunc)(\s*)\(((?:[^()]++|(?<p>\((?:[^()]++|(?&p))*\)))*)\)}{
		my ($f, $o, $in) = ($1, $2, $3);
		my @a = _split_args($in);
		if (@a && ($cls->($a[0]) // '') eq 'text') {
			my ($lead, $e, $trail) = $a[0] =~ /^(\s*)(.*?)(\s*)$/s;
			$a[0] = $lead . $cast->($e, 'numeric') . $trail;
		}
		"$f$o(" . join(',', @a) . ')';
	}gie;
	# DECODE(expr, search, result, ...) compares expr with every search value like a comparison does (it is turned into CASE later)
	$str =~ s{\bdecode(\s*)\(((?:[^()]++|(?<p>\((?:[^()]++|(?&p))*\)))*)\)}{
		my ($o, $in) = ($1, $2);
		my @a = _split_args($in);
		my $c0 = $cls->($a[0]);
		# Oracle converts expr and every search value to the type of the FIRST search value; an untyped NULL there means VARCHAR2
		my $textual = $c0 && $c0 ne 'text' && @a > 3 && $a[1] =~ /^\s*null\s*$/i
			&& grep { ($cls->($a[$_]) // '') ne $c0 && $a[$_] !~ /^\s*null\s*$/i } grep { $_ % 2 } 3 .. $#a - 1;
		if ($textual) {
			# shortcut: compared as text on both sides (PG text of a date differs from NLS text, but a date never equals a number either way)
			for my $i (0, grep { $_ % 2 && $_ < $#a && $a[$_] !~ /^\s*null\s*$/i } 1 .. $#a) {
				my ($ld, $e, $tr) = $a[$i] =~ /^(\s*)(.*?)(\s*)$/s;
				$a[$i] = $ld . $cast->($e, 'text') . $tr;
			}
		}
		elsif ($c0) {
			for (my $i = 1; $i < $#a; $i += 2) {
				my $c = $cls->($a[$i]);
				next unless $c && $c ne $c0;
				my ($ld, $e, $tr) = $a[$i] =~ /^(\s*)(.*?)(\s*)$/s;
				if    ($c0 eq 'text' && $c eq 'numeric') { $a[$i] = $ld . $cast->($e, 'text') . $tr }
				elsif ($c0 eq 'numeric' && $c eq 'text') { $a[0] = $cast->($a[0] =~ s/^\s+|\s+$//gr, 'text'); $c0 = 'text'; $a[$i] =~ s/^\s*/ /; }
				elsif ($c0 eq 'text') { $a[0] = $cast->($a[0] =~ s/^\s+|\s+$//gr, $c); $c0 = $c }
				elsif ($c eq 'text')  { $a[$i] = $ld . $cast->($e, $c0) . $tr }
			}
		}
		"decode$o(" . join(',', @a) . ')';
	}gie;
	# Oracle compares VARCHAR2 with NUMBER/DATE by converting the character side.
	$str =~ s{(?<l>$E)(?<op>\s*(?:=|<>|!=|<=|>=|<|>|IS\s+(?:NOT\s+)?DISTINCT\s+FROM)\s*)(?<r>$E)(?![\w\$#.(])}{
		my ($l, $op, $r) = @+{qw(l op r)};
		my @cl = map { $cls->($_) } ($l, $r);
		if ($cl[0] && $cl[1] && $cl[0] ne $cl[1]) {
			# shortcut: text vs number is compared as text (never fails on non-numeric data; Oracle converts to number), text vs date is converted to the date
			if    ($cl[0] eq 'text' && $cl[1] eq 'numeric') { $r = $cast->($r, 'text') }
			elsif ($cl[1] eq 'text' && $cl[0] eq 'numeric') { $l = $cast->($l, 'text') }
			elsif ($cl[0] eq 'text') { $l = $cast->($l, $cl[1]) }
			elsif ($cl[1] eq 'text') { $r = $cast->($r, $cl[0]) }
		}
		$l . $op . $r;
	}gie;
	return $str;
}

# ---- CREATE FOREIGN TABLE script for every table seen by rewrite() ----

sub foreign_tables_sql
{
	my ($class) = @_;
	my (%out, @todo);
	for my $server (keys %{$class->{dblink_used} || {}}) {
		my $link = $class->{dblink_link}{$server};
		for my $path (keys %{$class->{dblink_used}{$server}}) {
			my @p = split /\./, $path;
			my $schema = _fdw_schema($class, $server, @p > 1 ? $p[0] : undef);
			my $m = meta($class, $link, 'TABLE', $path);
			my $key = "$schema.\L$p[-1]";
			if (!$m || !$m->{columns} || !@{$m->{columns}}) {
				$out{$key} = "-- TODO dblink $link: no columns for $path\n";
				next;
			}
			my %pk = map { $_ => 1 } @{$m->{pk} || []};
			my @cols = map {
				"\t" . _col_ident($_->{name}) . ' ' . _pg_type($class, $_) . ($pk{uc $_->{name}} ? " OPTIONS (key 'true')" : '');
			} @{$m->{columns}};
			my $owner = $m->{owner} // (@p > 1 ? $p[0] : undef);
			my $opts = (defined $owner ? "schema '" . uc($owner) . "', " : '') . "table '" . uc($m->{name} // $p[-1]) . "'";
			$out{$key} = "CREATE SCHEMA IF NOT EXISTS $schema;\nCREATE FOREIGN TABLE IF NOT EXISTS $key (\n"
				. join(",\n", @cols) . "\n) SERVER $server OPTIONS ($opts);\n";
		}
	}
	return join("\n", map { $out{$_} } sort keys %out);
}

# called at the end of export_*: dblink_tables_<type>.sql
sub write_tables_file
{
	my ($self) = @_;
	return unless ($self->{dblink_mode} // 'off') eq 'fdw';
	my %text;
	$text{tables} = foreign_tables_sql($self) if %{$self->{dblink_used} || {}};
	$text{functions} = join("\n", map { $self->{dblink_funcs}{$_} } sort keys %{$self->{dblink_funcs}}) if %{$self->{dblink_funcs} || {}};
	for my $w (qw(tables functions)) {
		next unless defined $text{$w};
		my $fh = $self->open_export_file("dblink_${w}_" . lc($self->{type}) . '.sql');
		$self->set_binmode($fh) if (!$self->{compress});
		$self->dump($text{$w}, $fh);
		$self->close_export_file($fh);
	}
}

# ---- view whose sources all live on one link -> foreign table on a query ----

# shortcut: FROM-list parsing is textual; anything unusual -> not eligible (normal path)
sub _all_sources_remote
{
	my ($s) = @_;
	$s =~ s/$MASK/x/g;
	return 0 if $s =~ /^\s*WITH\b/i;
	my $seen = 0;
	while ($s =~ /\bFROM\b/gi) {
		my $from = pos($s);
		my ($d, $i, $n) = (0, $from, length $s);
		for (; $i < $n; $i++) {
			my $c = substr($s, $i, 1);
			if ($c eq '(') { $d++ } elsif ($c eq ')') { last if !$d; $d-- }
			elsif ($c eq ';') { last }
			elsif (!$d && substr($s, $i) =~ /^(?:WHERE|GROUP|ORDER|HAVING|UNION|MINUS|INTERSECT|CONNECT|START|MODEL|FOR)\b/i
				&& substr($s, $i - 1, 1) !~ /[\w\$#]/) { last }
		}
		my $cl = substr($s, $from, $i - $from);
		# split on top-level commas and JOIN keywords
		my @items;
		my ($cur, $dd) = ('', 0);
		my $j = 0;
		while ($j < length $cl) {
			my $c = substr($cl, $j, 1);
			if ($c eq '(') { $dd++ } elsif ($c eq ')') { $dd-- }
			if (!$dd && $c eq ',') { push @items, $cur; $cur = ''; $j++; next; }
			if (!$dd && substr($cl, $j) =~ /^((?:(?:INNER|LEFT|RIGHT|FULL|CROSS|NATURAL|OUTER)\s+)*JOIN)\b/i
				&& ($j == 0 || substr($cl, $j - 1, 1) !~ /[\w\$#]/)) {
				push @items, $cur; $cur = ''; $j += length $1; next;
			}
			$cur .= $c; $j++;
		}
		push @items, $cur;
		for my $it (@items) {
			$it =~ s/^\s+//;
			next if $it =~ /^$/;
			# drop ON <cond> / USING (...) of the previous join
			if ($it =~ /^\(/) { return 0 unless $it =~ /^\(\s*SELECT\b/i; next; }
			$it =~ s/\s+(?:ON|USING)\b.*$//is;
			$it =~ s/\s+$//;
			return 0 unless $it =~ /^$PATH\@$LINK(?:\s+(?:AS\s+)?$ID)?$/i;
			$seen++;
		}
	}
	return $seen;
}

# columns: $class->{views}{$view}{columns} (test / preset) or ALL_TAB_COLUMNS of the local Oracle
sub _view_columns
{
	my ($class, $view) = @_;
	my $v = $class->{views}{$view};
	return $v->{columns} if $v->{columns};
	return undef if !$class->{dbh} || $class->{input_file};
	my ($owner, $name) = $view =~ /^(.+?)\.(.+)$/ ? ($1, $2) : ($v->{owner} // $class->{schema}, $view);
	my $rows = eval {
		$class->{dbh}->selectall_arrayref("SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE FROM $class->{prefix}_TAB_COLUMNS WHERE OWNER = ? AND TABLE_NAME = ? ORDER BY COLUMN_ID", undef, uc($owner // ''), uc $name);
	};
	return undef if !$rows || !@$rows;
	return [ map { { name => $_->[0], type => $_->[1] =~ s/^(TIMESTAMP)\(\d+\)/$1/r, len => $_->[2], precision => $_->[3], scale => $_->[4] } } @$rows ];
}

# ($ddl, $todo): $ddl defined -> use it instead of CREATE VIEW; $todo -> comment line for the normal path
sub view_ddl
{
	my ($class, $view, $pgname, $text) = @_;
	return () unless ($class->{dblink_mode} // 'off') eq 'fdw';
	# the caller has run _remove_comments: literals and comments are already masked
	my $m = $text;
	my @refs = scan_refs($m);
	return () if !@refs || grep { $_->{link} ne $refs[0]{link} || $_->{kind} =~ /^(?:rowtype|expr)$/ } @refs;
	my $link = $refs[0]{link};
	return () if $class->{dblink_exclude}{$link} || !_all_sources_remote($m);
	my $cols = _view_columns($class, $view);
	return (undef, "-- TODO dblink: view on one link, no column types\n") if !$cols || !@$cols;
	my @c = map {
		"\t" . _col_ident($_->{name}) . ' ' . _pg_type($class, $_);
	} @$cols;
	my $q = oracle_text($class, $m);
	$q =~ s/[\s;]+$//;
	return ("CREATE FOREIGN TABLE $pgname (\n" . join(",\n", @c) . "\n) SERVER " . server_for($class, $link)
		. " OPTIONS (table \$ora\$($q)\$ora\$);\n");
}

# ---- DBMS_SCHEDULER / DBA_JOBS interval -> pg_cron expression (undef: not supported) ----

sub cron_expr
{
	my ($iv) = @_;
	my %p;
	for my $kv (split /;/, $iv // '') {
		next unless $kv =~ /\S/;
		$kv =~ /^\s*(\w+)\s*=\s*([\w,]+)\s*$/ or return undef;
		$p{uc $1} = uc $2;
	}
	delete $p{BYSECOND} if ($p{BYSECOND} // '') =~ /^0+$/;
	my $f = delete $p{FREQ} // return undef;
	my $n = delete $p{INTERVAL} // 1;
	my ($h, $m, $d) = delete @p{qw(BYHOUR BYMINUTE BYMONTHDAY)};
	my $list = qr/^\d+(?:,\d+)*$/;
	s/\b0+(?=\d)//g for grep { defined } $h, $m, $d;	# 00 -> 0
	return undef if %p || $n !~ /^[1-9]\d*$/ || grep { defined && !/$list/ } $h, $m, $d;
	if ($f eq 'MINUTELY') { return undef if defined $h || defined $m || defined $d; return "*/$n * * * *"; }
	if ($f eq 'HOURLY')   { return undef if defined $h || defined $d; return ($m // 0) . " */$n * * *"; }
	if ($f eq 'DAILY')    { return undef if $n != 1 || !defined $h || defined $d; return ($m // 0) . " $h * * *"; }
	if ($f eq 'MONTHLY')  { return undef if $n != 1 || !defined $h || !defined $d; return ($m // 0) . " $h $d * *"; }
	return undef;
}

# one job -> cron.schedule(...) with the body converted by convert_plsql_code
sub job_sql
{
	my ($class, $name, $interval, $what) = @_;
	my $cron = cron_expr($interval);
	return "-- TODO cron: $interval\n" if !defined $cron;
	my $body = $what // '';
	$body =~ s/^\s+|\s+$//g;
	$body .= '()' if $body =~ /^[\w\$#."]+$/;	# stored procedure name
	# a lone local procedure call needs CALL in PL/pgSQL (a remote one is rewritten by convert_plsql_code)
	$body = "CALL $body" if $body =~ /^[\w\$#."]+\s*\(.*\);?$/s && $body !~ /\@/;
	$body .= ';' if $body !~ /;$/;
	$body = "BEGIN\n$body\nEND;" if $body !~ /^(?:DECLARE|BEGIN)\b/i;
	local $class->{text_values} = {};
	local $class->{text_values_pos} = 0;
	local $class->{comment_values} = {};
	$class->_remove_text_constant_part(\$body);
	$class->_remove_comments(\$body);
	my $out = Ora2Pg::PLSQL::convert_plsql_code($class, $body);
	$class->_restore_text_constant_part(\$out);
	$class->_restore_comments(\$out);
	(my $nm = lc $name) =~ s/"//g;
	$nm =~ s/'/''/g;
	return "SELECT cron.schedule('$nm', '$cron', \$cron\$DO \$do\$\n$out\n\$do\$;\$cron\$);\n";
}

# SHOW_REPORT detail line for one cost key
sub report_item
{
	my ($d, $cd) = @_;
	return "\tDBLINK: $cd->{DBLINK} refs, " . ($cd->{DBLINK_TODO} || 0) . " TODO" if $d eq 'DBLINK';
	return "\t$d => $cd->{$d}";
}

1;
