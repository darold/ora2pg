package Ora2Pg::DbLink;

# Pure helpers to locate Oracle database-link references (ident.ident@link)
# in PL/SQL text already masked by ora2pg (?TEXTVALUEn?, %ORA2PG_COMMENTn%).

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
		if ($str =~ /\G$MASK/gc) {
			$prev = 'TEXT';
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
		} elsif ($str =~ /\G\s+/gc) {
			# whitespace does not change $prev
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
		$t =~ s/\s*\(.*\)\s*$//;
		push @{$r{args}}, { name => $n, mode => $m, type => uc $t };
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
		my ($link, $kind, $path, $spec) = split /\|/, $l, 4;
		$kind = uc($kind // '');
		my $r = (defined $spec && $kind =~ /^(?:COLUMN|TABLE|FUNCTION|PROCEDURE)$/ && length $path)
			? _parse_spec($kind, $spec) : undef;
		unless ($r) { _warn($class, "DBLINK_SIGNATURES $f line $.: bad format, skipped"); next; }
		if ($kind eq 'TABLE') {
			my @p = split /\./, $path;
			@$r{qw(owner name)} = (@p > 1 ? uc $p[-2] : undef, uc $p[-1]);
		} elsif ($kind ne 'COLUMN') { $r->{type} = $kind; $kind = 'ROUTINE'; }
		$h->{uc($link) . "|$kind|" . lc $path} = $r;
	}
	close $fh;
	return $h;
}

sub _q
{
	my ($class, $sql, @b) = @_;
	my $rows = eval { $class->{dbh}->selectall_arrayref($sql, undef, @b) };
	die(($@ || ($class->{dbh}->errstr // 'unknown error')) . "\n") unless $rows;
	return $rows;
}

sub _ident { my $s = shift; return $s =~ /^"(.*)"$/ ? $1 : uc $s; }

sub _table_cols
{
	my ($class, $L, $owner, $name) = @_;
	my $rows = _q($class, "SELECT column_name, data_type, data_length, data_precision, data_scale, nullable FROM all_tab_columns\@$L WHERE owner = ? AND table_name = ? ORDER BY column_id", $owner, $name);
	return [ map { { name => $_->[0], type => $_->[1], len => $_->[2], precision => $_->[3], scale => $_->[4], nullable => $_->[5] } } @$rows ];
}

# segs: (name) or (owner, name); returns TABLE hashref or undef
sub _live_table
{
	my ($class, $L, @segs) = @_;
	my ($owner, $name) = @segs > 1 ? @segs : (_schema($class, $L), @segs);
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

sub _schema
{
	my ($class, $L) = @_;
	return $class->{dblink_meta}{$L}{'_SCHEMA'} //= _q($class, "SELECT USER FROM DUAL\@$L")->[0][0];
}

sub _live_routine
{
	my ($class, $L, @segs) = @_;
	my @try = @segs == 3 ? ([ @segs ])
		: @segs == 2 ? ([ $segs[0], '-', $segs[1] ], [ _schema($class, $L), @segs ])
		: ([ _schema($class, $L), '-', $segs[0] ]);
	for my $t (@try) {
		my $rows = _q($class, "SELECT argument_name, position, in_out, data_type, overload FROM all_arguments\@$L WHERE owner = ? AND NVL(package_name, '-') = ? AND object_name = ? AND data_level = 0 ORDER BY overload, position", @$t);
		next unless @$rows;
		my %ov = map { $_->[4] => 1 } grep { defined $_->[4] } @$rows;
		if (keys %ov > 1) { _warn($class, "$L: overloaded routine " . join('.', @$t) . " is not supported"); return undef; }
		my %r = (type => 'PROCEDURE', return => undef, args => []);
		for my $r (@$rows) {
			my ($n, $pos, $io, $dt) = @$r;
			if ($pos == 0) { @r{qw(type return)} = ('FUNCTION', $dt); next; }
			next unless defined $dt;	# procedure without arguments
			push @{$r{args}}, { name => $n, mode => $io, type => $dt };
		}
		return \%r;
	}
	return undef;
}

sub _live
{
	my ($class, $L, $kind, $path) = @_;
	my @s = map { _ident($_) } $path =~ /("[^"]*"|[^.]+)/g;
	return undef unless @s;
	if ($kind eq 'ROUTINE') { return @s > 3 ? undef : _live_routine($class, $L, @s); }
	my $col = $kind eq 'COLUMN' ? pop @s : undef;
	return undef if !@s || @s > 2;
	my $t = _live_table($class, $L, @s) or return undef;
	return $t unless defined $col;
	my ($c) = grep { $_->{name} eq $col } @{$t->{columns}};
	return $c ? { map { $_ => $c->{$_} } qw(type len precision scale) } : undef;
}

sub meta
{
	my ($class, $link, $kind, $path) = @_;
	$kind = uc $kind;
	my $c = $class->{dblink_meta}{$link}{$kind} ||= {};
	my $k = lc $path;
	return $c->{$k} if exists $c->{$k};
	my $sig = $class->{dblink_sigfile_cache} || _load_signatures($class);
	# links mapped to one server (DBLINK_SERVER) share their signatures
	my $r = $sig->{"$link|$kind|$k"} // $sig->{uc(server_for($class, $link)) . "|$kind|$k"};
	if (!$r && $class->{dbh} && !$class->{input_file}) {
		if ($link =~ /^[A-Z0-9_\$#]+(\.[A-Z0-9_\$#]+)*(\@[A-Z0-9_\$#]+)?$/) {
			$r = eval { _live($class, $link, $kind, $path) };
			_warn($class, "$link $kind $path: dictionary query failed: " . ($@ =~ s/\s+$//r)) if $@;
		} else { _warn($class, "invalid database link name '$link'"); }
	}
	return $c->{$k} = $r;
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
	return $str;
}

# fdw schema for a table reference: lc(prefix . server . '_' . owner)
sub _fdw_schema
{
	my ($class, $server, $owner) = @_;
	my $s = lc(($class->{dblink_schema_prefix} // 'fdw_') . $server . (defined $owner ? "_$owner" : ''));
	$s =~ s/[^a-z0-9_]/_/g;
	return $s;
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
	my $sig = meta($class, $link, 'ROUTINE', $path);
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
	my $sig = meta($class, $link, 'ROUTINE', $path);
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

sub rewrite
{
	my ($class, $str) = @_;
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
	# refs were handled right to left: number _dbl_rN left to right
	my (%m, $n);
	$str =~ s/\b_dbl_r(\d+)\b/'_dbl_r' . ($m{$1} \/\/= ++$n)/ge;
	$class->{dblink_declare} .= "_dbl_r$_ text;\n" for 1 .. ($n // 0);
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
				my $n = lc $_->{name};
				$n = qq("$n") unless $n =~ /^[\w\$#]+$/ && $n !~ /^\d/;
				"\t$n " . _pg_type($class, $_) . ($pk{uc $_->{name}} ? " OPTIONS (key 'true')" : '');
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
	return unless ($self->{dblink_mode} // 'off') eq 'fdw' && %{$self->{dblink_used} || {}};
	my $fh = $self->open_export_file('dblink_tables_' . lc($self->{type}) . '.sql');
	$self->set_binmode($fh) if (!$self->{compress});
	$self->dump(foreign_tables_sql($self), $fh);
	$self->close_export_file($fh);
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
		my $n = lc $_->{name};
		$n = qq("$n") unless $n =~ /^[\w\$#]+$/ && $n !~ /^\d/;
		"\t$n " . _pg_type($class, $_);
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
