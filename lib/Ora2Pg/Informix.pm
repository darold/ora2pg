package Ora2Pg::Informix;

use vars qw($VERSION);
use strict;

use DBI;
use POSIX qw(locale_h);
use Benchmark;

#set locale to LC_NUMERIC C
setlocale(LC_NUMERIC,"C");


$VERSION = '23.1';

# Some function might be excluded from export and assessment.
our @EXCLUDED_FUNCTION = ('SQUIRREL_GET_ERROR_OFFSET');

# These definitions can be overriden from configuration
# file using the DATA_TYPË configuration directive.
our %SQL_TYPE = (
	'CHAR' => 'char',
	'SMALLINT' => 'smallint',
	'INTEGER' => 'integer',
	'FLOAT' => 'float',
	'SMALLFLOAT' => 'float',
	'DECIMAL' => 'numeric',
	'SERIAL' => 'serial',
	'DATE' => 'date',
	'MONEY' => 'money',
	'DATETIME' => 'timestamp',
	'BYTE' => 'bytea',
	'TEXT' => 'text',
	'VARCHAR' => 'varchar',
	'INTERVAL' => 'interval',
	'NCHAR' => 'char',
	'NVARCHAR' => 'varchar',
	'INT8' => 'bigint',
	'SERIAL8' => 'bigserial',
	'SET' => 'SET',
	'MULTISET' => 'MULTISET',
	'LIST' => 'array',
	'ROW' => 'ROW',
	'COLLECTION' => '',
	'BLOB' => 'bytea',
	'CLOB' => 'text',
	'LVARCHAR' => 'text',
	'BOOLEAN' => 'boolean',
	'BIGINT' => 'bigint',
	'BIGSERIAL' => 'bigserial',
	'IDSSECURITYLABEL' => 'varchar',
	'TIMESERIES' => 'TimeSeries',
);

sub _db_connection
{
	my $self = shift;

	$self->logit("Trying to connect to database: $self->{oracle_dsn}\n", 1) if (!$self->{quiet});

#	if (!defined $self->{oracle_pwd})
#	{
#		eval("use Term::ReadKey;");
#		if (!$@) {
#			$self->{oracle_user} = $self->_ask_username('Informix') unless (defined $self->{oracle_user});
#			$self->{oracle_pwd} = $self->_ask_password('Informix');
#		}
#	}

	my $trimblank = 0;
	$trimblank = 1 if ($self->{type} ne 'COPY' and $self->{type} ne 'INSERT');
	my $dbh = DBI->connect("$self->{oracle_dsn}", $self->{oracle_user}, $self->{oracle_pwd}, {
			'RaiseError' => 1,
			AutoInactiveDestroy => 1,
			PrintError => 0,
			ChopBlanks => $trimblank 
		}
	);
	$dbh->{LongReadLen} = $self->{longreadlen} if ($self->{longreadlen});
	$dbh->{LongTruncOk} = $self->{longtruncok} if (defined $self->{longtruncok});

	# Check for connection failure
	if (!$dbh) {
		$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
	}

	# Use consistent reads for concurrent dumping...
	if ($self->{debug} && !$self->{quiet}) {
		$self->logit("Isolation level: $self->{transaction}\n", 1);
	}

	$dbh->do("BEGIN WORK") or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	#$dbh->do($self->{transaction}) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);

	# Force execution of initial command
	$self->_ora_initial_command($dbh);

	# Instruct Ora2Pg that the database engine is Informix
	$self->{is_informix} = 1;

	return $dbh;
}

sub _get_version
{
	my $self = shift;

	my $dbver = '';
	my $sql = "SELECT DBINFO('version','full') FROM systables WHERE tabid = 1";

        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$dbver = $row[0];
		last;
	}
	$sth->finish();

	$dbver =~ s/ \- .*//;
	$dbver =~ s/\s+/ /gs;

	return $dbver;
}

sub _schema_list
{
	my $self = shift;

	my $sql = "select name, owner, created from sysmaster:sysdatabases WHERE owner != 'informix' ORDER BY name";
	my $sth = $self->{dbh}->prepare( $sql ) or return undef;
	$sth->execute or return undef;
	$sth;
}

sub _table_exists
{
	my ($self, $schema, $table) = @_;

	my $ret = '';

	my $sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table'";

	my $sth = $self->{dbh}->prepare( $sql ) or return undef;
	$sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$ret = $row[0];
	}
	$sth->finish();

	return $ret;
}



=head2 _get_encoding

This function retrieves the Oracle database encoding

Returns a handle to a DB query statement.

=cut

sub _get_encoding
{
	my ($self, $dbh) = @_;

	my $sql = qq{SELECT dbs_collate  FROM sysmaster:sysdbslocale WHERE dbs_dbsname = '$self->{schema}'};
        my $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
        $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	my $db_encoding = '';
	my $pg_encoding = '';
	my $db_collation = '';
	while ( my @row = $sth->fetchrow())
	{
		$db_encoding = $row[0];
		$db_collation = $row[0];
	}
	$sth->finish();

	my $db_timestamp_format = '';
	my $db_date_format = '';
#	$sql = qq{SELECT DBDATE
#FROM sys.dm_exec_sessions
#WHERE session_id = \@\@spid
#	};
#        $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
#        $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
#	while ( my @row = $sth->fetchrow()) {
#		$db_date_format = $row[0];
#	}
#	$sth->finish();

	($db_encoding, $pg_encoding) = auto_set_encoding($db_encoding);

	return ($db_encoding, $db_collation, $pg_encoding, $db_timestamp_format, $db_date_format);
}

=head2 auto_set_encoding

This function is used to find the PostgreSQL charset corresponding to the
Informix encoding value

=cut

sub auto_set_encoding
{
	my $db_encoding = shift;
	#en_US.819

	my ($locale, $code_set) = split(/\./, $db_encoding);

	my %ENCODING = (
		'819' => '8859-1',
		'912' => '8859-2',
		'57346' => '8859-3',
		'57347' => '8859-4',
		'915' => '8859-5',
		'1089' => '8859-6',
		'813' => '8859-7',
		'916' => '8859-8',
		'920' => '8859-9',
		'57390' => '8859-13',
		'364' => 'ASCII',
		'932' => 'sjis-s',
		'57350' => 'sjis',
		'57372' => 'utf8',
		'57352' => 'big5',
		'1250' => 'CP1250',
		'1251' => 'CP1251',
		'1252' => 'CP1252',
		'1253' => 'CP1253',
		'1254' => 'CP1254',
		'1255' => 'CP1255',
		'1256' => 'CP1256',
		'1257' => 'CP1257',
		'57357' => 'cp936',
		'57356' => 'cp_949',
		'5488' => 'GB18030-2000',
		'57356' => 'KS5601',
		'57356' => 'ksc',
		'57357' => 'gb',
		'57357' => 'GB2312-80',
		'5488' => 'GB18030-2000',
		'57351' => 'ujis',
	);

	return ($locale . '.' . $ENCODING{$code_set}, 'iso-' . $ENCODING{$code_set});
}


=head2 _table_info

This function retrieves all Informix tables information.

Returns a handle to a DB query statement.

=cut

sub _table_info
{
	my $self = shift;
	my $do_real_row_count = shift;

	# When read from input file use dedicated function
	return _table_info_from_file($self) if ($self->{input_file});

	# First register all tablespace/table in memory from this database
	my %tbspname = ();

	my $sql2 = qq{SELECT
  sum(pt.npused  * pt.pagesize)/(1024*1024) as UsedSpaceMB,
  sum(pt.nptotal * pt.pagesize)/(1024*1024) as TotalSpaceMB
FROM sysmaster:sysptnhdr pt
LEFT JOIN sysmaster:systabnames tn ON tn.partnum = pt.partnum
WHERE tn.tabname = ? AND tn.dbsname='$self->{schema}'
};
	my $sth2 = $self->{dbh}->prepare( $sql2 ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my $sql = qq{SELECT
  t.tabname AS TABLE_NAME, '' AS comment, t.tabtype as TABLE_TYPE,
  t.nrows AS RowCounts, '' AS UsedSpaceMB,
  '' AS TotalSpaceMB, t.dbname AS TABLE_SCHEMA, t.owner
FROM informix.systables t
WHERE t.tabtype='T' AND t.owner != 'informix'
};
	my %tables_infos = ();
	$sql .= $self->limit_to_objects('TABLE', 't.tabname');
	$sql .= " ORDER BY t.tabname";

	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[6].$row->[0]";
		}
		$tables_infos{$row->[0]}{owner} = $row->[7] || $row->[6];
		$tables_infos{$row->[0]}{num_rows} = $row->[3] || 0;
		$tables_infos{$row->[0]}{comment} = ''; # Informix doesn't have COMMENT
		$tables_infos{$row->[0]}{type} =  'TABLE';
		$tables_infos{$row->[0]}{nested} = 'NO';
		$tables_infos{$row->[0]}{tablespace} = 0;
		$tables_infos{$row->[0]}{auto_increment} = 0;
		$tables_infos{$row->[0]}{tablespace} = $tbspname{$row->[0]} || '';
		$tables_infos{$row->[0]}{partitioned} = 1 if (exists $self->{partitions_list}{"\L$row->[0]\E"});
		$sth2->execute($row->[0]) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		my $r = $sth2->fetch;
		$tables_infos{$row->[0]}{size} = sprintf("%.3f", $r->[0]) || 0;
	}
	$sth->finish();
	$sth2->finish();

	if ($do_real_row_count)
	{
		foreach my $t (keys %tables_infos)
		{
			$self->logit("DEBUG: looking for real row count for table $t (aka using count(*))...\n", 1);
			my $tbname = $t;
			$sql = "SELECT COUNT(*) FROM $tbname";
			$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			my $size = $sth->fetch();
			$sth->finish();
			$tables_infos{$t}{num_rows} = $size->[0];
		}
	}

	return %tables_infos;
}

sub _table_info_from_file
{
	my $self = shift;

	my %tables_infos = ();

	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	my $table = '';
	while (my $l = <$fh>)
	{
		chomp($l);
		if ($l =~ /create table ([^\.]+).([^\s]+)/)
		{
			next if ($1 eq '"informix"' || $1 eq 'informix');
			$table = $2;
			$tables_infos{$table}{owner} = $1;
			$tables_infos{$table}{num_rows} = 0;
			$tables_infos{$table}{comment} = ''; # Informix doesn't have COMMENT
			$tables_infos{$table}{type} =  'TABLE';
			$tables_infos{$table}{nested} = 'NO';
			$tables_infos{$table}{auto_increment} = 0;
			$tables_infos{$table}{tablespace} = '';
			$tables_infos{$table}{size} = 0;
			$tables_infos{$table}{owner} =~ s/"//g ;
		} elsif ($table && $l =~ /;/) {
			$table = '';
		} elsif ($table && $l =~ /fragment by/) {
			$tables_infos{$table}{partitioned} = 1;
		}
	}
	$fh->close();

	return %tables_infos;
}


sub _column_comments
{
	my ($self, $table) = @_;

	return ; # SQL Server doesn't have COMMENT and we don't play with "Extended Properties"
}

sub _column_info
{
	my ($self, $table, $owner, $objtype, $recurs) = @_;

	return _column_info_from_file($self, $table, $owner, $objtype, $recurs) if ($self->{input_file});

	my %informix_coltype = (
		0 => 'CHAR',
		1 => 'SMALLINT',
		2 => 'INTEGER',
		3 => 'FLOAT',
		4 => 'SMALLFLOAT',
		5 => 'DECIMAL',
		6 => 'SERIAL',
		7 => 'DATE',
		8 => 'MONEY',
		9 => 'NULL',
		10 => 'DATETIME',
		11 => 'BYTE',
		12 => 'TEXT',
		13 => 'VARCHAR',
		14 => 'INTERVAL',
		15 => 'NCHAR',
		16 => 'NVARCHAR',
		17 => 'INT8',
		18 => 'SERIAL8',
		19 => 'SET',
		20 => 'MULTISET',
		21 => 'LIST',
		22 => 'ROW (unnamed)',
		23 => 'COLLECTION',
		40 => 'LVARCHAR',
		41 => 'BLOB, BOOLEAN, CLOB variable-length opaque types 2',
		43 => 'LVARCHAR',
		45 => 'BOOLEAN',
		52 => 'BIGINT',
		53 => 'BIGSERIAL',
		2061 => 'IDSSECURITYLABEL',
		3880 => 'TimeSeries',
		4118 => 'ROW (named)',
	);

	$objtype ||= 'TABLE';

	my $condition = '';
	$condition = "AND tb.name='$table' " if ($table);
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE', 'tb.name');
	} else {
		@{$self->{query_bind_params}} = ();
	}
	#$condition =~ s/^\s*AND\s/ WHERE /;

	my $str = qq{SELECT 
    c.colname, (CASE WHEN c.coltype >= 256 THEN c.coltype - 256 ELSE c.coltype END), c.collength,
    (CASE WHEN c.coltype - 256 >= 0 THEN 0 ELSE 1 END), sd.default,
    (CASE WHEN c.coltype IN (5, 8, 261, 264) THEN ifx_bit_rightshift(c.collength, 8) ELSE 0 END),
    (CASE WHEN c.coltype IN (5, 8, 261, 264) THEN bitand(c.collength, "0xff") ELSE 0 END),
    '', tb.tabname, tb.owner, '', c.colno,
    (CASE WHEN c.coltype - 256 = 6 OR c.coltype - 256 = 18 OR c.coltype - 256 = 53 THEN c.colmin - 1 ELSE 0 END),
    c.extended_id, decode(bitand(c.coltype, "0xff"), 5, "decimal", 8, "money", "<...>") || "(" ||
        ifx_bit_rightshift(c.collength, 8) || "," || bitand(c.collength, "0xff") || ")",
    st.name
FROM syscolumns AS c
JOIN systables AS tb ON (tb.tabid = c.tabid)
LEFT JOIN sysdefaults AS sd ON (sd.tabid = c.tabid AND sd.colno = c.colno)
LEFT JOIN sysxtdtypes AS st ON (st.extended_id = c.extended_id)
WHERE tb.owner != 'informix' $condition
ORDER BY c.tabid, c.colno};

	my $sth = $self->{dbh}->prepare($str);
	if (!$sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	# Expected columns information stored in hash 
	# COLUMN_NAME,DATA_TYPE,DATA_LENGTH,NULLABLE,DATA_DEFAULT,DATA_PRECISION,DATA_SCALE,CHAR_LENGTH,TABLE_NAME,OWNER,VIRTUAL_COLUMN,POSITION,AUTO_INCREMENT,ENUM_INFO
	my %data = ();
	my $pos = 0;
	while (my $row = $sth->fetch)
	{
		my $old_type = $row->[1];
		if ($row->[1] == 41 || $row->[1] == (41+256)) {
			# Get the real datatype for variable-length opaque types
			$row->[1] = $row->[15];
		} else {
			# get the corresponding builtin datatype or UDT name. Keep the datatype id if not found.
			$row->[1] = $informix_coltype{$row->[1]} || $row->[15] || $row->[1];
		}
		if ($row->[1] =~ /serial/i)
		{
			$row->[12] = $row->[6];
			$self->{tables}{$row->[8]}{table_info}{auto_increment} = $row->[12];
		}
		push(@{$data{"$row->[8]"}{"$row->[0]"}}, @$row);
		$pos++;
	}

	return %data;
}

sub _column_info_from_file
{
	my ($self, $table, $owner, $objtype, $recurs) = @_;

	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	my %data = ();
	my $pos = 0;
	while (my $l = <$fh>)
	{
		chomp($l);
		if ($l =~ /create table ([^\.]+)\.([^\s\(]+)/)
		{
			next if ($1 eq '"informix"' || $1 eq 'informix');
			my $tbname = $2;
			next if ($table && lc($table) ne lc($tbname));
			my $tb_code = '';
			while ($l = <$fh>)
			{
				chomp($l);
				$tb_code .= "\n" if ($l =~ /^\s*\)/);
				$tb_code .= $l;
				$tb_code .= "\n" if ($l =~ /^\s*\(/ || $l =~ /,\s*$/);
				last if ($l =~ /;$/);
			}
			$pos++;
		}
	}
	$fh->close;

	return %data;
}


sub _get_indexes
{
	my ($self, $table, $owner, $generated_indexes) = @_;

	return _get_indexes_from_file($self, $table, $owner, $generated_indexes) if ($self->{input_file});

	my $condition = '';
	$condition .= "AND t.tabname='$table' " if ($table);
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE|INDEX', "t.tabname|Id.idxname");
	} else {
		@{$self->{query_bind_params}} = ();
	}

	my $t0 = Benchmark->new;
	my $sth = '';
	my $sql = qq{SELECT Id.idxname, Id.idxtype, Id.clustered,
	(CASE WHEN Id.idxtype = 'U' THEN 1 ELSE 0 END), Id.indexkeys, t.tabname, Id.owner,
	c1.colname col1, c2.colname col2, c3.colname col3, c4.colname col4, c5.colname col5,
	c6.colname col6, c7.colname col7, c8.colname col8, c9.colname col9, c10.colname col10
FROM systables AS T
INNER JOIN sysindices Id ON T.tabid = Id.tabid 
  left JOIN sysindexes i ON (i.tabid = Id.tabid AND i.idxname = Id.idxname)
  left outer join syscolumns c1 on c1.tabid = T.tabid and c1.colno = abs(i.part1)
  left outer join syscolumns c2 on c2.tabid = T.tabid and c2.colno = abs(i.part2)
  left outer join syscolumns c3 on c3.tabid = T.tabid and c3.colno = abs(i.part3)
  left outer join syscolumns c4 on c4.tabid = T.tabid and c4.colno = abs(i.part4)
  left outer join syscolumns c5 on c5.tabid = T.tabid and c5.colno = abs(i.part5)
  left outer join syscolumns c6 on c6.tabid = T.tabid and c6.colno = abs(i.part6)
  left outer join syscolumns c7 on c7.tabid = T.tabid and c7.colno = abs(i.part7)
  left outer join syscolumns c8 on c8.tabid = T.tabid and c8.colno = abs(i.part8)
  left outer join syscolumns c9 on c9.tabid = T.tabid and c9.colno = abs(i.part9)
  left outer join syscolumns c10 on c10.tabid = T.tabid and c10.colno = abs(i.part10)
WHERE Id.owner != 'informix' $condition
ORDER BY T.tabname, Id.idxname
};

	$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	my %unique = ();
	my %idx_type = ();
	my %index_tablespace = ();
	my $nidx = 0;
	while (my $row = $sth->fetch)
	{
		my $save_tb = $row->[5];
		next if (!$self->is_in_struct($row->[5], $row->[1]));

		# Show a warning when an index has the same name as the table
		if ( !$self->{indexes_renaming} && !$self->{indexes_suffix} && (lc($row->[0]) eq lc($table)) ) {
			 print STDERR "WARNING: index $row->[0] has the same name as the table itself. Please rename it before export or enable INDEXES_RENAMING.\n";
		}
		$row->[4] =~ s/\s*\[\d+\]//g;
		$row->[4] =~ s/\-(\d+)/$1 DESC/g;

		if ($row->[1] eq 'U') {
			$unique{$row->[5]}{$row->[0]} = 'UNIQUE';
		}

		$idx_type{$row->[5]}{$row->[0]}{type_name} = $row->[11];
		$idx_type{$row->[5]}{$row->[0]}{type} = $row->[1];

		my @cols = ();
		for (my $i = 7; $i <= 16; $i++) {
			push(@cols, $self->quote_object_name($row->[$i])) if ($row->[$i]);
		}
		push(@{$data{$row->[5]}{$row->[0]}}, @cols);
		$nidx++;
	}
	$sth->finish();
	my $t1 = Benchmark->new;
	my $td = timediff($t1, $t0);
	$self->logit("Collecting $nidx indexes in sysindices took: " . timestr($td) . "\n", 1);

	return \%unique, \%data, \%idx_type, \%index_tablespace;
}

sub _get_indexes_from_file
{
	my ($self, $table, $owner, $generated_indexes) = @_;

	my %data = ();
	my %unique = ();
	my %idx_type = ();
	my %index_tablespace = ();


	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	while (my $l = <$fh>)
	{
		chomp($l);
		if ($l =~ /create index|create unique index/)
		{
			my $idx_code = $l;
			while ($l = <$fh>)
			{
				chomp($l);
				$l =~ s/^\s+\./\./;
				$idx_code .= $l;
				last if ($l =~ /;/);
			}

			my $tbname = '';
			my $idxname = '';
			if ($idx_code =~ /create index ([^\.]+).([^\s]+) on ([^\.]+).([^\s]+)/i)
			{
				next if ($1 eq '"informix"' || $1 eq 'informix');
				$idxname = $2;
				$tbname = $4;
				next if ($table && $tbname ne $table);
			}
			elsif ($idx_code =~ /create unique index ([^\.]+).([^\s]+) on ([^\.]+).([^\s]+)/i)
			{
				next if ($1 eq '"informix"' || $1 eq 'informix');
				$idxname = $2;
				$tbname = $4;
				next if ($table && $tbname ne $table);
				$unique{$tbname}{$idxname} = 'UNIQUE';
				$idx_type{$tbname}{$idxname}{type} = 'U';
			}

			if ( $idxname && !$self->{indexes_renaming} && !$self->{indexes_suffix} && (lc($idxname) eq lc($table)) ) {
				 print STDERR "WARNING: index $idxname has the same name as the table itself. Please rename it before export or enable INDEXES_RENAMING.\n";
			}

			if ($idxname && $l =~ /using ([^\s]+)/) {
				$idx_type{$tbname}{$idxname}{type_name} = $1;
			}

			$idx_code =~ s/.*\(([^\)]+)\).*/$1/;
			my @cols = split(/\s*,\s*/, $idx_code);
			push(@{$data{$tbname}{$idxname}}, @cols);
		}
	}
	$fh->close;

	return \%unique, \%data, \%idx_type, \%index_tablespace;
}


sub _count_indexes
{
	my ($self, $table, $owner) = @_;

	my $condition = '';
	$condition = " FROM $self->{schema}" if ($self->{schema});
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE|INDEX', "`Table`|`Key_name`");
	} else {
		@{$self->{query_bind_params}} = ();
	}
	$condition =~ s/ AND / WHERE /;

	my %tables_infos = ();
	if ($table) {
		$tables_infos{$table} = 1;
	} else {
		%tables_infos = Ora2Pg::Informix::_table_info($self);
	}
	my %data = ();

	# Retrieve all indexes for the given table
	foreach my $t (keys %tables_infos)
	{
		my $sql = "SHOW INDEX FROM `$t` $condition";
		my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

		my $i = 1;
		while (my $row = $sth->fetch)
		{
			push(@{$data{$row->[0]}{$row->[2]}}, $row->[4]);
		}
	}

	return \%data;
}


sub _foreign_key
{
        my ($self, $table, $owner) = @_;

        my $condition = '';
	$condition .= "AND t.tabname = '$table'" if ($table);

        my $deferrable = $self->{fkey_deferrable} ? "'DEFERRABLE' AS DEFERRABLE" : "DEFERRABLE";
	my $sql = qq{select
  tab.tabname,
  constr.*, 
  c1.colname col1,
  c2.colname col2,
  c3.colname col3,
  c4.colname col4,
  c5.colname col5,
  c6.colname col6,
  c7.colname col7
from sysconstraints constr
  join systables tab on tab.tabid = constr.tabid
  left outer join sysindexes i on i.idxname = constr.idxname
  left outer join syscolumns c1 on c1.tabid = tab.tabid and c1.colno = abs(i.part1)
  left outer join syscolumns c2 on c2.tabid = tab.tabid and c2.colno = abs(i.part2)
  left outer join syscolumns c3 on c3.tabid = tab.tabid and c3.colno = abs(i.part3)
  left outer join syscolumns c4 on c4.tabid = tab.tabid and c4.colno = abs(i.part4)
  left outer join syscolumns c5 on c5.tabid = tab.tabid and c5.colno = abs(i.part5)
  left outer join syscolumns c6 on c6.tabid = tab.tabid and c6.colno = abs(i.part6)
  left outer join syscolumns c7 on c7.tabid = tab.tabid and c7.colno = abs(i.part7)
where constr.constrtype = 'R' AND tab.owner != 'informix';
  };

	$sql = qq{SELECT st.tabname, st.owner, rt.tabname, rt.owner, sr.primary, sr.ptabid,
      sr.delrule, sc.constrid, sc.constrname, sc.constrtype, sc.owner, 
      si.idxname, si.tabid, si.part1, si.part2, si.part3,  
      si.part4, si.part5, si.part6, si.part7, si.part8,  
      si.part9, si.part10, si.part11, si.part12, si.part13,  
      si.part14, si.part15, si.part16, rc.tabid, os.state, os2.state 
FROM informix.systables st, informix.sysconstraints sc, 
     informix.sysindexes si, informix.sysreferences sr, 
     informix.systables rt, informix.sysconstraints rc, 
     informix.sysobjstate os, informix.sysobjstate os2 
WHERE st.tabid = sc.tabid 
  AND st.tabtype != 'Q' 
  AND st.tabname NOT MATCHES 'cdr_deltab_[0-9][0-9][0-9][0-9][0-9][0-9]*' 
  AND rt.tabid = sr.ptabid 
  AND sc.constrid = sr.constrid 
  AND sc.tabid = si.tabid 
  AND sc.idxname = si.idxname 
  AND sc.constrtype = 'R' 
  AND os.tabid = st.tabid AND os.name = sc.constrname AND os.objtype = 'C' 
  AND os2.tabid = st.tabid AND os2.name = si.idxname AND os2.objtype = 'I' 
  AND sr.primary = rc.constrid
  AND st.owner != 'informix'
};

        $self->{dbh}->do($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

        my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);
        my @cons_columns = ();
	my $i = 1;
        my %data = ();
        my %link = ();
        while (my $r = $sth->fetch)
	{
		push(@{$link{$r->[0]}{$r->[8]}{local}}, $r->[2]);
		push(@{$link{$r->[0]}{$r->[8]}{remote}{$r->[4]}}, $r->[5]);
		# SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,$deferrable,DEFERRED,R_OWNER,TABLE_NAME,OWNER,UPDATE_RULE
                push(@{$data{$r->[0]}}, [ ($r->[8], $r->[8], '', $r->[5], 'DEFERRABLE', 'Y', '', $r->[0], '', '') ]);
		$i++;
        }
	$sth->finish();

        return \%link, \%data;
}

=head2 _get_views

This function implements an Oracle-native views information.

Returns a hash of view names with the SQL queries they are based on.

=cut

sub _get_views
{
	my ($self) = @_;

	return _get_views_from_file($self) if ($self->{input_file});

	my %comments = ();
	# Retrieve all views
	my $str = qq{SELECT
	t.tabname, v.seqno, v.viewtext
FROM systables t
JOIN sysviews v ON (v.tabid = t.tabid)
WHERE t.tabtype = 'V' AND t.owner != 'informix'
};
	$str .= $self->limit_to_objects('VIEW', 't.tabname');
	$str .= " ORDER BY t.tabname, v.seqno";


	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %ordered_view = ();
	my %data = ();
	while (my $row = $sth->fetch)
	{
		$row->[2] =~ s///g;
		$data{$row->[0]}{text} .= $row->[2];
		$data{$row->[0]}{owner} = '';
		$data{$row->[0]}{comment} = '';
		$data{$row->[0]}{check_option} = '';
		$data{$row->[0]}{updatable} = 'Y';
		$data{$row->[0]}{definer} = '';
		$data{$row->[0]}{security} = '';
	}
	return %data;
}

sub _get_views_from_file
{
	my ($self) = @_;

	# Retrieve all views
	my %data = ();
	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	my $vname = '';
	while (my $l = <$fh>)
	{
		chomp($l);

		if ($l =~ s/create view ([^\.]+).([^\s]+)//)
		{
			next if ($1 eq '"informix"' || $1 eq 'informix');
			my $vname = $2;
			$data{$vname}{text} = "$l\n";
			while ($l = <$fh>)
			{
				chomp($l);
				$data{$vname}{text} .= "$l\n";
				last if ($l =~ /;\s*$/);
			}
			$data{$vname}{owner} = '';
			$data{$vname}{comment} = '';
			$data{$vname}{check_option} = '';
			$data{$vname}{updatable} = 'Y';
			$data{$vname}{definer} = '';
			$data{$vname}{security} = '';
		}
	}
	$fh->close;

	return %data;
}


sub _get_triggers
{
	my($self) = @_;

	return _get_triggers_from_file($self) if ($self->{input_file});

	my $str = qq{SELECT 
    t.tabname,
    trg.trigid,
    trg.trigname,
    trg.owner,
    trg.tabid,
    trg.event,
    trg.old,
    trg.new,
    trg.mode,
    trg.collation,
    b.datakey,
    b.seqno,
    b.data
FROM systriggers trg
INNER JOIN systables t ON t.tabid = trg.tabid
JOIN systrigbody b ON b.trigid = trg.trigid
WHERE t.owner != 'informix' AND trg.owner != 'informix' AND b.datakey IN ('A', 'D')
};

	$str .= " " . $self->limit_to_objects('TABLE|VIEW|TRIGGER','t.tabname|t.tabname|trg.trigname');

	$str .= " ORDER BY t.tabname, trg.trigname, b.seqno, b.datakey";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @triggers = ();
	while (my $row = $sth->fetch)
	{
		my $kind = 'AFTER'; # only FOR=AFTER trigger in this field, no BEFORE
		my $text = $row->[12];
		if ($row->[5] =~ /^(d|i|u)$/) {
			$kind = 'INSTEAD OF';
		} elsif ($text =~ s/^\s*(after|before)\b//i) {
			$kind = uc($1);
		} elsif ($text =~ s/for each row//i) {
		}
		my @actions = ();
		push(@actions, 'INSERT') if ($row->[5] =~ /I/i);
		push(@actions, 'UPDATE') if ($row->[5] =~ /U/i);
		push(@actions, 'DELETE') if ($row->[5] =~ /D/i);
		my $act = join(' OR ', @actions);
		if ($row->[11] == 0 && $row->[10] eq 'A')
		{
			push(@triggers, [ ($row->[2], $kind, $act, $row->[0], $text, '', 'ROW', $row->[1]) ]);
		}
		else
		{
			if ($text =~ /update of (.*?) on /is) {
				$triggers[-1]->[2] = 'UPDATE OF ' . $1;
			}
			$triggers[-1]->[6] = '' if ($text =~ /\breferencing /);
			my $head = 'REFERENCING ';
			if ($text =~ s/\breferencing ([^\s]+) as ([^\s]+) ([^\s]+) as ([^\s]+)//is) {
				$triggers[-1]->[6] = 'REFERENCING ' . uc($1) . ' TABLE AS ' . $2 . ' ' . uc($3) . ' TABLE AS ' . $4;
			}  elsif ($text =~ s/\breferencing ([^\s]+) as ([^\s]+)//is) {
				$triggers[-1]->[6] = 'REFERENCING ' . uc($1) . ' TABLE AS ' . $2;
			}
			$triggers[-1]->[4] .= $text if ($row->[10] eq 'A');
		}
	}

	# clean triggers code
	for (my $i = 0; $i <= $#triggers; $i++)
	{
		$triggers[$i]->[4] =~ s/^\s*\(\s*(.*)\s*\)\s*;\s*$/$1;/s;
		$triggers[$i]->[4] =~ s/execute procedure/CALL/igs;
		$triggers[$i]->[4] =~ s/ with trigger references//is;
	}

	return \@triggers;
}

sub _get_triggers_from_file
{
	my($self) = @_;

	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	my $tbname = '';
	my @triggers = ();
	while (my $l = <$fh>)
	{
		chomp($l);

		if ($l =~ /create trigger ([^\.]+).([^\s]+)/)
		{
			next if ($1 eq '"informix"' || $1 eq 'informix');
			my $trigname = $2;
			my $trig_code = $l;
			my $code_pos = 0;
			my $nline = 1;
			while ($l = <$fh>)
			{
				chomp($l);
				$code_pos = length($trig_code), $trig_code .= "\n" if ($l =~ /^\s*\(\s*$/);
				$trig_code .= $l;
				$trig_code .= "\n" if ($l =~ /^\s*\(\s*$/);
				last if ($l =~ /;$/);
				$nline++;
			}

			if ($trig_code =~ /create trigger ([^\.]+).([^\s]+) ([^\s]+) on ([^\.]+).([^\s]+)/is)
			{
				my @actions = ($3);
				my $tbname = $5;
				my $kind = 'AFTER'; # only FOR=AFTER trigger in this field, no BEFORE
				my $type = 'ROW';
				my $owner = $1;

				if ($trig_code =~ /instead of/is) {
					$kind = 'INSTEAD OF';
				} elsif ($trig_code =~ /^\s*(after|before)\b/i) {
					$kind = uc($1);
				}
				my $act = join(' OR ', @actions);
				if ($trig_code =~ /update of (.*?) on /is) {
					$act = 'UPDATE OF ' . $1;
				}
				$type = '' if ($trig_code =~ /\breferencing /);
				my $head = 'REFERENCING ';
				if ($trig_code =~ /\breferencing ([^\s]+) as ([^\s]+) ([^\s]+) as ([^\s]+)/is) {
					$type = 'REFERENCING ' . uc($1) . ' TABLE AS ' . $2 . ' ' . uc($3) . ' TABLE AS ' . $4;
				}  elsif ($trig_code =~ /\breferencing ([^\s]+) as ([^\s]+)/is) {
					$type = 'REFERENCING ' . uc($1) . ' TABLE AS ' . $2;
				}
				$trig_code = substr($trig_code, $code_pos);
				push(@triggers, [ ($trigname, $kind, $act, $tbname, $trig_code, '', 'ROW', $owner) ]);
			}
		}
	}
	$fh->close;

	# clean triggers code
	for (my $i = 0; $i <= $#triggers; $i++)
	{
		$triggers[$i]->[4] =~ s/^\s*\(\s*(.*)\s*\)\s*;\s*$/$1;/s;
		$triggers[$i]->[4] =~ s/ with trigger references//is;
	}

	return \@triggers;
}


sub _unique_key
{
	my ($self, $table, $owner) = @_;

	return _unique_key_from_file($self, $table, $owner) if ($self->{input_file});

	my %result = ();
        my @accepted_constraint_types = ();

        push @accepted_constraint_types, "'P'" unless($self->{skip_pkeys});
        push @accepted_constraint_types, "'U'" unless($self->{skip_ukeys});
        return %result unless(@accepted_constraint_types);

        my $condition = '';
        $condition .= " AND tab.tabname = '$table' " if ($table);
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE|INDEX', "tab.tabname|constr.constrname");
	} else {
		@{$self->{query_bind_params}} = ();
	}

	my $sql = qq{SELECT
  tab.tabname,
  constr.constrid,
  constr.constrname,
  constr.owner,
  constr.tabid,
  constr.constrtype,
  constr.idxname,
  constr.collation,
  c1.colname col1,
  c2.colname col2,
  c3.colname col3,
  c4.colname col4,
  c5.colname col5,
  c6.colname col6,
  c7.colname col7
FROM sysconstraints constr
  JOIN systables tab ON tab.tabid = constr.tabid
  LEFT OUTER JOIN sysindexes i on i.idxname = constr.idxname
  LEFT OUTER JOIN syscolumns c1 ON c1.tabid = tab.tabid AND c1.colno = abs(i.part1)
  LEFT OUTER JOIN syscolumns c2 ON c2.tabid = tab.tabid AND c2.colno = abs(i.part2)
  LEFT OUTER JOIN syscolumns c3 ON c3.tabid = tab.tabid AND c3.colno = abs(i.part3)
  LEFT OUTER JOIN syscolumns c4 ON c4.tabid = tab.tabid AND c4.colno = abs(i.part4)
  LEFT OUTER JOIN syscolumns c5 ON c5.tabid = tab.tabid AND c5.colno = abs(i.part5)
  LEFT OUTER JOIN syscolumns c6 ON c6.tabid = tab.tabid AND c6.colno = abs(i.part6)
  LEFT OUTER JOIN syscolumns c7 ON c7.tabid = tab.tabid AND c7.colno = abs(i.part7)
WHERE constr.constrtype IN ('P', 'U') AND tab.owner != 'informix';
  };

	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch)
	{
		my $name = $row->[0];
		my $idxname = $row->[2];
		my $key_type = $row->[5];
		next if (!grep(/$key_type/, @accepted_constraint_types));

		my @cols = ();
		for (my $i = 8; $i <= 14; $i++) {
			push(@cols, $row->[$i]) if ($row->[$i]);
		}
		my %constraint = (type => $row->[5], 'generated' => 'N', 'index_name' => $idxname, columns => \@cols );
		$result{$name}{$idxname} = \%constraint;
	}

	return %result;
}

sub _unique_key_from_file
{
	my ($self, $table, $owner) = @_;


	my %result = ();
        my @accepted_constraint_types = ();

        push @accepted_constraint_types, "P" unless($self->{skip_pkeys});
        push @accepted_constraint_types, "U" unless($self->{skip_ukeys});
        return %result unless(@accepted_constraint_types);

	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	my $tbname = '';
	while (my $l = <$fh>)
	{
		chomp($l);

		$tbname = '' if ($l =~ /;$/);

		if ($l =~ /create table ([^\.]+).([^\s]+)/)
		{
			next if ($1 eq '"informix"' || $1 eq 'informix');
			$tbname = $2;
			next if ($table && lc($tbname) ne lc($table));

			my $tb_code = '';
			while ($l = <$fh>)
			{
				chomp($l);
				$tb_code .= "\n" if ($l =~ /^\s*\)/);
				$tb_code .= $l;
				$tb_code .= "\n" if ($l =~ /^\s*\(/ || $l =~ /,\s*$/);
				last if ($l =~ /;$/);
			}

			foreach $l (split(/\n/, $tb_code))
			{
				next if ($l !~ /^\s*(unique\s*\(|primary\s+key)/);
				$l =~ /(?:unique|primary key)\s+\(([^\)]+)\)\s+constraint ([^\.]+)\.([^\s,]+)/;
				my $idxname = $3;
				my @cols = split(/\s*,\s*/, $1);
				my $key_type = "U";
				$key_type = "P" if ($l =~ /^\s*primary\s+key/);

				next if (!grep(/$key_type/, @accepted_constraint_types));

				my %constraint = (type => $key_type, 'generated' => 'N', 'index_name' => $idxname, columns => \@cols );
				$result{$tbname}{$idxname} = \%constraint;
			}
		}
	}
	$fh->close;

	return %result;
}

sub _check_constraint
{
	my ($self, $table, $owner) = @_;

	return _check_constraint_from_file($self, $table, $owner) if ($self->{input_file});

	my $condition = '';
	$condition .= " AND st.tabname = '$table' " if ($table);
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE|INDEX', "st.tabname|co.constrname");
	} else {
		@{$self->{query_bind_params}} = ();
	}

	my $sql = qq{SELECT st.tabname, st.owner, co.constrname, ch.*
FROM systables st, sysconstraints co, syschecks ch
WHERE co.constrtype = 'C' AND co.tabid = st.tabid AND co.constrid = ch.constrid
AND ch.type = 'T' AND st.owner != 'informix'
$condition
ORDER BY st.tabname, ch.seqno};

        my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

        my %data = ();
        while (my $row = $sth->fetch)
	{
		$row->[6] =~ s/^\(//s;
		$row->[6] =~ s/\)$//s;
                $data{$row->[0]}{constraint}{$row->[2]}{condition} = $row->[6];
		$data{$row->[0]}{constraint}{$row->[2]}{validate}  = 'VALIDATED';
        }

	return %data;
}

sub _check_constraint_from_file
{
	my ($self, $table, $owner) = @_;

	my %data = ();

	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	my $tbname = '';
	while (my $l = <$fh>)
	{
		chomp($l);

		$tbname = '' if ($l =~ /;$/);

		if ($l =~ /create table ([^\.]+).([^\s]+)/)
		{
			next if ($1 eq '"informix"' || $1 eq 'informix');
			$tbname = $2;

			next if ($table && lc($tbname) ne lc($table));

			my $tb_code = '';
			while ($l = <$fh>)
			{
				chomp($l);
				$tb_code .= "\n" if ($l =~ /^\s*\)/);
				$tb_code .= $l;
				$tb_code .= "\n" if ($l =~ /^\s*\(/ || $l =~ /,\s*$/);
				last if ($l =~ /;$/);
			}

			foreach $l (split(/\n/, $tb_code))
			{
				next if ($l !~ /^\s*check\s*\(/);
				$l =~ /check\s*\((.*?)\)\s+constraint ([^\.]+)\.([^\s,]+)/;
				my $idxname = $3;
				my $expr = $1;

				$data{$tbname}{constraint}{$idxname}{condition} = $expr;
				$data{$tbname}{constraint}{$idxname}{validate}  = 'VALIDATED';
			}
		}
	}
	$fh->close;


	return %data;
}


sub _get_external_tables
{
	my ($self) = @_;

	# There is no external table in Informix
	return;
}

sub _get_directory
{
	my ($self) = @_;

	# There is no external table in Informix
	return;
}

sub _get_functions
{
	my $self = shift;

	return _get_functions_from_file($self) if ($self->{input_file});

	# Retrieve all functions 
	my $str = qq{SELECT
    p.procname,
    p.externalname, -- location of externale routine
    p.paramtypes,
    p.handlesnulls,
    p.isproc,
    l.langname,
    b.datakey,
    b.seqno,
    b.data
FROM sysprocedures p
    join sysroutinelangs l ON (l.langid = p.langid)
    join sysprocbody b ON (b.procid = p.procid)
WHERE 
    p.isproc = 'f' AND p.owner NOT IN ('informix', 'sysibm', 'sysproc', 'sysfun', 'sqlj')
    AND b.datakey IN ('T', 'D')
};
	$str .= " " . $self->limit_to_objects('FUNCTION','p.procname');
	$str .= " ORDER BY p.procname, b.seqno";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %functions = ();
	while (my $row = $sth->fetch)
	{
		$functions{"$row->[0]"}{kind} = 'FUNCTION';
		$functions{"$row->[0]"}{name} = $row->[0];
		if ($row->[6] eq 'T') {
			$functions{"$row->[0]"}{text} .= $row->[8];
		} else {
			$functions{"$row->[0]"}{comment} .= $row->[8];
		}
		$functions{"$row->[0]"}{strict} = $row->[3] || '';
		$functions{"$row->[0]"}{security} = 'EXECUTER';
	}

	return \%functions;
}

sub _get_functions_from_file
{
	my $self = shift;

	# Retrieve all functions 
	my %functions = ();

	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	my %data = ();
	my $pos = 0;
	while (my $l = <$fh>)
	{
		chomp($l);
		if ($l =~ /create function ([^\.]+)\.([^\s\(]+)/is)
		{
			next if ($1 eq '"informix"' || $1 eq 'informix');
			my $fctname = $2;
			my $fct_code = $l;
			my $end_found = 0;
			if (exists $self->{limited}{FUNCTION}) {
				next if (!grep(/^$fctname$/i, @{$self->{limited}{FUNCTION}}));
			} elsif (exists $self->{limited}{ALL}) {
				next if (!grep(/^$fctname$/i, @{$self->{limited}{ALL}}));
			} elsif (exists $self->{excluded}{FUNCTION}) {
				next if (grep(/^$fctname$/i, @{$self->{excluded}{FUNCTION}}));
			} elsif (exists $self->{excluded}{ALL}) {
				next if (grep(/^$fctname$/i, @{$self->{excluded}{ALL}}));
			}
			while ($l = <$fh>)
			{
				chomp($l);
				$fct_code .= "$l\n";
				if ($fct_code =~ /end function/i) {
					$end_found = 1;
				}
				last if ($end_found && $l =~ /;$/);
			}
			$functions{$fctname}{kind} = 'FUNCTION';
			$functions{$fctname}{name} = $fctname;
			$functions{$fctname}{text} = $fct_code;
			if ($fct_code =~ s/(end function)\s+(.*);/$1;/is) {
				$functions{$fctname}{comment} = $1;
			}
			$functions{$fctname}{strict} = '';
			$functions{$fctname}{security} = 'EXECUTER';
		}
	}
	$fh->close;

	return \%functions;
}

sub _get_procedures
{
	my $self = shift;

	return _get_procedures_from_file($self) if ($self->{input_file});

	# Retrieve all functions 
	my $str = qq{SELECT
    p.procname,
    p.externalname, -- location of externale routine
    p.paramtypes,
    p.handlesnulls,
    p.isproc,
    l.langname,
    b.datakey,
    b.seqno,
    b.data,
    p.owner
FROM sysprocedures p
    join sysroutinelangs l ON (l.langid = p.langid)
    join sysprocbody b ON (b.procid = p.procid)
WHERE 
    p.isproc = 't' AND p.owner NOT IN ('informix', 'sysibm', 'sysproc', 'sysfun', 'sqlj')
    AND b.datakey IN ('T', 'D')
};
	$str .= " " . $self->limit_to_objects('PROCEDURE','p.procname');
	$str .= " ORDER BY p.procname, b.seqno";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %functions = ();
	while (my $row = $sth->fetch)
	{
		$functions{"$row->[0]"}{name} = $row->[0];
		if ($row->[6] eq 'T') {
			$functions{"$row->[0]"}{text} .= $row->[8];
		} else {
			$functions{"$row->[0]"}{comment} .= $row->[8];
		}
		$functions{"$row->[0]"}{kind} = 'PROCEDURE';
		$functions{"$row->[0]"}{strict} = $row->[3] || '';
		$functions{"$row->[0]"}{security} = 'EXECUTER';
	}

	return \%functions;
}

sub _get_procedures_from_file
{
	my $self = shift;

	# Retrieve all procedures 
	my %procedures = ();

	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	my %data = ();
	while (my $l = <$fh>)
	{
		chomp($l);
		if ($l =~ /create procedure ([^\.]+)\.([^\s\(]+)/is)
		{
			next if ($1 eq '"informix"' || $1 eq 'informix');
			my $fctname = $2;
			my $fct_code = $l;
			my $end_found = 0;
			if (exists $self->{limited}{PROCEDURE}) {
				next if (!grep(/^$fctname$/i, @{$self->{limited}{PROCEDURE}}));
			} elsif (exists $self->{limited}{ALL}) {
				next if (!grep(/^$fctname$/i, @{$self->{limited}{ALL}}));
			} elsif (exists $self->{excluded}{PROCEDURE}) {
				next if (grep(/^$fctname$/i, @{$self->{excluded}{PROCEDURE}}));
			} elsif (exists $self->{excluded}{ALL}) {
				next if (grep(/^$fctname$/i, @{$self->{excluded}{ALL}}));
			}
			while ($l = <$fh>)
			{
				chomp($l);
				$fct_code .= "$l\n";
				if ($fct_code =~ /end procedure/i) {
					$end_found = 1;
				}
				last if ($end_found && $l =~ /;$/);
			}
			$procedures{$fctname}{kind} = 'PROCEDURE';
			$procedures{$fctname}{name} = $fctname;
			$procedures{$fctname}{text} = $fct_code;
			if ($fct_code =~ s/(end procedure)\s+(.*);/$1;/is) {
				$procedures{$fctname}{comment} = $1;
			}
			$procedures{$fctname}{strict} = '';
			$procedures{$fctname}{security} = 'EXECUTER';
		}
	}
	$fh->close;

	return \%procedures;
}

sub _lookup_function
{
	my ($self, $code, $fctname) = @_;

	my $type = 'functions';
	$type = lc($self->{type}) . 's' if ($self->{type} eq 'FUNCTION' or $self->{type} eq 'PROCEDURE');

	# Remove some unused code
	$code =~ s/\s+IF NOT EXISTS//is;

        my %fct_detail = ();
        $fct_detail{func_ret_type} = 'OPAQUE';
	my $split_word = '';

        # Split data into declarative and code part
        ($fct_detail{declare}, $split_word, $fct_detail{code}) = split(/\b(;|DEFINE|BEGIN|ON EXCEPTION|IF|WHILE|FOREACH|INSERT|DELETE|UPDATE|SELECT|DROP)\b/i, $code, 2);
	$fct_detail{code} = $split_word . $fct_detail{code} if ($split_word ne ';');
	return if (!$fct_detail{code});

	# Fix DECLARE section
	$fct_detail{declare} =~ s/;//s;
	$fct_detail{declare} =~ s/(FUNCTION|PROCEDURE)\s+([^\s\(]+)\s*(.*)/$1 $2 $3\nDECLARE/is;

	# Move all DEFINE statements found in the code into the DECLARE section
	my @lines = split(/\n/, $fct_detail{code});
	$fct_detail{code} = '';
	foreach my $l (@lines)
	{
		if ($l =~ /^\s*DEFINE\s+(.*)/i) {
			$fct_detail{declare} .= "\n$1";
			$fct_detail{declare} .= ';' if ($fct_detail{declare} !~ /;\s*$/s);
		} else {
			$fct_detail{code} .= "$l\n";
		}
	}

	# Remove empty DECLARE section
	$fct_detail{declare} =~ s/\s+DECLARE\s*$//is;

        @{$fct_detail{param_types}} = ();

	if ( ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE)\s+([^\s]+)\s+(RETURNING\s+.*)//is)
		|| ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE)\s+([^\s\(]+)(.*?)\s+(RETURNING\s+.*)//is)
		|| ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE)\s+(.*?)\s+(RETURNING\s+.*)//is)
		|| ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE)\s+([^\s\(]+)\s*(\(.*\))//is) )
	{
                $fct_detail{before} = $1;
                $fct_detail{type} = uc($2);
                $fct_detail{name} = $3;
                $fct_detail{args} = $4;
		my $tmp_returned = $5;

		if ($fct_detail{args} !~ /^\(/ && !$tmp_returned)
		{
			$tmp_returned = $fct_detail{args};
			$fct_detail{args} = '';
		}
		$type = lc($fct_detail{type} . 's');
		$tmp_returned =~ s/RETURNING\s+ROW\s*\(/RETURNS TABLE \(/is;


		if ($tmp_returned =~ /RETURNING\s+.* AS .*,.* AS /is) {
			$tmp_returned =~ s/RETURNING\s+(.*)/RETURNS record/is;
			$fct_detail{declare} .= "\nret record; -- Original returned clause: $1";
		}
		$tmp_returned =~ s/RETURNING\s+(.*)/RETURNS $1/is;
		chomp($tmp_returned);

		if ($tmp_returned =~ s/\s(DECLARE\s.*)//is) {
			$fct_detail{code} = $1 . "\n" . $fct_detail{code};
		}
		$fct_detail{args} =~ s/^\s*\(\s*\((.*)\)\s*\)$/$1/s;
		$fct_detail{args} =~ s/^\s*\(\s*(.*)\s*\)$/$1/s;
		$fct_detail{code} = "\n" . $fct_detail{code};
		$fct_detail{immutable} = '';
		$fct_detail{before} = ''; # There is only garbage for the moment

                $fct_detail{name} =~ s/['"]//g;
                $fct_detail{name} =~ s/.*\.//;
                $fct_detail{fct_name} = $fct_detail{name};
		if (!$fct_detail{args}) {
			$fct_detail{args} = '()';
		}
		$tmp_returned =~ s/^\s+//;
		$tmp_returned =~ s/\s+$//;

		if ($self->{plsql_pgsql}) {
			$fct_detail{code} =~ s/\s*on exception(.*?\send) exception\s*[;]*begin\s+(.*?)end (?:function|procedure)\s*;/\nBEGIN\n$2\nEXCEPTION$1;/isg;
			$fct_detail{code} =~ s/\s*on exception(.*?\send) exception\s+(.*?)end (?:function|procedure)\s*;/\nBEGIN\n$2\nEXCEPTION$1;/isg;
		}
		$fctname = $fct_detail{name} || $fctname;
		if ($type eq 'functions' && exists $self->{$type}{$fctname}{return} && $self->{$type}{$fctname}{return})
		{
			$fct_detail{hasreturn} = 1;
			$fct_detail{func_ret_type} = $self->_sql_type($self->{$type}{$fctname}{return});
		}
		elsif ($type eq 'functions' && !exists $self->{$type}{$fctname}{return} && $tmp_returned)
		{
			$fct_detail{func_ret_type} = replace_sql_type($self, $tmp_returned);
			$fct_detail{hasreturn} = 1;
		}
		$fct_detail{language} = $self->{$type}{$fctname}{language};
		$fct_detail{security} = $self->{$type}{$fctname}{security};

		# Procedure that have out parameters are functions with PG
		if ($type eq 'procedures' && $fct_detail{args} =~ /\b(OUT|INOUT)\b/) {
			# set return type to empty to avoid returning void later
			$fct_detail{func_ret_type} = ' ';
		}

		# IN OUT should be INOUT
		$fct_detail{args} =~ s/\bIN\s+OUT/INOUT/igs;

		if ($fct_detail{code} =~ s/\s*DECLARE\s+(.*?)\s+(BEGIN|ON EXCEPTION|IF|WHILE|FOREACH|INSERT|DELETE|UPDATE|SELECT|DROP|LET|SET)\s/$2 /is) {
			$fct_detail{declare} = "DECLARE\n$1";
		}

		# Now convert types
		if ($fct_detail{args}) {
			$fct_detail{args} = replace_sql_type($self, $fct_detail{args});
		}
		if ($fct_detail{declare}) {
			$fct_detail{declare} = replace_sql_type($self, $fct_detail{declare});
		}

		$fct_detail{args} =~ s/\s+/ /gs;
		push(@{$fct_detail{param_types}}, split(/\s*,\s*/, $fct_detail{args}));

		# Store type used in parameter list to lookup later for custom types
		map { s/^\(//; } @{$fct_detail{param_types}};
		map { s/\)$//; } @{$fct_detail{param_types}};
		map { s/\%ORA2PG_COMMENT\d+\%//gs; }  @{$fct_detail{param_types}};
		map { s/^\s*[^\s]+\s+(IN|OUT|INOUT)/$1/i; s/^((?:IN|OUT|INOUT)\s+[^\s]+)\s+[^\s]*$/$1/i; s/\(.*//; s/\s*\)\s*$//; s/\s+$//; } @{$fct_detail{param_types}};
	}
	else
	{
                delete $fct_detail{func_ret_type};
                delete $fct_detail{declare};
                $fct_detail{code} = $code;
	}

	# Mark the function as having out parameters if any
	my @nout = $fct_detail{args} =~ /\bOUT\s+([^,\)]+)/igs;
	my @ninout = $fct_detail{args} =~ /\bINOUT\s+([^,\)]+)/igs;
	my $nbout = $#nout+1 + $#ninout+1;
	$fct_detail{inout} = 1 if ($nbout > 0);

	$fct_detail{args} =~ s/\s*$//s;
	$fct_detail{args} =~ s/^\s*//s;
	$fct_detail{args} =~ s/"/'/gs;
	$fct_detail{code} =~ s/^[\r\n]*/\n/s;
	$fct_detail{code} =~ s/^\s*BEGIN\b//is;
	$fct_detail{func_ret_type} =~ s/RETURNS //is;

	# Replace LIKE in parameters
	$fct_detail{args} =~ s/\s+LIKE\s+([^\s,\)]+)/ $1%TYPE/igs;

	# Fix LIKE in variables declaration
	$fct_detail{declare} =~ s/\s+LIKE\s+([^\s]+)\s*;/ $1%TYPE;/igs;

	# Fix end of conditional block
	# Remove %ROWTYPE from return type
	$fct_detail{func_ret_type} =~ s/\%ROWTYPE//igs;

	return %fct_detail;
}

sub replace_mssql_params
{
	my ($self, $args, $declare, $code) = @_;

	if ($args =~ s/\s+(?:DECLARE|AS)\s+(.*)//is) {
		$declare .= "\n$1";
	}
	while ($args =~ s/\@([^\s]+)\b/p_$1/s)
	{
		my $p = $1;
		$code =~ s/\@$p\b/p_$p/gis;
	}

	return ($args, $declare, $code);
}

sub _list_all_functions
{
	my $self = shift;

	# Retrieve all functions and procedure
	my $str = "SELECT p.procname FROM sysprocedures p WHERE p.owner NOT IN ('informix', 'sysibm', 'sysproc', 'sysfun', 'sqlj')";

	$str .= " " . $self->limit_to_objects('FUNCTION','p.procname');
	$str .= " ORDER BY p.procname";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @functions = ();
	while (my $row = $sth->fetch) {
		push(@functions, $row->[0]);
	}
	$sth->finish();

	return @functions;
}

sub _sql_type
{
        my ($self, $type, $len, $precision, $scale, $default, $no_blob_to_oid) = @_;

	my $data_type = '';
	chomp($type);

	# Some length and scale may have not been extracted before
	if ($type =~ s/\(\s*(\d+)\s*\)//) {
		$len   = $1;
	} elsif ($type =~ s/\(\s*(\d+)\s*,\s*(\d+)\s*\)//) {
		$len   = $1;
		$scale = $2;
	}

	if ($type !~ /CHAR/i) {
		$precision = $len if (!$precision);
	}
        # Override the length
        if (exists $self->{data_type}{uc($type)})
	{
		$type = uc($type); # Force uppercase
		if ($len)
		{
			if ( $type =~ /CHAR|TEXT/ )
			{
				# Type CHAR have default length set to 1
				# Type VARCHAR(2) must have a specified length
				$len = 1 if (!$len && ($type eq "CHAR" || $type eq "NCHAR"));
				if ($self->{data_type}{$type} eq 'text') {
					return $self->{data_type}{$type};
				} else {
					return "$self->{data_type}{$type}($len)";
				}
			}
			elsif ($type eq 'BIT')
			{
				if ($precision > 1) {
					return "bit($precision)";
				} else {
					return $self->{data_type}{$type};
				}
			}
			elsif ($type eq 'INTERVAL')
			{
				return $self->{data_type}{$type};
			}
			elsif ($type =~ /(TINYINT|SMALLINT|INTEGER|BIGINT|INT|REAL|FLOAT|DECIMAL|NUMERIC|SMALLMONEY|MONEY)/i)
			{
				if (!$scale)
				{
					if ($precision)
					{
						if ($type =~ /(REAL|DOUBLE|FLOAT)/i)
						{
							return $self->{data_type}{$type};
						}
						elsif ($self->{pg_integer_type})
						{
							if ($precision < 5) {
								return 'smallint';
							} elsif ($precision <= 9) {
								return 'integer'; # The speediest in PG
							} else {
								return 'bigint';
							}
						}
						return "numeric($precision)";
					}
					else
					{
						# Most of the time interger should be enought?
						return $self->{data_type}{$type};
					}
				}
				else
				{
					if ($precision)
					{
						if ($type !~ /(DOUBLE|DECIMAL)/ && $self->{pg_numeric_type})
						{
							if ($precision <= 6) {
								return 'real';
							} else {
								return 'double precision';
							}
						}
						return "decimal($precision,$scale)";
					}
				}
			}
			return $self->{data_type}{$type};
		}
		else
		{
			return $self->{data_type}{$type};
		}
        }

        return $type;
}

sub replace_sql_type
{
        my ($self,  $str) = @_;

	$str =~ s/with local time zone/with time zone/igs;
	$str =~ s/([A-Z])ORA2PG_COMMENT/$1 ORA2PG_COMMENT/igs;
	$str =~ s/\(\s*MAX\s*\)//igs;

	# Replace type with precision
	my $mssqltype_regex = '';
	foreach (keys %{$self->{data_type}}) {
		$mssqltype_regex .= quotemeta($_) . '|';
	}
	$mssqltype_regex =~ s/\|$//;

	while ($str =~ /(.*)\b($mssqltype_regex)\s*\(([^\)]+)\)/i)
	{
		my $backstr = $1;
		my $type = uc($2);
		my $args = $3;
		if (exists $self->{data_type}{"$type($args)"}) {
			$str =~ s/\b$type\($args\)/$self->{data_type}{"$type($args)"}/igs;
			next;
		}
		if ($backstr =~ /_$/) {
		    $str =~ s/\b($mssqltype_regex)\s*\(([^\)]+)\)/$1\%\|$2\%\|\%/is;
		    next;
		}

		my ($precision, $scale) = split(/,/, $args);
		$scale ||= 0;
		my $len = $precision || 0;
		$len =~ s/\D//;
		if ( $type =~ /CHAR|TEXT/i )
		{
			# Type CHAR have default length set to 1
			# Type VARCHAR must have a specified length
			$len = 1 if (!$len && ($type eq 'CHAR' || $type eq 'NCHAR'));
			$str =~ s/\b$type\b\s*\([^\)]+\)/$self->{data_type}{$type}\%\|$len\%\|\%/is;
		}
		elsif ($type eq 'BIT')
		{
			if ($precision > 1) {
				return "bit($precision)";
			} else {
				return $self->{data_type}{$type};
			}
		}
		elsif ($precision && ($type =~ /(TINYINT|SMALLINT|INTEGER|BIGINT|INT|REAL|FLOAT|DECIMAL|NUMERIC|SMALLMONEY|MONEY)/))
		{
			if (!$scale)
			{
				if ($type =~ /(TINYINT|SMALLINT|INTEGER|BIGINT|INT)/)
				{
					if ($self->{pg_integer_type})
					{
						if ($precision < 5) {
							$str =~ s/\b$type\b\s*\([^\)]+\)/smallint/is;
						} elsif ($precision <= 9) {
							$str =~ s/\b$type\b\s*\([^\)]+\)/integer/is;
						} else {
							$str =~ s/\b$type\b\s*\([^\)]+\)/bigint/is;
						}
					}
					else {
						$str =~ s/\b$type\b\s*\([^\)]+\)/numeric\%\|$precision\%\|\%/i;
					}
				}
				else {
					$str =~ s/\b$type\b\s*\([^\)]+\)/$self->{data_type}{$type}\%\|$precision\%\|\%/is;
				}
			}
			else
			{
				$str =~ s/\b$type\b\s*\([^\)]+\)/$self->{data_type}{$type}\%\|$args\%\|\%/is;
			}
		}
		else
		{
			# Prevent from infinit loop
			$str =~ s/\(/\%\|/s;
			$str =~ s/\)/\%\|\%/s;
		}
	}
	$str =~ s/\%\|\%/\)/gs;
	$str =~ s/\%\|/\(/gs;

	# Replace datatype even without precision
	my %recover_type = ();
	my $i = 0;
	foreach my $type (sort { length($b) <=> length($a) } keys %{$self->{data_type}})
	{
		while ($str =~ s/\b$type\b/%%RECOVER_TYPE$i%%/is)
		{
			$recover_type{$i} = $self->{data_type}{$type};
			$i++;
		}
	}

	foreach $i (keys %recover_type) {
		$str =~ s/\%\%RECOVER_TYPE$i\%\%/$recover_type{$i}/;
	}

	if (($self->{type} eq 'COPY' || $self->{type} eq 'INSERT') && exists $SQL_TYPE{uc($str)}) {
		$str = $SQL_TYPE{uc($str)};
	}
	# Set varchar without length to text
	$str =~ s/\bVARCHAR(\s*(?!\())/text$1/igs;

        return $str;
}

sub _get_job
{
	my($self) = @_;

	# Don't work with Azure
	#return if ($self->{db_version} =~ /Microsoft SQL Azure/);
	return;

	# Retrieve all database job from user_jobs table
	my $str = qq{SELECT
     job.job_id,
     notify_level_email,
     name,
     enabled,
     description,
     step_name,
     command,
     server,
     database_name
FROM
    msdb.dbo.sysjobs job
INNER JOIN 
    msdb.dbo.sysjobsteps steps        
ON
    job.job_id = steps.job_id
WHERE
    job.enabled = 1 AND database_name = $self->{database}
};
	$str .= $self->limit_to_objects('JOB', 'NAME');
	$str .= " ORDER BY NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		$data{$row->[0]}{what} = $row->[6];
		$data{$row->[0]}{interval} = $row->[0];
	}

	return %data;
}

sub _get_dblink
{
	my($self) = @_;

	return; # Not supported
}

=head2 _get_partitions

This function implements an Informix-native partitions information.
Return two hash ref with partition details and partition default.

=cut

sub _get_partitions
{
	my ($self) = @_;

	my %part_types = (
		'R' => 'ROUND ROBIN',
		'E' => 'RANGE',
		'I' => 'IN DBSPACE',
		'N' => 'RANGE',
		'L' => 'LIST',
		'T' => 'TABLE BASED',
		'H' => 'TABLE HIERARCHY'
	);

	# Retrieve all partitions.
	my $str = qq{
SELECT t.tabid, t.tabname, f.indexname, f.evalpos, f.partition, '', '', f.exprtext, f.strategy
FROM sysfragments f
JOIN systables t ON (f.tabid = t.tabid)
WHERE f.fragtype = 'T'
};

	$str .= $self->limit_to_objects('TABLE|PARTITION','t.tabname|f.partition');
	$str .= " ORDER BY t.tabname, f.partn\n";

	my $str2 = "SELECT colno FROM syscolumns WHERE colname = ? AND tabid = ?";
	my $sth2 = $self->{dbh}->prepare($str2) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	my %default = ();
	my $i = 1;
	while (my $row = $sth->fetch)
	{
		my $tbname = $row->[1];
		$parts{$row->[1]}{$row->[3]}{name} = $row->[1] . '_part' . $i++;
		my $col = $row->[7];
		$col =~ s/^[\(]*\s*([^\s>=<\+\-\*\/]+).*/$1/;
		$sth2->execute($col, $row->[0]) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		my $r = $sth2->fetch;
		push(@{$parts{$row->[1]}{$row->[3]}{info}}, { 'type' => $part_types{$row->[8]}, 'value' => $row->[7], 'column' => $col, 'colpos' => $row->[10], 'tablespace' => '', 'owner' => ''});
	}
	$sth->finish;
	$sth2->finish;

	return \%parts, \%default;
}

=head2 _get_subpartitions

This function implements a Informix subpartitions information.
Return two hash ref with partition details and partition default.
=cut

sub _get_subpartitions
{
	my($self) = @_;

	my %subparts = ();
	my %default = ();

	# For what I know, subpartition is not supported by Informix, or HYBRID?
	return \%subparts, \%default;
}

=head2 _get_partitions_list

This function implements a Informix-native partitions information.
Return a hash of the partition table_name => type

=cut

sub _get_partitions_list
{
	my($self) = @_;

	return _get_partitions_list_from_file($self) if ($self->{input_file});

	# Retrieve all partitions except the ones not supported
	my $str = qq{
SELECT t.tabid, t.tabname, f.indexname, f.evalpos, f.partition, '', '', f.exprtext, f.strategy
FROM sysfragments f
JOIN systables t ON (f.tabid = t.tabid)
WHERE f.fragtype = 'T' AND f.strategy NOT IN ('R', 'I', 'T', 'H');
};

	$str .= $self->limit_to_objects('TABLE|PARTITION','t.tabname|f.partition');
	$str .= " ORDER BY t.tabname, f.partn\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch)
	{
		$parts{"\L$row->[1]\E"}++;
	}
	$sth->finish;

	return %parts;
}

sub _get_partitions_list_from_file
{
	my($self) = @_;

	# Retrieve all partitions.
	my %parts = ();
	my $has_partition = 0;
	my $tbname = '';
	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	while (my $l = <$fh>)
	{
		chomp($l);
		if ($l =~ /create table ([^\.])\.([^\s])/i) {
			next if ($1 eq '"informix"' || $1 eq 'informix');
			$tbname = $2;
		}
		if ($tbname && $l =~ /fragment by (.*)/i)
		{
			my $frag_type = $1;
			# Do not take care of unsupported partitioning
			if ($frag_type !~ /round robin/) {
				$has_partition = 1;
			}
		}
		if ($tbname && $has_partition) {
			while ($l =~ s/partition ([^\s]+) in//) {
				$parts{"\L$tbname\E"}++;
			}
		}
		if ($tbname && $l =~ /;$/) {
			$has_partition = 0;
			$tbname = '';
		}
	}
	$fh->close;

	return %parts;
}


=head2 _get_partitioned_table

Return a hash of the partitioned table with the number of partition

=cut

sub _get_partitioned_table
{
	my ($self, %subpart) = @_;

	my %part_types = (
		'R' => 'ROUND ROBIN',
		'E' => 'RANGE',
		'I' => 'IN DBSPACE',
		'N' => 'RANGE',
		'L' => 'LIST',
		'T' => 'TABLE BASED',
		'H' => 'TABLE HIERARCHY'
	);

	# Retrieve all partitions.
	my $str = qq{
SELECT t.tabid, t.tabname, f.indexname, f.evalpos, f.partition, '', '', f.exprtext, f.strategy
FROM sysfragments f
JOIN systables t ON (f.tabid = t.tabid)
WHERE f.fragtype = 'T'
};

	$str .= $self->limit_to_objects('TABLE|PARTITION','t.tabname|f.partition');
	$str .= " ORDER BY t.tabname, f.partn\n";

	my $str2 = "SELECT colno FROM syscolumns WHERE colname = ? AND tabid = ?";
	my $sth2 = $self->{dbh}->prepare($str2) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch)
	{
                $parts{$row->[1]}{count}++;
                $parts{$row->[1]}{composite} = 0;
		$parts{$row->[1]}{type} = $part_types{$row->[8]};
		my $col = $row->[7];
		$col =~ s/^[\(]*\s*([^\s>=<\+\-\*\/]+).*/$1/;
		$sth2->execute($col, $row->[0]) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		my $r = $sth2->fetch;
		push(@{ $parts{$row->[1]}{columns} }, $col);
	}
	$sth->finish;
	$sth2->finish;

	return %parts;
}


=head2 _get_objects

This function retrieves all object the Oracle information

=cut

sub _get_objects
{
	my $self = shift;

	my %infos = ();

	# TABLE
	if (!$self->{input_file})
	{
		my $sql = "SELECT t.tabname FROM informix.systables t WHERE t.tabtype='T' AND t.owner != 'informix'";
		my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while ( my @row = $sth->fetchrow()) {
			push(@{$infos{TABLE}}, { ( name => $row[0], invalid => 0) });
		}
		$sth->finish();
	}
	else
	{
		my @rows = `grep -i "^CREATE TABLE" $self->{input_file} | sed 's/.*\.//' | sort`;
		chomp(@rows);
		foreach my $o (@rows) {
			push(@{$infos{TABLE}}, { ( name => $o, invalid => 0) });
		}
	}

	# VIEW
	if (!$self->{input_file})
	{
		my $sql = "SELECT t.tabname FROM informix.systables t WHERE t.tabtype='V' AND t.owner != 'informix'";
		my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while ( my @row = $sth->fetchrow()) {
			push(@{$infos{VIEW}}, { ( name => $row[0], invalid => 0) });
		}
		$sth->finish();
	}
	else
	{
		my @rows = `grep -i "^CREATE VIEW" $self->{input_file} | sed 's/.*\.//' | sed 's/ .*//' | sort`;
		chomp(@rows);
		foreach my $o (@rows) {
			push(@{$infos{VIEW}}, { ( name => $o, invalid => 0) });
		}
	}

	# TRIGGER
	if (!$self->{input_file})
	{
		my $sql = "SELECT trg.trigname FROM systriggers trg INNER JOIN systables t ON t.tabid = trg.tabid WHERE t.owner != 'informix' AND trg.owner != 'informix'";
		my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while ( my @row = $sth->fetchrow()) {
			push(@{$infos{TRIGGER}}, { ( name => $row[0], invalid => 0) });
		}
		$sth->finish();
	}
	else
	{
		my @rows = `grep -i "^CREATE TRIGGER" $self->{input_file} | sed 's/.*\.//' | sed 's/ .*//' | sort`;
		chomp(@rows);
		foreach my $o (@rows) {
			push(@{$infos{TRIGGER}}, { ( name => $o, invalid => 0) });
		}
	}

	# INDEX
	if (!$self->{input_file})
	{
		my $sql = "SELECT i.idxname FROM sysindices i JOIN systables t ON (t.tabid = i.tabid) WHERE t.owner != 'informix'";
		my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my @row = $sth->fetchrow()) {
			push(@{$infos{INDEX}}, { ( name => $row[2], invalid => 0) });
		}
	}
	else
	{
		my @rows = `grep -iE "^CREATE (INDEX|UNIQUE) " $self->{input_file} | sed 's/.*\.//' | sed 's/ .*//' | sort`;
		chomp(@rows);
		foreach my $o (@rows) {
			push(@{$infos{INDEX}}, { ( name => $o, invalid => 0) });
		}
	}

	# FUNCTION
	if (!$self->{input_file})
	{
		my $sql = "SELECT p.procname FROM sysprocedures p WHERE p.isproc = 'f' AND p.owner NOT IN ('informix', 'sysibm', 'sysproc', 'sysfun', 'sqlj')";
		my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while ( my @row = $sth->fetchrow()) {
			push(@{$infos{FUNCTION}}, { ( name => $row[0], invalid => 0) });
		}
		$sth->finish();
	}
	else
	{
		my @rows = `grep -iE "^CREATE FUNCTION " $self->{input_file} | sed 's/.*\.//' | sed 's/[( ].*//' | sort`;
		chomp(@rows);
		foreach my $o (@rows) {
			push(@{$infos{FUNCTION}}, { ( name => $o, invalid => 0) });
		}
	}

	# PROCEDURE
	if (!$self->{input_file})
	{
		my $sql = "SELECT p.procname FROM sysprocedures p WHERE p.isproc = 't' AND p.owner NOT IN ('informix', 'sysibm', 'sysproc', 'sysfun', 'sqlj')";
		my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while ( my @row = $sth->fetchrow()) {
			push(@{$infos{PROCEDURE}}, { ( name => $row[0], invalid => 0) });
		}
		$sth->finish();
	}
	else
	{
		my @rows = `grep -iE "^CREATE PROCEDURE " $self->{input_file} | sed 's/.*\.//' | sed 's/[( ].*//' | sort`;
		chomp(@rows);
		foreach my $o (@rows) {
			push(@{$infos{PROCEDURE}}, { ( name => $o, invalid => 0) });
		}
	}

	# PARTITION.
	if (!$self->{input_file})
	{
		my $sql = "SELECT t.tabid, t.tabname, f.strategy FROM sysfragments f JOIN systables t ON (f.tabid = t.tabid) WHERE f.fragtype = 'T' ORDER BY t.tabname, f.partn";
		my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		my $i = 1;
		while ( my @row = $sth->fetchrow()) {
			push(@{$infos{'TABLE PARTITION'}}, { ( name => $row[1] . '_part' . $i++, invalid => 0) });
		}
		$sth->finish;
	}
	else
	{
		my $fh = new IO::File;
		$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
		my $table = '';
		my $invalid = 0;
		while (my $l = <$fh>)
		{
			chomp($l);
			if ($l =~ /^create table [^\.]+\.([^\s]+) /i) {
				$table = $1;
				$invalid = 0;
			}
			if ($table)
			{
				if ($l =~ /fragment by ([^\s]+) /i) {
					$invalid = 1 if (lc($1) eq 'round');
				}
				if ($l =~ / partition [^\s]+ in /i)
				{
					my $i = 1;
					while ($l =~ s/ partition ([^\s]+) in //i) {
						push(@{$infos{'TABLE PARTITION'}}, { ( name => $table . '_part' . $i++, invalid => $invalid) });
					}
				}
				$table = '' if ($l =~ /;/);
			}
		}
		$fh->close;
	}

	# MATERIALIZED VIEW => not supported by Informix

	return %infos;
}

sub _get_privilege
{
	my($self) = @_;

	my %privs = ();
	my %roles = ();

	# Retrieve all privilege per table defined in this database
	my $str = "SELECT GRANTEE,TABLE_NAME,PRIVILEGE_TYPE,IS_GRANTABLE FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES";
	if ($self->{schema}) {
		$str .= " WHERE TABLE_SCHEMA = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('GRANT|TABLE|VIEW|FUNCTION|PROCEDURE|SEQUENCE', 'GRANTEE|TABLE_NAME|TABLE_NAME|TABLE_NAME|TABLE_NAME|TABLE_NAME');
	$str .= " ORDER BY TABLE_NAME, GRANTEE";
	my $error = "\n\nFATAL: You must be connected as an oracle dba user to retrieved grants\n\n";
	my $sth = $self->{dbh}->prepare($str) or $self->logit($error . "FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		# Remove the host part of the user
		$row->[0] =~ s/\@.*//;
		$row->[0] =~ s/'//g;
		$privs{$row->[1]}{type} = $row->[2];
		if ($row->[3] eq 'YES') {
			$privs{$row->[1]}{grantable} = $row->[3];
		}
		$privs{$row->[1]}{owner} = '';
		push(@{$privs{$row->[1]}{privilege}{$row->[0]}}, $row->[2]);
		push(@{$roles{grantee}}, $row->[0]) if (!grep(/^$row->[0]$/, @{$roles{grantee}}));
	}
	$sth->finish();

	# Retrieve all privilege per column table defined in this database
	$str = "SELECT GRANTEE,TABLE_NAME,PRIVILEGE_TYPE,COLUMN_NAME,IS_GRANTABLE FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES";
	if ($self->{schema}) {
		$str .= " WHERE TABLE_SCHEMA = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('GRANT|TABLE|VIEW|FUNCTION|PROCEDURE|SEQUENCE', 'GRANTEE|TABLE_NAME|TABLE_NAME|TABLE_NAME|TABLE_NAME|TABLE_NAME');

	$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		$row->[0] =~ s/\@.*//;
		$row->[0] =~ s/'//g;
		$privs{$row->[1]}{owner} = '';
		push(@{$privs{$row->[1]}{column}{$row->[3]}{$row->[0]}}, $row->[2]);
		push(@{$roles{grantee}}, $row->[0]) if (!grep(/^$row->[0]$/, @{$roles{grantee}}));
	}
	$sth->finish();

	return (\%privs, \%roles);
}

=head2 _get_database_size

This function retrieves the size of the Informix database in MB

=cut

sub _get_database_size
{
	my $self = shift;

	# Don't work with Azure
	return if ($self->{db_version} =~ /Microsoft SQL Azure/);

	my $mb_size = '';
	my $condition = '';

       my $sql = qq{
SELECT
    dbsname,
    SUM(ti_npused  * ti_pagesize / 1024 / 1024) :: INT AS Mb_used,
    SUM(ti_nptotal * ti_pagesize / 1024 / 1024) :: INT AS Mb_alloc
FROM
    sysmaster:sysdatabases AS d,
    sysmaster:systabnames AS n,
    sysmaster:systabinfo AS i
WHERE n.dbsname = d.name
AND ti_partnum = n.partnum
AND d.name = '$self->{schema}'
GROUP BY 1
ORDER BY 1
};
        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$mb_size = sprintf("%.2f MB", $row[1]);
		last;
	}
	$sth->finish();
	return $mb_size;
}

=head2 _get_largest_tables

This function retrieves the list of largest table of the Oracle database in MB

=cut

sub _get_largest_tables
{
	my $self = shift;

	my %table_size = ();

	my $sql = qq{SELECT tn.tabname,
  sum(pt.npused  * pt.pagesize)/(1024*1024) as UsedSpaceMB,
  sum(pt.nptotal  * pt.pagesize)/(1024*1024) as TotalSpaceMB
FROM informix.systables t
LEFT JOIN sysmaster:sysptnhdr pt ON (t.partnum = pt.partnum)
LEFT JOIN sysmaster:systabnames tn ON tn.partnum = pt.partnum
WHERE t.tabtype='T' AND tn.dbsname='$self->{schema}' AND tn.owner != 'informix'
};

	$sql .= $self->limit_to_objects('TABLE', 'tn.tabname');
	$sql .= " GROUP BY tn.tabname ORDER BY TotalSpaceMB";
	$sql .= " LIMIT $self->{top_max}" if ($self->{top_max});

        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute(@{$self->{query_bind_params}}) or return undef;
	while ( my @row = $sth->fetchrow()) {
		$table_size{$row[0]} = $row[1];
	}
	$sth->finish();

	return %table_size;
}

sub _get_audit_queries
{
	my($self) = @_;

	return if (!$self->{audit_user});

	my @users = ();
	push(@users, split(/[,;\s]/, lc($self->{audit_user})));

	# Retrieve all object with tablespaces.
	my $str = "SELECT argument FROM mysql.general_log WHERE command_type='Query' AND argument REGEXP '^(INSERT|UPDATE|DELETE|SELECT)'";
	if (($#users >= 0) && !grep(/^all$/, @users)) {
		$str .= " AND user_host REGEXP '(" . join("'|'", @users) . ")'";
	}
	my $error = "\n\nFATAL: You must be connected as an oracle dba user to retrieved audited queries\n\n";
	my $sth = $self->{dbh}->prepare($str) or $self->logit($error . "FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %tmp_queries = ();
	while (my $row = $sth->fetch) {
		$self->_remove_comments(\$row->[0]);
		$row->[0] =  $self->normalize_query($row->[0]);
		$tmp_queries{$row->[0]}++;
		$self->logit(".",1);
	}
	$sth->finish;
	$self->logit("\n", 1);

	my %queries = ();
	my $i = 1;
	foreach my $q (keys %tmp_queries) {
		$queries{$i} = $q;
		$i++;
	}

	return %queries;
}

sub _get_synonyms
{
	my ($self) = shift;

	return _get_synonyms_from_file($self) if ($self->{input_file});

	# Retrieve all synonym
	my $str = qq{SELECT
	t.owner,
	t.tabname, 
	s.owner,
	s.tabname,
	s.servername,
	s.dbname,
	s.btabid,
	t2.tabname,
	t2.owner
FROM systables t
JOIN syssyntable s ON (s.tabid = t.tabid)
LEFT JOIN systables t2 ON (t2.tabid = s.btabid)
WHERE t.tabtype = 'S' AND t.owner != 'informix'
};
	$str .= $self->limit_to_objects('SYNONYM','s.synname');

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %synonyms = ();
	while (my $row = $sth->fetch)
	{
		$synonyms{$row->[1]}{owner} = $row->[0];
		$synonyms{$row->[1]}{table_name} = $row->[7];
		if ($row->[4] || $row->[5]) {
			$synonyms{$row->[1]}{dblink} = $row->[4] . '@' . $row->[5];
		}
	}
	$sth->finish;

	return %synonyms;
}

sub _get_synonyms_from_file
{
	my ($self) = shift;


	# Retrieve all synonym
	my %synonyms = ();
	my $fh = new IO::File;
	$fh->open("<$self->{input_file}") or $self->logit("FATAL: can't read file $self->{input_file}, $!\n", 0, 1);
	while (my $l = <$fh>)
	{
		chomp($l);
		if ($l =~ /create synonym ([^\.]+).([^\s]+)\s+for\s+(.*);/i)
		{
			next if ($1 eq '"informix"' || $1 eq 'informix');
			my $synname = $2;
			$synonyms{$synname}{owner} = $1;
			my $link = $3;
			my $tbname = $3;
			$tbname =~ s/.*\.//;
			$synonyms{$synname}{table_name} = $tbname;
			$synonyms{$synname}{dblink} = $link;
		}
	}
	$fh->close;

	return %synonyms;
}


sub _get_tablespaces
{
	my ($self) = shift;

	return;
}

sub _list_tablespaces
{
	my ($self) = shift;

	return;
}

sub _get_sequences
{
	my ($self) = shift;

	my $str = qq{SELECT
  t.tabname,
  s.min_val,
  s.max_val,
  s.inc_val,
  s.start_val,
  s.cache,
  s.cycle,
  s.restart_val
FROM systables t
JOIN syssequences s ON (s.tabid = t.tabid)
WHERE t.tabtype = 'Q' AND t.owner != 'informix'
};
        $str .= $self->limit_to_objects('SEQUENCE', 't.tabname');
        $str .= " ORDER BY t.tabname, s.seqid";

        my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

        my %seqs = ();
        while (my $row = $sth->fetch)
        {
		$row->[6] = 'Y' if ($row->[6]);
                push(@{$seqs{$row->[0]}}, @$row);
        }

        return \%seqs;
}

sub _extract_sequence_info
{
	my ($self) = shift;

        my $str = qq{SELECT
  t.tabname,
  s.min_val,
  s.max_val,
  s.inc_val,
  s.start_val,
  s.cache,
  s.cycle,
  s.restart_val
FROM systables t
JOIN syssequences s ON (s.tabid = t.tabid)
WHERE t.tabtype = 'Q' AND t.owner != 'informix'
};
        $str .= $self->limit_to_objects('SEQUENCE', 't.tabname');

        my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @script = ();
        while (my $row = $sth->fetch)
        {
		my $sth2 = $self->{dbh}->prepare("SELECT $row->[0].CURRVAL") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute() or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		my $r = $sth2->fetch;
		$sth2->finish;
		$r->[0] ||= 1;
		my $alter = "ALTER SEQUENCE $self->{pg_supports_ifexists} " .  $self->quote_object_name($row->[0]) . " RESTART WITH $r->[0];";
		push(@script, $alter);
		$self->logit("Extracted sequence information for sequence \"$row->[0]\", nextvalue: $r->[0]\n", 1);
	}
	$sth->finish();

	return @script;
}

sub _count_sequences
{
	my $self = shift;

	my %seqs = ();
        my $sql = qq{SELECT
  t.tabname,
  s.min_val,
  s.max_val,
  s.inc_val,
  s.start_val,
  s.cache,
  s.cycle,
  s.restart_val
FROM systables t
JOIN syssequences s ON (s.tabid = t.tabid)
WHERE t.tabtype = 'Q' AND t.owner != 'informix'
};
	$sql .= $self->limit_to_objects('TABLE', 't.tabname');
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		push(@{$seqs{$row->[0]}}, @$row);
	}
	$sth->finish();

	return \%seqs;
}

sub _column_attributes
{
	my ($self, $table, $owner, $objtype) = @_;

	
	my $condition = '';
	if ($self->{schema}) {
		$condition .= "AND s.name='$self->{schema}' ";
	}
	$condition .= "AND tb.name='$table' " if ($table);
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE', 'tb.name');
	} else {
		@{$self->{query_bind_params}} = ();
	}
	$condition =~ s/^\s*AND\s/ WHERE /;

	my $sql = qq{SELECT 
    c.name 'Column Name',
    c.is_nullable,
    object_definition(c.default_object_id),
    tb.name,
    t.Name 'Data type',
    c.column_id,
    s.name
FROM sys.columns c
INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
INNER JOIN sys.tables AS tb ON tb.object_id = c.object_id
INNER JOIN sys.schemas AS s ON s.schema_id = tb.schema_id
$condition
ORDER BY c.column_id};

	my $sth = $self->{dbh}->prepare($sql);
	if (!$sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch)
	{
		next if ($self->{drop_rowversion} && ($row->[4] eq 'rowversion' || $row->[4] eq 'timestamp'));
                if (!$self->{schema} && $self->{export_schema}) {
                        $row->[3] = $row->[6] . '.' . $row->[3];
                }
		$data{$row->[3]}{$row->[0]}{nullable} = 'N';
		if ($row->[1]) {
			$data{$row->[3]}{$row->[0]}{nullable} = 'Y';
		}
		$row->[2] =~ s/[\[\]]+//g;
		$data{$row->[3]}{$row->[0]}{default} = $row->[2];
		# Store the data type of the column following its position
		$data{$row->[3]}{data_type}{$row->[5]} = $row->[4];
	}

	return %data;
}

sub _list_triggers
{
        my ($self) = @_;

	my $str = qq{SELECT 
     trg.trigname
    ,t.tabname 
FROM systriggers trg
INNER JOIN systables t ON t.tabid = trg.tabid
WHERE t.owner != 'informix'
};

	$str .= " " . $self->limit_to_objects('TABLE|VIEW|TRIGGER','t.tabname|t.tabname|trg.trigname');

	$str .= " ORDER BY t.tabname, trg.trigname";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %triggers = ();
	while (my $row = $sth->fetch)
	{
		push(@{$triggers{$row->[1]}}, $row->[0]);
	}

	return %triggers;
}

sub _global_temp_table_info
{
        my($self) = @_;

	# Useless, SQL Server has automatic removal of the GTT
	# when the session that create it is closed so it might
	# not persist.

	return;
}

sub _encrypted_columns
{
        my ($self, $table, $owner) = @_;

	return;
}

sub _get_subpartitioned_table
{
        my($self) = @_;

	return;
}

# Replace IF("user_status"=0,"username",NULL)
# PostgreSQL (CASE WHEN "user_status"=0 THEN "username" ELSE NULL END)
sub replace_if
{
	my $str = shift;

	# First remove all IN (...) before processing
	my %in_clauses = ();
	my $j = 0;
	while ($str =~ s/\b(IN\s*\([^\(\)]+\))/,\%INCLAUSE$j\%/is) {
		$in_clauses{$j} = $1;
		$j++;
	}

	while ($str =~ s/\bIF\s*\(((?:(?!\)\s*THEN|\s*SELECT\s+|\bIF\s*\().)*)$/\%IF\%$2/is || $str =~ s/\bIF\s*\(([^\(\)]+)\)(\s+AS\s+)/(\%IF\%)$2/is) {
		my @if_params = ('');
		my $stop_learning = 0;
		my $idx = 1;
		foreach my $c (split(//, $1)) {
			$idx++ if (!$stop_learning && $c eq '(');
			$idx-- if (!$stop_learning && $c eq ')');
		
			if ($idx == 0) {
				# Do not copy last parenthesis in the output string
				$c = '' if (!$stop_learning);
				# Inform the loop that we don't want to process any charater anymore
				$stop_learning = 1;
				# We have reach the end of the if() parameter
				# next character must be restored to the final string.
				$str .= $c;
			} elsif ($idx > 0) {
				# We are parsing the if() parameter part, append
				# the caracter to the right part of the param array.
				if ($c eq ',' && ($idx - 1) == 0) {
					# we are switching to a new parameter
					push(@if_params, '');
				} elsif ($c ne "\n") {
					$if_params[-1] .= $c;
				}
			}
		}
		my $case_str = 'CASE ';
		for (my $i = 1; $i <= $#if_params; $i+=2) {
			$if_params[$i] =~ s/^\s+//gs;
			$if_params[$i] =~ s/\s+$//gs;
			if ($i < $#if_params) {
				if ($if_params[$i] !~ /INCLAUSE/) {
					$case_str .= "WHEN $if_params[0] THEN $if_params[$i] ELSE $if_params[$i+1] ";
				} else {
					$case_str .= "WHEN $if_params[0] $if_params[$i] THEN $if_params[$i+1] ";
				}
			} else {
				$case_str .= " ELSE $if_params[$i] ";
			}
		}
		$case_str .= 'END ';

		$str =~ s/\%IF\%/$case_str/s;
	}
	$str =~ s/\%INCLAUSE(\d+)\%/$in_clauses{$1}/gs;
	$str =~ s/\s*,\s*IN\s*\(/ IN \(/igs;

	return $str;
}

sub _get_plsql_metadata
{
        my $self = shift;
        my $owner = shift;

        my $schema_clause = '';
        $schema_clause = "WHERE s.name='$self->{schema}'" if ($self->{schema});

	# Retrieve all functions
	my $str = qq{SELECT
   OBJECT_NAME(sm.object_id) AS object_name,
   SCHEMA_NAME(o.schema_id),
   o.type_desc,   
   sm.definition,  
   o.type,   
   sm.uses_ansi_nulls,  
   sm.uses_quoted_identifier,  
   sm.is_schema_bound,  
   sm.execute_as_principal_id  
FROM sys.sql_modules AS sm  
JOIN sys.objects AS o ON sm.object_id = o.object_id
LEFT OUTER JOIN sys.schemas s ON o.schema_id = s.schema_id $schema_clause
ORDER BY 1;};
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %functions = ();
	my @fct_done = ();
	push(@fct_done, @EXCLUDED_FUNCTION);
	while (my $row = $sth->fetch)
	{
		next if (grep(/^$row->[0]$/i, @fct_done));
		push(@fct_done, "$row->[0]");
		$self->{function_metadata}{'unknown'}{'none'}{$row->[0]}{type} = $row->[2];
		$self->{function_metadata}{'unknown'}{'none'}{$row->[0]}{text} = $row->[3];
	}
	$sth->finish();

	# Look for functions/procedures
	foreach my $name (sort keys %{$self->{function_metadata}{'unknown'}{'none'}})
	{
		# Retrieve metadata for this function after removing comments
		$self->_remove_comments(\$self->{function_metadata}{'unknown'}{'none'}{$name}{text}, 1);
		$self->{comment_values} = ();
		$self->{function_metadata}{'unknown'}{'none'}{$name}{text} =~ s/\%ORA2PG_COMMENT\d+\%//gs;
		my %fct_detail = $self->_lookup_function($self->{function_metadata}{'unknown'}{'none'}{$name}{text}, $name);
		if (!exists $fct_detail{name}) {
			delete $self->{function_metadata}{'unknown'}{'none'}{$name};
			next;
		}
		delete $fct_detail{code};
		delete $fct_detail{before};
		%{$self->{function_metadata}{'unknown'}{'none'}{$name}{metadata}} = %fct_detail;
		delete $self->{function_metadata}{'unknown'}{'none'}{$name}{text};
	}
}

sub _get_security_definer
{
	my ($self, $type) = @_;

	# Not supported by SQL Server
	return;
}

=head2 _get_identities

This function retrieve information about IDENTITY columns that must be
exported as PostgreSQL serial.

=cut

sub _get_identities
{
	my ($self) = @_;

	return; # not supported by Informix
}

=head2 _get_materialized_views

This function implements a mssql-native materialized views information.

Returns a hash of view names with the SQL queries they are based on.

=cut

sub _get_materialized_views
{
	my($self) = @_;

	return; # Informix do not have materialized view
}

sub _get_materialized_view_names
{
	my($self) = @_;

	return; # not supported by Informix
}

sub _get_package_function_list
{
	my ($self, $owner) = @_;

	# no package in Informix
	return;
}

sub _get_types
{
	my ($self, $name) = @_;

	# Retrieve all user defined types => PostgreSQL DOMAIN
	my $idx = 1;
	my $str = qq{SELECT t.name, d.seqno, d.description
FROM sysxtdtypes t JOIN sysxtddesc d ON (d.extended_id = t.extended_id)
WHERE t.owner != 'informix'
};

	if ($name) {
		$str .= " AND t.name='$name'";
		@{$self->{query_bind_params}} = ();
	} else {
		$str .= $self->limit_to_objects('TYPE', 't.name');
	}
	$str .= " ORDER BY t.name, d.seqno";
	# use a separeate connection
	my $local_dbh = _db_connection($self);

	my $sth = $local_dbh->prepare($str) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);

	my @types = ();
	while (my $row = $sth->fetch)
	{
		$self->logit("\tFound Type: $row->[0]\n", 1);
		if ($row->[1] == 0) {
			push(@types, { ('name' => $row->[0], 'pos' => $idx++, 'code' => $row->[2]) });
		} else {
			$types[-1]->{'code'} .= $row->[2];
		}
	}
	$sth->finish();

	$local_dbh->disconnect() if ($local_dbh);

	return \@types;
}

sub _col_count
{
	my ($self, $table, $schema) = @_;

	my $condition = '';
	if ($schema) {
		$condition .= "AND s.name='$self->{schema}' ";
	}
	$condition .= "AND t.name='$table' " if ($table);
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE', 't.name');
	} else {
		@{$self->{query_bind_params}} = ();
	}
	if ($self->{drop_rowversion}) {
		$condition .= "AND typ.name NOT IN ( 'rowversion', 'timestamp')";
	}
	$condition =~ s/^\s*AND\s/ WHERE /;

	my $sql = qq{SELECT 
    s.name,
    t.name,
    count(*)
FROM sys.columns c
INNER JOIN sys.tables AS t ON t.object_id = c.object_id
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
INNER JOIN sys.types AS typ ON c.user_type_id = typ.user_type_id
$condition
GROUP BY s.name, t.name};

	my $sth = $self->{dbh}->prepare($sql) || $self->logit("FATAL: _col_count() " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: _column_attributes() " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch)
	{
		if ($self->{export_schema} && !$self->{schema}) {
			$data{"$row->[0].$row->[1]"} = $row->[2];
		} else {
			$data{$row->[1]} = $row->[2];
		}
	}

	return %data;
}

1;

