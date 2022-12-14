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

sub _column_comments
{
	my ($self, $table) = @_;

	return ; # SQL Server doesn't have COMMENT and we don't play with "Extended Properties"
}

sub _column_info
{
	my ($self, $table, $owner, $objtype, $recurs) = @_;

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

sub _get_indexes
{
	my ($self, $table, $owner, $generated_indexes) = @_;

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

sub _get_triggers
{
	my($self) = @_;

	my $str = qq{SELECT 
     o.name AS trigger_name 
    ,USER_NAME(o.uid) AS trigger_owner 
    ,OBJECT_NAME(o.parent_obj) AS table_name 
    ,s.name AS table_schema 
    ,OBJECTPROPERTY( o.id, 'ExecIsAfterTrigger') AS isafter 
    ,OBJECTPROPERTY( o.id, 'ExecIsInsertTrigger') AS isinsert 
    ,OBJECTPROPERTY( o.id, 'ExecIsUpdateTrigger') AS isupdate 
    ,OBJECTPROPERTY( o.id, 'ExecIsDeleteTrigger') AS isdelete 
    ,OBJECTPROPERTY( o.id, 'ExecIsInsteadOfTrigger') AS isinsteadof 
    ,OBJECTPROPERTY( o.id, 'ExecIsTriggerDisabled') AS [disabled]
    , c.text
FROM sys.sysobjects o
INNER JOIN sys.syscomments AS c ON o.id = c.id
INNER JOIN sys.tables t ON o.parent_obj = t.object_id 
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id 
WHERE o.type = 'TR'
};

	if ($self->{schema}) {
		$str .= " AND s.name = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('TABLE|VIEW|TRIGGER','t.name|t.name|o.name');

	$str .= " ORDER BY t.name, o.name";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @triggers = ();
	while (my $row = $sth->fetch)
	{
		$row->[4] = 'AFTER'; # only FOR=AFTER trigger in this field, no BEFORE
		$row->[4] = 'INSTEAD OF' if ($row->[8]);
		my @actions = ();
		push(@actions, 'INSERT') if ($row->[5]);
		push(@actions, 'UPDATE') if ($row->[6]);
		push(@actions, 'DELETE') if ($row->[7]);
		my $act = join(' OR ', @actions);
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[2] = "$row->[3].$row->[2]";
		}
		$row->[10] =~ s///g;
		$row->[10] =~ s/^(?:.*?)\sAS\s(.*)\s*;\s*$/$1/is;
		push(@triggers, [ ($row->[0], $row->[4], $act, $row->[2], $row->[10], '', 'ROW', $row->[1]) ]);
	}

	return \@triggers;
}

sub _unique_key
{
	my($self, $table, $owner) = @_;

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

sub _check_constraint
{
	my ($self, $table, $owner) = @_;

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

	# Retrieve all functions 
	my $str = qq{SELECT
    O.name, M.definition, O.type_desc, s.name, M.null_on_null_input,
    M.execute_as_principal_id
FROM sys.sql_modules M
JOIN sys.objects O ON M.object_id=O.object_id
JOIN sys.schemas AS s ON o.schema_id = s.schema_id
WHERE O.type IN ('IF','TF','FN')
};
	if ($self->{schema}) {
		$str .= " AND s.name = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('FUNCTION','O.name');
	$str .= " ORDER BY O.name";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %functions = ();
	while (my $row = $sth->fetch)
	{
		my $kind = 'FUNCTION';
		next if ( ($kind ne $self->{type}) && ($self->{type} ne 'SHOW_REPORT') );
		my $fname = $row->[0];
		if ($self->{export_schema} && !$self->{schema}) {
			$row->[0] = "$row->[3].$row->[0]";
		}
		$functions{"$row->[0]"}{name} = $row->[0];
		$functions{"$row->[0]"}{text} = $row->[1];
		$functions{"$row->[0]"}{kind} = $row->[2];
		$functions{"$row->[0]"}{strict} = $row->[4];
		$functions{"$row->[0]"}{security} = ($row->[5] == -2) ? 'DEFINER' : 'EXECUTER';
		$functions{"$row->[0]"}{text} =~ s///gs;
		if ($self->{plsql_pgsql})
		{
			$functions{"$row->[0]"}{text} =~ s/[\[\]]//gs;
		}
	}

	return \%functions;
}

sub _get_procedures
{
	my $self = shift;

	# Retrieve all functions 
	my $str = qq{SELECT
    O.name, M.definition, O.type_desc, s.name, M.null_on_null_input,
    M.execute_as_principal_id
FROM sys.sql_modules M
JOIN sys.objects O ON M.object_id=O.object_id
JOIN sys.schemas AS s ON o.schema_id = s.schema_id
WHERE O.type = 'P'
};
	if ($self->{schema}) {
		$str .= " AND s.name = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('PROCEDURE','O.name');
	$str .= " ORDER BY O.name";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %functions = ();
	while (my $row = $sth->fetch)
	{
		my $kind = 'PROCEDURE';
		next if ( ($kind ne $self->{type}) && ($self->{type} ne 'SHOW_REPORT') );
		my $fname = $row->[0];
		if ($self->{export_schema} && !$self->{schema}) {
			$row->[0] = "$row->[3].$row->[0]";
		}
		$functions{"$row->[0]"}{name} = $row->[0];
		$functions{"$row->[0]"}{text} = $row->[1];
		$functions{"$row->[0]"}{kind} = $row->[2];
		$functions{"$row->[0]"}{strict} = $row->[4];
		$functions{"$row->[0]"}{security} = ($row->[5] == -2) ? 'DEFINER' : 'EXECUTER';
		$functions{"$row->[0]"}{text} =~ s///gs;
		if ($self->{plsql_pgsql}) {
			$functions{"$row->[0]"}{text} =~ s/[\[\]]//gs;
		}
	}

	return \%functions;
}

sub _lookup_function
{
	my ($self, $code, $fctname) = @_;

	my $type = 'functions';
	$type = lc($self->{type}) . 's' if ($self->{type} eq 'FUNCTION' or $self->{type} eq 'PROCEDURE');

	# Replace all double quote with single quote
	$code =~ s/"/'/g;
	# replace backquote with double quote
	$code =~ s/`/"/g;
	# Remove some unused code
	$code =~ s/\s+READS SQL DATA//igs;
	$code =~ s/\s+UNSIGNED\b((?:.*?)\bFUNCTION\b)/$1/igs;
	while ($code =~ s/(\s*DECLARE\s+)([^\r\n]+?),\s*\@/$1 $2\n$1 \@/is) {};

        my %fct_detail = ();
        $fct_detail{func_ret_type} = 'OPAQUE';

        # Split data into declarative and code part
        ($fct_detail{declare}, $fct_detail{code}) = split(/\b(?:BEGIN|SET|SELECT|INSERT|UPDATE|IF)\b/i, $code, 2);
	return if (!$fct_detail{code});

	# Look for table variables in code and rewrite them as temporary tables
	my $records = '';
	while ($fct_detail{code} =~ s/DECLARE\s+\@([^\s]+)\s+TABLE\s+(\(.*?[\)\w]\s*\))\s*([^,])/"CREATE TEMPORARY TABLE v_$1 $2" . (($3 eq ")") ? $3 : "") . ";"/eis)
	{
		my $varname = $1;
		$fct_detail{code} =~ s/\@$varname\b/v_$varname/igs;
	}

	# Move all DECLARE statements found in the code into the DECLARE section
	my @lines = split(/\n/, $fct_detail{code});
	$fct_detail{code} = '';
	foreach my $l (@lines)
	{
		if ($l !~ /^\s*DECLARE\s+.*CURSOR/ && $l =~ /^\s*DECLARE\s+(.*)/i) {
			$fct_detail{declare} .= "\n$1;";
		} else {
			$fct_detail{code} .= "$l\n";
		}
	}

	# Fix DECLARE section
	$fct_detail{declare} =~ s/\bDECLARE\s+//igs;
	if ($fct_detail{declare} !~ /\bDECLARE\b/i)
	{
		if ($fct_detail{declare} !~ s/(FUNCTION|PROCEDURE|PROC)\s+([^\s\(]+)[\)\s]+AS\s+(.*)/$1 $2\nDECLARE\n$3/is) {
			$fct_detail{declare} =~ s/(FUNCTION|PROCEDURE|PROC)\s+([^\s\(]+)\s+(.*\@.*?[\)\s]+)(RETURNS|AS)\s+(.*)/$1 $2 ($3)\n$4\nDECLARE\n$5/is;
		}
	}
	# Remove any label that was before the main BEGIN block
	$fct_detail{declare} =~ s/\s+[^\s\:]+:\s*$//gs;
	$fct_detail{declare} =~ s/(RETURNS.*TABLE.*\))\s*\)\s*AS\b/) $1 AS/is;

        @{$fct_detail{param_types}} = ();

	if ( ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE|PROC)\s+([^\s]+)\s+((?:RETURNS|AS)\s+.*)//is)
		|| ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE|PROC)\s+([^\s\(]+)(.*?)\s+((?:RETURNS|AS)\s+.*)//is)
		|| ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE|PROC)\s+(.*?)\s+((?:RETURNS|AS)\s+.*)//is)
		|| ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE|PROC)\s+([^\s\(]+)\s*(\(.*\))//is) )
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
		$fct_detail{type} = 'PROCEDURE' if ($fct_detail{type} eq 'PROC');
		$type = lc($fct_detail{type} . 's');
		$tmp_returned =~ s/RETURNS\s+DECLARE/RETURNS /is;
		if ($tmp_returned =~ s/\s+AS\s+(DECLARE.*)//is) {
			$fct_detail{declare} .= "$1\n";
		}
		$tmp_returned =~ s/RETURNS\s+(.*)\s+AS\s+.*/$1/is;
		if ($fct_detail{args} =~ s/\b(AS|DECLARE)\s+(.*)//is) {
			$tmp_returned = "DECLARE\n$1";
		}
		chomp($tmp_returned);

		$tmp_returned =~ s/[\)\s]AS\s+.*//is;
		$fct_detail{code} = "\n" . $fct_detail{code};

		$tmp_returned =~ s/\)\)$/\)/;
		$tmp_returned =~ s/\(MAX\)$//i;
		$fct_detail{args} =~ s/^\s*\(\s*\((.*)\)\s*\)$/$1/s;
		$fct_detail{args} =~ s/^\s*\(\s*(.*)\s*\)$/$1/s;
		#$fct_detail{code} =~ s/^DECLARE\b//is;
		if ($fct_detail{declare} =~ s/\s*COMMENT\s+(\?TEXTVALUE\d+\?|'[^\']+')//) {
			$fct_detail{comment} = $1;
		}
		$fct_detail{immutable} = 1 if ($fct_detail{declare} =~ s/\s*\bDETERMINISTIC\b//is);
		$fct_detail{before} = ''; # There is only garbage for the moment

                $fct_detail{name} =~ s/['"]//g;
                $fct_detail{fct_name} = $fct_detail{name};
		if (!$fct_detail{args}) {
			$fct_detail{args} = '()';
		}
		$fct_detail{immutable} = 1 if ($fct_detail{return} =~ s/\s*\bDETERMINISTIC\b//is);
		$fct_detail{immutable} = 1 if ($tmp_returned =~ s/\s*\bDETERMINISTIC\b//is);
		$tmp_returned =~ s/^\s+//;
		$tmp_returned =~ s/\s+$//;

		$fctname = $fct_detail{name} || $fctname;
		if ($type eq 'functions' && exists $self->{$type}{$fctname}{return} && $self->{$type}{$fctname}{return})
		{
			$fct_detail{hasreturn} = 1;
			$fct_detail{func_ret_type} = $self->_sql_type($self->{$type}{$fctname}{return});
		}
		elsif ($type eq 'functions' && !exists $self->{$type}{$fctname}{return} && $tmp_returned)
		{
			$tmp_returned =~ s/\s+CHARSET.*//is;
			#$fct_detail{func_ret_type} = $self->_sql_type($tmp_returned);
			$fct_detail{func_ret_type} = replace_sql_type($self, $tmp_returned);
			$fct_detail{hasreturn} = 1;
		}
		$fct_detail{language} = $self->{$type}{$fctname}{language};
		$fct_detail{immutable} = 1 if ($self->{$type}{$fctname}{immutable} eq 'YES');
		$fct_detail{security} = $self->{$type}{$fctname}{security};

                if ($fct_detail{func_ret_type} =~ s/RETURNS\s+\@(.*?)\s+TABLE/TABLE/is) {
			$fct_detail{declare} .= "v_$1 record;\n";
		}
                $fct_detail{func_ret_type} =~ s/RETURNS\s*//is;

		# Procedure that have out parameters are functions with PG
		if ($type eq 'procedures' && $fct_detail{args} =~ /\b(OUT|INOUT)\b/) {
			# set return type to empty to avoid returning void later
			$fct_detail{func_ret_type} = ' ';
		}

		# IN OUT should be INOUT
		$fct_detail{args} =~ s/\bIN\s+OUT/INOUT/igs;

		# Move the DECLARE statement from code to the declare section.
		#$fct_detail{declare} = '';
		while ($fct_detail{code} =~ s/DECLARE\s+([^;\n\r]+)//is)
		{
			my $var = $1;
			$fct_detail{declare} .= "\n$var" if ($fct_detail{declare} !~ /v_$var /is);
		}
		# Rename arguments with @ replaced by p_
		($fct_detail{args}, $fct_detail{declare}, $fct_detail{code}) = replace_mssql_params($self, $fct_detail{args}, $fct_detail{declare}, $fct_detail{code});

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

	# Append TABLE declaration to the declare section
	$fct_detail{declare} .= "\n$records" if ($records);
	# Rename variables with @ replaced by v_
	($fct_detail{code}, $fct_detail{declare}) = replace_mssql_variables($self, $fct_detail{code}, $fct_detail{declare});

	$fct_detail{args} =~ s/\s*$//s;
	$fct_detail{args} =~ s/^\s*//s;
	$fct_detail{code} =~ s/^[\r\n]*/\n/s;


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

sub replace_mssql_variables
{
	my ($self, $code, $declare) = @_;

	# Look for mssql global variables and add them to the custom variable list
	while ($code =~ s/\b(?:SET\s+)?\@\@(?:SESSION\.)?([^\s:=]+)\s*:=\s*([^;]+);/PERFORM set_config('$2', $2, false);/is)
	{
		my $n = $1;
		my $v = $2;
		$self->{global_variables}{$n}{name} = lc($n);
		# Try to set a default type for the variable
		$self->{global_variables}{$n}{type} = 'bigint';
		if ($v =~ /'[^\']*'/) {
			$self->{global_variables}{$n}{type} = 'varchar';
		}
		if ($n =~ /datetime/i) {
			$self->{global_variables}{$n}{type} = 'timestamp';
		} elsif ($n =~ /time/i) {
			$self->{global_variables}{$n}{type} = 'time';
		} elsif ($n =~ /date/i) {
			$self->{global_variables}{$n}{type} = 'date';
		} 
	}

	my @to_be_replaced = ();
	# Look for local variable definition and append them to the declare section
	while ($code =~ s/SET\s+\@([^\s=]+)\s*=\s*/v_$1 := /is)
	{
		my $n = $1;
		push(@to_be_replaced, $n);
	}

	# Look for local variable definition and append them to the declare section
	while ($code =~ s/(^|[^\@])\@([^\s:=,]+)\s*:=\s*(.*)/$1v_$2 := $3/is)
	{
		my $n = $2;
		my $v = $3;
		# Try to set a default type for the variable
		my $type = 'integer';
		$type = 'varchar' if ($v =~ /'[^']*'/);
		if ($n =~ /datetime/i) {
			$type = 'timestamp';
		} elsif ($n =~ /time/i) {
			$type = 'time';
		} elsif ($n =~ /date/i) {
			$type = 'date';
		} 
		$declare .= "v_$n $type;\n" if ($declare !~ /\b$n $type;/s);
		push(@to_be_replaced, $n);
	}

	# Fix other call to the same variable in the code
	foreach my $n (@to_be_replaced) {
		$code =~ s/\@$n\b/v_$n/gs;
	}

	# Look for variable definition in DECLARE section and rename them in the code too
	while ($declare =~ s/(^|[^\@])\@([a-z0-9_]+)/$1v_$2/is)
	{
		my $n = $2;
		# Fix other call to the same variable in the code
		$code =~ s/\@$n\b/v_$n/gs;
	}

	# Look for some global variable definition and append them to the declare section
	while ($code =~ /\@\@(ROWCOUNT|VERSION|LANGUAGE|SPID|MICROSOFTVERSION)/is)
	{
		my $v = uc($1);
		if ($v eq 'VERSION') {
			$code =~ s/\@\@$v/version()/igs;
		} elsif ($v eq 'LANGUAGE') {
			$code =~ s/\@\@$v/current_setting('client_encoding')/igs;
		} elsif ($v eq 'ROWCOUNT') {
			$declare .= "v_v_rowcount bigint;\n" if ($declare !~ /v_v_rowcount/s);
			$code =~ s/([\r\n])([^\r\n]+?)\@\@$v/\nGET DIAGNOSTICS v_v_rowcount := ROWCOUNT;\n$2 v_v_rowcount/igs;
		} elsif ($v eq 'SPID') {
			$code =~ s/\@\@$v/pg_backend_pid()/igs;
		} elsif ($v eq 'MICROSOFTVERSION') {
			$code =~ s/\@\@$v/current_setting('server_version')/igs;
		}
	}

	# Look for local variable definition and append them to the declare section
	while ($code =~ s/(^|[^\@])\@([a-z0-9_\$]+)/$1v_$2/is)
	{
		my $n = $2;
		next if ($n =~ /^v_/);
		# Try to set a default type for the variable
		my $type = 'varchar';
		if ($n =~ /datetime/i) {
			$type = 'timestamp';
		} elsif ($n =~ /time/i) {
			$type = 'time';
		} elsif ($n =~ /date/i) {
			$type = 'date';
		} 
		$declare .= "v_$n $type;\n" if ($declare !~ /v_$n ($type|record);/is);
		# Fix other call to the same variable in the code
		$code =~ s/\@$n\b/v_$n/gs;
	}

	# Look for variable definition with SELECT statement
	$code =~ s/\bSET\s+([^\s=]+)\s*=\s*([^;]+\bSELECT\b[^;]+);/$1 = $2;/igs;

	return ($code, $declare);
}

sub _list_all_functions
{
	my $self = shift;

	# Retrieve all functions 
	# ROUTINE_SCHEMA           | varchar(64)   | NO   |     |                     |       |
	# ROUTINE_NAME             | varchar(64)   | NO   |     |                     |       |
	# ROUTINE_TYPE             | varchar(9)    | NO   |     |                     |       |

	my $str = "SELECT ROUTINE_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.ROUTINES";
	if ($self->{schema}) {
		$str .= " AND ROUTINE_SCHEMA = '$self->{schema}'";
	}
	if ($self->{db_version} < '5.5.0') {
		$str =~ s/\bDATA_TYPE\b/DTD_IDENTIFIER/;
	}
	$str .= " " . $self->limit_to_objects('FUNCTION','ROUTINE_NAME');
	$str =~ s/ AND / WHERE /;
	$str .= " ORDER BY ROUTINE_NAME";
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

	# Don't work with Azure
	return if ($self->{db_version} =~ /Microsoft SQL Azure/);

	# Retrieve all database link from dba_db_links table
	my $str = qq{SELECT 
  name,
  provider,
  data_source,
  catalog
FROM sys.servers
WHERE is_linked = 1
};
	$str .= $self->limit_to_objects('DBLINK', 'name');
	$str .= " ORDER BY name";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch)
	{
		my $port = '1433';
		if ($row->[2] =~ s/,(\d+)$//) {
			$port = $1;
		}
		$data{$row->[0]}{owner} = 'unknown';
		$data{$row->[0]}{username} = 'unknown';
		$data{$row->[0]}{host} = $row->[2];
		$data{$row->[0]}{db} = $row->[3] || 'unknown';
		$data{$row->[0]}{port} = $port;
		$data{$row->[0]}{backend} = $row->[1] || 'SQL Server';
	}

	return %data;
}

=head2 _get_partitions

This function implements an Informix-native partitions information.
Return two hash ref with partition details and partition default.
=cut

sub _get_partitions
{
	my ($self) = @_;

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
		push(@{$parts{$row->[1]}{$row->[3]}{info}}, { 'type' => 'RANGE', 'value' => $row->[7], 'column' => $col, 'colpos' => $row->[10], 'tablespace' => '', 'owner' => ''});
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

	# For what I know, subpartition is not supported by Informix
	return \%subparts, \%default;
}

=head2 _get_partitions_list

This function implements a Informix-native partitions information.
Return a hash of the partition table_name => type

=cut

sub _get_partitions_list
{
	my($self) = @_;

	# Retrieve all partitions.
	my $str = qq{
SELECT sch.name AS SchemaName, t.name AS TableName, i.name AS IndexName,
    p.partition_number, p.partition_id, i.data_space_id, f.function_id, f.type_desc,
    r.boundary_id, r.value AS BoundaryValue, ic.column_id AS PartitioningColumnID,
    c.name AS PartitioningColumnName
FROM sys.tables AS t
JOIN sys.indexes AS i ON t.object_id = i.object_id AND i.[type] <= 1
JOIN sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.partition_schemes AS s ON i.data_space_id = s.data_space_id
JOIN sys.index_columns AS ic ON ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id AND ic.partition_ordinal >= 1 -- because 0 = non-partitioning column
JOIN sys.columns AS c ON t.[object_id] = c.[object_id] AND ic.column_id = c.column_id
JOIN sys.partition_functions AS f ON s.function_id = f.function_id
LEFT JOIN sys.partition_range_values AS r ON f.function_id = r.function_id and r.boundary_id = p.partition_number
LEFT OUTER JOIN sys.schemas sch ON t.schema_id = sch.schema_id
};

	$str .= $self->limit_to_objects('TABLE|PARTITION','t.name|t.name');
	if ($self->{schema}) {
		$str .= " WHERE sch.name ='$self->{schema}'";
	}
	$str .= " ORDER BY sch.name, t.name, i.name, p.partition_number, ic.column_id\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch)
	{
		$parts{"\L$row->[1]\E"}++;
#		if ($self->{export_schema} && !$self->{schema}) {
#			$row->[1] = "$row->[0].$row->[1]";
#		}
#               $parts{"\L$row->[1]\E"}{count}++;
#               $parts{"\L$row->[1]\E"}{composite} = 0;
#		$parts{"\L$row->[1]\E"}{type} = 'RANGE';
#		push(@{ $parts{"\L$row->[1]\E"}{columns} }, $row->[11]) if (!grep(/^$row->[11]$/, @{ $parts{"\L$row->[1]\E"}{columns} }));
	}
	$sth->finish;

	return %parts;
}

=head2 _get_partitioned_table

Return a hash of the partitioned table with the number of partition

=cut

sub _get_partitioned_table
{
	my ($self, %subpart) = @_;

	return;

	# Retrieve all partitions.
	my $str = qq{
SELECT sch.name AS SchemaName, t.name AS TableName, i.name AS IndexName,
    p.partition_number, p.partition_id, i.data_space_id, f.function_id, f.type_desc,
    r.boundary_id, r.value AS BoundaryValue, ic.column_id AS PartitioningColumnID,
    c.name AS PartitioningColumnName
FROM sys.tables AS t
JOIN sys.indexes AS i ON t.object_id = i.object_id AND i.[type] <= 1
JOIN sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.partition_schemes AS s ON i.data_space_id = s.data_space_id
JOIN sys.index_columns AS ic ON ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id AND ic.partition_ordinal >= 1 -- because 0 = non-partitioning column
JOIN sys.columns AS c ON t.[object_id] = c.[object_id] AND ic.column_id = c.column_id
JOIN sys.partition_functions AS f ON s.function_id = f.function_id
LEFT JOIN sys.partition_range_values AS r ON f.function_id = r.function_id and r.boundary_id = p.partition_number
LEFT OUTER JOIN sys.schemas sch ON t.schema_id = sch.schema_id
};

	$str .= $self->limit_to_objects('TABLE|PARTITION','t.name|t.name');
	if ($self->{schema}) {
		$str .= " WHERE sch.name ='$self->{schema}'";
	}
	$str .= " ORDER BY sch.name, t.name, i.name, p.partition_number, ic.column_id\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch)
	{
		if ($self->{export_schema} && !$self->{schema}) {
			$row->[1] = "$row->[0].$row->[1]";
		}
                $parts{$row->[1]}{count}++;
                $parts{$row->[1]}{composite} = 0;
		$parts{$row->[1]}{type} = 'RANGE';
		push(@{ $parts{$row->[1]}{columns} }, $row->[11]) if (!grep(/^$row->[11]$/, @{ $parts{$row->[1]}{columns} }));
		#dbo | PartitionTable | PK__Partitio__357D0D3E1290FD9F | 2 | 72057594048872448 | 65601 | 65536 | RANGE | 2 | 2022-05-01 00:00:00 | 1 | col1
	}
	$sth->finish;

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
	my $sql = "SELECT t.name FROM sys.tables t INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.is_ms_shipped = 0 AND i.OBJECT_ID > 255 AND t.type='U' AND t.NAME NOT LIKE '#%'";
	if (!$self->{schema}) {
		$sql .= " AND s.name NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$sql.= " AND s.name = '$self->{schema}'";
	}
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{TABLE}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	# VIEW
	$sql = "SELECT v.name from sys.views v join sys.sql_modules m on m.object_id = v.object_id WHERE NOT EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = v.object_id and i.index_id = 1 and i.ignore_dup_key = 0) AND is_date_correlation_view=0";
	if (!$self->{schema}) {
		$sql .= " AND schema_name(v.schema_id) NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$sql.= " AND schema_name(v.schema_id) = '$self->{schema}'";
	}
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{VIEW}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	# TRIGGER
	$sql = "SELECT o.name FROM sys.sysobjects o INNER JOIN sys.tables t ON o.parent_obj = t.object_id INNER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE o.type = 'TR'";
	if (!$self->{schema}) {
		$sql .= " AND s.name NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$sql.= " AND s.name = '$self->{schema}'";
	}
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{TRIGGER}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	# INDEX
	foreach my $t (@{$infos{TABLE}})
	{
		my $sql = "SELECT Id.name AS index_name FROM sys.tables AS T INNER JOIN sys.indexes Id ON T.object_id = Id.object_id LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE T.is_ms_shipped = 0 AND Id.auto_created = 0 AND OBJECT_NAME(Id.object_id, DB_ID())='$t' AND Id.is_primary_key = 0";
		if (!$self->{schema}) {
			$sql .= " AND s.name NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
		} else {
			$sql.= " AND s.name = '$self->{schema}'";
		}
		$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my @row = $sth->fetchrow()) {
			next if ($row[2] eq 'PRIMARY');
			push(@{$infos{INDEX}}, { ( name => $row[2], invalid => 0) });
		}
	}
	# FUNCTION
	$sql = "SELECT O.name FROM sys.sql_modules M JOIN sys.objects O ON M.object_id=O.object_id JOIN sys.schemas AS s ON o.schema_id = s.schema_id WHERE O.type IN ('IF','TF','FN')";
	if (!$self->{schema}) {
		 $sql .= " AND s.name NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$sql .= " AND s.name = '$self->{schema}'";
	}
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{FUNCTION}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();

	# PROCEDURE
	$sql = "SELECT O.name FROM sys.sql_modules M JOIN sys.objects O ON M.object_id=O.object_id JOIN sys.schemas AS s ON o.schema_id = s.schema_id WHERE O.type = 'P'";
	if (!$self->{schema}) {
		 $sql .= " AND s.name NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$sql .= " AND s.name = '$self->{schema}'";
	}
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{PROCEDURE}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();

	# PARTITION.
	$sql = qq{
SELECT sch.name AS SchemaName, t.name AS TableName, i.name AS IndexName,
    p.partition_number, p.partition_id, i.data_space_id, f.function_id, f.type_desc,
    r.boundary_id, r.value AS BoundaryValue, ic.column_id AS PartitioningColumnID,
    c.name AS PartitioningColumnName
FROM sys.tables AS t
JOIN sys.indexes AS i ON t.object_id = i.object_id AND i.[type] <= 1
JOIN sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.partition_schemes AS s ON i.data_space_id = s.data_space_id
JOIN sys.index_columns AS ic ON ic.[object_id] = i.[object_id] AND ic.index_id = i.index_id AND ic.partition_ordinal >= 1 -- because 0 = non-partitioning column
JOIN sys.columns AS c ON t.[object_id] = c.[object_id] AND ic.column_id = c.column_id
JOIN sys.partition_functions AS f ON s.function_id = f.function_id
LEFT JOIN sys.partition_range_values AS r ON f.function_id = r.function_id and r.boundary_id = p.partition_number
LEFT OUTER JOIN sys.schemas sch ON t.schema_id = sch.schema_id
};
	if ($self->{schema}) {
		$sql .= " WHERE sch.name ='$self->{schema}'";
	}

	$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{'TABLE PARTITION'}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish;

	# MATERIALIZED VIEW
	$sql = qq{select
       v.name as view_name,
       schema_name(v.schema_id) as schema_name,
       i.name as index_name,
       m.definition
from sys.views v
join sys.indexes i on i.object_id = v.object_id and i.index_id = 1 and i.ignore_dup_key = 0
join sys.sql_modules m on m.object_id = v.object_id
};

	if (!$self->{schema}) {
		$sql .= " WHERE schema_name(v.schema_id) NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$sql .= " WHERE schema_name(v.schema_id) = '$self->{schema}'";
	}
	$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{'MATERIALIZED VIEW'}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish;

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

       my $sql = qq{SELECT
   d.name,
   m.size * 8 / 1024
FROM sys.master_files m JOIN sys.databases d ON d.database_id = m.database_id and m.type = 0
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

	# Retrieve all synonym
	my $str = qq{SELECT
	n.name AS SchemaName, 
	sy.name AS synonym_name,
	sy.base_object_name AS synonym_definition,
	COALESCE(PARSENAME(sy.base_object_name, 4), \@\@servername) AS server_name,
	COALESCE(PARSENAME(sy.base_object_name, 3), DB_NAME(DB_ID())) AS DB_name,
	COALESCE(PARSENAME (sy.base_object_name, 2), SCHEMA_NAME(SCHEMA_ID ())) AS schema_name,
	PARSENAME(sy.base_object_name, 1) AS table_name,
	\@\@servername AS local_server
FROM sys.synonyms sy
LEFT OUTER JOIN sys.schemas n ON sy.schema_id = n.schema_id
};
	if ($self->{schema}) {
		$str .= " WHERE n.name='$self->{schema}' ";
	} else {
		$str .= " WHERE n.name NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	$str .= $self->limit_to_objects('SYNONYM','sy.name');

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %synonyms = ();
	while (my $row = $sth->fetch)
	{
                if (!$self->{schema} && $self->{export_schema}) {
                        $row->[1] = $row->[0] . '.' . $row->[1];
                }
		$synonyms{$row->[1]}{owner} = $row->[0];
		$synonyms{$row->[1]}{table_owner} = $row->[5];
		$synonyms{$row->[1]}{table_name} = $row->[6];
		if ($row->[3] ne $row->[7]) {
			$synonyms{$row->[1]}{dblink} = $row->[3];
		}
	}
	$sth->finish;

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
     o.name AS trigger_name 
    ,OBJECT_NAME(o.parent_obj) AS table_name 
    ,s.name AS table_schema 
FROM sys.sysobjects o
INNER JOIN sys.tables t ON o.parent_obj = t.object_id 
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id 
WHERE o.type = 'TR'
};

	if ($self->{schema}) {
		$str .= " AND s.name = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('TABLE|VIEW|TRIGGER','t.name|t.name|o.name');

	$str .= " ORDER BY t.name, o.name";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %triggers = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[1] = "$row->[2].$row->[1]";
		}
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

	return; # not supported 
	# Retrieve all indexes 
	my $str = qq{SELECT
	s.name As SchemaName,
	t.name As TableName,
	i.name as ColumnName,
	i.seed_value,
	i.increment_value,
	i.last_value
FROM sys.tables t
JOIN sys.identity_columns i ON t.object_id=i.object_id
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
};
	if (!$self->{schema}) {
		$str .= " WHERE s.name NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE s.name = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('TABLE', 't.name');

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %seqs = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[1] = "$row->[0].$row->[1]";
		}
		# GENERATION_TYPE can be ALWAYS, BY DEFAULT and BY DEFAULT ON NULL
		$seqs{$row->[1]}{$row->[2]}{generation} = 'BY DEFAULT';
		# SEQUENCE options
		$row->[5] = $row->[3] || 1 if ($row->[5] eq '');
		$seqs{$row->[1]}{$row->[2]}{options} = "START WITH $row->[5]";
		$seqs{$row->[1]}{$row->[2]}{options} .= " INCREMENT BY $row->[4]";
		$seqs{$row->[1]}{$row->[2]}{options} .= " MINVALUE $row->[3]" if ($row->[3] ne '');
		# For default values don't use option at all
		if ( $seqs{$row->[1]}{$row->[2]}{options} eq 'START WITH 1 INCREMENT BY 1 MINVALUE 1') {
			delete $seqs{$row->[1]}{$row->[2]}{options};
		}
	}

	return %seqs;
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
	my $str = qq{SELECT
	t1.name, s.name, t2.name, t1.precision, t1.scale, t1.max_length, t1.is_nullable,
	object_definition(t1.default_object_id), object_definition(t1.rule_object_id), t1.is_table_type
FROM sys.types t1
JOIN sys.types t2 ON t2.system_type_id = t1.system_type_id AND t2.is_user_defined = 0
LEFT OUTER JOIN sys.schemas s ON t1.schema_id = s.schema_id
WHERE t1.is_user_defined = 1 AND t2.name <> 'sysname'};

	if ($name) {
		$str .= " AND t1.name='$name'";
	}
	if ($self->{schema}) {
		$str .= "AND s.name='$self->{schema}' ";
	}
	if (!$name) {
		$str .= $self->limit_to_objects('TYPE', 't1.name');
	} else {
		@{$self->{query_bind_params}} = ();
	}
	$str .= " ORDER BY t1.name";
	# use a separeate connection
	my $local_dbh = _db_connection($self);

	my $sth = $local_dbh->prepare($str) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);

	my @types = ();
	my @fct_done = ();
	while (my $row = $sth->fetch)
	{
		my $origname = $row->[0];
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
		$self->logit("\tFound Type: $row->[0]\n", 1);
		next if (grep(/^$row->[0]$/, @fct_done));
		push(@fct_done, $row->[0]);
		my %tmp = ();
		if (!$row->[9])
		{
			my $precision = '';
			if ($row->[3]) {
				$precision .= "($row->[3]";
				$precision .= ",$row->[4]" if ($row->[4]);
			} elsif ($row->[5]) {
				$precision .= "($row->[5]";
			}
			$precision .= ")" if ($precision);
			my $notnull = '';
			$notnull = ' NOT NULL' if (!$row->[6]);
			my $default = '';
			if ($row->[7]) {
				$row->[7] =~ s/\s*CREATE\s+DEFAULT\s+.*\s+AS\s*//is;
				$default = " DEFAULT $row->[7]";
			}
			my $rule = '';
			if ($row->[8]) {
				$row->[8] =~ s/\s*CREATE\s+RULE\s+.*\s+AS\s*//is;
				$row->[8] =~ s/\@[a-z0-1_\$\#]+/VALUE/igs;
				$rule = " CHECK ($row->[8])";
				$rule =~ s/[\r\n]+/ /gs;
			}
			$tmp{code} = "CREATE TYPE $row->[0] FROM $row->[2]$precision$notnull$default$rule;";
			# Add domain type to main type convertion hash
			if ($self->{is_informix} && !exists $self->{data_type}{uc($origname)}
					&& ($self->{type} eq 'COPY' || $self->{type} eq 'INSERT')
			)
			{
				$self->{data_type}{uc($origname)} = replace_sql_type($self, $row->[2]);
			}
		}
		$tmp{name} = $row->[0];
		$tmp{owner} = $row->[1];
		$tmp{pos} = $idx++;
		if (!$self->{preserve_case})
		{
			$tmp{code} =~ s/(TYPE\s+)"[^"]+"\."[^"]+"/$1\L$row->[0]\E/igs;
			$tmp{code} =~ s/(TYPE\s+)"[^"]+"/$1\L$row->[0]\E/igs;
		}
		else
		{
			$tmp{code} =~ s/((?:CREATE|REPLACE|ALTER)\s+TYPE\s+)([^"\s]+)\s/$1"$2" /igs;
		}
		$tmp{code} =~ s/\s+ALTER/;\nALTER/igs;
		push(@types, \%tmp);
	}
	$sth->finish();

	# Retrieve all user defined table types => PostgreSQL TYPE
	$str = qq{SELECT
	t1.name AS table_Type, s.name SchemaName, c.name AS ColName, c.column_id, y.name AS DataType,
	c.precision, c.scale, c.max_length, c.is_nullable, object_definition(t1.default_object_id),
	object_definition(t1.rule_object_id)
FROM sys.table_types t1
INNER JOIN sys.columns c ON c.object_id = t1.type_table_object_id
INNER JOIN sys.types y ON y.user_type_id = c.user_type_id
LEFT OUTER JOIN sys.schemas s ON t1.schema_id = s.schema_id
};
	if ($name) {
		$str .= " AND t1.name='$name'";
	}
	if ($self->{schema}) {
		$str .= "AND s.name='$self->{schema}' ";
	}
	if (!$name) {
		$str .= $self->limit_to_objects('TYPE', 't1.name');
	} else {
		@{$self->{query_bind_params}} = ();
	}
	$str =~ s/ AND / WHERE /s;
	$str .= " ORDER BY t1.name, c.column_id";

	$sth = $local_dbh->prepare($str) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);

	my $current_type = '';
	my $old_type = '';
	while (my $row = $sth->fetch)
	{
		next if ($self->{drop_rowversion} && ($row->[4] eq 'rowversion' || $row->[4] eq 'timestamp'));

		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
		if ($old_type ne $row->[0]) {
			$self->logit("\tFound Type: $row->[0]\n", 1);

			if ($current_type ne '') {
				$current_type =~ s/,$//s;
				$current_type .= ");";

				my %tmp = (
					code => $current_type,
					name => $old_type,
					owner => '',
					pos => $idx++
				);
				if (!$self->{preserve_case})
				{
					$tmp{code} =~ s/(TYPE\s+)"[^"]+"\."[^"]+"/$1\L$old_type\E/igs;
					$tmp{code} =~ s/(TYPE\s+)"[^"]+"/$1\L$old_type\E/igs;
				}
				else
				{
					$tmp{code} =~ s/((?:CREATE|REPLACE|ALTER)\s+TYPE\s+)([^"\s]+)\s/$1"$2" /igs;
				}
				$tmp{code} =~ s/\s+ALTER/;\nALTER/igs;
				push(@types, \%tmp);
				$current_type = '';
			}
			$old_type = $row->[0];
		}
		if ($current_type eq '') {
			$current_type = "CREATE TYPE $row->[0] AS OBJECT ("
		}
		
		my $precision = '';
		if ($row->[5]) {
			$precision .= "($row->[5]";
			$precision .= ",$row->[6]" if ($row->[6]);
		} elsif ($row->[7]) {
			$precision .= "($row->[7]";
		}
		$precision .= ")" if ($precision);
		my $notnull = '';
		$notnull = 'NOT NULL' if (!$row->[8]);
		my $default = '';
		if ($row->[9]) {
			$row->[9] =~ s/\s*CREATE\s+DEFAULT\s+.*\s+AS\s*//is;
			$default = " DEFAULT $row->[9]";
		}
		my $rule = '';
		if ($row->[10]) {
			$row->[10] =~ s/\s*CREATE\s+RULE\s+.*\s+AS\s*//is;
			$row->[10] =~ s/\@[a-z0-1_\$\#]+/VALUE/igs;
			$rule = " CHECK ($row->[10])";
		}
		$current_type .= "\n\t$row->[2] $row->[4]$precision $notnull$default$rule,"
	}
	$sth->finish();

	$local_dbh->disconnect() if ($local_dbh);

	# Process last table type
	if ($current_type ne '')
	{
		$current_type =~ s/,$//s;
		$current_type .= ");";

		my %tmp = (
			code => $current_type,
			name => $old_type,
			owner => '',
			pos => $idx++
		);
		if (!$self->{preserve_case})
		{
			$tmp{code} =~ s/(TYPE\s+)"[^"]+"\."[^"]+"/$1\L$old_type\E/igs;
			$tmp{code} =~ s/(TYPE\s+)"[^"]+"/$1\L$old_type\E/igs;
		}
		else
		{
			$tmp{code} =~ s/((?:CREATE|REPLACE|ALTER)\s+TYPE\s+)([^"\s]+)\s/$1"$2" /igs;
		}
		$tmp{code} =~ s/\s+ALTER/;\nALTER/igs;
		push(@types, \%tmp);
	}

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

