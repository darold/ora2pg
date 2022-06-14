package Ora2Pg::MSSQL;

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
	'TINYINT' => 'smallint', # 1 byte
	'SMALLINT' => 'smallint', # 2 bytes
	'INT' => 'integer', # 4 bytes
	'BIGINT' => 'bigint', # 8 bytes
	'DECIMAL' => 'numeric',
	'NUMERIC' => 'numeric',
	'BIT' => 'boolean',
	'MONEY' => 'numeric(15,4)',
	'SMALLMONEY' => 'numeric(6,4)',
	'FLOAT' => 'double precision',
	'REAL' => 'real',
	'DATE' => 'date',
	'SMALLDATETIME' => 'timestamp(0) without time zone',
	'DATETIME' => 'timestamp(3) without time zone',
	'DATETIME2' => 'timestamp without time zone',
	'DATETIMEOFFSET' => 'timestamp with time zone',
	'TIME' => 'time without time zone',
	'CHAR' => 'char',
	'VARCHAR' => 'varchar',
	'TEXT' => 'text',
	'NCHAR' => 'char',
	'NVARCHAR' => 'varchar',
	'NTEXT' => 'text',
	'VARBINARY' => 'bytea',
	'BINARY' => 'bytea',
	'IMAGE' => 'bytea',
	'UNIQUEIDENTIFIER' => 'uuid',
	'ROWVERSION' => 'bytea',
	'TIMESTAMP' => 'timestamp without time zone DEFAULT now()', # synonym of ROWVERSION
	'XML' => 'xml',
	'HIERARCHYID' => 'varchar', # The application need to handle the value, no PG equivalent
	'GEOMETRY' => 'geometry',
	'GEOGRAPHY' => 'geometry',
	'SYSNAME' => 'varchar(256)',
	'SQL_VARIANT' => 'text'
);

sub _db_connection
{
	my $self = shift;

	$self->logit("Trying to connect to database: $self->{oracle_dsn}\n", 1) if (!$self->{quiet});

	if (!defined $self->{oracle_pwd})
	{
		eval("use Term::ReadKey;");
		if (!$@) {
			$self->{oracle_user} = $self->_ask_username('MSSQL') unless (defined $self->{oracle_user});
			$self->{oracle_pwd} = $self->_ask_password('MSSQL');
		}
	}

	my $dbh = DBI->connect("$self->{oracle_dsn}", $self->{oracle_user}, $self->{oracle_pwd}, {
			'RaiseError' => 1,
			AutoInactiveDestroy => 1,
			odbc_cursortype => 2,
		}
	);

	# Check for connection failure
	if (!$dbh) {
		$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
	}

	# Use consistent reads for concurrent dumping...
	#$dbh->do('START TRANSACTION WITH CONSISTENT SNAPSHOT;') || $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	if ($self->{debug} && !$self->{quiet}) {
		$self->logit("Isolation level: $self->{transaction}\n", 1);
	}
	my $sth = $dbh->prepare($self->{transaction}) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	$sth->finish;

#	if ($self->{nls_lang})
#	{
#		if ($self->{debug} && !$self->{quiet}) {
#			$self->logit("Set default encoding to '$self->{nls_lang}' and collate to '$self->{nls_nchar}'\n", 1);
#		}
#		my $collate = '';
#		$collate = " COLLATE '$self->{nls_nchar}'" if ($self->{nls_nchar});
#		$sth = $dbh->prepare("SET NAMES '$self->{nls_lang}'$collate") or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
#		$sth->execute or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
#		$sth->finish;
#	}
	# Force execution of initial command
	$self->_ora_initial_command($dbh);

	# Instruct Ora2Pg that the database engine is mysql
	$self->{is_mssql} = 1;

	return $dbh;
}

sub _get_version
{
	my $self = shift;

	my $oraver = '';
	my $sql = "SELECT \@\@VERSION";

        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$oraver = $row[0];
		last;
	}
	$sth->finish();

	$oraver =~ s/ \- .*//;

	return $oraver;
}

sub _schema_list
{
	my $self = shift;

	my $sql = qq{
SELECT s.name as schema_name, 
    s.schema_id,
    u.name as schema_owner
from sys.schemas s
    inner join sys.sysusers u
        on u.uid = s.principal_id
WHERE s.name NOT IN ('information_schema', 'sys', 'db_accessadmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_ddladmin', 'db_denydatareader', 'db_denydatawriter', 'db_owner', 'db_securityadmin')
order by s.name;
};

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

	my $sql = qq{SELECT
--       os_language_version,
--       SERVERPROPERTY('LCID') AS 'Instance-LCID',
       SERVERPROPERTY('Collation') AS 'Instance-Collation',
--       SERVERPROPERTY('ComparisonStyle') AS 'Instance-ComparisonStyle',
--       SERVERPROPERTY('SqlSortOrder') AS 'Instance-SqlSortOrder',
       SERVERPROPERTY('SqlSortOrderName') AS 'Instance-SqlSortOrderName'
--       SERVERPROPERTY('SqlCharSet') AS 'Instance-SqlCharSet'
--       SERVERPROPERTY('SqlCharSetName') AS 'Instance-SqlCharSetName',
--       DATABASEPROPERTYEX(N'{database_name}', 'LCID') AS 'Database-LCID',
--       DATABASEPROPERTYEX(N'{database_name}', 'Collation') AS 'Database-Collation',
--       DATABASEPROPERTYEX(N'{database_name}', 'ComparisonStyle') AS 'Database-ComparisonStyle',
--       DATABASEPROPERTYEX(N'{database_name}', 'SQLSortOrder') AS 'Database-SQLSortOrder'
-- FROM   sys.dm_os_windows_info;
};
        my $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
        $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	my $my_encoding = '';
	my $my_client_encoding = '';
	while ( my @row = $sth->fetchrow())
	{
		$my_encoding = $row[0];
		$my_client_encoding = $row[1];
	}
	$sth->finish();

	my $my_timestamp_format = '';
	my $my_date_format = '';
	$sql = qq{SELECT date_format
FROM sys.dm_exec_sessions
WHERE session_id = \@\@spid
	};
        $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
        $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		$my_date_format = $row[0];
	}
	$sth->finish();

	#my $pg_encoding = auto_set_encoding($charset);
	my $pg_encoding = $my_encoding;

	return ($my_encoding, $my_client_encoding, $pg_encoding, $my_timestamp_format, $my_date_format);
}


=head2 _table_info

This function retrieves all MSSQL tables information.

Returns a handle to a DB query statement.

=cut

sub _table_info
{
	my $self = shift;
	my $do_real_row_count = shift;

	# First register all tablespace/table in memory from this database
	my %tbspname = ();
#	my $sth = $self->{dbh}->prepare("SELECT DISTINCT TABLE_NAME, TABLESPACE_NAME FROM INFORMATION_SCHEMA.FILES WHERE table_schema = '$self->{schema}' AND TABLE_NAME IS NOT NULL") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0);
#	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
#	while (my $r = $sth->fetch) {
#		$tbspname{$r->[0]} = $r->[1];
#	}
#	$sth->finish();

	# Table: information_schema.tables
	# TABLE_CATALOG   | nvarchar(128)
	# TABLE_SCHEMA    | nvarchar(128)
	# TABLE_NAME      | sysname
	# TABLE_TYPE      | varchar(10)

	# Use SYS.TABLES instead, there is more information available

        my $schema_clause = '';
        $schema_clause = " AND s.name='$self->{schema}'" if ($self->{schema});
	my $sql = qq{SELECT t.NAME AS TABLE_NAME, NULL AS comment, t.type_desc as TABLE_TYPE, p.rows AS RowCounts, SUM(a.used_pages)  * 8 / 1024 AS UsedSpaceMB, CONVERT(DECIMAL,SUM(a.total_pages)) * 8 / 1024 AS TotalSpaceMB, s.Name AS TABLE_SCHEMA, SCHEMA_NAME(t.principal_id)
FROM sys.tables t
INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0 AND i.OBJECT_ID > 255 AND t.type='U' AND t.NAME NOT LIKE '#%' $schema_clause
};
	my %tables_infos = ();
	my %comments = ();
	$sql .= $self->limit_to_objects('TABLE', 't.Name');
	$sql .= " GROUP BY t.type_desc, s.Name, t.Name, SCHEMA_NAME(t.principal_id), p.Rows ORDER BY s.Name, t.Name";
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[6].$row->[0]";
		}
		$row->[2] =~ s/^USER_//;
		$comments{$row->[0]}{comment} = $row->[1];
		$comments{$row->[0]}{table_type} = $row->[2];
		$tables_infos{$row->[0]}{owner} = $row->[7] || $row->[6];
		$tables_infos{$row->[0]}{num_rows} = $row->[3] || 0;
		$tables_infos{$row->[0]}{comment} = ''; # SQL Server doesn't have COMMENT and we don't play with "Extended Properties"
		$tables_infos{$row->[0]}{type} =  $comments{$row->[0]}{table_type} || '';
		$tables_infos{$row->[0]}{nested} = '';
		$tables_infos{$row->[0]}{size} = sprintf("%.3f", $row->[5]) || 0;
		$tables_infos{$row->[0]}{tablespace} = 0;
		$tables_infos{$row->[0]}{auto_increment} = 0;
		$tables_infos{$row->[0]}{tablespace} = $tbspname{$row->[0]} || '';

		if ($do_real_row_count)
		{
			$self->logit("DEBUG: looking for real row count for table $row->[0] (aka using count(*))...\n", 1);
			$sql = "SELECT COUNT(*) FROM `$row->[0]`";
			my $sth2 = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			$sth2->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			my $size = $sth2->fetch();
			$sth2->finish();
			$tables_infos{$row->[0]}{num_rows} = $size->[0];
		}
	}
	$sth->finish();

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

	$objtype ||= 'TABLE';

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

	my $str = qq{SELECT 
    c.name 'Column Name',
    t.Name 'Data type',
    c.max_length 'Max Length',
    c.is_nullable,
    object_definition(c.default_object_id),
    c.precision ,
    c.scale ,
    '',
    tb.name,
    s.name,
    '',
    c.column_id,
    NULL as AUTO_INCREMENT,
    NULL AS ENUM_INFO,
    object_definition(c.rule_object_id),
    t.is_user_defined
FROM sys.columns c
INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
INNER JOIN sys.tables AS tb ON tb.object_id = c.object_id
INNER JOIN sys.schemas AS s ON s.schema_id = tb.schema_id
$condition
ORDER BY c.column_id};

	my $sth = $self->{dbh}->prepare($str);
	if (!$sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	$sth->{'LongReadLen'} = 1000000;
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	# Expected columns information stored in hash 
	# COLUMN_NAME,DATA_TYPE,DATA_LENGTH,NULLABLE,DATA_DEFAULT,DATA_PRECISION,DATA_SCALE,CHAR_LENGTH,TABLE_NAME,OWNER,VIRTUAL_COLUMN,POSITION,AUTO_INCREMENT,ENUM_INFO
	my %data = ();
	my $pos = 0;
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[8] = "$row->[9].$row->[8]";
		}
		if (!$row->[15])
		{
			if ($row->[4]) {
				$row->[4] =~ s/\s*CREATE\s+DEFAULT\s+.*\s+AS\s*//is;
			}
			if ($row->[14]) {
				$row->[14] =~ s/\s*CREATE\s+RULE\s+.*\s+AS\s*//is;
				$row->[14] =~ s/\@[a-z0-1_\$\#]+/VALUE/igs;
				$row->[14] = " CHECK ($row->[14])";
				$row->[14] =~ s/[\r\n]+/ /gs;
			}
		}
		else
		{
			# For user data type the NOT NULL, DEFAULT and RULES belongs to
			# the user defined data type and are appended at type creation
			$row->[3] = 1;
			$row->[4] = '';
			$row->[14] = '';
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
	$condition .= "AND OBJECT_NAME(Id.object_id, DB_ID())='$table' " if ($table);
	if ($owner) {
		$condition .= "AND s.name='$owner' ";
	} else {
		$condition .= " AND s.name NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE|INDEX', "OBJECT_NAME(Id.object_id, DB_ID())|Id.NAME");
	} else {
		@{$self->{query_bind_params}} = ();
	}

	# When comparing number of index we need to retrieve generated index (mostly PK)
	my $generated = '';
	$generated = " AND Id.auto_created = 0" if (!$generated_indexes);

	my $t0 = Benchmark->new;
	my $sth = '';
	my $sql = qq{SELECT Id.name AS index_name, AC.name AS column_name, Id.is_unique AS UNIQUENESS, AC.column_id AS COLUMN_POSITION, Id.type AS INDEX_TYPE, 'U' AS TABLE_TYPE, Id.auto_created AS GENERATED, NULL AS JOIN_INDEX, t.name AS TABLE_NAME, s.name as TABLE_SCHEMA, Id.data_space_id AS TABLESPACE_NAME, Id.type_desc AS ITYP_NAME, Id.filter_definition AS PARAMETERS, IC.is_descending_key AS DESCEND, id.is_primary_key PRIMARY_KEY
FROM sys.tables AS T
INNER JOIN sys.indexes Id ON T.object_id = Id.object_id 
INNER JOIN sys.index_columns IC ON Id.object_id = IC.object_id
INNER JOIN sys.all_columns AC ON T.object_id = AC.object_id AND IC.column_id = AC.column_id 
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE T.is_ms_shipped = 0 $generated $condition
ORDER BY T.name, Id.index_id, IC.key_ordinal
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
		# Exclude log indexes of materialized views, there must be a better
		# way to exclude then than looking at index name, fill free to fix it.
		next if ($row->[0] =~ /^I_SNAP\$_/);

		# Handle case where indexes name include the schema at create time
		$row->[0] =~ s/^$self->{schema}\.//i if ($self->{schema});

		my $save_tb = $row->[8];
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[8] = "$row->[9].$row->[8]";
		}
		next if (!$self->is_in_struct($row->[8], $row->[1]));

		# Show a warning when an index has the same name as the table
		if ( !$self->{indexes_renaming} && !$self->{indexes_suffix} && (lc($row->[0]) eq lc($table)) ) {
			 print STDERR "WARNING: index $row->[0] has the same name as the table itself. Please rename it before export or enable INDEXES_RENAMING.\n";
		}
		$unique{$row->[8]}{$row->[0]} = $row->[2];

		# Save original column name
		my $colname = $row->[1];
		# Quote column with unsupported symbols
		$row->[1] = $self->quote_object_name($row->[1]);
		# Replace function based index type
		if ( $row->[13] )
		{
			# Append DESC sort order when not default to ASC
			if ($row->[13] eq 'DESC') {
				$row->[1] .= " DESC";
			}
		}

		$idx_type{$row->[8]}{$row->[0]}{type_name} = $row->[11];
		if (($#{$row} > 6) && ($row->[7] eq 'Y')) {
			$idx_type{$row->[8]}{$row->[0]}{type} = $row->[4] . ' JOIN';
		} else {
			$idx_type{$row->[8]}{$row->[0]}{type} = $row->[4];
		}
		my $idx_name = $row->[0];
		if (!$self->{schema} && $self->{export_schema}) {
			$idx_name = "$row->[9].$row->[0]";
		}
		if ($row->[11] =~ /SPATIAL_INDEX/) {
			$idx_type{$row->[8]}{$row->[0]}{type} = 'SPATIAL INDEX';
			if ($row->[12] =~ /layer_gtype=([^\s,]+)/i) {
				$idx_type{$row->[9]}{$row->[0]}{type_constraint} = uc($1);
			}
			if ($row->[12] =~ /sdo_indx_dims=(\d+)/i) {
				$idx_type{$row->[8]}{$row->[0]}{type_dims} = $1;
			}
		}
		if ($row->[4] eq 'BITMAP') {
			$idx_type{$row->[8]}{$row->[0]}{type} = $row->[4];
		}
		push(@{$data{$row->[8]}{$row->[0]}}, $row->[1]);
		$index_tablespace{$row->[8]}{$row->[0]} = $row->[10];
		$nidx++;
	}
	$sth->finish();
	my $t1 = Benchmark->new;
	my $td = timediff($t1, $t0);
	$self->logit("Collecting $nidx indexes in $self->{prefix}_INDEXES took: " . timestr($td) . "\n", 1);

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
		%tables_infos = Ora2Pg::MSSQL::_table_info($self);
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
        $condition .= " AND OBJECT_NAME (f.referenced_object_id) = '$table' " if ($table);
        $condition .= " AND SCHEMA_NAME(f.schema_id) = '$self->{schema}' " if ($self->{schema});
	$condition =~ s/^ AND / WHERE /;

        my $deferrable = $self->{fkey_deferrable} ? "'DEFERRABLE' AS DEFERRABLE" : "DEFERRABLE";
	my $sql = qq{SELECT f.name ConsName, SCHEMA_NAME(f.schema_id) SchemaName, COL_NAME(fc.parent_object_id,fc.parent_column_id) ColName, OBJECT_NAME(f.parent_object_id) TableName, t.name as ReferencedTableName, COL_NAME(f.referenced_object_id, key_index_id) as ReferencedColumnName,update_referential_action_desc UPDATE_RULE, delete_referential_action_desc DELETE_RULE, SCHEMA_NAME(t.schema_id)
FROM sys.foreign_keys AS f
INNER JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id
INNER JOIN sys.tables t ON t.OBJECT_ID = fc.referenced_object_id
LEFT OUTER JOIN sys.schemas s ON f.principal_id = s.schema_id
$condition};

        my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);
        my @cons_columns = ();
	my $i = 1;
        my %data = ();
        my %link = ();
        while (my $r = $sth->fetch)
	{
		my $key_name = $r->[3] . '_' . $r->[2] . '_fk' . $i;
		if ($r->[0]) {
			$key_name = uc($r->[0]);
		}
		if (!$self->{schema} && $self->{export_schema}) {
			$r->[3] = "$r->[1].$r->[3]";
			$r->[4] = "$r->[8].$r->[4]";
		}
		push(@{$link{$r->[3]}{$key_name}{local}}, $r->[2]);
		push(@{$link{$r->[3]}{$key_name}{remote}{$r->[4]}}, $r->[5]);
		# SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,$deferrable,DEFERRED,R_OWNER,TABLE_NAME,OWNER,UPDATE_RULE
		$r->[3] =~ s/_/ /;
		$r->[7] =~ s/_/ /;
                push(@{$data{$r->[3]}}, [ ($key_name, $key_name, '', $r->[7], 'DEFERRABLE', 'Y', '', $r->[3], '', $r->[6]) ]);
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

        my $condition = '';
        $condition .= "AND TABLE_SCHEMA='$self->{schema}' " if ($self->{schema});

	# Retrieve comment of each columns
	# TABLE_CATALOG        | varchar(512) | NO   |     |         |       |
	# TABLE_SCHEMA         | varchar(64)  | NO   |     |         |       |
	# TABLE_NAME           | varchar(64)  | NO   |     |         |       |
	# VIEW_DEFINITION      | longtext     | NO   |     | NULL    |       |
	# CHECK_OPTION         | varchar(8)   | NO   |     |         |       |
	# IS_UPDATABLE         | varchar(3)   | NO   |     |         |       |
	# DEFINER              | varchar(77)  | NO   |     |         |       |
	# SECURITY_TYPE        | varchar(7)   | NO   |     |         |       |
	# CHARACTER_SET_CLIENT | varchar(32)  | NO   |     |         |       |
	# COLLATION_CONNECTION | varchar(32)  | NO   |     |         |       |
	my %comments = ();
	# Retrieve all views
#	my $str = "SELECT TABLE_NAME,TABLE_SCHEMA,VIEW_DEFINITION,CHECK_OPTION,IS_UPDATABLE FROM INFORMATION_SCHEMA.VIEWS $condition";
#	$str .= $self->limit_to_objects('VIEW', 'TABLE_NAME');
#	$str .= " ORDER BY TABLE_NAME";
#	$str =~ s/ AND / WHERE /;

	my $str = qq{select
       v.name as view_name,
       schema_name(v.schema_id) as schema_name,
       m.definition,
       v.with_check_option
from sys.views v
join sys.sql_modules m on m.object_id = v.object_id
WHERE NOT EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = v.object_id and i.index_id = 1 and i.ignore_dup_key = 0) AND is_date_correlation_view=0};

	if (!$self->{schema}) {
		$str .= " AND schema_name(v.schema_id) NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND schema_name(v.schema_id) = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('VIEW', 'v.name');
	$str .= " ORDER BY schema_name, view_name";


	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->{'LongReadLen'} = 1000000;
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %ordered_view = ();
	my %data = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
		$row->[2] =~ s/
		$row->[2] =~ s/[\[\]]//g;
		$row->[2] =~ s/^CREATE VIEW [^\s]+//;
		$data{$row->[0]}{text} = $row->[2];
		$data{$row->[0]}{owner} = '';
		$data{$row->[0]}{comment} = '';
		$data{$row->[0]}{check_option} = $row->[3];
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
		my $act = join(', ', @actions);
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[2] = "$row->[3].$row->[2]";
		}
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
        $condition .= " AND t.name = '$table' " if ($table);
        $condition .= " AND sh.name = '$self->{schema}' " if ($self->{schema});
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE|INDEX', "t.name|i.name");
	} else {
		@{$self->{query_bind_params}} = ();
	}


	my $sql = qq{SELECT sh.name AS schema_name,
   i.name AS constraint_name,
   t.name AS table_name,
   c.name AS column_name,
   ic.key_ordinal AS column_position,
   ic.is_descending_key AS is_desc,
   i.is_unique_constraint AS unique_key,
   i.is_primary_key AS primary_key
FROM sys.indexes i
   INNER JOIN sys.index_columns ic ON i.index_id = ic.index_id AND i.object_id = ic.object_id
   INNER JOIN sys.tables AS t ON t.object_id = i.object_id
   INNER JOIN sys.columns c ON t.object_id = c.object_id AND ic.column_id = c.column_id
   INNER JOIN sys.objects AS syso ON syso.object_id = t.object_id AND syso.is_ms_shipped = 0 
   INNER JOIN sys.schemas AS sh ON sh.schema_id = t.schema_id 
WHERE (i.is_unique_constraint = 1 OR i.is_primary_key = 1) $condition
ORDER BY sh.name, i.name, ic.key_ordinal;
};

	my %tables_infos = ();
	if ($table) {
		$tables_infos{$table} = 1;
	} else {
		%tables_infos = Ora2Pg::MSSQL::_table_info($self);
	}

	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my $i = 1;
	while (my $row = $sth->fetch)
	{
		my $name = $row->[2];
		if (!$self->{schema} && $self->{export_schema}) {
			$name = "$row->[0].$row->[2]";
		}

		my $idxname = $row->[3] . '_idx' . $i;
		$idxname = $row->[2] if ($row->[2]);
		my $key_type = 'U';
		$key_type = 'P' if ($row->[7]);

		next if (!grep(/$key_type/, @accepted_constraint_types));

		if (!exists $result{$name}{$idxname})
		{
			my %constraint = (type => $key_type, 'generated' => 'N', 'index_name' => $idxname, columns => [ ($row->[3]) ] );
			$result{$name}{$idxname} = \%constraint if ($row->[3]);
			$i++;
		} else {
			push(@{$result{$name}{$idxname}->{columns}}, $row->[3]);
		}
	}

	return %result;
}

sub _check_constraint
{
	my ($self, $table, $owner) = @_;

	my $condition = '';
	$condition .= " AND t.name = '$table' " if ($table);
	$condition .= " AND s.name = '$self->{schema}' " if ($self->{schema});
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE|INDEX', "t.name|i.name");
	} else {
		@{$self->{query_bind_params}} = ();
	}

	my $sql = qq{SELECT
    schema_name(t.schema_id) SchemaName,
    t.name as TableName,
    col.name as column_name,
    con.name as constraint_name,
    con.definition,
    con.is_disabled 
FROM sys.check_constraints con
LEFT OUTER JOIN sys.objects t ON con.parent_object_id = t.object_id
LEFT OUTER JOIN sys.all_columns col ON con.parent_column_id = col.column_id AND con.parent_object_id = col.object_id
$condition
ORDER BY SchemaName, t.Name, col.name
};

        my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

        my %data = ();
        while (my $row = $sth->fetch)
	{
                if ($self->{export_schema} && !$self->{schema}) {
                        $row->[1] = "$row->[0].$row->[1]";
                }
		$row->[4] =~ s/[\[\]]//gs;
		$row->[4] =~ s/^\(//s;
		$row->[4] =~ s/\)$//s;
                $data{$row->[1]}{constraint}{$row->[3]}{condition} = $row->[4];
		if ($row->[5]) {
			$data{$row->[1]}{constraint}{$row->[3]}{validate}  = 'NOT VALIDATED';
		} else {
			$data{$row->[1]}{constraint}{$row->[3]}{validate}  = 'VALIDATED';
		}
        }

	return %data;
}

sub _get_external_tables
{
	my ($self) = @_;

	# There is no external table in MSSQL
	return;
}

sub _get_directory
{
	my ($self) = @_;

	# There is no external table in MSSQL
	return;
}

sub _get_functions
{
	my $self = shift;

	# Retrieve all functions 
	# SPECIFIC_NAME            | varchar(64)   | NO   |     |                     |       |
	# ROUTINE_CATALOG          | varchar(512)  | NO   |     |                     |       |
	# ROUTINE_SCHEMA           | varchar(64)   | NO   |     |                     |       |
	# ROUTINE_NAME             | varchar(64)   | NO   |     |                     |       |
	# ROUTINE_TYPE             | varchar(9)    | NO   |     |                     |       |
	# DATA_TYPE                | varchar(64)   | NO   |     |                     |       |
	#  or DTD_IDENTIFIER < 5.5 | varchar(64)   | NO   |     |                     |       |
	# CHARACTER_MAXIMUM_LENGTH | int(21)       | YES  |     | NULL                |       |
	# CHARACTER_OCTET_LENGTH   | int(21)       | YES  |     | NULL                |       |
	# NUMERIC_PRECISION        | int(21)       | YES  |     | NULL                |       |
	# NUMERIC_SCALE            | int(21)       | YES  |     | NULL                |       |
	# CHARACTER_SET_NAME       | varchar(64)   | YES  |     | NULL                |       |
	# COLLATION_NAME           | varchar(64)   | YES  |     | NULL                |       |
	# DTD_IDENTIFIER           | longtext      | YES  |     | NULL                |       |
	# ROUTINE_BODY             | varchar(8)    | NO   |     |                     |       |
	# ROUTINE_DEFINITION       | longtext      | YES  |     | NULL                |       |
	# EXTERNAL_NAME            | varchar(64)   | YES  |     | NULL                |       |
	# EXTERNAL_LANGUAGE        | varchar(64)   | YES  |     | NULL                |       |
	# PARAMETER_STYLE          | varchar(8)    | NO   |     |                     |       |
	# IS_DETERMINISTIC         | varchar(3)    | NO   |     |                     |       |
	# SQL_DATA_ACCESS          | varchar(64)   | NO   |     |                     |       |
	# SQL_PATH                 | varchar(64)   | YES  |     | NULL                |       |
	# SECURITY_TYPE            | varchar(7)    | NO   |     |                     |       |
	# CREATED                  | datetime      | NO   |     | 0000-00-00 00:00:00 |       |
	# LAST_ALTERED             | datetime      | NO   |     | 0000-00-00 00:00:00 |       |
	# SQL_MODE                 | varchar(8192) | NO   |     |                     |       |
	# ROUTINE_COMMENT          | longtext      | NO   |     | NULL                |       |
	# DEFINER                  | varchar(77)   | NO   |     |                     |       |
	# CHARACTER_SET_CLIENT     | varchar(32)   | NO   |     |                     |       |
	# COLLATION_CONNECTION     | varchar(32)   | NO   |     |                     |       |
	# DATABASE_COLLATION       | varchar(32)   | NO   |     |                     |       |

	my $str = "SELECT ROUTINE_NAME,ROUTINE_DEFINITION,DATA_TYPE,ROUTINE_BODY,EXTERNAL_LANGUAGE,SECURITY_TYPE,IS_DETERMINISTIC,ROUTINE_TYPE FROM INFORMATION_SCHEMA.ROUTINES";
	if ($self->{schema}) {
		$str .= " AND ROUTINE_SCHEMA = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('FUNCTION','ROUTINE_NAME');
	$str =~ s/ AND / WHERE /;
	$str .= " ORDER BY ROUTINE_NAME";
	# Version below 5.5 do not have DATA_TYPE column it is named DTD_IDENTIFIER
	if ($self->{db_version} < '5.5.0') {
		$str =~ s/\bDATA_TYPE\b/DTD_IDENTIFIER/;
	}
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %functions = ();
	while (my $row = $sth->fetch)
	{
		my $kind = $row->[7]; # FUNCTION or PROCEDURE
		next if ( ($kind ne $self->{type}) && ($self->{type} ne 'SHOW_REPORT') );
		my $sth2 = $self->{dbh}->prepare("SHOW CREATE $kind $row->[0]") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch)
		{
			$functions{"$row->[0]"}{text} = $r->[2];
			last;
		}
		$sth2->finish();
		if ($self->{plsql_pgsql} || ($self->{type} eq 'SHOW_REPORT'))
		{
			$functions{"$row->[0]"}{name} = $row->[0];
			$functions{"$row->[0]"}{return} = $row->[2];
			$functions{"$row->[0]"}{definition} = $row->[1];
			$functions{"$row->[0]"}{language} = $row->[3];
			$functions{"$row->[0]"}{security} = $row->[5];
			$functions{"$row->[0]"}{immutable} = $row->[6];
		}
	}

	return \%functions;
}

sub _lookup_function
{
	my ($self, $code, $fctname) = @_;

	my $type = lc($self->{type}) . 's';

	# Replace all double quote with single quote
	$code =~ s/"/'/g;
	# replace backquote with double quote
	$code =~ s/`/"/g;
	# Remove some unused code
	$code =~ s/\s+READS SQL DATA//igs;
	$code =~ s/\s+UNSIGNED\b((?:.*?)\bFUNCTION\b)/$1/igs;

        my %fct_detail = ();
        $fct_detail{func_ret_type} = 'OPAQUE';

        # Split data into declarative and code part
        ($fct_detail{declare}, $fct_detail{code}) = split(/\bBEGIN\b/i, $code, 2);
	return if (!$fct_detail{code});

	# Remove any label that was before the main BEGIN block
	$fct_detail{declare} =~ s/\s+[^\s\:]+:\s*$//gs;

        @{$fct_detail{param_types}} = ();

        if ( ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE)\s+([^\s\(]+)\s*(\(.*\))\s+RETURNS\s+(.*)//is) ||
        ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE)\s+([^\s\(]+)\s*(\(.*\))//is) ) {
                $fct_detail{before} = $1;
                $fct_detail{type} = uc($2);
                $fct_detail{name} = $3;
                $fct_detail{args} = $4;
		my $tmp_returned = $5;
		chomp($tmp_returned);
		if ($tmp_returned =~ s/\b(DECLARE\b.*)//is) {
			$fct_detail{code} = $1 . $fct_detail{code};
		}
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

		$fctname = $fct_detail{name} || $fctname;
		if ($type eq 'functions' && exists $self->{$type}{$fctname}{return} && $self->{$type}{$fctname}{return}) {
			$fct_detail{hasreturn} = 1;
			$fct_detail{func_ret_type} = $self->_sql_type($self->{$type}{$fctname}{return});
		} elsif ($type eq 'functions' && !exists $self->{$type}{$fctname}{return} && $tmp_returned) {
			$tmp_returned =~ s/\s+CHARSET.*//is;
			$fct_detail{func_ret_type} = $self->_sql_type($tmp_returned);
			$fct_detail{hasreturn} = 1;
		}
		$fct_detail{language} = $self->{$type}{$fctname}{language};
		$fct_detail{immutable} = 1 if ($self->{$type}{$fctname}{immutable} eq 'YES');
		$fct_detail{security} = $self->{$type}{$fctname}{security};

		# Procedure that have out parameters are functions with PG
		if ($type eq 'procedures' && $fct_detail{args} =~ /\b(OUT|INOUT)\b/) {
			# set return type to empty to avoid returning void later
			$fct_detail{func_ret_type} = ' ';
		}
		# IN OUT should be INOUT
		$fct_detail{args} =~ s/\bIN\s+OUT/INOUT/igs;

		# Move the DECLARE statement from code to the declare section.
		$fct_detail{declare} = '';
		while ($fct_detail{code} =~ s/DECLARE\s+([^;]+;)//is) {
				$fct_detail{declare} .= "\n$1";
		}
		# Now convert types
		if ($fct_detail{args}) {
			$fct_detail{args} = replace_sql_type($fct_detail{args}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type}, %{ $self->{data_type} });
		}
		if ($fct_detail{declare}) {
			$fct_detail{declare} = replace_sql_type($fct_detail{declare}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type}, %{ $self->{data_type} });
		}

		$fct_detail{args} =~ s/\s+/ /gs;
		push(@{$fct_detail{param_types}}, split(/\s*,\s*/, $fct_detail{args}));
		# Store type used in parameter list to lookup later for custom types
		map { s/^\(//; } @{$fct_detail{param_types}};
		map { s/\)$//; } @{$fct_detail{param_types}};
		map { s/\%ORA2PG_COMMENT\d+\%//gs; }  @{$fct_detail{param_types}};
		map { s/^\s*[^\s]+\s+(IN|OUT|INOUT)/$1/i; s/^((?:IN|OUT|INOUT)\s+[^\s]+)\s+[^\s]*$/$1/i; s/\(.*//; s/\s*\)\s*$//; s/\s+$//; } @{$fct_detail{param_types}};

	} else {
                delete $fct_detail{func_ret_type};
                delete $fct_detail{declare};
                $fct_detail{code} = $code;
	}

	# Mark the function as having out parameters if any
	my @nout = $fct_detail{args} =~ /\bOUT\s+([^,\)]+)/igs;
	my @ninout = $fct_detail{args} =~ /\bINOUT\s+([^,\)]+)/igs;
	my $nbout = $#nout+1 + $#ninout+1;
	$fct_detail{inout} = 1 if ($nbout > 0);

	($fct_detail{code}, $fct_detail{declare}) = replace_mysql_variables($self, $fct_detail{code}, $fct_detail{declare});

	# Remove %ROWTYPE from return type
	$fct_detail{func_ret_type} =~ s/\%ROWTYPE//igs;

	return %fct_detail;
}

sub replace_mysql_variables
{
	my ($self, $code, $declare) = @_;

	# Look for mysql global variables and add them to the custom variable list
	while ($code =~ s/\b(?:SET\s+)?\@\@(?:SESSION\.)?([^\s:=]+)\s*:=\s*([^;]+);/PERFORM set_config('$1', $2, false);/is) {
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
	while ($code =~ s/SET\s+\@([^\s:]+)\s*:=\s*([^;]+);/SET $1 = $2;/is) {
		my $n = $1;
		my $v = $2;
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
		$declare .= "$n $type;\n" if ($declare !~ /\b$n $type;/s);
		push(@to_be_replaced, $n);
	}

	# Look for local variable definition and append them to the declare section
	while ($code =~ s/(\s+)\@([^\s:=]+)\s*:=\s*([^;]+);/$1$2 := $3;/is) {
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
		$declare .= "$n $type;\n" if ($declare !~ /\b$n $type;/s);
		push(@to_be_replaced, $n);
	}

	# Fix other call to the same variable in the code
	foreach my $n (@to_be_replaced) {
		$code =~ s/\@$n\b(\s*[^:])/$n$1/gs;
	}

	# Look for local variable definition and append them to the declare section
	while ($code =~ s/\@([a-z0-9_]+)/$1/is) {
		my $n = $1;
		# Try to set a default type for the variable
		my $type = 'varchar';
		if ($n =~ /datetime/i) {
			$type = 'timestamp';
		} elsif ($n =~ /time/i) {
			$type = 'time';
		} elsif ($n =~ /date/i) {
			$type = 'date';
		} 
		$declare .= "$n $type;\n" if ($declare !~ /\b$n $type;/s);
		# Fix other call to the same variable in the code
		$code =~ s/\@$n\b/$n/gs;
	}

	# Look for variable definition with SELECT statement
	$code =~ s/\bSET\s+([^\s=]+)\s*=\s*([^;]+\bSELECT\b[^;]+);/$1 = $2;/igs;

	return ($code, $declare);
}

sub _list_all_funtions
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

	# Simplify timestamp type
	$type =~ s/TIMESTAMP\s*\(\s*\d+\s*\)/TIMESTAMP/i;
	$type =~ s/TIME\s*\(\s*\d+\s*\)/TIME/i;
	$type =~ s/DATE\s*\(\s*\d+\s*\)/DATE/i;
	# Remove BINARY from CHAR(n) BINARY, TEXT(n) BINARY, VARCHAR(n) BINARY ...
	$type =~ s/(CHAR|TEXT)\s*(\(\s*\d+\s*\)) BINARY/$1$2/i;
	$type =~ s/(CHAR|TEXT)\s+BINARY/$1/i;

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
        $len = $precision if ( ((uc($type) eq 'NUMBER') || (uc($type) eq 'BIT')) && $precision );
        if (exists $self->{data_type}{uc($type)}) {
		$type = uc($type); # Force uppercase
		if ($len) {
			if ( ($type eq "CHAR") || ($type =~ /VARCHAR/) ) {
				# Type CHAR have default length set to 1
				# Type VARCHAR(2) must have a specified length
				$len = 1 if (!$len && ($type eq "CHAR"));
                		return "$self->{data_type}{$type}($len)";
			} elsif ($type eq 'BIT') {
				if ($precision) {
					return "$self->{data_type}{$type}($precision)";
				} else {
					return $self->{data_type}{$type};
				}
			} elsif ($type =~ /(TINYINT|SMALLINT|MEDIUMINT|INTEGER|BIGINT|INT|REAL|DOUBLE|FLOAT|DECIMAL|NUMERIC)/i) {
				# This is an integer
				if (!$scale) {
					if ($precision) {
						if ($self->{pg_integer_type}) {
							if ($precision < 5) {
								return 'smallint';
							} elsif ($precision <= 9) {
								return 'integer'; # The speediest in PG
							} else {
								return 'bigint';
							}
						}
						return "numeric($precision)";
					} else {
						# Most of the time interger should be enought?
						return $self->{data_type}{$type};
					}
				} else {
					if ($precision) {
						if ($type !~ /DOUBLE/ && $self->{pg_numeric_type}) {
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
		} else {
			return $self->{data_type}{$type};
		}
        }

        return $type;
}

sub replace_sql_type
{
        my ($str, $pg_numeric_type, $default_numeric, $pg_integer_type, %data_type) = @_;

	$str =~ s/with local time zone/with time zone/igs;
	$str =~ s/([A-Z])ORA2PG_COMMENT/$1 ORA2PG_COMMENT/igs;

	# Remove any reference to UNSIGNED AND ZEROFILL
	# but translate CAST( ... AS unsigned) before.
	$str =~ s/(\s+AS\s+)UNSIGNED/$1$data_type{'UNSIGNED'}/gis;
	$str =~ s/\b(UNSIGNED|ZEROFILL)\b//gis;

	# Remove BINARY from CHAR(n) BINARY and VARCHAR(n) BINARY
	$str =~ s/(CHAR|TEXT)\s*(\(\s*\d+\s*\))\s+BINARY/$1$2/gis;
	$str =~ s/(CHAR|TEXT)\s+BINARY/$1/gis;

	# Replace type with precision
	my $mysqltype_regex = '';
	foreach (keys %data_type) {
		$mysqltype_regex .= quotemeta($_) . '|';
	}
	$mysqltype_regex =~ s/\|$//;
	while ($str =~ /(.*)\b($mysqltype_regex)\s*\(([^\)]+)\)/i) {
		my $backstr = $1;
		my $type = uc($2);
		my $args = $3;
		if (uc($type) eq 'ENUM') {
			# Prevent from infinit loop
			$str =~ s/\(/\%\|/s;
			$str =~ s/\)/\%\|\%/s;
			next;
		}
		if (exists $data_type{"$type($args)"}) {
			$str =~ s/\b$type\($args\)/$data_type{"$type($args)"}/igs;
			next;
		}
		if ($backstr =~ /_$/) {
		    $str =~ s/\b($mysqltype_regex)\s*\(([^\)]+)\)/$1\%\|$2\%\|\%/is;
		    next;
		}

		my ($precision, $scale) = split(/,/, $args);
		$scale ||= 0;
		my $len = $precision || 0;
		$len =~ s/\D//;
		if ( $type =~ /CHAR/i ) {
			# Type CHAR have default length set to 1
			# Type VARCHAR must have a specified length
			$len = 1 if (!$len && ($type eq "CHAR"));
			$str =~ s/\b$type\b\s*\([^\)]+\)/$data_type{$type}\%\|$len\%\|\%/is;
		} elsif ($precision && ($type =~ /(BIT|TINYINT|SMALLINT|MEDIUMINT|INTEGER|BIGINT|INT|REAL|DOUBLE|FLOAT|DECIMAL|NUMERIC)/)) {
			if (!$scale) {
				if ($type =~ /(BIT|TINYINT|SMALLINT|MEDIUMINT|INTEGER|BIGINT|INT)/) {
					if ($pg_integer_type) {
						if ($precision < 5) {
							$str =~ s/\b$type\b\s*\([^\)]+\)/smallint/is;
						} elsif ($precision <= 9) {
							$str =~ s/\b$type\b\s*\([^\)]+\)/integer/is;
						} else {
							$str =~ s/\b$type\b\s*\([^\)]+\)/bigint/is;
						}
					} else {
						$str =~ s/\b$type\b\s*\([^\)]+\)/numeric\%\|$precision\%\|\%/i;
					}
				} else {
					$str =~ s/\b$type\b\s*\([^\)]+\)/$data_type{$type}\%\|$precision\%\|\%/is;
				}
			} else {
				if ($type =~ /DOUBLE/) {
					$str =~ s/\b$type\b\s*\([^\)]+\)/decimal\%\|$args\%\|\%/is;
				} else {
					$str =~ s/\b$type\b\s*\([^\)]+\)/$data_type{$type}\%\|$args\%\|\%/is;
				}
			}
		} else {
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
	foreach my $type (sort { length($b) <=> length($a) } keys %data_type) {
		# Keep enum as declared, we are not in table definition
		next if (uc($type) eq 'ENUM');
		while ($str =~ s/\b$type\b/%%RECOVER_TYPE$i%%/is) {
			$recover_type{$i} = $data_type{$type};
			$i++;
		}
	}

	foreach $i (keys %recover_type) {
		$str =~ s/\%\%RECOVER_TYPE$i\%\%/$recover_type{$i}/;
	}

	# Set varchar without length to text
	$str =~ s/\bVARCHAR(\s*(?!\())/text$1/igs;

        return $str;
}

sub _get_job
{
	my($self) = @_;

	# Retrieve all database job from user_jobs table
	my $str = "SELECT EVENT_NAME,EVENT_DEFINITION,EXECUTE_AT FROM INFORMATION_SCHEMA.EVENTS WHERE STATUS = 'ENABLED'";
	if ($self->{schema}) {
		$str .= " AND EVENT_SCHEMA = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('JOB', 'EVENT_NAME');
	$str .= " ORDER BY EVENT_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		$data{$row->[0]}{what} = $row->[1];
		$data{$row->[0]}{interval} = $row->[2];
	}

	return %data;
}

sub _get_dblink
{
	my($self) = @_;

	# Must be able to read mysql.servers table
	return if ($self->{user_grants});

	# Retrieve all database link from dba_db_links table
	my $str = "SELECT OWNER,SERVER_NAME,USERNAME,HOST,DB,PORT,PASSWORD FROM mysql.servers";
	$str .= $self->limit_to_objects('DBLINK', 'SERVER_NAME');
	$str .= " ORDER BY SERVER_NAME";
	$str =~ s/mysql.servers AND /mysql.servers WHERE /;

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		$data{$row->[1]}{owner} = $row->[0];
		$data{$row->[1]}{username} = $row->[2];
		$data{$row->[1]}{host} = $row->[3];
		$data{$row->[1]}{db} = $row->[4];
		$data{$row->[1]}{port} = $row->[5];
		$data{$row->[1]}{password} = $row->[6];
	}

	return %data;
}

=head2 _get_partitions

This function implements an MSSQL-native partitions information.
Return two hash ref with partition details and partition default.
=cut

sub _get_partitions
{
	my($self) = @_;

	# Retrieve all partitions.
	my $str = qq{
SELECT TABLE_NAME, PARTITION_ORDINAL_POSITION, PARTITION_NAME, PARTITION_DESCRIPTION, TABLESPACE_NAME, PARTITION_METHOD, PARTITION_EXPRESSION
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE PARTITION_NAME IS NOT NULL AND SUBPARTITION_NAME IS NULL AND (PARTITION_METHOD LIKE 'RANGE%' OR PARTITION_METHOD LIKE 'LIST%')
};
	$str .= $self->limit_to_objects('TABLE|PARTITION', 'TABLE_NAME|PARTITION_NAME');
	if ($self->{schema}) {
		$str .= "\tAND TABLE_SCHEMA ='$self->{schema}'\n";
	}
	$str .= "ORDER BY TABLE_NAME,PARTITION_ORDINAL_POSITION\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %parts = ();
	my %default = ();
	while (my $row = $sth->fetch)
	{
		if ($row->[3] =~ /^MAXVALUE(?:,MAXVALUE)*$/ || $row->[3] eq 'DEFAULT')
		{
			$default{$row->[0]} = $row->[2];
			next;
		}
		$parts{$row->[0]}{$row->[1]}{name} = $row->[2];
		$row->[6] =~ s/\`//g;
		$row->[3] =~ s/\`//g;
		$row->[5] =~ s/ COLUMNS//;
		my $i = 0;
		foreach my $c (split(',', $row->[6]))
		{
			push(@{$parts{$row->[0]}{$row->[1]}{info}}, { 'type' => $row->[5], 'value' => $row->[3], 'column' => $c, 'colpos' => $i, 'tablespace' => $row->[4], 'owner' => ''});
			$i++;
		}
	}
	$sth->finish;

	return \%parts, \%default;
}

=head2 _get_subpartitions

This function implements a MSSQL subpartitions information.
Return two hash ref with partition details and partition default.
=cut

sub _get_subpartitions
{
	my($self) = @_;

	# Retrieve all partitions.
	my $str = qq{
SELECT TABLE_NAME, SUBPARTITION_ORDINAL_POSITION, SUBPARTITION_NAME, PARTITION_DESCRIPTION, TABLESPACE_NAME, SUBPARTITION_METHOD, SUBPARTITION_EXPRESSION,PARTITION_NAME
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE SUBPARTITION_NAME IS NOT NULL AND SUBPARTITION_EXPRESSION IS NOT NULL AND (SUBPARTITION_METHOD = 'RANGE' OR SUBPARTITION_METHOD = 'LIST')
};
	$str .= $self->limit_to_objects('TABLE|PARTITION', 'TABLE_NAME|PARTITION_NAME');
	if ($self->{schema}) {
		$str .= " AND TABLE_SCHEMA ='$self->{schema}'\n";
	}
	$str .= " ORDER BY TABLE_NAME,PARTITION_ORDINAL_POSITION,SUBPARTITION_ORDINAL_POSITION\n";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %subparts = ();
	my %default = ();
	while (my $row = $sth->fetch)
	{
		if ($row->[3] =~ /^MAXVALUE(?:,MAXVALUE)*$/ || $row->[3] eq 'DEFAULT')
		{
			$default{$row->[0]} = $row->[2];
			next;
		}
		$subparts{$row->[0]}{$row->[7]}{$row->[1]}{name} = $row->[2];
		my $i = 0;
		$row->[6] =~ s/\`//g;
		$row->[3] =~ s/\`//g;
		$row->[5] =~ s/ COLUMNS//;
		foreach my $c (split(',', $row->[6]))
		{
			push(@{$subparts{$row->[0]}{$row->[7]}{$row->[1]}{info}}, { 'type' => $row->[5], 'value' => $row->[3], 'column' => $c, 'colpos' => $i, 'tablespace' => $row->[4], 'owner' => ''});
			$i++;
		}
	}
	$sth->finish;

	return \%subparts, \%default;
}

=head2 _get_partitions_list

This function implements a MSSQL-native partitions information.
Return a hash of the partition table_name => type

=cut

sub _get_partitions_list
{
	my($self) = @_;

	# Retrieve all partitions.
	my $str = qq{
SELECT
    t.name AS [Table],
    i.name AS [Index],
    s.name,
    i.type_desc,
    i.is_primary_key,
    ps.name AS [Partition Scheme]
FROM sys.tables t
INNER JOIN sys.indexes i
    ON t.object_id = i.object_id
    AND i.type IN (0,1)
INNER JOIN sys.partition_schemes ps   
    ON i.data_space_id = ps.data_space_id
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
};

	$str .= $self->limit_to_objects('TABLE|PARTITION','t.name|t.name');
	if ($self->{schema}) {
		$str .= " WHERE s.name ='$self->{schema}'";
	}
	$str .= " ORDER BY t.name\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch) {
		$parts{$row->[0]}++;
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

	# Retrieve all partitions.
	my $str = qq{
    SELECT p.partition_id, t.name, s.name, p.partition_number, p.rows, p.data_compression_desc
FROM sys.partitions p
INNER JOIN sys.tables t ON t.object_id = p.object_id
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE p.index_id = 0
};
	$str .= $self->limit_to_objects('TABLE|PARTITION','t.name|t.name');
	if ($self->{schema}) {
		$str .= " AND s.name = '$self->{schema}'";
	}
	$str .= " ORDER BY t.name\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch) {
		$parts{$row->[0]}++;
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
	my $sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = '$self->{schema}'";
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{TABLE}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	# VIEW
	$sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '$self->{schema}'";
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{VIEW}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	# TRIGGER
	$sql = "SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA = '$self->{schema}'";
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{TRIGGER}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	# INDEX
	foreach my $t (@{$infos{TABLE}})
	{
		my $sql = "SHOW INDEX FROM `$t->{name}` FROM $self->{schema}";
		$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my @row = $sth->fetchrow()) {
			next if ($row[2] eq 'PRIMARY');
			push(@{$infos{INDEX}}, { ( name => $row[2], invalid => 0) });
		}
	}
	# FUNCTION
	$sql = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_SCHEMA = '$self->{schema}'";
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{FUNCTION}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	# PROCEDURE
	$sql = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = '$self->{schema}'";
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{PROCEDURE}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();

	# PARTITION.
	my $str = qq{
SELECT TABLE_NAME||'_'||PARTITION_NAME
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE SUBPARTITION_NAME IS NULL AND (PARTITION_METHOD = 'RANGE' OR PARTITION_METHOD = 'LIST')
};
	$sql .= $self->limit_to_objects('TABLE|PARTITION', 'TABLE_NAME|PARTITION_NAME');
	if ($self->{schema}) {
		$sql .= "\tAND TABLE_SCHEMA ='$self->{schema}'\n";
	}
	$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{'TABLE PARTITION'}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish;

	# SUBPARTITION.
	$str = qq{
SELECT TABLE_NAME||'_'||SUBPARTITION_NAME
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE SUBPARTITION_NAME IS NOT NULL
};
	$sql .= $self->limit_to_objects('TABLE|PARTITION', 'TABLE_NAME|SUBPARTITION_NAME');
	if ($self->{schema}) {
		$sql .= "\tAND TABLE_SCHEMA ='$self->{schema}'\n";
	}
	$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{'TABLE PARTITION'}}, { ( name => $row[0], invalid => 0) });
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

This function retrieves the size of the MSSQL database in MB

=cut

sub _get_database_size
{
	my $self = shift;

	my $mb_size = '';
	my $condition = '';

	my $sql = qq{
SELECT TABLE_SCHEMA "DB Name",
   sum(DATA_LENGTH + INDEX_LENGTH)/1024/1024 "DB Size in MB"
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='$self->{schema}'
GROUP BY TABLE_SCHEMA
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

        my $schema_clause = '';
        $schema_clause = " AND s.name='$self->{schema}'" if ($self->{schema});
	my $sql = qq{SELECT t.NAME AS TABLE_NAME, p.rows AS RowCounts, SUM(a.used_pages)  * 8 / 1024 AS UsedSpaceMB, CONVERT(DECIMAL,SUM(a.total_pages)) * 8 / 1024 AS TotalSpaceMB, s.Name AS TABLE_SCHEMA
FROM sys.tables t
INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0 AND i.OBJECT_ID > 255 AND t.type='U' $schema_clause
};

	$sql .= $self->limit_to_objects('TABLE', 't.Name');
	$sql .= " GROUP BY t.NAME ORDER BY TotalSpaceMB";
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

	return;
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
  s.name,
  s.minimum_value AS minimum_value,
  s.maximum_value AS maximum_value,
  s.increment AS increment,
  s.current_value AS current_value,
  s.cache_size AS cache_size,
  s.is_cycling AS cycling,
  n.name,
  s.is_cached AS cached
FROM sys.sequences s
LEFT OUTER JOIN sys.schemas n ON s.schema_id = n.schema_id
};
	
        if (!$self->{schema}) {
                $str .= " WHERE n.name NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
        } else {
                $str .= " WHERE n.name = '$self->{schema}'";
        }
        $str .= $self->limit_to_objects('SEQUENCE', 's.name');
        #$str .= " ORDER BY SEQUENCE_NAME";


        my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

        my %seqs = ();
        while (my $row = $sth->fetch)
        {
		$row->[5] = '' if ($row->[8]);
                if (!$self->{schema} && $self->{export_schema}) {
                        $row->[0] = $row->[7] . '.' . $row->[0];
                }
                push(@{$seqs{$row->[0]}}, @$row);
        }

        return \%seqs;
}

sub _extract_sequence_info
{
	my ($self) = shift;

	return;
}

# MSSQL does not have sequences but we count auto_increment as sequences
sub _count_sequences
{
	my $self = shift;

	# Table: information_schema.tables
	# TABLE_CATALOG   | varchar(512)        | NO   |     |         |       |
	# TABLE_SCHEMA    | varchar(64)         | NO   |     |         |       |
	# TABLE_NAME      | varchar(64)         | NO   |     |         |       |
	# TABLE_TYPE      | varchar(64)         | NO   |     |         |       |
	# ENGINE          | varchar(64)         | YES  |     | NULL    |       |
	# VERSION         | bigint(21) unsigned | YES  |     | NULL    |       |
	# ROW_FORMAT      | varchar(10)         | YES  |     | NULL    |       |
	# TABLE_ROWS      | bigint(21) unsigned | YES  |     | NULL    |       |
	# AVG_ROW_LENGTH  | bigint(21) unsigned | YES  |     | NULL    |       |
	# DATA_LENGTH     | bigint(21) unsigned | YES  |     | NULL    |       |
	# MAX_DATA_LENGTH | bigint(21) unsigned | YES  |     | NULL    |       |
	# INDEX_LENGTH    | bigint(21) unsigned | YES  |     | NULL    |       |
	# DATA_FREE       | bigint(21) unsigned | YES  |     | NULL    |       |
	# AUTO_INCREMENT  | bigint(21) unsigned | YES  |     | NULL    |       |
	# CREATE_TIME     | datetime            | YES  |     | NULL    |       |
	# UPDATE_TIME     | datetime            | YES  |     | NULL    |       |
	# CHECK_TIME      | datetime            | YES  |     | NULL    |       |
	# TABLE_COLLATION | varchar(32)         | YES  |     | NULL    |       |
	# CHECKSUM        | bigint(21) unsigned | YES  |     | NULL    |       |
	# CREATE_OPTIONS  | varchar(255)        | YES  |     | NULL    |       |
	# TABLE_COMMENT   | varchar(2048)       | NO   |     |         |       |

	my %seqs = ();
	my $sql = "SELECT TABLE_NAME, AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = '$self->{schema}' AND AUTO_INCREMENT IS NOT NULL";
	$sql .= $self->limit_to_objects('TABLE', 'TABLE_NAME');
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

	$objtype ||= 'TABLE';

	my $condition = '';
	if ($self->{schema}) {
		$condition .= "AND TABLE_SCHEMA='$self->{schema}' ";
	}
	$condition .= "AND TABLE_NAME='$table' " if ($table);
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE', 'TABLE_NAME');
	} else {
		@{$self->{query_bind_params}} = ();
	}
	$condition =~ s/^AND/WHERE/;

	# TABLE_CATALOG            | varchar(512)        | NO   |     |         |       |
	# TABLE_SCHEMA             | varchar(64)         | NO   |     |         |       |
	# TABLE_NAME               | varchar(64)         | NO   |     |         |       |
	# COLUMN_NAME              | varchar(64)         | NO   |     |         |       |
	# ORDINAL_POSITION         | bigint(21) unsigned | NO   |     | 0       |       |
	# COLUMN_DEFAULT           | longtext            | YES  |     | NULL    |       |
	# IS_NULLABLE              | varchar(3)          | NO   |     |         |       |
	# DATA_TYPE                | varchar(64)         | NO   |     |         |       |
	# CHARACTER_MAXIMUM_LENGTH | bigint(21) unsigned | YES  |     | NULL    |       |
	# CHARACTER_OCTET_LENGTH   | bigint(21) unsigned | YES  |     | NULL    |       |
	# NUMERIC_PRECISION        | bigint(21) unsigned | YES  |     | NULL    |       |
	# NUMERIC_SCALE            | bigint(21) unsigned | YES  |     | NULL    |       |
	# CHARACTER_SET_NAME       | varchar(32)         | YES  |     | NULL    |       |
	# COLLATION_NAME           | varchar(32)         | YES  |     | NULL    |       |
	# COLUMN_TYPE              | longtext            | NO   |     | NULL    |       |
	# COLUMN_KEY               | varchar(3)          | NO   |     |         |       |
	# EXTRA                    | varchar(27)         | NO   |     |         |       |
	# PRIVILEGES               | varchar(80)         | NO   |     |         |       |
	# COLUMN_COMMENT           | varchar(1024)       | NO   |     |         |       |

	my $sql = qq{SELECT COLUMN_NAME, IS_NULLABLE,
	(CASE WHEN COLUMN_DEFAULT IS NOT NULL THEN COLUMN_DEFAULT ELSE EXTRA END) AS COLUMN_DEFAULT,
	TABLE_NAME, DATA_TYPE, ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
$condition
ORDER BY ORDINAL_POSITION
};

	if ($self->{db_version} < '5.5.0') {
		$sql =~ s/\bDATA_TYPE\b/DTD_IDENTIFIER/;
	}
	my $sth = $self->{dbh}->prepare($sql);
	if (!$sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch)
	{
		$data{$row->[3]}{"$row->[0]"}{nullable} = $row->[1];
		$data{$row->[3]}{"$row->[0]"}{default} = $row->[2];
		# Store the data type of the column following its position
		$data{$row->[3]}{data_type}{$row->[5]} = $row->[4];
	}

	return %data;
}

sub _list_triggers
{
        my($self) = @_;

	my $str = "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE FROM INFORMATION_SCHEMA.TRIGGERS";
	if ($self->{schema}) {
		$str .= " AND TRIGGER_SCHEMA = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('TABLE|VIEW|TRIGGER','EVENT_OBJECT_TABLE|EVENT_OBJECT_TABLE|TRIGGER_NAME');
	$str =~ s/ AND / WHERE /;

	$str .= " ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %triggers = ();
	while (my $row = $sth->fetch) {
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
        $schema_clause = "WHERE SCHEMA_NAME(t.schema_id)='$self->{schema}'" if ($self->{schema});

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
JOIN sys.objects AS o ON sm.object_id = o.object_id $schema_clause
ORDER BY 1;};
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->{'LongReadLen'} = 1000000;
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

	# nothing to do, AUTO_INCREMENT column are converted to serial/bigserial
	return;
}

=head2 _get_materialized_views

This function implements a mysql-native materialized views information.

Returns a hash of view names with the SQL queries they are based on.

=cut

sub _get_materialized_views
{
	my($self) = @_;

	my $str = qq{select
       v.name as view_name,
       schema_name(v.schema_id) as schema_name,
       i.name as index_name,
       m.definition
from sys.views v
join sys.indexes i on i.object_id = v.object_id and i.index_id = 1 and i.ignore_dup_key = 0
join sys.sql_modules m on m.object_id = v.object_id
};

	if (!$self->{schema}) {
		$str .= " WHERE schema_name(v.schema_id) NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE schema_name(v.schema_id) = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('MVIEW', 'v.name');
	$str .= " ORDER BY schema_name, view_name";

	my $sth = $self->{dbh}->prepare($str);
	$sth->{'LongReadLen'} = 1000000;
	if (not defined $sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	if (not $sth->execute(@{$self->{query_bind_params}})) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		return ();
	}

	my %data = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
		$row->[3] =~ s/
		$row->[3] =~ s/[\[\]]//g;
		$row->[3] =~ s/^CREATE VIEW [^\s]+//;
		$data{$row->[0]}{text} = $row->[3];
		$data{$row->[0]}{updatable} = 0;
		$data{$row->[0]}{refresh_mode} = '';
		$data{$row->[0]}{refresh_method} = '';
		$data{$row->[0]}{no_index} = 0;
		$data{$row->[0]}{rewritable} = 0;
		$data{$row->[0]}{build_mode} = '';
		$data{$row->[0]}{owner} = $row->[1];
	}

	return %data;
}

sub _get_materialized_view_names
{
	my($self) = @_;

	my $str = qq{select
       v.name as view_name,
       schema_name(v.schema_id) as schema_name,
       i.name as index_name,
from sys.views v
join sys.indexes i on i.object_id = v.object_id and i.index_id = 1 and i.ignore_dup_key = 0
};
	if (!$self->{schema}) {
		$str .= " WHERE schema_name(v.schema_id) NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE schema_name(v.schema_id) = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('MVIEW', 'v.name');
	$str .= " ORDER BY schema_name, view_name";
	my $sth = $self->{dbh}->prepare($str);
	if (not defined $sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	if (not $sth->execute(@{$self->{query_bind_params}})) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}

	my @data = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
		push(@data, uc($row->[0]));
	}

	return @data;
}

sub _get_package_function_list
{
	my ($self, $owner) = @_;

	# not package in MSSQL
	return;
}

sub _get_procedures
{
	my ($self) = @_;

	# not package in MSSQL
	return _get_functions($self);
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
	$sth->{'LongReadLen'} = 1000000;
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);

	my @types = ();
	my @fct_done = ();
	while (my $row = $sth->fetch)
	{
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
	$sth->{'LongReadLen'} = 1000000;
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);

	my $current_type = '';
	my $old_type = '';
	while (my $row = $sth->fetch)
	{
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
        my ($self, $name) = @_;

	# Not supported
	return;
}

1;
