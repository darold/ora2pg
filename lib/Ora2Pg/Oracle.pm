package Ora2Pg::Oracle;

use vars qw($VERSION);
use strict;

use POSIX qw(locale_h);
use Benchmark;
use DBI;
use Encode;

#set locale to LC_NUMERIC C
setlocale(LC_NUMERIC,"C");

$VERSION = '23.2';

# Some function might be excluded from export and assessment.
our @EXCLUDED_FUNCTION = ('SQUIRREL_GET_ERROR_OFFSET');

# These definitions can be overriden from configuration
# file using the DATA_TYPË configuration directive.
our %SQL_TYPE = (
       # Oracle only has one flexible underlying numeric type, NUMBER.
	# Without precision and scale it is set to the PG type float8
	# to match all needs
	'NUMBER' => 'numeric',
	# CHAR types limit of 2000 bytes with defaults to 1 if no length
	# is specified. PG char type has max length set to 8104 so it
	# should match all needs
	'CHAR' => 'char',
	'NCHAR' => 'char',
	# VARCHAR types the limit is 2000 bytes in Oracle 7 and 4000 in
	# Oracle 8. PG varchar type has max length iset to 8104 so it
	# should match all needs
	'VARCHAR' => 'varchar',
	'NVARCHAR' => 'varchar',
	'VARCHAR2' => 'varchar',
	'NVARCHAR2' => 'varchar',
	'STRING' => 'varchar',
	# The DATE data type is used to store the date and time
	# information. PG type timestamp should match all needs.
	'DATE' => 'timestamp',
	# Type LONG is like VARCHAR2 but with up to 2Gb. PG type text
	# should match all needs or if you want you could use blob
	'LONG' => 'text', # Character data of variable length
	'LONG RAW' => 'bytea',
	# Types LOB and FILE are like LONG but with up to 4Gb. PG type
	# text should match all needs or if you want you could use blob
	# (large object)
	'CLOB' => 'text', # A large object containing single-byte characters
	'NCLOB' => 'text', # A large object containing national character set data
	'BLOB' => 'bytea', # Binary large object
	# The full path to the external file is returned if destination type is text.
	# If the destination type is bytea the content of the external file is returned.
	'BFILE' => 'bytea', # Locator for external large binary file
	# RAW column with a length of 16 or 32 bytes are usually GUID, convert them to uuid.
	'RAW(16)' => 'uuid',
	'RAW(32)' => 'uuid',
	# The RAW type is presented as hexadecimal characters. The
	# contents are treated as binary data. Limit of 2000 bytes
	# PG type text should match all needs or if you want you could
	# use blob (large object)
	'RAW' => 'bytea',
	'ROWID' => 'oid',
	'UROWID' => 'oid',
	'FLOAT' => 'double precision',
	'DEC' => 'decimal',
	'DECIMAL' => 'decimal',
	'DOUBLE PRECISION' => 'double precision',
	'INT' => 'integer',
	'INTEGER' => 'integer',
	'BINARY_INTEGER' => 'integer',
	'PLS_INTEGER' => 'integer',
	'SMALLINT' => 'smallint',
	'REAL' => 'real',
	'BINARY_FLOAT' => 'numeric',
	'BINARY_DOUBLE' => 'numeric',
	'TIMESTAMP' => 'timestamp',
	'BOOLEAN' => 'boolean',
	'INTERVAL' => 'interval',
	'XMLTYPE' => 'xml',
	'TIMESTAMP WITH TIME ZONE' => 'timestamp with time zone',
	'TIMESTAMP WITH LOCAL TIME ZONE' => 'timestamp with time zone',
	'SDO_GEOMETRY' => 'geometry',
);

our %GTYPE = (
	'UNKNOWN_GEOMETRY' => 'GEOMETRY',
	'GEOMETRY' => 'GEOMETRY',
	'POINT' => 'POINT',
	'LINE' => 'LINESTRING',
	'CURVE' => 'LINESTRING',
	'POLYGON' => 'POLYGON',
	'SURFACE' => 'POLYGON',
	'COLLECTION' => 'GEOMETRYCOLLECTION',
	'MULTIPOINT' => 'MULTIPOINT',
	'MULTILINE' => 'MULTILINESTRING',
	'MULTICURVE' => 'MULTILINESTRING',
	'MULTIPOLYGON' => 'MULTIPOLYGON',
	'MULTISURFACE' => 'MULTIPOLYGON',
	'SOLID' => 'SOLID',
	'MULTISOLID' => 'MULTISOLID'
);

our %ORA2PG_SDO_GTYPE = (
	'0' => 'GEOMETRY',
	'1' => 'POINT',
	'2' => 'LINESTRING',
	'3' => 'POLYGON',
	'4' => 'GEOMETRYCOLLECTION',
	'5' => 'MULTIPOINT',
	'6' => 'MULTILINESTRING',
	'7' => 'MULTIPOLYGON',
	'8' => 'SOLID',
	'9' => 'MULTISOLID'
);

sub _db_connection
{
	my $self = shift;

	if (!defined $self->{oracle_pwd})
	{
		eval("use Term::ReadKey;") unless $self->{oracle_user} eq '/';
		if (!$@) {
			$self->{oracle_user} = $self->_ask_username('Oracle') unless (defined $self->{oracle_user});
			$self->{oracle_pwd} = $self->_ask_password('Oracle') unless ($self->{oracle_user} eq '/');
		}
	}
	my $ora_session_mode = ($self->{oracle_user} eq "/" || $self->{oracle_user} eq "sys") ? 2 : undef;

	$self->logit("ORACLE_HOME = $ENV{ORACLE_HOME}\n", 1);
	$self->logit("NLS_LANG = $ENV{NLS_LANG}\n", 1);
	$self->logit("NLS_NCHAR = $ENV{NLS_NCHAR}\n", 1);
	$self->logit("Trying to connect to database: $self->{oracle_dsn}\n", 1) if (!$self->{quiet});

	my $dbh = DBI->connect($self->{oracle_dsn}, $self->{oracle_user}, $self->{oracle_pwd},
		{
			ora_envhp => 0,
			LongReadLen=>$self->{longreadlen},
			LongTruncOk=>$self->{longtruncok},
			AutoInactiveDestroy => 1,
			PrintError => 0,
			ora_session_mode => $ora_session_mode,
			ora_client_info => 'ora2pg ' || $VERSION
		}
	);

	# Check for connection failure
	if (!$dbh) {
		$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
	}

	# Get Oracle version, needed to set date/time format
	my $sth = $dbh->prepare( "SELECT BANNER FROM v\$version" ) or return undef;
	$sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$self->{db_version} = $row[0];
		last;
	}
	$sth->finish();
	chomp($self->{db_version});
	$self->{db_version} =~ s/ \- .*//;

	# Check if the connection user has the DBA privilege
	$sth = $dbh->prepare( "SELECT 1 FROM DBA_ROLE_PRIVS" );
	if (!$sth) {
		my $ret = $dbh->err;
		if ($ret == 942 && $self->{prefix} eq 'DBA') {
			$self->logit("HINT: you should activate USER_GRANTS for a connection without DBA privilege. Continuing with USER privilege.\n");
			# No DBA privilege, set use of ALL_* tables instead of DBA_* tables
			$self->{prefix} = 'ALL';
			$self->{user_grants} = 1;
		}
	} else {
		$sth->finish();
	}

	# Fix a problem when exporting type LONG and LOB
	$dbh->{'LongReadLen'} = $self->{longreadlen};
	$dbh->{'LongTruncOk'} = $self->{longtruncok};
	# Embedded object (user defined type) must be returned as an
	# array rather than an instance. This is normally the default.
	$dbh->{'ora_objects'} = 0;

	# Force datetime format
	$self->_datetime_format($dbh);
	# Force numeric format
	$self->_numeric_format($dbh);

	# Use consistent reads for concurrent dumping...
	$dbh->begin_work || $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	if ($self->{debug} && !$self->{quiet}) {
		$self->logit("Isolation level: $self->{transaction}\n", 1);
	}
	$sth = $dbh->prepare($self->{transaction}) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	$sth->finish;

	# Get the current SCN to get data if required
	if (grep(/^$self->{type}$/i, 'INSERT', 'COPY', 'TEST_DATA') && lc($self->{oracle_scn}) eq 'current')
	{
		$sth = $dbh->prepare("SELECT CURRENT_SCN FROM v\$database") or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
		my @row = $sth->fetchrow();
		$self->{oracle_scn} = $row[0];
		$sth->finish;
	}
	$self->logit("Using SCN: $self->{oracle_scn}\n", 1) if ($self->{oracle_scn});

	# Force execution of initial command
	$self->_ora_initial_command($dbh);

	return $dbh;
}

sub _get_version
{
	my $self = shift;

	my $oraver = '';
	my $sql = "SELECT BANNER FROM v\$version";

	my $sth = $self->{dbh}->prepare( $sql ) or return undef;
	$sth->execute or return undef;
	while ( my @row = $sth->fetchrow())
	{
		$oraver = $row[0];
		last;
	}
	$sth->finish();

	chomp($oraver);
	$oraver =~ s/ \- .*//;

	return $oraver;
}

sub _schema_list
{
	my $self = shift;

	my $sql = "SELECT DISTINCT OWNER FROM $self->{prefix}_TABLES WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ORDER BY OWNER";

	my $sth = $self->{dbh}->prepare( $sql ) or return undef;
	$sth->execute or return undef;
	$sth;
}

sub _table_exists
{
	my ($self, $schema, $table) = @_;

        my $ret = '';

	my $sql = "SELECT TABLE_NAME FROM $self->{prefix}_TABLES WHERE OWNER = '$schema' AND TABLE_NAME = '$table'";
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

	my $sql = "SELECT * FROM NLS_DATABASE_PARAMETERS";
	my $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	$sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	my $language = '';
	my $territory = '';
	my $charset = '';
	my $nls_timestamp_format = '';
	my $nls_date_format = '';
	while ( my @row = $sth->fetchrow()) {
		if ($row[0] eq 'NLS_LANGUAGE') {
			$language = $row[1];
		} elsif ($row[0] eq 'NLS_TERRITORY') {
			$territory = $row[1];
		} elsif ($row[0] eq 'NLS_CHARACTERSET') {
			$charset = $row[1];
		} elsif ($row[0] eq 'NLS_TIMESTAMP_FORMAT') {
			$nls_timestamp_format = $row[1];
		} elsif ($row[0] eq 'NLS_DATE_FORMAT') {
			$nls_date_format = $row[1];
		}
	}
	$sth->finish();
	$sql = "SELECT * FROM NLS_SESSION_PARAMETERS";
	$sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	$sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	my $ora_encoding = '';
	while ( my @row = $sth->fetchrow()) {
		#$self->logit("SESSION PARAMETERS: $row[0] $row[1]\n", 1);
		if ($row[0] eq 'NLS_LANGUAGE') {
			$language = $row[1];
		} elsif ($row[0] eq 'NLS_TERRITORY') {
			$territory = $row[1];
		} elsif ($row[0] eq 'NLS_TIMESTAMP_FORMAT') {
			$nls_timestamp_format = $row[1];
		} elsif ($row[0] eq 'NLS_DATE_FORMAT') {
			$nls_date_format = $row[1];
		}
	}
	$sth->finish();

	$ora_encoding = $language . '_' . $territory . '.' . $charset;
	my $pg_encoding = auto_set_encoding($charset);

	return ($ora_encoding, $charset, $pg_encoding, $nls_timestamp_format, $nls_date_format);
}

# Return the lower value between two
sub min
{
	return $_[0] if ($_[0] < $_[1]);

	return $_[1];
}


=head2 _table_info

This function retrieves all tables information.

Returns a handle to a DB query statement.

=cut

sub _table_info
{
	my $self = shift;
	my $do_real_row_count = shift;

	my $owner = '';
	if ($self->{schema}) {
		$owner .= " A.OWNER='$self->{schema}' ";
	} else {
	    $owner .= " A.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}

	####
	# Get name of all TABLE objects in ALL_OBJECTS loking at OBJECT_TYPE='TABLE'
	####
	my $sql = "SELECT A.OWNER,A.OBJECT_NAME,A.OBJECT_TYPE FROM $self->{prefix}_OBJECTS A WHERE A.OBJECT_TYPE IN ('TABLE','VIEW') AND $owner";
	$sql .= $self->limit_to_objects('TABLE', 'A.OBJECT_NAME');
	$self->logit("DEBUG: $sql\n", 2);
	my $t0 = Benchmark->new;
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my $nrows = 0;
	my %tbtype = ();
	while (my $row = $sth->fetch)
	{
		$self->{all_objects}{"$row->[0].$row->[1]"} =  $row->[2];
		$nrows++;
	}
	$sth->finish();
	my $t1 = Benchmark->new;
	my $td = timediff($t1, $t0);
	$self->logit("Collecting $nrows tables in $self->{prefix}_OBJECTS took: " . timestr($td) . "\n", 1);

	####
	# Get comments for all tables
	####
	my %comments = ();
	if ($self->{type} eq 'TABLE')
	{
		$sql = "SELECT A.TABLE_NAME,A.COMMENTS,A.TABLE_TYPE,A.OWNER FROM $self->{prefix}_TAB_COMMENTS A WHERE $owner";
		if ($self->{db_version} !~ /Release 8/) {
			$sql .= $self->exclude_mviews('A.OWNER, A.TABLE_NAME');
		}
		$sql .= $self->limit_to_objects('TABLE', 'A.TABLE_NAME');
		$self->logit("DEBUG: $sql\n", 2);
		$t0 = Benchmark->new;
		$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$nrows = 0;
		my %tbtype = ();
		while (my $row = $sth->fetch)
		{
			next if (!exists $self->{all_objects}{"$row->[3].$row->[0]"} || $self->{all_objects}{"$row->[3].$row->[0]"} ne 'TABLE');
			if (!$self->{schema} && $self->{export_schema}) {
				$row->[0] = "$row->[3].$row->[0]";
			}
			$comments{$row->[0]}{comment} = $row->[1];
			$comments{$row->[0]}{table_type} = $row->[2];
			$tbtype{$row->[2]}++;
			$nrows++;
		}
		$sth->finish();
		$t1 = Benchmark->new;
		$td = timediff($t1, $t0);
		$self->logit("Collecting $nrows tables comments in $self->{prefix}_TAB_COMMENTS took: " . timestr($td) . "\n", 1);
	}

	####
	# Get information about all tables
	####
	$sql = "SELECT A.OWNER,A.TABLE_NAME,NVL(num_rows,1) NUMBER_ROWS,A.TABLESPACE_NAME,A.NESTED,A.LOGGING,A.PARTITIONED,A.PCT_FREE,A.TEMPORARY,A.DURATION FROM $self->{prefix}_TABLES A WHERE $owner";
	$sql .= " AND A.TEMPORARY='N'" if (!$self->{export_gtt});
	$sql .= " AND (A.NESTED != 'YES' OR A.LOGGING != 'YES') AND A.SECONDARY = 'N'";
	if ($self->{db_version} !~ /Release [89]/) {
		$sql .= " AND (A.DROPPED IS NULL OR A.DROPPED = 'NO')";
	}
	if ($self->{db_version} !~ /Release 8/) {
		$sql .= $self->exclude_mviews('A.OWNER, A.TABLE_NAME');
	}
	$sql .= $self->limit_to_objects('TABLE', 'A.TABLE_NAME');
	$sql .= " AND (A.IOT_TYPE IS NULL OR A.IOT_TYPE = 'IOT')";
	#$sql .= " ORDER BY A.OWNER, A.TABLE_NAME";

	$self->logit("DEBUG: $sql\n", 2);
	$t0 = Benchmark->new;
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %tables_infos = ();
	$nrows = 0;
	while (my $row = $sth->fetch)
	{
		next if (!exists $self->{all_objects}{"$row->[0].$row->[1]"} || $self->{all_objects}{"$row->[0].$row->[1]"} ne 'TABLE');
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[1] = "$row->[0].$row->[1]";
		}
		$tables_infos{$row->[1]}{owner} = $row->[0] || '';
		$tables_infos{$row->[1]}{num_rows} = $row->[2] || 0;
		$tables_infos{$row->[1]}{tablespace} = $row->[3] || 0;
		$tables_infos{$row->[1]}{comment} =  $comments{$row->[1]}{comment} || '';
		$tables_infos{$row->[1]}{type} =  $comments{$row->[1]}{table_type} || '';
		$tables_infos{$row->[1]}{nested} = $row->[4] || '';
		if ($row->[5] eq 'NO') {
			$tables_infos{$row->[1]}{nologging} = 1;
		} else {
			$tables_infos{$row->[1]}{nologging} = 0;
		}
		if ($row->[6] eq 'NO') {
			$tables_infos{$row->[1]}{partitioned} = 0;
		} else {
			$tables_infos{$row->[1]}{partitioned} = 1;
		}
		# Only take care of PCTFREE upper than the Oracle default value
		if (($row->[7] || 0) > 10) {
			$tables_infos{$row->[1]}{fillfactor} = 100 - min(90, $row->[7]);
		}
		# Global temporary table ?
		$tables_infos{$row->[1]}{temporary} = $row->[8];
		$tables_infos{$row->[1]}{duration} = $row->[9];
		$nrows++;
	}
	$sth->finish();

	$t1 = Benchmark->new;
	$td = timediff($t1, $t0);
	$self->logit("Collecting $nrows tables information in $self->{prefix}_TABLES took: " . timestr($td) . "\n", 1);

	return %tables_infos;
}

sub _column_comments
{
	my ($self, $table) = @_;

	my $condition = '';

	my $sql = "SELECT A.COLUMN_NAME,A.COMMENTS,A.TABLE_NAME,A.OWNER FROM $self->{prefix}_COL_COMMENTS A $condition";
	if ($self->{schema}) {
		$sql .= "WHERE A.OWNER='$self->{schema}' ";
	} else {
		$sql .= " WHERE A.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$sql .= "AND A.TABLE_NAME='$table' " if ($table);
	if ($self->{db_version} !~ /Release 8/) {
		$sql .= $self->exclude_mviews('A.OWNER, A.TABLE_NAME');
	}
	if (!$table) {
		$sql .= $self->limit_to_objects('TABLE','TABLE_NAME');
	} else {
		@{$self->{query_bind_params}} = ();
	}

	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %data = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[2] = "$row->[3].$row->[2]";
		}
		next if (!$self->is_in_struct($row->[2], $row->[0]));
		$data{$row->[2]}{$row->[0]} = $row->[1];
	}

	return %data;
}

sub _column_info
{
	my ($self, $table, $owner, $objtype, $recurs) = @_;

	$objtype ||= 'TABLE';

	my $condition = '';
	$condition .= "AND A.TABLE_NAME='$table' " if ($table);
	if ($owner) {
		$condition .= "AND A.OWNER='$owner' ";
	} else {
		$condition .= " AND A.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE', 'A.TABLE_NAME');
	} else {
		@{$self->{query_bind_params}} = ();
	}

	my $sth = '';
	my $sql = '';
	if ($self->{db_version} !~ /Release 8/)
	{
		my $exclude_mview = $self->exclude_mviews('A.OWNER, A.TABLE_NAME');
		$sql = qq{
SELECT A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, A.NULLABLE, A.DATA_DEFAULT,
    A.DATA_PRECISION, A.DATA_SCALE, A.CHAR_LENGTH, A.TABLE_NAME, A.OWNER
FROM $self->{prefix}_TAB_COLUMNS A
WHERE 1=1 $condition
ORDER BY A.COLUMN_ID
};
		$sth = $self->{dbh}->prepare($sql);
		if (!$sth)
		{
			my $ret = $self->{dbh}->err;
			if (!$recurs && ($ret == 942) && ($self->{prefix} eq 'DBA'))
			{
				$self->logit("HINT: Please activate USER_GRANTS or connect using a user with DBA privilege.\n");
				$self->{prefix} = 'ALL';
				return $self->_column_info($table, $owner, $objtype, 1);
			}
			$self->logit("FATAL: _column_info() " . $self->{dbh}->errstr . "\n", 0, 1);
		}
	}
	else
	{
		# an 8i database.
		$sql = qq{
SELECT A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, A.NULLABLE, A.DATA_DEFAULT,
    A.DATA_PRECISION, A.DATA_SCALE, A.DATA_LENGTH, A.TABLE_NAME, A.OWNER
FROM $self->{prefix}_TAB_COLUMNS A
    $condition
ORDER BY A.COLUMN_ID
};
		$sth = $self->{dbh}->prepare($sql);
		if (!$sth)
		{
			my $ret = $self->{dbh}->err;
			if (!$recurs && ($ret == 942) && ($self->{prefix} eq 'DBA'))
			{
				$self->logit("HINT: Please activate USER_GRANTS or connect using a user with DBA privilege.\n");
				$self->{prefix} = 'ALL';
				return $self->_column_info($table, $owner, $objtype, 1);
			}
			$self->logit("FATAL: _column_info() " . $self->{dbh}->errstr . "\n", 0, 1);
		}
	}
	$self->logit("DEBUG, $sql", 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: _column_info() " . $self->{dbh}->errstr . "\n", 0, 1);

	# Default number of line to scan to grab the geometry type of the column.
	# If it not limited, the query will scan the entire table which may take a very long time.
	my $max_lines = 50000;
	$max_lines = $self->{autodetect_spatial_type} if ($self->{autodetect_spatial_type} > 1);
	my $spatial_gtype =  'SELECT DISTINCT c.%s.SDO_GTYPE FROM %s c WHERE ROWNUM < ' . $max_lines;
	my $st_spatial_gtype =  'SELECT DISTINCT ST_GeometryType(c.%s) FROM %s c WHERE ROWNUM < ' . $max_lines;
	# Set query to retrieve the SRID
	my $spatial_srid = "SELECT SRID FROM ALL_SDO_GEOM_METADATA WHERE TABLE_NAME=? AND COLUMN_NAME=? AND OWNER=?";
	my $st_spatial_srid = "SELECT ST_SRID(c.%s) FROM %s c";
	if ($self->{convert_srid})
	{
		# Translate SRID to standard EPSG SRID, may return 0 because there's lot of Oracle only SRID.
		$spatial_srid = 'SELECT sdo_cs.map_oracle_srid_to_epsg(SRID) FROM ALL_SDO_GEOM_METADATA WHERE TABLE_NAME=? AND COLUMN_NAME=? AND OWNER=?';
	}
	# Get the dimension of the geometry by looking at the number of element in the SDO_DIM_ARRAY
	my $spatial_dim = "SELECT t.SDO_DIMNAME, t.SDO_LB, t.SDO_UB FROM ALL_SDO_GEOM_METADATA m, TABLE (m.diminfo) t WHERE m.TABLE_NAME=? AND m.COLUMN_NAME=? AND OWNER=?";
	my $st_spatial_dim = "SELECT ST_DIMENSION(c.%s) FROM %s c";

	my $is_virtual_col = "SELECT V.VIRTUAL_COLUMN FROM $self->{prefix}_TAB_COLS V WHERE V.OWNER=? AND V.TABLE_NAME=? AND V.COLUMN_NAME=?";
	my $sth3 = undef;
	if ($self->{db_version} !~ /Release 8/) {
		$sth3 = $self->{dbh}->prepare($is_virtual_col);
	}

	my $t0 = Benchmark->new;
	my %data = ();
	my $pos = 0;
	my $ncols = 0;
	while (my $row = $sth->fetch)
	{
		my $tmptable = "$row->[9].$row->[8]";
		# Skip object if it is not in the object list and if this is not
		# a view or materialized view that must be exported as table.
		next if (!exists $self->{all_objects}{$tmptable}
				|| ($self->{all_objects}{$tmptable} eq 'VIEW'
					&& !grep(/^$row->[8]$/i, @{$self->{view_as_table}}))
				|| ($self->{all_objects}{$tmptable} eq 'MATERIALIZED VIEW'
					&& !grep(/^$row->[8]$/i, @{$self->{mview_as_table}}))
			);

		$row->[2] = $row->[7] if $row->[1] =~ /char/i;
		# Seems that for a NUMBER with a DATA_SCALE to 0, no DATA_PRECISION and a DATA_LENGTH of 22
		# Oracle use a NUMBER(38) instead
		if ( ($row->[1] eq 'NUMBER') && ($row->[6] eq '0') && ($row->[5] eq '') && ($row->[2] == 22) ) {
			$row->[2] = 38;
		}

		# Use FQDN table name otherwise a table not exist error can occurs.
		$tmptable = "$row->[9].$row->[8]";

		# In case we have a default value, check if this is a virtual column
		my $virtual = 'NO';
		if ($self->{pg_supports_virtualcol} and defined $sth3 and $row->[4])
		{
			$sth3->execute($row->[9],$row->[8],$row->[0]) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			my $r = $sth3->fetch;
			$virtual = $r->[0];
		}

		# check if this is a spatial column (srid, dim, gtype)
		my @geom_inf = ();
		if ($row->[1] eq 'SDO_GEOMETRY' || $row->[1] =~ /^ST_|STGEOM_/)
		{
			# Get the SRID of the column
			if ($self->{convert_srid} > 1) {
				push(@geom_inf, $self->{convert_srid});
			}
			else
			{
				my @result = ();
				if ($row->[1] =~ /^ST_|STGEOM_/) {
					$spatial_srid = sprintf($st_spatial_srid, $row->[0], $tmptable);
				}
				my $sth2 = $self->{dbh}->prepare($spatial_srid);
				if (!$sth2)
				{
					if ($self->{dbh}->errstr !~ /ORA-01741/) {
						$self->logit("FATAL: _column_info() " . $self->{dbh}->errstr . "\n", 0, 1);
					} else {
						# No SRID defined, use default one
						$self->logit("WARNING: Error retreiving SRID, no matter default SRID will be used: $spatial_srid\n", 0);
					}
				}
				else
				{
					if ($row->[1] =~ /^ST_|STGEOM_/) {
						$sth2->execute() or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
					} else {
						$sth2->execute($row->[8],$row->[0],$row->[9]) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
					}
					while (my $r = $sth2->fetch) {
						push(@result, $r->[0]) if ($r->[0] =~ /\d+/);
					}
					$sth2->finish();
				}
				if ($#result == 0) {
					push(@geom_inf, $result[0]);
				} elsif ($self->{default_srid}) {
					push(@geom_inf, $self->{default_srid});
				} else {
					push(@geom_inf, 0);
				}
			}

			# Grab constraint type and dimensions from index definition
			my $found_contraint = 0;
			my $found_dims = 0;
			foreach my $idx (keys %{$self->{tables}{$tmptable}{idx_type}})
			{
				if (exists $self->{tables}{$tmptable}{idx_type}{$idx}{type_constraint})
				{
					foreach my $c (@{$self->{tables}{$tmptable}{indexes}{$idx}})
					{
						if ($c eq $row->[0])
						{
							if ($self->{tables}{$tmptable}{idx_type}{$idx}{type_dims}) {
								$found_dims = $self->{tables}{$tmptable}{idx_type}{$idx}{type_dims};
							}
							if ($self->{tables}{$tmptable}{idx_type}{$idx}{type_constraint}) {
								$found_contraint = $GTYPE{$self->{tables}{$tmptable}{idx_type}{$idx}{type_constraint}} || $self->{tables}{$tmptable}{idx_type}{$idx}{type_constraint};
							}
						}
					}
				}
			}

			# Get the dimension of the geometry column
			if (!$found_dims)
			{
				if ($row->[1] =~ /^ST_|STGEOM_/) {
					$spatial_dim = sprintf($st_spatial_dim, $row->[0], $tmptable);
				}
				my $sth2 = $self->{dbh}->prepare($spatial_dim);
				if (!$sth2) {
					$self->logit("FATAL: _column_info() " . $self->{dbh}->errstr . "\n", 0, 1);
				}
				if ($row->[1] =~ /^ST_|STGEOM_/) {
					$sth2->execute() or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
				} else {
					$sth2->execute($row->[8],$row->[0],$row->[9]) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
				}
				my $count = 0;
				while (my $r = $sth2->fetch) {
					$count++;
				}
				$sth2->finish();
				push(@geom_inf, $count);
			}
			else
			{
				push(@geom_inf, $found_dims);
			}

			# Set dimension and type of the spatial column
			if (!$found_contraint && $self->{autodetect_spatial_type})
			{
				# Get spatial information
				my $squery = sprintf($spatial_gtype, $row->[0], $tmptable);
				if ($row->[1] =~ /^ST_|STGEOM_/) {
					$squery = sprintf($st_spatial_gtype, $row->[0], $tmptable);
				}
				my $sth2 = $self->{dbh}->prepare($squery);
				if (!$sth2) {
					$self->logit("FATAL: _column_info() " . $self->{dbh}->errstr . "\n", 0, 1);
				}
				$sth2->execute or $self->logit("FATAL: _column_info() " . $self->{dbh}->errstr . "\n", 0, 1);
				my @result = ();
				while (my $r = $sth2->fetch)
				{
					if ($r->[0] =~ /(\d)$/) {
						push(@result, $Ora2Pg::Oracle::ORA2PG_SDO_GTYPE{$1});
					} elsif ($r->[0] =~ /ST_(.*)$/) {
						push(@result, $1);
					}
				}
				$sth2->finish();
				if ($#result == 0) {
					push(@geom_inf, $result[0]);
				} else {
					push(@geom_inf, join(',', @result));
				}
			}
			elsif ($found_contraint)
			{
				push(@geom_inf, $found_contraint);
			}
			else
			{
				push(@geom_inf, $Ora2Pg::Oracle::ORA2PG_SDO_GTYPE{0});
			}
		}

		# Replace dot in column name by underscore
		if ($row->[0] =~ /\./ && (!exists $self->{replaced_cols}{"\L$tmptable\E"}
					|| !exists $self->{replaced_cols}{"\L$tmptable\E"}{"\L$row->[0]\E"})) {
			$self->{replaced_cols}{"\L$tmptable\E"}{"\L$row->[0]\E"} = $row->[0];
			$self->{replaced_cols}{"\L$tmptable\E"}{"\L$row->[0]\E"} =~ s/\./_/g;
		}

		if (!$self->{schema} && $self->{export_schema})
		{
			next if (!$self->is_in_struct($tmptable, $row->[0]));
			push(@{$data{$tmptable}{"$row->[0]"}}, (@$row, $virtual, $pos, @geom_inf));
		}
		else
		{
			next if (!$self->is_in_struct($row->[8], $row->[0]));
			push(@{$data{"$row->[8]"}{"$row->[0]"}}, (@$row, $virtual, $pos, @geom_inf));
		}
		$pos++;
		$ncols++;
	}
	my $t1 = Benchmark->new;
	my $td = timediff($t1, $t0);
	$self->logit("Collecting $ncols columns in $self->{prefix}_INDEXES took: " . timestr($td) . "\n", 1);

	$sth3->finish() if (defined $sth3);

	return %data;
}

=head2 _get_fts_indexes_info

This function retrieve FTS index attributes informations

Returns a hash of containing all useful attribute values for all FTS indexes

=cut

sub _get_fts_indexes_info
{
	my ($self, $owner) = @_;

	my $condition = '';
	$condition .= "AND IXV_INDEX_OWNER='$owner' " if ($owner);
	$condition .= $self->limit_to_objects('INDEX', "IXV_INDEX_NAME");

	# Retrieve all indexes informations
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT DISTINCT IXV_INDEX_OWNER,IXV_INDEX_NAME,IXV_CLASS,IXV_ATTRIBUTE,IXV_VALUE
FROM CTXSYS.CTX_INDEX_VALUES
WHERE (IXV_CLASS='WORDLIST' AND IXV_ATTRIBUTE='STEMMER') $condition
END

	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %indexes_info = ();
	while (my $row = $sth->fetch) {
		my $save_idx = $row->[1];
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[1] = "$row->[0].$row->[1]";
		}
		$indexes_info{$row->[1]}{"\L$row->[3]\E"} = $row->[4];
	}

	return %indexes_info;
}

sub _get_indexes
{
	my ($self, $table, $owner, $generated_indexes) = @_;

	# Retrieve FTS indexes information before.
	my %idx_info = ();
	%idx_info = _get_fts_indexes_info($self, $owner) if ($self->_table_exists('CTXSYS', 'CTX_INDEX_VALUES'));

	my $sub_owner = '';
	if ($owner) {
		$sub_owner = "AND A.INDEX_OWNER=B.TABLE_OWNER";
	}

	my $condition = '';
	$condition .= "AND A.TABLE_NAME='$table' " if ($table);
	if ($owner) {
		$condition .= "AND A.INDEX_OWNER='$owner' ";
	} else {
		$condition .= " AND A.INDEX_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	if (!$self->{export_gtt}) {
		$condition .= " AND B.TEMPORARY = 'N' ";
	}
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE|INDEX', "A.TABLE_NAME|A.INDEX_NAME");
	} else {
		@{$self->{query_bind_params}} = ();
	}

	# When comparing number of index we need to retrieve generated index (mostly PK)
	my $generated = '';
	$generated = " B.GENERATED = 'N'" if (!$generated_indexes);

	my $t0 = Benchmark->new;
	my $sth = '';
	my $sql = '';
	if ($self->{db_version} !~ /Release 8/)
	{
		my $no_mview = $self->exclude_mviews('A.INDEX_OWNER, A.TABLE_NAME');
		$no_mview = '' if ($self->{type} eq 'MVIEW');
		$sql = qq{SELECT DISTINCT A.INDEX_NAME,A.COLUMN_NAME,B.UNIQUENESS,A.COLUMN_POSITION,B.INDEX_TYPE,B.TABLE_TYPE,B.GENERATED,B.JOIN_INDEX,A.TABLE_NAME,A.INDEX_OWNER,B.TABLESPACE_NAME,B.ITYP_NAME,B.PARAMETERS,A.DESCEND
FROM $self->{prefix}_IND_COLUMNS A
JOIN $self->{prefix}_INDEXES B ON (B.INDEX_NAME=A.INDEX_NAME AND B.OWNER=A.INDEX_OWNER)
WHERE$generated $condition $no_mview
ORDER BY A.COLUMN_POSITION};
	}
	else
	{
		# an 8i database.
		$sql = qq{SELECT DISTINCT A.INDEX_NAME,A.COLUMN_NAME,B.UNIQUENESS,A.COLUMN_POSITION,B.INDEX_TYPE,B.TABLE_TYPE,B.GENERATED, 'NO', A.TABLE_NAME,A.INDEX_OWNER,B.TABLESPACE_NAME,B.ITYP_NAME,B.PARAMETERS,A.DESCEND
FROM $self->{prefix}_IND_COLUMNS A, $self->{prefix}_INDEXES B
WHERE $generated $condition AND B.INDEX_NAME=A.INDEX_NAME AND B.OWNER=A.INDEX_OWNER
ORDER BY A.COLUMN_POSITION};
	}
	$sql =~ s/WHERE\s+AND/WHERE/s;
	$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my $idxnc = qq{SELECT IE.COLUMN_EXPRESSION FROM $self->{prefix}_IND_EXPRESSIONS IE, $self->{prefix}_IND_COLUMNS IC
WHERE  IE.INDEX_OWNER = IC.INDEX_OWNER
AND    IE.INDEX_NAME = IC.INDEX_NAME
AND    IE.TABLE_OWNER = IC.TABLE_OWNER
AND    IE.TABLE_NAME = IC.TABLE_NAME
AND    IE.COLUMN_POSITION = IC.COLUMN_POSITION
AND    IC.COLUMN_NAME = ?
AND    IE.TABLE_NAME = ?
AND    IC.TABLE_OWNER = ?
};
	my $sth2 = $self->{dbh}->prepare($idxnc) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
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
		# Replace function based index type
		if ( ($row->[4] =~ /FUNCTION-BASED/i) && ($colname =~ /^SYS_NC\d+\$$/) )
		{
			$sth2->execute($colname,$save_tb,$row->[-5]) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			my $nc = $sth2->fetch();
			$row->[1] = $nc->[0];
			$row->[1] =~ s/"//g;
			$row->[1] =~ s/'//g if ($row->[1] =~ /^'[^'\s]+'$/);
			# Single row constraint based on a constant and a function based unique index
			if ($nc->[0] =~ /^\d+$/ && $row->[4] =~ /FUNCTION-BASED/i) {
				$row->[1] = '(' . $nc->[0] . ')';
			}
			# Enclose with double quote if required when is is not an index function
			elsif ($row->[1] !~ /\(.*\)/ && $row->[4] !~ /FUNCTION-BASED/i) {
				$row->[1] = $self->quote_object_name($row->[1]);
			}
			# Append DESC sort order when not default to ASC
			if ($row->[13] eq 'DESC') {
				$row->[1] .= " DESC";
			}
		}
		else
		{
			# Quote column with unsupported symbols
			$row->[1] = $self->quote_object_name($row->[1]);
		}

		$row->[1] =~ s/SYS_EXTRACT_UTC\s*\(([^\)]+)\)/$1/isg;

		# Index with DESC are declared as FUNCTION-BASED, fix that
		if (($row->[4] =~ /FUNCTION-BASED/i) && ($row->[1] !~ /\(.*\)/)) {
			$row->[4] =~ s/FUNCTION-BASED\s*//;
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
		if (exists $idx_info{$idx_name}) {
			$idx_type{$row->[8]}{$row->[0]}{stemmer} = $idx_info{$idx_name}{stemmer};
		}
		if ($row->[11] =~ /SPATIAL_INDEX/) {
			$idx_type{$row->[8]}{$row->[0]}{type} = 'SPATIAL INDEX';
			if ($row->[12] =~ /layer_gtype=([^\s,]+)/i) {
				$idx_type{$row->[8]}{$row->[0]}{type_constraint} = uc($1);
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
	$sth2->finish();
	my $t1 = Benchmark->new;
	my $td = timediff($t1, $t0);
	$self->logit("Collecting $nidx indexes in $self->{prefix}_INDEXES took: " . timestr($td) . "\n", 1);

	return \%unique, \%data, \%idx_type, \%index_tablespace;
}

sub _foreign_key
{
	my ($self, $table, $owner) = @_;

	my @tmpparams = ();
	my $condition = '';
	$condition .= "AND CONS.TABLE_NAME='$table' " if ($table);
	if ($owner) {
		$condition .= "AND CONS.OWNER = '$owner' ";
	} else {
		$condition .= "AND CONS.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	$condition .= $self->limit_to_objects('FKEY|TABLE','CONS.CONSTRAINT_NAME|CONS.TABLE_NAME');

	my $deferrable = $self->{fkey_deferrable} ? "'DEFERRABLE' AS DEFERRABLE" : "DEFERRABLE";
	my $defer = $self->{fkey_deferrable} ? "'DEFERRABLE' AS DEFERRABLE" : "CONS.DEFERRABLE";

	my $sql = <<END;
SELECT
    CONS.TABLE_NAME,
    CONS.CONSTRAINT_NAME,
    COLS.COLUMN_NAME,
    CONS_R.TABLE_NAME R_TABLE_NAME,
    CONS.R_CONSTRAINT_NAME,
    COLS_R.COLUMN_NAME R_COLUMN_NAME,
    CONS.SEARCH_CONDITION,CONS.DELETE_RULE,$defer,CONS.DEFERRED,
    CONS.OWNER,CONS.R_OWNER,
    COLS.POSITION,COLS_R.POSITION,
    CONS.VALIDATED
FROM $self->{prefix}_CONSTRAINTS CONS
    LEFT JOIN $self->{prefix}_CONS_COLUMNS COLS ON (COLS.CONSTRAINT_NAME = CONS.CONSTRAINT_NAME AND COLS.OWNER = CONS.OWNER AND COLS.TABLE_NAME = CONS.TABLE_NAME)
    LEFT JOIN $self->{prefix}_CONSTRAINTS CONS_R ON (CONS_R.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME AND CONS_R.OWNER = CONS.R_OWNER)
    LEFT JOIN $self->{prefix}_CONS_COLUMNS COLS_R ON (COLS_R.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME AND COLS_R.POSITION=COLS.POSITION AND COLS_R.OWNER = CONS.R_OWNER)
WHERE CONS.CONSTRAINT_TYPE = 'R' $condition
END
	if ($self->{db_version} !~ /Release 8/) {
		$sql .= $self->exclude_mviews('CONS.OWNER, CONS.TABLE_NAME');
	}

	$sql .= "\nORDER BY CONS.TABLE_NAME, CONS.CONSTRAINT_NAME, COLS.POSITION";

	if ($self->{db_version} =~ /Release 8/) {
		$sql = <<END;
SELECT
    CONS.TABLE_NAME,
    CONS.CONSTRAINT_NAME,
    COLS.COLUMN_NAME,
    CONS_R.TABLE_NAME R_TABLE_NAME,
    CONS.R_CONSTRAINT_NAME,
    COLS_R.COLUMN_NAME R_COLUMN_NAME,
    CONS.SEARCH_CONDITION,CONS.DELETE_RULE,$defer,CONS.DEFERRED,
    CONS.OWNER,CONS.R_OWNER,
    COLS.POSITION,COLS_R.POSITION,
    CONS.VALIDATED
FROM $self->{prefix}_CONSTRAINTS CONS,  $self->{prefix}_CONS_COLUMNS COLS, $self->{prefix}_CONSTRAINTS CONS_R, $self->{prefix}_CONS_COLUMNS COLS_R
WHERE CONS_R.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME AND CONS_R.OWNER = CONS.R_OWNER
    AND COLS.CONSTRAINT_NAME = CONS.CONSTRAINT_NAME AND COLS.OWNER = CONS.OWNER AND COLS.TABLE_NAME = CONS.TABLE_NAME
    AND COLS_R.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME AND COLS_R.POSITION=COLS.POSITION AND COLS_R.OWNER = CONS.R_OWNER
    AND CONS.CONSTRAINT_TYPE = 'R' $condition
ORDER BY CONS.TABLE_NAME, CONS.CONSTRAINT_NAME, COLS.POSITION
END
	}
	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);

	my %data = ();
	my %link = ();
	#my @tab_done = ();
	while (my $row = $sth->fetch) {
		my $local_table = $row->[0];
		my $remote_table = $row->[3];
		if (!$self->{schema} && $self->{export_schema}) {
			$local_table = "$row->[10].$row->[0]";
			$remote_table = "$row->[11].$row->[3]";
		}
		next if (!$self->is_in_struct($local_table, $row->[2]));
		next if (!$self->is_in_struct($remote_table, $row->[2]));
		push(@{$data{$local_table}}, [ ($row->[1],$row->[4],$row->[6],$row->[7],$row->[8],$row->[9],$row->[11],$row->[0],$row->[10],$row->[14]) ]);
		#            TABLENAME     CONSTNAME           COLNAME
		push(@{$link{$local_table}{$row->[1]}{local}}, $row->[2]);
		#            TABLENAME     CONSTNAME          TABLENAME        COLNAME
		push(@{$link{$local_table}{$row->[1]}{remote}{$remote_table}}, $row->[5]);
	}

	return \%link, \%data;
}

=head2 _alias_info

This function implements an Oracle-native column information.

Returns a list of array references containing the following information
for each alias of the specified view:

[(
  column name,
  column id
)]

=cut

sub _alias_info
{
	my ($self, $view) = @_;

	my $str = "SELECT COLUMN_NAME, COLUMN_ID, OWNER FROM $self->{prefix}_TAB_COLUMNS WHERE TABLE_NAME='$view'";
	if ($self->{schema}) {
		$str .= " AND OWNER = '$self->{schema}'";
	} else {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$str .= " ORDER BY COLUMN_ID ASC";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my $data = $sth->fetchall_arrayref();
	#$self->logit("View $view column aliases:\n", 1);
	foreach my $d (@$data)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$d->[0] = "$d->[2].$d->[0]";
		}
		#$self->logit("\t$d->[0] =>  column id:$d->[1]\n", 1);
	}

	return @$data;

}

=head2 _get_views

This function implements an Oracle-native views information.

Returns a hash of view names with the SQL queries they are based on.

=cut

sub _get_views
{
	my ($self) = @_;

	my $owner = '';
	if ($self->{schema}) {
		$owner = "AND A.OWNER='$self->{schema}' ";
	} else {
		$owner = "AND A.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}

	####
	# Get name of all VIEW objects in ALL_OBJECTS looking at OBJECT_TYPE='VIEW' or OBJECT_TYPE='MVIEW'
	####
	my $sql = "SELECT A.OWNER,A.OBJECT_NAME,A.OBJECT_TYPE FROM $self->{prefix}_OBJECTS A WHERE A.OBJECT_TYPE IN ('VIEW', 'MATERIALIZED VIEW') $owner";
	if (!$self->{export_invalid}) {
		$sql .= " AND A.STATUS='VALID'";
	} elsif ($self->{export_invalid} == 2) {
		$sql .= " AND A.STATUS <> 'VALID'";
	}
	$sql .= $self->limit_to_objects('VIEW', 'A.OBJECT_NAME');
	$self->logit("DEBUG: $sql\n", 2);
	my $t0 = Benchmark->new;
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my $nrows = 0;
	my %tbtype = ();
	my %all_objects = ();
	while (my $row = $sth->fetch)
	{
		$all_objects{"$row->[0].$row->[1]"} =  $row->[2];
		$nrows++;
	}
	$sth->finish();
	my $t1 = Benchmark->new;
	my $td = timediff($t1, $t0);
	$self->logit("Collecting $nrows tables in $self->{prefix}_OBJECTS took: " . timestr($td) . "\n", 1);

	my %comments = ();
	if ($self->{type} ne 'SHOW_REPORT')
	{
		$sql = "SELECT A.TABLE_NAME,A.COMMENTS,A.TABLE_TYPE,A.OWNER FROM $self->{prefix}_TAB_COMMENTS A WHERE 1=1 $owner";
		$sql .= $self->limit_to_objects('VIEW', 'A.TABLE_NAME');
		$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch)
		{
			next if ($row->[2] ne 'VIEW');
			next if (scalar keys %{ $self->{all_objects} } > 0 && !exists $self->{all_objects}{"$row->[3].$row->[0]"});
			if (!$self->{schema} && $self->{export_schema}) {
				$row->[0] = "$row->[3].$row->[0]";
			} 
			$comments{$row->[0]}{comment} = $row->[1];
			$comments{$row->[0]}{table_type} = $row->[2];
		}
		$sth->finish();
	}

	# Retrieve all views
	my $str = "SELECT v.VIEW_NAME,v.TEXT,v.OWNER FROM $self->{prefix}_VIEWS v";
	if (!$self->{schema}) {
		$str .= " WHERE v.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE v.OWNER = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('VIEW', 'v.VIEW_NAME');

	# Compute view order, where depended view appear before using view
	my %view_order = ();
	if ($self->{type} ne 'SHOW_REPORT' && !$self->{no_view_ordering})
	{
		if ($self->{db_version} !~ /Release (8|9|10|11\.1)/)
		{
			if ($self->{schema}) {
				$owner = "AND o.OWNER='$self->{schema}' ";
			} else {
				$owner = "AND o.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
			}
			$sql = qq{
WITH x (ITER, OWNER, OBJECT_NAME) AS
( SELECT 1 , o.OWNER, o.OBJECT_NAME FROM $self->{prefix}_OBJECTS o WHERE OBJECT_TYPE = 'VIEW' $owner
  AND NOT EXISTS (SELECT 1 FROM $self->{prefix}_DEPENDENCIES d WHERE TYPE LIKE 'VIEW' AND REFERENCED_TYPE = 'VIEW'
  AND REFERENCED_OWNER = o.OWNER AND d.OWNER = o.OWNER and o.OBJECT_NAME=d.NAME)
UNION ALL
  SELECT ITER + 1, d.OWNER, d.NAME FROM $self->{prefix}_DEPENDENCIES d
     JOIN x ON d.REFERENCED_OWNER = x.OWNER and d.REFERENCED_NAME = x.OBJECT_NAME
    WHERE TYPE LIKE 'VIEW' AND REFERENCED_TYPE = 'VIEW'
)
SELECT max(ITER) ITER, OWNER, OBJECT_NAME FROM x
GROUP BY OWNER, OBJECT_NAME
ORDER BY ITER ASC, 2, 3
};

			my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			while (my $row = $sth->fetch) {
				$view_order{"\U$row->[1].$row->[2]\E"} = $row->[0];
			}
			$sth->finish();
		}
	}

	$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch)
	{
		next if (!exists $all_objects{"$row->[2].$row->[0]"});
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[2].$row->[0]";
		}
		$data{$row->[0]}{text} = $row->[1];
		$data{$row->[0]}{owner} = $row->[2];
		$data{$row->[0]}{comment} = $comments{$row->[0]}{comment} || '';
		if ($self->{type} ne 'SHOW_REPORT')
		{
			@{$data{$row->[0]}{alias}} = _alias_info ($self, $row->[0]);
		}
		if ($self->{type} ne 'SHOW_REPORT' && exists $view_order{"\U$row->[2].$row->[0]\E"})
		{
			$data{$row->[0]}{iter} = $view_order{"\U$row->[2].$row->[0]\E"};
		}
	}

	return %data;
}

sub _get_triggers
{
	my($self) = @_;

	# Retrieve all indexes 
	my $str = "SELECT TRIGGER_NAME, TRIGGER_TYPE, TRIGGERING_EVENT, TABLE_NAME, TRIGGER_BODY, WHEN_CLAUSE, DESCRIPTION, ACTION_TYPE, OWNER FROM $self->{prefix}_TRIGGERS WHERE STATUS='ENABLED'";
	if (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('TABLE|VIEW|TRIGGER','TABLE_NAME|TABLE_NAME|TRIGGER_NAME');

	#$str .= " ORDER BY TABLE_NAME, TRIGGER_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @triggers = ();
	while (my $row = $sth->fetch) {
		push(@triggers, [ @$row ]);
	}

	return \@triggers;
}

sub _unique_key
{
	my ($self, $table, $owner, $type) = @_;

	my %result = ();

	my @accepted_constraint_types = ();
	if ($type) {
		push @accepted_constraint_types, "'$type'";
	} else {
		push @accepted_constraint_types, "'P'" unless($self->{skip_pkeys});
		push @accepted_constraint_types, "'U'" unless($self->{skip_ukeys});
	}
	return %result unless(@accepted_constraint_types);

	my $cons_types = '('. join(',', @accepted_constraint_types) .')';

	my $indexname = "'' AS INDEX_NAME";
	if ($self->{db_version} !~ /Release 8/) {
		$indexname = 'B.INDEX_NAME';
	}
	# Get columns of all the table in the specified schema or excluding the list of system schema
	my $sql = qq{SELECT DISTINCT A.COLUMN_NAME,A.CONSTRAINT_NAME,A.OWNER,A.POSITION,B.CONSTRAINT_NAME,B.CONSTRAINT_TYPE,B.DEFERRABLE,B.DEFERRED,B.GENERATED,B.TABLE_NAME,B.OWNER,$indexname
FROM $self->{prefix}_CONS_COLUMNS A JOIN $self->{prefix}_CONSTRAINTS B ON (B.CONSTRAINT_NAME = A.CONSTRAINT_NAME AND B.OWNER = A.OWNER)
};
	if ($owner) {
		$sql .= " WHERE A.OWNER = '$owner'";
	} else {
		$sql .= " WHERE A.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$sql .= " AND B.CONSTRAINT_TYPE IN $cons_types";
	$sql .= " AND B.TABLE_NAME='$table'" if ($table);
	$sql .= " AND B.STATUS='ENABLED' ";
	if ($self->{db_version} !~ /Release 8/) {
		$sql .= $self->exclude_mviews('B.OWNER, B.TABLE_NAME');
	}

	# Get the list of constraints in the specified schema or excluding the list of system schema
	my @tmpparams = ();
	if ($self->{type} ne 'SHOW_REPORT')
	{
		$sql .= $self->limit_to_objects('UKEY|TABLE', 'B.CONSTRAINT_NAME|B.TABLE_NAME');
		push(@tmpparams, @{$self->{query_bind_params}}) if (defined $self->{query_bind_params});
		$sql .= $self->limit_to_objects('UKEY', 'B.CONSTRAINT_NAME');
		push(@tmpparams, @{$self->{query_bind_params}}) if (defined $self->{query_bind_params});
	}
	$sql .=  " ORDER BY A.POSITION";

	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@tmpparams) or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch)
	{
		my $name = $row->[9];
		if (!$self->{schema} && $self->{export_schema})
		{
			$name = "$row->[10].$row->[9]";
		}
		if (!exists $result{$name}{$row->[4]})
		{
			$result{$name}{$row->[4]} = { (type => $row->[5], 'generated' => $row->[8], 'index_name' => $row->[11], 'deferrable' => $row->[6], 'deferred' => $row->[7], columns => ()) };
			push(@{ $result{$name}{$row->[4]}->{columns} }, $row->[0]) if ($row->[4] !~ /^SYS_NC/i);
		}
		elsif ($row->[4] !~ /^SYS_NC/i)
		{
			push(@{ $result{$name}{$row->[4]}->{columns} }, $row->[0]);
		}
	}
	return %result;
}

sub _check_constraint
{
	my ($self, $table, $owner) = @_;

	my $condition = '';
	$condition .= "AND TABLE_NAME='$table' " if ($table);
	if ($owner) {
		$condition .= "AND OWNER = '$owner' ";
	} else {
		$condition .= "AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	$condition .= $self->limit_to_objects('CKEY|TABLE', 'CONSTRAINT_NAME|TABLE_NAME');

	my $sql = qq{
SELECT A.CONSTRAINT_NAME,A.R_CONSTRAINT_NAME,A.SEARCH_CONDITION,A.DELETE_RULE,A.DEFERRABLE,A.DEFERRED,A.R_OWNER,A.TABLE_NAME,A.OWNER,A.VALIDATED
FROM $self->{prefix}_CONSTRAINTS A
WHERE A.CONSTRAINT_TYPE='C' $condition
AND A.STATUS='ENABLED'
};

	if ($self->{db_version} !~ /Release 8/) {
		$sql .= $self->exclude_mviews('A.OWNER, A.TABLE_NAME');
	}
	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		if ($self->{export_schema} && !$self->{schema}) {
			$row->[7] = "$row->[8].$row->[7]";
		}
		$data{$row->[7]}{constraint}{$row->[0]}{condition} = $row->[2];
		$data{$row->[7]}{constraint}{$row->[0]}{validate}  = $row->[9];
	}

	return %data;
}

sub _get_external_tables
{
	my ($self) = @_;

	# Retrieve all database link from dba_db_links table
	my $str = "SELECT a.*,b.DIRECTORY_PATH,c.LOCATION,a.OWNER FROM $self->{prefix}_EXTERNAL_TABLES a, $self->{prefix}_DIRECTORIES b, $self->{prefix}_EXTERNAL_LOCATIONS c";
	if (!$self->{schema}) {
		$str .= " WHERE a.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE a.OWNER = '$self->{schema}'";
	}
	$str .= " AND a.DEFAULT_DIRECTORY_NAME = b.DIRECTORY_NAME AND a.TABLE_NAME=c.TABLE_NAME AND a.DEFAULT_DIRECTORY_NAME=c.DIRECTORY_NAME AND a.OWNER=c.OWNER";
	$str .= $self->limit_to_objects('TABLE', 'a.TABLE_NAME');
	#$str .= " ORDER BY a.TABLE_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[1] = "$row->[0].$row->[1]";
		}
		$data{$row->[1]}{directory} = $row->[5];
		$data{$row->[1]}{directory_path} = $row->[10];
		if ($data{$row->[1]}{directory_path} =~ /([\/\\])/) {
			$data{$row->[1]}{directory_path} .= $1 if ($data{$row->[1]}{directory_path} !~ /$1$/);
		}
		$data{$row->[1]}{location} = $row->[11];
		$data{$row->[1]}{delimiter} = ',';
		if ($row->[8] =~ /FIELDS TERMINATED BY '(.)'/is) {
			$data{$row->[1]}{delimiter} = $1;
		}
		if ($row->[8] =~ /PREPROCESSOR EXECDIR\s*:\s*'([^']+)'/is) {
			$data{$row->[1]}{program} = $1;
		}
	}
	$sth->finish();

	return %data;
}

sub _get_directory
{
	my ($self) = @_;

	# Retrieve all database link from dba_db_links table
	my $str = "SELECT d.DIRECTORY_NAME, d.DIRECTORY_PATH, d.OWNER, p.GRANTEE, p.PRIVILEGE FROM $self->{prefix}_DIRECTORIES d, $self->{prefix}_TAB_PRIVS p";
	$str .= " WHERE d.DIRECTORY_NAME = p.TABLE_NAME";
	if (!$self->{schema}) {
		$str .= " AND p.GRANTEE NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND p.GRANTEE = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('TABLE', 'd.DIRECTORY_NAME');
	#$str .= " ORDER BY d.DIRECTORY_NAME";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {

		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[2].$row->[0]";
		}
		$data{$row->[0]}{path} = $row->[1];
		if ($row->[1] !~ /\/$/) {
			$data{$row->[0]}{path} .= '/';
		}
		$data{$row->[0]}{grantee}{$row->[3]} .= $row->[4];
	}
	$sth->finish();

	return %data;
}

sub _get_functions
{
	my $self = shift;

	# Retrieve all functions 
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE='FUNCTION'";
	if (!$self->{export_invalid}) {
		$str .= " AND STATUS='VALID'";
	} elsif ($self->{export_invalid} == 2) {
		$str .= " AND STATUS <> 'VALID'";
	}
	if (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('FUNCTION','OBJECT_NAME');
	#$str .= " ORDER BY OBJECT_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %functions = ();
	my @fct_done = ();
	push(@fct_done, @EXCLUDED_FUNCTION);
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
		next if (grep(/^$row->[0]$/i, @fct_done));
		push(@fct_done, $row->[0]);
		$functions{"$row->[0]"}{owner} = $row->[1];
	}
	$sth->finish();

	my $sql = "SELECT NAME,OWNER,TEXT FROM $self->{prefix}_SOURCE";
	if (!$self->{schema}) {
		$sql .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$sql .= " WHERE OWNER = '$self->{schema}'";
	}
	$sql .= " " . $self->limit_to_objects('FUNCTION','NAME');
	$sql .= " ORDER BY OWNER,NAME,LINE";
	$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
		# Fix possible Malformed UTF-8 character
		$row->[2] = encode('UTF-8', $row->[2]) if ($self->{force_plsql_encoding});
		# Remove some bargage when migrating from 8i
		$row->[2] =~ s/\bAUTHID\s+[^\s]+\s+//is;
		if (exists $functions{"$row->[0]"}) {
			$functions{"$row->[0]"}{text} .= $row->[2];
		}
	}

	return \%functions;
}

sub _lookup_function
{
	my ($self, $plsql, $pname) = @_;

	my %fct_detail = ();

	$fct_detail{func_ret_type} = 'OPAQUE';

	# Split data into declarative and code part
	($fct_detail{declare}, $fct_detail{code}) = split(/\bBEGIN\b/i, $plsql, 2);

	return if (!$fct_detail{code});

	@{$fct_detail{param_types}} = ();
	$fct_detail{declare} =~ s/(\b(?:FUNCTION|PROCEDURE)\s+(?:[^\s\(]+))(\s*\%ORA2PG_COMMENT\d+\%\s*)+/$2$1 /is;
	$fct_detail{declare} =~ s/RETURN\%ORA2PG_COMMENT\d+\%/RETURN/is;
	if ( ($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE)\s+([^\s\(]+)\s*(\([^\)]*\))//is) ||
			($fct_detail{declare} =~ s/(.*?)\b(FUNCTION|PROCEDURE)\s+([^\s\(]+)\s+(RETURN|IS|AS)/$4/is) )
	{
		$fct_detail{before} = $1;
		$fct_detail{type} = uc($2);
		$fct_detail{name} = $3;
		$fct_detail{args} = $4;

		$fct_detail{fct_name} = $3;
		$fct_detail{fct_name} =~ s/^[^\.]+\.//;
		$fct_detail{fct_name} =~ s/"//g;

		# When the function comes from a package remove global declaration
		# outside comments. They have already been extracted before.
		if ($pname && $fct_detail{before})
		{
			$self->_remove_comments(\$fct_detail{before});
			my $cmt = '';
			while ($fct_detail{before} =~ s/(\s*\%ORA2PG_COMMENT\d+\%\s*)//is)
			{
				# only keep comment
				$cmt .= $1;
			}
			$fct_detail{before} = $cmt;
		}
		if ($fct_detail{args} =~ /\b(RETURN|IS|AS)\b/is) {
			$fct_detail{args} = '()';
		}
		my $clause = '';
		my $code = '';
		$fct_detail{name} =~ s/"//g;

		$fct_detail{immutable} = 1 if ($fct_detail{declare} =~ s/\bDETERMINISTIC\b//is);
		$fct_detail{setof} = 1 if ($fct_detail{declare} =~ s/\bPIPELINED\b//is);
		$fct_detail{declare} =~ s/\bDEFAULT\b/:=/igs;
		if ($fct_detail{declare} =~ s/(.*?)\bRETURN\s+self\s+AS RESULT IS//is)
		{
			$fct_detail{args} .= $1;
			$fct_detail{hasreturn} = 1;
			$fct_detail{func_ret_type} = 'OPAQUE';
		}
		elsif ($fct_detail{declare} =~ s/(.*?)\bRETURN\s+([^\s]+)//is)
		{
			$fct_detail{args} .= $1;
			$fct_detail{hasreturn} = 1;
			my $ret_typ = $2 || '';
			$ret_typ =~ s/(\%ORA2PG_COMMENT\d+\%)+//i;
			$fct_detail{func_ret_type} = $self->_sql_type($ret_typ) || 'OPAQUE';
		}
		if ($fct_detail{declare} =~ s/(.*?)(USING|AS|IS)(\s+(?!REF\s+))/$3/is)
		{
			$fct_detail{args} .= $1 if (!$fct_detail{hasreturn});
			$clause = $2;
		}
		$fct_detail{args} =~ s/;.*//s;

		$fct_detail{declare} =~ s/\s*AS\%ORA2PG_COMMENT\d+\%//is;
		if ($fct_detail{declare} =~ /LANGUAGE\s+([^\s="'><\!\(\)]+)/is)
		{
			$fct_detail{language} = $1;
			if ($fct_detail{declare} =~ /LIBRARY\s+([^\s="'><\!\(\)]+)/is) {
				$fct_detail{library} = $1;
			}
			if ($fct_detail{declare} =~ /NAME\s+"([^"]+)"/is) {
				$fct_detail{library_fct} = $1;
			}
		}
		# rewrite argument syntax
		# Replace alternate syntax for default value
		$fct_detail{args} =~ s/:=/DEFAULT/igs;
		# NOCOPY not supported
		$fct_detail{args} =~ s/\s*NOCOPY//igs;
		# IN OUT should be INOUT
		$fct_detail{args} =~ s/\bIN\s+OUT/INOUT/igs;
		# Remove %ROWTYPE from arguments, we can use the table name as type
		$fct_detail{args} =~ s/\%ROWTYPE//igs;

		# Replace DEFAULT EMPTY_BLOB() from function/procedure arguments by DEFAULT NULL
		$fct_detail{args} =~ s/\s+DEFAULT\s+EMPTY_[CB]LOB\(\)/DEFAULT NULL/igs;

		# Now convert types
		$fct_detail{args} = Ora2Pg::PLSQL::replace_sql_type($fct_detail{args}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type}, $self->{varchar_to_text}, %{$self->{data_type}});
		$fct_detail{declare} = Ora2Pg::PLSQL::replace_sql_type($fct_detail{declare}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type}, $self->{varchar_to_text}, %{$self->{data_type}});

		# Sometime variable used in FOR ... IN SELECT loop is not declared
		# Append its RECORD declaration in the DECLARE section.
		my $tmp_code = $fct_detail{code};
		while ($tmp_code =~ s/\bFOR\s+([^\s]+)\s+IN(.*?)LOOP//is)
		{
			my $varname = quotemeta($1);
			my $clause = $2;
			if ($fct_detail{declare} !~ /\b$varname\s+/is) {
				chomp($fct_detail{declare});
				# When the cursor is refereing to a statement, declare
				# it as record otherwise it don't need to be replaced
				if ($clause =~ /\bSELECT\b/is) {
					$fct_detail{declare} .= "\n  $varname RECORD;\n";
				}
			}
		}

		# Set parameters for AUTONOMOUS TRANSACTION
		$fct_detail{args} =~ s/\s+/ /gs;
		push(@{$fct_detail{at_args}}, split(/\s*,\s*/, $fct_detail{args}));
		# Remove type parts to only get parameter's name
		push(@{$fct_detail{param_types}}, @{$fct_detail{at_args}});
		map { s/\s(IN|OUT|INOUT)\s/ /i; } @{$fct_detail{at_args}};
		map { s/^\(//; } @{$fct_detail{at_args}};
		map { s/^\s+//; } @{$fct_detail{at_args}};
		map { s/\s.*//; } @{$fct_detail{at_args}};
		map { s/\)$//; } @{$fct_detail{at_args}};
		@{$fct_detail{at_args}} = grep(/^.+$/, @{$fct_detail{at_args}});
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
		$fct_detail{code} = $plsql;
	}

	# PostgreSQL procedure do not support OUT parameter, translate them into INOUT params
	if (!$fct_detail{hasreturn} && $self->{pg_supports_procedure} && ($fct_detail{args} =~ /\bOUT\s+[^,\)]+/i)) {
		$fct_detail{args} =~ s/\bOUT(\s+[^,\)]+)/INOUT$1/igs;
	}

	# Mark the function as having out parameters if any
	my @nout = $fct_detail{args} =~ /\bOUT\s+([^,\)]+)/igs;
	my @ninout = $fct_detail{args} =~ /\bINOUT\s+([^,\)]+)/igs;
	my $nbout = $#nout+1 + $#ninout+1;
	$fct_detail{inout} = 1 if ($nbout > 0);

	# Mark function as having custom type in parameter list
	if ($fct_detail{inout} and $nbout > 1)
	{
		foreach my $t (@{$fct_detail{param_types}})
		{
			# Consider column type reference to never be a composite type this
			# is clearly not right but the false positive case might be very low
			next if ($t =~ /\%TYPE/i || ($t !~ s/^(OUT|INOUT)\s+//i));
			# Mark out parameter as using composite type
			if (!grep(/^\Q$t\E$/i, 'int', 'bigint', 'date',
								values %SQL_TYPE,
								values %ORA2PG_SDO_GTYPE))
			{
				$fct_detail{inout}++;
			}
		}
	}

	# Collect user defined function
	while ($fct_detail{declare} =~ s/\b([^\s]+)\s+EXCEPTION\s*;//)
	{
		my $e = lc($1);
		if (!exists $self->{custom_exception}{$e}) {
			$self->{custom_exception}{$e} = $self->{exception_id}++;
		}
	}
	$fct_detail{declare} =~ s/PRAGMA\s+EXCEPTION_INIT[^;]*;//igs;

	# Replace call to global variables declared in this package
	foreach my $n (keys %{$self->{global_variables}})
	{
		next if (!$n || ($pname && (uc($n) !~ /^\U$pname\E\./)));
		my $tmpname = $n;
		$tmpname =~ s/^$pname\.//i;
		next if ($fct_detail{code} !~ /\b$tmpname\b/is);
		my $i = 0;
		while ($fct_detail{code} =~ s/(SELECT\s+(?:.*?)\s+)INTO\s+$tmpname\s+([^;]+);/PERFORM set_config('$n', ($1$2), false);/is) { last if ($i++ > 100); };
		$i = 0;
		while ($fct_detail{code} =~ s/\b$n\s*:=\s*([^;]+)\s*;/PERFORM set_config('$n', $1, false);/is) { last if ($i++ > 100); };
		$i = 0;
		while ($fct_detail{code} =~ s/([^\.]+)\b$self->{global_variables}{$n}{name}\s*:=\s*([^;]+);/$1PERFORM set_config('$n', $2, false);/is) { last if ($i++ > 100); };
		$i = 0;
		while ($fct_detail{code} =~ s/([^']+)\b$n\s+IS NOT NULL/$1current_setting('$n') != ''/is) { last if ($i++ > 100); };
		$i = 0;
		while ($fct_detail{code} =~ s/([^']+)\b$n\s+IS NULL/$1current_setting('$n') = ''/is) { last if ($i++ > 100); };
		$i = 0;
		while ($fct_detail{code} =~ s/([^']+)\b$n\b([^']+)/$1current_setting('$n')::$self->{global_variables}{$n}{type}$2/is) { last if ($i++ > 100); };
		$i = 0;
		while ($fct_detail{code} =~ s/([^\.']+)\b$self->{global_variables}{$n}{name}\s+IS NOT NULL/$1current_setting('$n') != ''/is) { last if ($i++ > 100); };
		$i = 0;
		while ($fct_detail{code} =~ s/([^\.']+)\b$self->{global_variables}{$n}{name}\s+IS NULL/$1current_setting('$n') = ''/is) { last if ($i++ > 100); };
		$i = 0;
		while ($fct_detail{code} =~ s/([^\.']+)\b$self->{global_variables}{$n}{name}\b([^']+)/$1current_setting('$n')::$self->{global_variables}{$n}{type}$2/is) { last if ($i++ > 100); };

		# Replace global variable in DECLARE section too
		$i = 0;
		while ($fct_detail{declare} =~ s/([^']+)\b$n\b([^']+)/$1current_setting('$n')::$self->{global_variables}{$n}{type}$2/is) { last if ($i++ > 100); };
		$i = 0;
		while ($fct_detail{declare} =~ s/([^\.']+)\b$self->{global_variables}{$n}{name}\b([^']+)/$1current_setting('$n')::$self->{global_variables}{$n}{type}$2/is) { last if ($i++ > 100); };
	}

	# Replace call to raise exception
	foreach my $e (keys %{$self->{custom_exception}})
	{
		$fct_detail{code} =~ s/\bRAISE\s+$e\b/RAISE EXCEPTION '$e' USING ERRCODE = '$self->{custom_exception}{$e}'/igs;
		$fct_detail{code} =~ s/(\s+(?:WHEN|OR)\s+)$e\s+/$1SQLSTATE '$self->{custom_exception}{$e}' /igs;
	}

	# Remove %ROWTYPE from return type
	$fct_detail{func_ret_type} =~ s/\%ROWTYPE//igs;

	return %fct_detail;
}

sub _list_all_funtions
{
	my $self = shift;

	my $oraver = '';
	# OWNER|OBJECT_NAME|PROCEDURE_NAME|OBJECT_TYPE
	my $sql = qq{
SELECT p.owner,p.object_name,p.procedure_name,o.object_type
  FROM $self->{prefix}_PROCEDURES p
  JOIN $self->{prefix}_OBJECTS o ON p.owner = o.owner
   AND p.object_name = o.object_name 
 WHERE o.object_type IN ('PROCEDURE','PACKAGE','FUNCTION')
   AND o.TEMPORARY='N' AND o.GENERATED='N' AND o.SECONDARY='N'
};
	if ($self->{db_version} =~ /Release 8/) {
		$sql = qq{
SELECT p.owner,p.object_name,p.procedure_name,o.object_type
  FROM $self->{prefix}_PROCEDURES p, $self->{prefix}_OBJECTS o
 WHERE o.object_type IN ('PROCEDURE','PACKAGE','FUNCTION')
   AND p.owner = o.owner AND p.object_name = o.object_name
   AND o.TEMPORARY='N' AND o.GENERATED='N' AND o.SECONDARY='N'
};
	}
	if (!$self->{export_invalid}) {
		$sql .= " AND o.STATUS = 'VALID'";
	} elsif ($self->{export_invalid} == 2) {
		$sql .= " AND o.STATUS <> 'VALID'";
	}
	if ($self->{schema}) {
		$sql .= " AND p.OWNER='$self->{schema}'";
	} else {
		$sql .= " AND p.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	my @infos = ();
	my $sth = $self->{dbh}->prepare( $sql ) or return undef;
	$sth->execute or return undef;
	while ( my @row = $sth->fetchrow())
	{
		next if (($row[3] eq 'PACKAGE') && !$row[2]);
		if ( $row[2] )
		{
			# package_name.fct_name
			push(@infos, lc("$row[1].$row[2]"));
		}
		elsif ( $self->{export_schema} )
		{
			# package_name.fct_name
			push(@infos, lc("$row[0].$row[1]"));
		}
		else
		{
			# owner.fct_name
			push(@infos, lc($row[1]));
		}
	}
	$sth->finish();

	return @infos;
}

sub _sql_type
{
        my ($self, $type, $len, $precision, $scale, $default, $no_blob_to_oid) = @_;

	my $data_type = '';

	# Simplify timestamp type
	$type =~ s/TIMESTAMP\(\d+\)/TIMESTAMP/;

	# Interval precision for year/month/day is not supported by PostgreSQL
	if ($type =~ /INTERVAL/)
	{
		$type =~ s/(INTERVAL\s+YEAR)\s*\(\d+\)/$1/;
		$type =~ s/(INTERVAL\s+YEAR\s+TO\s+MONTH)\s*\(\d+\)/$1/;
		$type =~ s/(INTERVAL\s+DAY)\s*\(\d+\)/$1/;
		# maximum precision allowed for seconds is 6
		if ($type =~ /INTERVAL\s+DAY\s+TO\s+SECOND\s*\((\d+)\)/)
		{
			if ($1 > 6) {
				$type =~ s/(INTERVAL\s+DAY\s+TO\s+SECOND)\s*\(\d+\)/$1(6)/;
			}
		}
	}

	# Overide the length
	if ( ($type eq 'NUMBER') && $precision )
	{
		$len = $precision;
		return $self->{data_type}{'NUMBER(*)'} if ($scale eq '0' && exists $self->{data_type}{'NUMBER(*)'});
		return $self->{data_type}{"NUMBER(*,$scale)"} if (exists $self->{data_type}{"NUMBER(*,$scale)"});
	}
	elsif ( ($type eq 'NUMBER') && ($len == 38) )
	{
		if ($scale eq '0' && $precision eq '')
		{
			# Allow custom type rewrite for NUMBER(*,0)
			return $self->{data_type}{'NUMBER(*,0)'} if (exists $self->{data_type}{'NUMBER(*,0)'});
		}
		$precision = $len;
	}
	elsif ( $type =~ /CHAR/ && $len && exists $self->{data_type}{"$type($len)"})
	{
		return $self->{data_type}{"$type($len)"};
	}
	elsif ( $type =~ /RAW/ )
	{
		$self->{use_uuid} = 1 if (($len && exists $self->{data_type}{"$type($len)"}) || ($default =~ /(SYS_GUID|$self->{uuid_function})/i));
		return $self->{data_type}{"$type($len)"} if ($len && exists $self->{data_type}{"$type($len)"});
		return 'uuid' if ($default =~ /(SYS_GUID|$self->{uuid_function})/i);
	}
	elsif ($type =~ /BLOB/ && $self->{blob_to_lo} && !$no_blob_to_oid)
	{
		# we want to convert BLOB into large object
		return 'oid';
	}

	# Special case of * precision
	if ($precision eq '*')
	{
		if ($len ne '*') {
			$precision = $len;
		} else {
			$precision = 38;
		}
	}

	if (exists $self->{data_type}{$type})
	{
		if ($len)
		{
			if ( ($type eq "CHAR") || ($type eq "NCHAR") || ($type =~ /VARCHAR/) )
			{
				# Type CHAR have default length set to 1
				# Type VARCHAR(2) must have a specified length
				$len = 1 if (!$len && (($type eq "CHAR") || ($type eq "NCHAR")) );
				return "$self->{data_type}{$type}($len)";
			}
			elsif ($type eq "NUMBER")
			{
				# This is an integer
				if (!$scale)
				{
					if ($precision)
					{
						if (exists $self->{data_type}{"$type($precision)"}) {
							return $self->{data_type}{"$type($precision)"};
						}
						if ($self->{pg_integer_type})
						{
							if ($precision < 5) {
								return 'smallint';
							} elsif ($precision <= 9) {
								return 'integer'; # The speediest in PG
							} elsif ($precision <= 19) {
								return 'bigint';
							} else {
								return "numeric($precision)";
							}
						}
						return "numeric($precision)";
					}
					elsif ($self->{pg_integer_type})
					{
						# For number without precision default is to use bigint
						# but mark the column for review (#) if it needs to be
						# translated into numeric instead
						my $need_review = '';
						$need_review = '#' if ($self->{type} eq 'SHOW_COLUMN');
						return $self->{default_numeric} || 'bigint' . $need_review;
					}
				}
				else
				{
					if (exists $self->{data_type}{"$type($precision,$scale)"}) {
						return $self->{data_type}{"$type($precision,$scale)"};
					}
					if ($self->{pg_numeric_type})
					{
						if ($precision eq '') {
							return "decimal(38, $scale)";
						}
						if ($precision >= $scale)
						{
							if ($precision <= 6)
							{
								if ($self->{pg_supports_negative_scale}) {
									return "decimal($precision,$scale)";
								} else {
									return 'real';
								}
							} elsif ($precision <= 15) {
								return 'double precision';
							}
						}
					}
					$precision = 38 if ($precision eq '');
					if ($scale > $precision) {
						return "numeric";
					}
					return "decimal($precision,$scale)";
				}
			}
			return "$self->{data_type}{$type}";
		}
		else
		{
			if (($type eq 'NUMBER') && $self->{pg_integer_type}) {
				# For number without precision default is to use bigint
				# but mark the column for review (#) if it needs to be
				# translated into numeric instead
				my $need_review = '';
				$need_review = '#' if ($self->{type} eq 'SHOW_COLUMN');
				return $self->{default_numeric} . $need_review;
			} else {
				return $self->{data_type}{$type};
			}
		}
	}

	return $type;
}

sub _get_job
{
	my($self) = @_;

	# Jobs appears in version 10 only
	return if ($self->{db_version} =~ /Release [8|9]/);

	# Retrieve all database job from user_jobs table
	my $str = "SELECT JOB,WHAT,INTERVAL,SCHEMA_USER FROM $self->{prefix}_JOBS";
	if (!$self->{schema}) {
		$str .= " WHERE SCHEMA_USER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE SCHEMA_USER = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('JOB', 'JOB');
	#$str .= " ORDER BY JOB";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[3].$row->[0]";
		}
		$data{$row->[0]}{what} = $row->[1];
		$data{$row->[0]}{interval} = $row->[2];
	}

	# Retrieve all database jobs from view [ALL|DBA]_SCHEDULER_JOBS
	$str = "SELECT job_name AS JOB, job_action AS WHAT, repeat_interval AS INTERVAL, owner AS SCHEMA_USER";
	$str .= " FROM $self->{prefix}_SCHEDULER_JOBS";
	$str .= " WHERE repeat_interval IS NOT NULL";
	$str .= " AND client_id IS NULL";
	if (!$self->{schema}) {
		$str .= " AND owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND owner = '$self->{schema}'";
	}
	$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[3].$row->[0]";
		}
		$data{$row->[0]}{what} = $row->[1];
		$data{$row->[0]}{interval} = $row->[2];
	}

	return %data;
}

sub _get_dblink
{
	my($self) = @_;

	# Retrieve all database link from dba_db_links table
	my $str = "SELECT OWNER,DB_LINK,USERNAME,HOST FROM $self->{prefix}_DB_LINKS";
	if (!$self->{schema}) {
		$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE OWNER = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('DBLINK', 'DB_LINK');
	#$str .= " ORDER BY DB_LINK";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[1] = "$row->[0].$row->[1]";
		}
		$data{$row->[1]}{owner} = $row->[0];
		$data{$row->[1]}{user} = $row->[2];
		$data{$row->[1]}{username} = $self->{pg_user} || $row->[2];
		$data{$row->[1]}{host} = $row->[3];
	}

	return %data;
}

=head2 _get_partitions

This function implements an MySQL-native partitions information.
Return two hash ref with partition details and partition default.
=cut

sub _get_partitions
{
	my ($self) = @_;

	my $highvalue = 'A.HIGH_VALUE';
	if ($self->{db_version} =~ /Release 8/) {
		$highvalue = "'' AS HIGH_VALUE";
	}
	my $condition = '';
	if ($self->{schema}) {
		$condition .= "AND A.TABLE_OWNER='$self->{schema}' ";
	} else {
		$condition .= " AND A.TABLE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	# Retrieve all partitions.
	my $str = qq{
SELECT
	A.TABLE_NAME,
	A.PARTITION_POSITION,
	A.PARTITION_NAME,
	$highvalue,
	A.TABLESPACE_NAME,
	B.PARTITIONING_TYPE,
	C.NAME,
	C.COLUMN_NAME,
	C.COLUMN_POSITION,
	A.TABLE_OWNER
FROM $self->{prefix}_TAB_PARTITIONS A, $self->{prefix}_PART_TABLES B, $self->{prefix}_PART_KEY_COLUMNS C
WHERE
	a.table_name = b.table_name AND
	(b.partitioning_type = 'RANGE' OR b.partitioning_type = 'LIST' OR b.partitioning_type = 'HASH')
	AND a.table_name = c.name
	$condition
};

	if ($self->{db_version} !~ /Release 8/) {
		$str .= $self->exclude_mviews('A.TABLE_OWNER, A.TABLE_NAME');
	}
	$str .= $self->limit_to_objects('TABLE|PARTITION', 'A.TABLE_NAME|A.PARTITION_NAME');

	if ($self->{prefix} ne 'USER')
	{
		if ($self->{schema}) {
			$str .= "\tAND A.TABLE_OWNER ='$self->{schema}' AND B.OWNER=A.TABLE_OWNER AND C.OWNER=A.TABLE_OWNER\n";
		} else {
			$str .= "\tAND A.TABLE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') AND B.OWNER=A.TABLE_OWNER AND C.OWNER=A.TABLE_OWNER\n";
		}
	}
	$str .= "ORDER BY A.TABLE_OWNER,A.TABLE_NAME,A.PARTITION_POSITION,C.COLUMN_POSITION\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	my %default = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[9].$row->[0]";
		}
		if ( ($row->[3] eq 'DEFAULT'))
		{
			$default{$row->[0]} = $row->[2];
			next;
		}
		$parts{$row->[0]}{$row->[1]}{name} = $row->[2];
		push(@{$parts{$row->[0]}{$row->[1]}{info}}, { 'type' => $row->[5], 'value' => $row->[3], 'column' => $row->[7], 'colpos' => $row->[8], 'tablespace' => $row->[4], 'owner' => $row->[9]});
	}
	$sth->finish;

	return \%parts, \%default;
}

=head2 _get_subpartitions

This function implements a MySQL subpartitions information.
Return two hash ref with partition details and partition default.
=cut

sub _get_subpartitions
{
	my($self) = @_;

	my $highvalue = 'A.HIGH_VALUE';
	if ($self->{db_version} =~ /Release [89]/) {
		$highvalue = "'' AS HIGH_VALUE";
	}
	my $condition = '';
	if ($self->{schema}) {
		$condition .= "AND A.TABLE_OWNER='$self->{schema}' ";
	} else {
		$condition .= " AND A.TABLE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	# Retrieve all partitions.
	my $str = qq{
SELECT
	A.TABLE_NAME,
	A.SUBPARTITION_POSITION,
	A.SUBPARTITION_NAME,
	$highvalue,
	A.TABLESPACE_NAME,
	B.SUBPARTITIONING_TYPE,
	C.NAME,
	C.COLUMN_NAME,
	C.COLUMN_POSITION,
	A.TABLE_OWNER,
	A.PARTITION_NAME
FROM $self->{prefix}_tab_subpartitions A, $self->{prefix}_part_tables B, $self->{prefix}_subpart_key_columns C
WHERE
	a.table_name = b.table_name AND
	(b.subpartitioning_type = 'RANGE' OR b.subpartitioning_type = 'LIST' OR b.subpartitioning_type = 'HASH')
	AND a.table_name = c.name
	$condition
};
	$str .= $self->limit_to_objects('TABLE|PARTITION', 'A.TABLE_NAME|A.SUBPARTITION_NAME');

	if ($self->{prefix} ne 'USER') {
		if ($self->{schema}) {
			$str .= "\tAND A.TABLE_OWNER ='$self->{schema}' AND B.OWNER=A.TABLE_OWNER AND C.OWNER=A.TABLE_OWNER\n";
		} else {
			$str .= "\tAND A.TABLE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') AND B.OWNER=A.TABLE_OWNER AND C.OWNER=A.TABLE_OWNER\n";
		}
	}
	if ($self->{db_version} !~ /Release 8/) {
		$str .= $self->exclude_mviews('A.TABLE_OWNER, A.TABLE_NAME');
	}
	$str .= "ORDER BY A.TABLE_OWNER,A.TABLE_NAME,A.PARTITION_NAME,A.SUBPARTITION_POSITION,C.COLUMN_POSITION\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %subparts = ();
	my %default = ();
	while (my $row = $sth->fetch) {
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[9].$row->[0]";
		}
		if ( ($row->[3] eq 'MAXVALUE') || ($row->[3] eq 'DEFAULT')) {
			$default{$row->[0]}{$row->[10]} = $row->[2];
			next;
		}

		$subparts{$row->[0]}{$row->[10]}{$row->[1]}{name} = $row->[2];
		push(@{$subparts{$row->[0]}{$row->[10]}{$row->[1]}{info}}, { 'type' => $row->[5], 'value' => $row->[3], 'column' => $row->[7], 'colpos' => $row->[8], 'tablespace' => $row->[4], 'owner' => $row->[9]});
	}
	$sth->finish;

	return \%subparts, \%default;
}

=head2 _get_partitions_type

This function implements a MySQL-native partitions information.
Return a hash of the partition table_name => type

=cut

sub _get_partitions_type
{
	my ($self) = @_;

	my $highvalue = 'A.HIGH_VALUE';
	if ($self->{db_version} =~ /Release [89]/) {
		$highvalue = "'' AS HIGH_VALUE";
	}
	my $condition = '';
	if ($self->{schema}) {
		$condition .= "AND A.TABLE_OWNER='$self->{schema}' ";
	} else {
		$condition .= " AND A.TABLE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	# Retrieve all partitions.
	my $str = qq{
SELECT
	A.TABLE_NAME,
	A.PARTITION_POSITION,
	A.PARTITION_NAME,
	$highvalue,
	A.TABLESPACE_NAME,
	B.PARTITIONING_TYPE,
	A.TABLE_OWNER
FROM $self->{prefix}_TAB_PARTITIONS A, $self->{prefix}_PART_TABLES B
WHERE A.TABLE_NAME = B.TABLE_NAME
$condition
};
	if ($self->{db_version} !~ /Release 8/) {
		$str .= $self->exclude_mviews('A.TABLE_OWNER, A.TABLE_NAME');
	}
	$str .= $self->limit_to_objects('TABLE|PARTITION','A.TABLE_NAME|A.PARTITION_NAME');

	if ($self->{prefix} ne 'USER') {
		if ($self->{schema}) {
			$str .= "\tAND A.TABLE_OWNER ='$self->{schema}'\n";
		} else {
			$str .= "\tAND A.TABLE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')\n";
		}
	}
	#$str .= "ORDER BY A.TABLE_NAME\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch) {
		$parts{$row->[5]}++;
	}
	$sth->finish;

	return %parts;
}

=head2 _get_partitioned_table

Return a hash of the partitioned table with the number of partition

=cut

sub _get_partitioned_table
{
	my($self, %subpart) = @_;

	my $highvalue = 'A.HIGH_VALUE';
	if ($self->{db_version} =~ /Release [89]/) {
		$highvalue = "'' AS HIGH_VALUE";
	}
	my $condition = '';
	if ($self->{schema}) {
		$condition .= "AND B.OWNER='$self->{schema}' ";
	} else {
		$condition .= " AND B.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	# Retrieve all partitions.
	my $str = "SELECT B.TABLE_NAME, B.PARTITIONING_TYPE, B.OWNER, B.PARTITION_COUNT, B.SUBPARTITIONING_TYPE";
	if ($self->{type} !~ /SHOW|TEST/)
	{
		$str .= ", C.COLUMN_NAME, C.COLUMN_POSITION";
		$str .= " FROM $self->{prefix}_PART_TABLES B, $self->{prefix}_PART_KEY_COLUMNS C";
		$str .= " WHERE B.TABLE_NAME = C.NAME AND (B.PARTITIONING_TYPE = 'RANGE' OR B.PARTITIONING_TYPE = 'LIST' OR B.PARTITIONING_TYPE = 'HASH')";
	}
	else
	{
		$str .= " FROM $self->{prefix}_PART_TABLES B WHERE (B.PARTITIONING_TYPE = 'RANGE' OR B.PARTITIONING_TYPE = 'LIST' OR B.PARTITIONING_TYPE = 'HASH') AND B.SUBPARTITIONING_TYPE <> 'SYSTEM' ";
	}
	$str .= $self->limit_to_objects('TABLE','B.TABLE_NAME');

	if ($self->{prefix} ne 'USER')
	{
		if ($self->{type} !~ /SHOW|TEST/)
		{
			if ($self->{schema}) {
				$str .= "\tAND B.OWNER ='$self->{schema}' AND C.OWNER=B.OWNER\n";
			} else {
				$str .= "\tAND B.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') AND B.OWNER=C.OWNER\n";
			}
		} else {
			if ($self->{schema}) {
				$str .= "\tAND B.OWNER ='$self->{schema}'\n";
			} else {
				$str .= "\tAND B.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')\n";
			}
		}
	}
	if ($self->{db_version} !~ /Release 8/) {
		$str .= $self->exclude_mviews('B.OWNER, B.TABLE_NAME');
	}
	if ($self->{type} !~ /SHOW|TEST/) {
		$str .= "ORDER BY B.OWNER,B.TABLE_NAME,C.COLUMN_POSITION\n";
	} else {
		$str .= "ORDER BY B.OWNER,B.TABLE_NAME\n";
	}

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[2].$row->[0]";
		}
		# when this is not a composite partition the count is defined
		# when this is not the default number of subpartition
		$parts{"\L$row->[0]\E"}{count} = 0;
		$parts{"\L$row->[0]\E"}{composite} = 0;
		if (exists $subpart{"\L$row->[0]\E"})
		{
			$parts{"\L$row->[0]\E"}{composite} = 1;
			foreach my $k (keys %{$subpart{"\L$row->[0]\E"}}) {
				$parts{"\L$row->[0]\E"}{count} += $subpart{"\L$row->[0]\E"}{$k}{count};
			}
			$parts{"\L$row->[0]\E"}{count} = $row->[3] if (!$parts{"\L$row->[0]\E"}{count});
		} else {
			$parts{"\L$row->[0]\E"}{count} = $row->[3];
		}
		$parts{"\L$row->[0]\E"}{type} = $row->[1];
		if ($self->{type} !~ /SHOW|TEST/) {
			push(@{ $parts{"\L$row->[0]\E"}{columns} }, $row->[5]);
		}
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

	my $temporary = "TEMPORARY='N'";
	if ($self->{export_gtt}) {
		$temporary = "(TEMPORARY='N' OR OBJECT_TYPE='TABLE')";
	}
	my $oraver = '';
	# OWNER|OBJECT_NAME|SUBOBJECT_NAME|OBJECT_ID|DATA_OBJECT_ID|OBJECT_TYPE|CREATED|LAST_DDL_TIME|TIMESTAMP|STATUS|TEMPORARY|GENERATED|SECONDARY
	my $sql = "SELECT OBJECT_NAME,OBJECT_TYPE,STATUS FROM $self->{prefix}_OBJECTS WHERE $temporary AND GENERATED='N' AND SECONDARY='N' AND OBJECT_TYPE <> 'SYNONYM'";
	if ($self->{schema}) {
		$sql .= " AND OWNER='$self->{schema}'";
	} else {
		$sql .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	my %infos = ();
	my $sth = $self->{dbh}->prepare( $sql ) or return undef;
	$sth->execute or return undef;
	my %count = ();
	while ( my @row = $sth->fetchrow())
	{
		my $valid = ($row[2] eq 'VALID') ? 0 : 1;
		push(@{$infos{$row[1]}}, { ( name => $row[0], invalid => $valid ) });
		$count{$row[1]}{$valid}++;
	}
	$sth->finish();

	if ($self->{debug})
	{
		foreach my $k (sort keys %count)
		{
			print STDERR "\tFound $count{$k}{0} valid and ", ($count{$k}{1}||0), " invalid object $k\n";
		}
	}

	return %infos;
}

sub _get_privilege
{
	my($self) = @_;

	my %privs = ();
	my %roles = ();

	# Retrieve all privilege per table defined in this database
	my $str = "SELECT b.GRANTEE,b.OWNER,b.TABLE_NAME,b.PRIVILEGE,a.OBJECT_TYPE,b.GRANTABLE FROM DBA_TAB_PRIVS b, DBA_OBJECTS a";
	if ($self->{schema}) {
		$str .= " WHERE b.GRANTOR = '$self->{schema}'";
	} else {
		$str .= " WHERE b.GRANTOR NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$str .= " AND b.TABLE_NAME=a.OBJECT_NAME AND a.OWNER=b.GRANTOR";
	if ($self->{grant_object} && $self->{grant_object} ne 'USER') {
		$str .= " AND a.OBJECT_TYPE = '\U$self->{grant_object}\E'";
	} else {
		$str .= " AND a.OBJECT_TYPE <> 'TYPE'";
	}
	$str .= " " . $self->limit_to_objects('GRANT|TABLE|VIEW|FUNCTION|PROCEDURE|SEQUENCE', 'b.GRANTEE|b.TABLE_NAME|b.TABLE_NAME|b.TABLE_NAME|b.TABLE_NAME|b.TABLE_NAME');

	if (!$self->{export_invalid}) {
		$str .= " AND a.STATUS='VALID'";
	} elsif ($self->{export_invalid} == 2) {
		$str .= " AND a.STATUS <> 'VALID'";
	}
	#$str .= " ORDER BY b.TABLE_NAME, b.GRANTEE";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		next if ($row->[0] eq 'PUBLIC');
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[2] = "$row->[1].$row->[2]";
		}
		$privs{$row->[2]}{type} = $row->[4];
		$privs{$row->[2]}{owner} = $row->[1] if (!$privs{$row->[2]}{owner});
		if ($row->[5] eq 'YES') {
			$privs{$row->[2]}{grantable} = $row->[5];
		}
		push(@{$privs{$row->[2]}{privilege}{$row->[0]}}, $row->[3]);
		push(@{$roles{owner}}, $row->[1]) if (!grep(/^$row->[1]$/, @{$roles{owner}}));
		push(@{$roles{grantee}}, $row->[0]) if (!grep(/^$row->[0]$/, @{$roles{grantee}}));
	}
	$sth->finish();

	# Retrieve all privilege per column table defined in this database
	$str = "SELECT b.GRANTEE,b.OWNER,b.TABLE_NAME,b.PRIVILEGE,b.COLUMN_NAME FROM DBA_COL_PRIVS b, DBA_OBJECTS a";
	if ($self->{schema}) {
		$str .= " WHERE b.GRANTOR = '$self->{schema}'";
	} else {
		$str .= " WHERE b.GRANTOR NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	if (!$self->{export_invalid}) {
		$str .= " AND a.STATUS='VALID'";
	} elsif ($self->{export_invalid} == 2) {
		$str .= " AND a.STATUS <> 'VALID'";
	}
	$str .= " AND b.TABLE_NAME=a.OBJECT_NAME AND a.OWNER=b.GRANTOR AND a.OBJECT_TYPE <> 'TYPE'";
	if ($self->{grant_object} && $self->{grant_object} ne 'USER') {
		$str .= " AND a.OBJECT_TYPE = '\U$self->{grant_object}\E'";
	} else {
		$str .= " AND a.OBJECT_TYPE <> 'TYPE'";
	}
	$str .= " " . $self->limit_to_objects('GRANT|TABLE|VIEW|FUNCTION|PROCEDURE|SEQUENCE', 'b.GRANTEE|b.TABLE_NAME|b.TABLE_NAME|b.TABLE_NAME|b.TABLE_NAME|b.TABLE_NAME');

	$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[2] = "$row->[1].$row->[2]";
		}
		$privs{$row->[2]}{owner} = $row->[1] if (!$privs{$row->[2]}{owner});
		push(@{$privs{$row->[2]}{column}{$row->[4]}{$row->[0]}}, $row->[3]);
		push(@{$roles{owner}}, $row->[1]) if (!grep(/^$row->[1]$/, @{$roles{owner}}));
		push(@{$roles{grantee}}, $row->[0]) if (!grep(/^$row->[0]$/, @{$roles{grantee}}));
	}
	$sth->finish();

	# Search if users have admin rights
	my @done = ();
	foreach my $r (@{$roles{owner}}, @{$roles{grantee}}) {
		next if (grep(/^$r$/, @done));
		push(@done, $r);
		# Get all system priviledge given to a role
		$str = "SELECT PRIVILEGE,ADMIN_OPTION FROM DBA_SYS_PRIVS WHERE GRANTEE = '$r'";
		$str .= " " . $self->limit_to_objects('GRANT', 'GRANTEE');
		#$str .= " ORDER BY PRIVILEGE";
		$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			push(@{$roles{admin}{$r}{privilege}}, $row->[0]);
			push(@{$roles{admin}{$r}{admin_option}}, $row->[1]);
		}
		$sth->finish();
	}
	# Now try to find if it's a user or a role 
	foreach my $u (@done) {
		$str = "SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE = '$u'";
		$str .= " " . $self->limit_to_objects('GRANT', 'GRANTEE');
		#$str .= " ORDER BY GRANTED_ROLE";
		$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			push(@{$roles{role}{$u}}, $row->[0]);
		}
		$str = "SELECT USERNAME FROM DBA_USERS WHERE USERNAME = '$u'";
		$str .= " " . $self->limit_to_objects('GRANT', 'USERNAME');
		#$str .= " ORDER BY USERNAME";
		$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			$roles{type}{$u} = 'USER';
		}
		next if  $roles{type}{$u};
		$str = "SELECT ROLE,PASSWORD_REQUIRED FROM DBA_ROLES WHERE ROLE='$u'";
		$str .= " " . $self->limit_to_objects('GRANT', 'ROLE');
		#$str .= " ORDER BY ROLE";
		$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			$roles{type}{$u} = 'ROLE';
			$roles{password_required}{$u} = $row->[1];
		}
		$sth->finish();
	}

	return (\%privs, \%roles);
}

=head2 _get_database_size

This function retrieves the size of the MySQL database in MB

=cut

sub _get_database_size
{
	my $self = shift;

	my $mb_size = '';
	my $sql = "SELECT sum(bytes)/1024/1024 FROM USER_SEGMENTS";
	if (!$self->{user_grants}) {
		$sql = "SELECT sum(bytes)/1024/1024 FROM DBA_SEGMENTS";
		if ($self->{schema}) {
			$sql .= " WHERE OWNER='$self->{schema}' ";
		} else {
			$sql .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
		}
	}
	my $sth = $self->{dbh}->prepare( $sql ) or return undef;
	$sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$mb_size = sprintf("%.2f MB", $row[0]);
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

        my $prefix = 'USER';
        my $owner_segment = '';
        $owner_segment = " AND A.OWNER='$self->{schema}'";
        if (!$self->{user_grants}) {
                $prefix = 'DBA';
                $owner_segment = ' AND S.OWNER=A.OWNER';
        }

        my $sql = "SELECT * FROM ( SELECT S.SEGMENT_NAME, ROUND(S.BYTES/1024/1024) SIZE_MB FROM ${prefix}_SEGMENTS S JOIN $self->{prefix}_TABLES A ON (S.SEGMENT_NAME=A.TABLE_NAME$owner_segment) WHERE S.SEGMENT_TYPE LIKE 'TABLE%' AND A.SECONDARY = 'N'";
        if ($self->{db_version} =~ /Release 8/) {
                $sql = "SELECT * FROM ( SELECT A.SEGMENT_NAME, ROUND(A.BYTES/1024/1024) SIZE_MB FROM ${prefix}_SEGMENTS A WHERE A.SEGMENT_TYPE LIKE 'TABLE%'";
        }
        if ($self->{db_version} !~ /Release 8/ || !$self->{user_grants}) {
                if ($self->{schema}) {
                        $sql .= " AND A.OWNER='$self->{schema}'";
                } else {
                        $sql .= " AND A.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
                }
        }
        if ($self->{db_version} =~ /Release 8/) {
                $sql .= $self->limit_to_objects('TABLE', 'A.SEGMENT_NAME');
        } else {
                $sql .= $self->limit_to_objects('TABLE', 'A.TABLE_NAME');
        }

        if ($self->{db_version} =~ /Release 8/) {
                $sql .= " ORDER BY A.BYTES DESC, A.SEGMENT_NAME ASC) WHERE ROWNUM <= $self->{top_max}";
        } else {
                $sql .= " ORDER BY S.BYTES DESC, S.SEGMENT_NAME ASC) WHERE ROWNUM <= $self->{top_max}";
        }

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

	my @users = ();
	push(@users, split(/[,;\s]/, uc($self->{audit_user})));

	# Retrieve all object with tablespaces.
	my $str = "SELECT SQL_TEXT FROM DBA_AUDIT_TRAIL WHERE ACTION_NAME IN ('INSERT','UPDATE','DELETE','SELECT')";
	if (($#users >= 0) && !grep(/^ALL$/, @users)) {
		$str .= " AND USERNAME IN ('" . join("','", @users) . "')";
	}
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %tmp_queries = ();
	while (my $row = $sth->fetch)
	{
		$self->_remove_comments(\$row->[0], 1);
		$self->{comment_values} = ();
		$row->[0] =~  s/\%ORA2PG_COMMENT\d+\%//gs;
		$row->[0] =  $self->normalize_query($row->[0]);
		$tmp_queries{$row->[0]}++;
	}
	$sth->finish;

	my %queries = ();
	my $i = 1;
	foreach my $q (keys %tmp_queries)
	{
		$queries{$i} = $q;
		$i++;
	}

	return %queries;
}

sub _get_synonyms
{
	my ($self) = shift;

	# Retrieve all synonym
	my $str = "SELECT OWNER,SYNONYM_NAME,TABLE_OWNER,TABLE_NAME,DB_LINK FROM $self->{prefix}_SYNONYMS";
	if ($self->{schema}) {
		$str .= " WHERE owner='$self->{schema}' AND table_owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	} else {
		$str .= " WHERE owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "') AND table_owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	$str .= $self->limit_to_objects('SYNONYM','SYNONYM_NAME');
	#$str .= " ORDER BY SYNONYM_NAME\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %synonyms = ();
	while (my $row = $sth->fetch) {
		next if ($row->[1] =~ /^\//); # Some not fully deleted synonym start with a slash
		$synonyms{$row->[1]}{owner} = $row->[0];
		$synonyms{$row->[1]}{table_owner} = $row->[2];
		$synonyms{$row->[1]}{table_name} = $row->[3];
		$synonyms{$row->[1]}{dblink} = $row->[4];
	}
	$sth->finish;

	return %synonyms;
}

sub _get_tablespaces
{
	my ($self) = shift;

	# Retrieve all object with tablespaces.
my $str = qq{
SELECT a.SEGMENT_NAME,a.TABLESPACE_NAME,a.SEGMENT_TYPE,c.FILE_NAME, a.OWNER
FROM DBA_SEGMENTS a, $self->{prefix}_OBJECTS b, DBA_DATA_FILES c
WHERE a.SEGMENT_TYPE IN ('INDEX', 'TABLE', 'INDEX PARTITION', 'TABLE PARTITION')
AND a.SEGMENT_NAME = b.OBJECT_NAME
AND a.SEGMENT_TYPE = b.OBJECT_TYPE
AND a.OWNER = b.OWNER
AND a.TABLESPACE_NAME = c.TABLESPACE_NAME
};
	if ($self->{schema}) {
		$str .= " AND a.OWNER='$self->{schema}'";
	} else {
		$str .= " AND a.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$str .= $self->limit_to_objects('TABLESPACE|TABLE', 'a.TABLESPACE_NAME|a.SEGMENT_NAME');
	#$str .= " ORDER BY TABLESPACE_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %tbs = ();
	while (my $row = $sth->fetch) {
		# TYPE - TABLESPACE_NAME - FILEPATH - OBJECT_NAME
		if ($self->{export_schema} && !$self->{schema}) {
			$row->[0] = "$row->[4].$row->[0]";
		}
		push(@{$tbs{$row->[2]}{$row->[1]}{$row->[3]}}, $row->[0]);
	}
	$sth->finish;

	return \%tbs;
}

sub _list_tablespaces
{
	my ($self) = shift;

	# list tablespaces.
	my $str = qq{
SELECT c.FILE_NAME, c.TABLESPACE_NAME, a.OWNER, ROUND(c.BYTES/1024000) MB
FROM DBA_DATA_FILES c, DBA_SEGMENTS a
WHERE a.TABLESPACE_NAME = c.TABLESPACE_NAME
};
	if ($self->{schema}) {
		$str .= " AND a.OWNER='$self->{schema}'";
	} else {
		$str .= " AND a.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$str .= $self->limit_to_objects('TABLESPACE', 'c.TABLESPACE_NAME');
	#$str .= " ORDER BY c.TABLESPACE_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %tbs = ();
	while (my $row = $sth->fetch) {
		$tbs{$row->[1]}{path} = $row->[0];
		$tbs{$row->[1]}{owner} = $row->[2];
	}
	$sth->finish;

	return \%tbs;
}

sub _get_sequences
{
	my ($self) = shift;

	# Retrieve all indexes 
	my $str = "SELECT DISTINCT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, LAST_NUMBER, CACHE_SIZE, CYCLE_FLAG, SEQUENCE_OWNER FROM $self->{prefix}_SEQUENCES";
	if (!$self->{schema}) {
		$str .= " WHERE SEQUENCE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE SEQUENCE_OWNER = '$self->{schema}'";
	}
	# Exclude sequence used for IDENTITY columns
	$str .= " AND SEQUENCE_NAME NOT LIKE 'ISEQ\$\$_%'";
	$str .= $self->limit_to_objects('SEQUENCE', 'SEQUENCE_NAME');
	#$str .= " ORDER BY SEQUENCE_NAME";


	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %seqs = ();
	while (my $row = $sth->fetch)
	{
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

	my $sql = "SELECT DISTINCT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, CYCLE_FLAG, ORDER_FLAG, CACHE_SIZE, LAST_NUMBER,SEQUENCE_OWNER FROM $self->{prefix}_SEQUENCES";
	if ($self->{schema}) {
		$sql .= " WHERE SEQUENCE_OWNER='$self->{schema}'";
	} else {
		$sql .= " WHERE SEQUENCE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$sql .= $self->limit_to_objects('SEQUENCE','SEQUENCE_NAME');

	my @script = ();

	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr ."\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $seq_info = $sth->fetchrow_hashref)
	{
		my $seqname = $seq_info->{SEQUENCE_NAME};
		if (!$self->{schema} && $self->{export_schema}) {
			$seqname = $seq_info->{SEQUENCE_OWNER} . '.' . $seq_info->{SEQUENCE_NAME};
		}

		my $nextvalue = $seq_info->{LAST_NUMBER} + $seq_info->{INCREMENT_BY};
		my $alter = "ALTER SEQUENCE $self->{pg_supports_ifexists} " .  $self->quote_object_name($seqname) . " RESTART WITH $nextvalue;";
		push(@script, $alter);
		$self->logit("Extracted sequence information for sequence \"$seqname\"\n", 1);
	}
	$sth->finish();

	return @script;
}

sub _column_attributes
{
	my ($self, $table, $owner, $objtype) = @_;

	$objtype ||= 'TABLE';

	my $condition = '';
	$condition .= "AND A.TABLE_NAME='$table' " if ($table);
	if ($owner) {
		$condition .= "AND A.OWNER='$owner' ";
	} else {
		$condition .= " AND A.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE', 'A.TABLE_NAME');
	} else {
		@{$self->{query_bind_params}} = ();
	}

	my $sth = '';
	if ($self->{db_version} !~ /Release 8/) {
		$sth = $self->{dbh}->prepare(<<END);
SELECT A.COLUMN_NAME, A.NULLABLE, A.DATA_DEFAULT, A.TABLE_NAME, A.OWNER, A.COLUMN_ID, A.DATA_TYPE
FROM $self->{prefix}_TAB_COLUMNS A, $self->{prefix}_OBJECTS O
WHERE A.OWNER=O.OWNER and A.TABLE_NAME=O.OBJECT_NAME and O.OBJECT_TYPE='$objtype'
    $condition
ORDER BY A.COLUMN_ID
END
		if (!$sth) {
			$self->logit("FATAL: _column_attributes() " . $self->{dbh}->errstr . "\n", 0, 1);
		}
	}
	else
	{
		# an 8i database.
		$sth = $self->{dbh}->prepare(<<END);
SELECT A.COLUMN_NAME, A.NULLABLE, A.DATA_DEFAULT, A.TABLE_NAME, A.OWNER, A.COLUMN_ID, A.DATA_TYPE
FROM $self->{prefix}_TAB_COLUMNS A, $self->{prefix}_OBJECTS O
WHERE A.OWNER=O.OWNER and A.TABLE_NAME=O.OBJECT_NAME and O.OBJECT_TYPE='$objtype'
    $condition
ORDER BY A.COLUMN_ID
END
		if (!$sth) {
			$self->logit("FATAL: _column_attributes() " . $self->{dbh}->errstr . "\n", 0, 1);
		}
	}
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: _column_attributes() " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch)
	{
		my $spatial_srid = 0;
		if ($self->{export_schema} && !$self->{schema})
		{
			$data{"$row->[4].$row->[3]"}{"$row->[0]"}{nullable} = $row->[1];
			$data{"$row->[4].$row->[3]"}{"$row->[0]"}{default} = $row->[2];
			# Store the data type of the column following its position
			$data{"$row->[4].$row->[3]"}{data_type}{$row->[5]} = $row->[6];
		}
		else
		{
			$data{$row->[3]}{"$row->[0]"}{nullable} = $row->[1];
			$data{$row->[3]}{"$row->[0]"}{default} = $row->[2];
			# Store the data type of the column following its position
			$data{$row->[3]}{data_type}{$row->[5]} = $row->[6];
		}
		my $f = $self->{tables}{"$table"}{column_info}{"$row->[0]"};
		if ( ($f->[1] =~ /SDO_GEOMETRY/i) && ($self->{convert_srid} <= 1) )
		{
			$spatial_srid = "SELECT COALESCE(SRID, $self->{default_srid}) FROM ALL_SDO_GEOM_METADATA WHERE TABLE_NAME='\U$table\E' AND COLUMN_NAME='$row->[0]' AND OWNER='\U$self->{tables}{$table}{table_info}{owner}\E'";
			if ($self->{convert_srid} == 1) {
				$spatial_srid = "SELECT COALESCE(sdo_cs.map_oracle_srid_to_epsg(SRID), $self->{default_srid}) FROM ALL_SDO_GEOM_METADATA WHERE TABLE_NAME='\U$table\E' AND COLUMN_NAME='$row->[0]' AND OWNER='\U$self->{tables}{$table}{table_info}{owner}\E'";
			}
			my $sth2 = $self->{dbh}->prepare($spatial_srid);
			if (!$sth2)
			{
				if ($self->{dbh}->errstr !~ /ORA-01741/) {
					$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
				}
				else
				{
					# No SRID defined, use default one
					$spatial_srid = $self->{default_srid} || '0';
					$self->logit("WARNING: Error retreiving SRID, no matter default SRID will be used: $spatial_srid\n", 0);
				}
			}
			else
			{
				$sth2->execute() or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
				my @result = ();
				while (my $r = $sth2->fetch) {
					push(@result, $r->[0]) if ($r->[0] =~ /\d+/);
				}
				$sth2->finish();
				if ($self->{export_schema} && !$self->{schema}) {
					  $data{"$row->[4].$row->[3]"}{"$row->[0]"}{spatial_srid} = $result[0] || $self->{default_srid} || '0';
				} else {
					  $data{$row->[3]}{"$row->[0]"}{spatial_srid} = $result[0] || $self->{default_srid} || '0';
				}
			}
		}
	}

	return %data;
}

sub _list_triggers
{
	my($self) = @_;

	# Retrieve all indexes 
	my $str = "SELECT TRIGGER_NAME, TABLE_NAME, OWNER FROM $self->{prefix}_TRIGGERS WHERE STATUS='ENABLED'";
	if (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('TABLE|VIEW|TRIGGER','TABLE_NAME|TABLE_NAME|TRIGGER_NAME');

	#$str .= " ORDER BY TABLE_NAME, TRIGGER_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %triggers = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			push(@{$triggers{"$row->[2].$row->[1]"}}, $row->[0]);
		} else {
			push(@{$triggers{$row->[1]}}, $row->[0]);
		}
	}

	return %triggers;
}

sub _global_temp_table_info
{
	my($self) = @_;

	my $owner = '';
	if ($self->{schema}) {
		$owner .= "AND A.OWNER='$self->{schema}' ";
	} else {
	    $owner .= "AND A.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}

	# Get comment on global temporary table
	my %comments = ();
	if ($self->{type} eq 'TABLE')
	{
		my $sql = "SELECT A.TABLE_NAME,A.COMMENTS,A.TABLE_TYPE,A.OWNER FROM $self->{prefix}_TAB_COMMENTS A, $self->{prefix}_OBJECTS O WHERE A.OWNER=O.OWNER and A.TABLE_NAME=O.OBJECT_NAME and O.OBJECT_TYPE='TABLE' $owner";
		if ($self->{db_version} !~ /Release 8/) {
			$sql .= $self->exclude_mviews('A.OWNER, A.TABLE_NAME');
		}
		$sql .= $self->limit_to_objects('TABLE', 'A.TABLE_NAME');
		my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			if (!$self->{schema} && $self->{export_schema}) {
				$row->[0] = "$row->[3].$row->[0]";
			}
			$comments{$row->[0]}{comment} = $row->[1];
			$comments{$row->[0]}{table_type} = $row->[2];
		}
		$sth->finish();
	}

	my $sql = "SELECT A.OWNER,A.TABLE_NAME,NVL(num_rows,1) NUMBER_ROWS,A.TABLESPACE_NAME,A.NESTED,A.LOGGING,A.DURATION FROM $self->{prefix}_TABLES A, $self->{prefix}_OBJECTS O WHERE A.OWNER=O.OWNER AND A.TABLE_NAME=O.OBJECT_NAME AND O.OBJECT_TYPE='TABLE' $owner";
	$sql .= " AND A.TEMPORARY='Y'";
	if ($self->{db_version} !~ /Release [89]/) {
		$sql .= " AND (A.DROPPED IS NULL OR A.DROPPED = 'NO')";
	}
	$sql .= $self->limit_to_objects('TABLE', 'A.TABLE_NAME');
	$sql .= " AND (A.IOT_TYPE IS NULL OR A.IOT_TYPE = 'IOT')";
	#$sql .= " ORDER BY A.OWNER, A.TABLE_NAME";

	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %tables_infos = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[1] = "$row->[0].$row->[1]";
		}
		$tables_infos{$row->[1]}{owner} = $row->[0] || '';
		$tables_infos{$row->[1]}{num_rows} = $row->[2] || 0;
		$tables_infos{$row->[1]}{tablespace} = $row->[3] || 0;
		$tables_infos{$row->[1]}{comment} =  $comments{$row->[1]}{comment} || '';
		$tables_infos{$row->[1]}{type} =  $comments{$row->[1]}{table_type} || '';
		$tables_infos{$row->[1]}{nested} = $row->[4] || '';
		if ($row->[5] eq 'NO') {
			$tables_infos{$row->[1]}{nologging} = 1;
		} else {
			$tables_infos{$row->[1]}{nologging} = 0;
		}
		$tables_infos{$row->[1]}{num_rows} = 0;
		$tables_infos{$row->[1]}{temporary} = 'Y';
		$tables_infos{$row->[1]}{duration} = $row->[6];
	}
	$sth->finish();

	return %tables_infos;
}

sub _encrypted_columns
{
	my ($self, $table, $owner) = @_;

	# Encryption appears in version 10 only
	return if ($self->{db_version} =~ /Release [8|9]/);

	my $condition = '';
	$condition .= "AND A.TABLE_NAME='$table' " if ($table);
	if ($owner) {
		$condition .= "AND A.OWNER='$owner' ";
	} else {
		$condition .= " AND A.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE', 'A.TABLE_NAME');
	} else {
		@{$self->{query_bind_params}} = ();
	}
	$condition =~ s/^\s*AND /WHERE /s;

	my $sth = $self->{dbh}->prepare(<<END);
SELECT A.COLUMN_NAME, A.TABLE_NAME, A.OWNER, A.ENCRYPTION_ALG
FROM $self->{prefix}_ENCRYPTED_COLUMNS A
$condition
END
	if (!$sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch)
	{
		if ($self->{export_schema} && !$self->{schema}) {
			$data{"$row->[2].$row->[1].$row->[0]"} = $row->[3];
		} else {
			$data{"$row->[1].$row->[0]"} = $row->[3];
		}
	}

	return %data;
}

sub _get_subpartitioned_table
{
	my($self) = @_;

	my $highvalue = 'A.HIGH_VALUE';
	if ($self->{db_version} =~ /Release [89]/) {
		$highvalue = "'' AS HIGH_VALUE";
	}
	my $condition = '';
	if ($self->{schema}) {
		$condition .= "AND A.TABLE_OWNER='$self->{schema}' ";
	} else {
		$condition .= " AND A.TABLE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	# Retrieve all partitions.
	my $str = "SELECT A.TABLE_NAME, A.PARTITION_NAME, A.SUBPARTITION_NAME, A.SUBPARTITION_POSITION, B.SUBPARTITIONING_TYPE, A.TABLE_OWNER, B.PARTITION_COUNT";
	if ($self->{type} !~ /SHOW|TEST/) {
		$str .= ", C.COLUMN_NAME, C.COLUMN_POSITION";
		$str .= " FROM $self->{prefix}_TAB_SUBPARTITIONS A, $self->{prefix}_PART_TABLES B, $self->{prefix}_SUBPART_KEY_COLUMNS C";
	} else {
		$str .= " FROM $self->{prefix}_TAB_SUBPARTITIONS A, $self->{prefix}_PART_TABLES B";
	}
	$str .= " WHERE A.TABLE_NAME = B.TABLE_NAME AND (B.SUBPARTITIONING_TYPE = 'RANGE' OR B.SUBPARTITIONING_TYPE = 'LIST' OR B.SUBPARTITIONING_TYPE = 'HASH')";

	$str .= " AND A.TABLE_NAME = C.NAME" if ($self->{type} !~ /SHOW|TEST/);

	$str .= $self->limit_to_objects('TABLE|PARTITION','A.TABLE_NAME|A.PARTITION_NAME');

	if ($self->{prefix} ne 'USER') {
		if ($self->{type} !~ /SHOW|TEST/) {
			if ($self->{schema}) {
				$str .= "\tAND A.TABLE_OWNER ='$self->{schema}' AND B.OWNER=A.TABLE_OWNER AND C.OWNER=A.TABLE_OWNER\n";
			} else {
				$str .= "\tAND A.TABLE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') AND B.OWNER=A.TABLE_OWNER AND C.OWNER=A.TABLE_OWNER\n";
			}
		} else {
			if ($self->{schema}) {
				$str .= "\tAND A.TABLE_OWNER ='$self->{schema}' AND B.OWNER=A.TABLE_OWNER\n";
			} else {
				$str .= "\tAND A.TABLE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') AND B.OWNER=A.TABLE_OWNER\n";
			}
		}
	}
	if ($self->{db_version} !~ /Release 8/) {
		$str .= $self->exclude_mviews('A.TABLE_OWNER, A.TABLE_NAME');
	}
	if ($self->{type} !~ /SHOW|TEST/) {
		$str .= "ORDER BY A.TABLE_OWNER,A.TABLE_NAME,A.PARTITION_NAME,A.SUBPARTITION_POSITION,C.COLUMN_POSITION\n";
	} else {
		$str .= "ORDER BY A.TABLE_OWNER,A.TABLE_NAME,A.PARTITION_NAME,A.SUBPARTITION_POSITION\n";
	}

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[5].$row->[0]";
		}
		$parts{"\L$row->[0]\E"}{"\L$row->[1]\E"}{type} = $row->[4];
		$parts{"\L$row->[0]\E"}{"\L$row->[1]\E"}{count}++;
		push(@{ $parts{"\L$row->[0]\E"}{"\L$row->[1]\E"}{columns} }, $row->[7]) if (!grep(/^$row->[7]$/, @{ $parts{"\L$row->[0]\E"}{"\L$row->[1]\E"}{columns} }));
	}
	$sth->finish;

	return %parts;
}

sub _get_plsql_metadata
{
	my $self = shift;
	my $owner = shift;

       # Retrieve all functions 
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER,OBJECT_TYPE FROM $self->{prefix}_OBJECTS WHERE (OBJECT_TYPE = 'FUNCTION' OR OBJECT_TYPE = 'PROCEDURE' OR OBJECT_TYPE = 'PACKAGE BODY')";
	if (!$self->{export_invalid}) {
		$str .= " AND STATUS='VALID'";
	} elsif ($self->{export_invalid} == 2) {
		$str .= " AND STATUS <> 'VALID'";
	}
	if ($owner) {
		$str .= " AND OWNER = '$owner'";
		$self->logit("Looking forward functions declaration in schema $owner.\n", 1) if (!$self->{quiet});
	} elsif (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
		$self->logit("Looking forward functions declaration in all schema.\n", 1) if (!$self->{quiet});
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
		$self->logit("Looking forward functions declaration in schema $self->{schema}.\n", 1) if (!$self->{quiet});
	}
	#$str .= " ORDER BY OBJECT_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %functions = ();
	my @fct_done = ();
	push(@fct_done, @EXCLUDED_FUNCTION);
	while (my $row = $sth->fetch) {
		next if (grep(/^$row->[1].$row->[0]$/i, @fct_done));
		push(@fct_done, "$row->[1].$row->[0]");
		$self->{function_metadata}{$row->[1]}{'none'}{$row->[0]}{type} = $row->[2];
	}
	$sth->finish();

	# Get content of package body
	my $sql = "SELECT NAME, OWNER, TYPE, TEXT FROM $self->{prefix}_SOURCE";
	if ($owner) {
		$sql .= " WHERE OWNER = '$owner'";
	} elsif (!$self->{schema}) {
		$sql .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$sql .= " WHERE OWNER = '$self->{schema}'";
	}
	$sql .= " AND TYPE <> 'PACKAGE'";
	$sql .= " ORDER BY OWNER, NAME, LINE";
	$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch)
	{
		next if (!exists $self->{function_metadata}{$row->[1]}{'none'}{$row->[0]});
		$self->{function_metadata}{$row->[1]}{'none'}{$row->[0]}{text} .= $row->[3];
	}
	$sth->finish();

	# For each schema in the Oracle instance
	foreach my $sch (sort keys %{ $self->{function_metadata} })
	{
		next if ( ($owner && ($sch ne $owner)) || (!$owner && $self->{schema} && ($sch ne $self->{schema})) );
		# Look for functions/procedures
		foreach my $name (sort keys %{$self->{function_metadata}{$sch}{'none'}})
		{
			if ($self->{function_metadata}{$sch}{'none'}{$name}{type} ne 'PACKAGE BODY')
			{
				# Retrieve metadata for this function after removing comments
				$self->_remove_comments(\$self->{function_metadata}{$sch}{'none'}{$name}{text}, 1);
				$self->{comment_values} = ();
				$self->{function_metadata}{$sch}{'none'}{$name}{text} =~  s/\%ORA2PG_COMMENT\d+\%//gs;
				my %fct_detail = $self->_lookup_function($self->{function_metadata}{$sch}{'none'}{$name}{text});
				if (!exists $fct_detail{name})
				{
					delete $self->{function_metadata}{$sch}{'none'}{$name};
					next;
				}
				delete $fct_detail{code};
				delete $fct_detail{before};
				%{$self->{function_metadata}{$sch}{'none'}{$name}{metadata}} = %fct_detail;
				delete $self->{function_metadata}{$sch}{'none'}{$name}{text};
			}
			else
			{
				$self->_remove_comments(\$self->{function_metadata}{$sch}{'none'}{$name}{text}, 1);
				$self->{comment_values} = ();
				$self->{function_metadata}{$sch}{'none'}{$name}{text} =~  s/\%ORA2PG_COMMENT\d+\%//gs;
				my %infos = $self->_lookup_package($self->{function_metadata}{$sch}{'none'}{$name}{text});
				delete $self->{function_metadata}{$sch}{'none'}{$name};
				$name =~ s/"//g;
				foreach my $f (sort keys %infos)
				{
					next if (!$f);
					my $fn = lc($f);
					delete $infos{$f}{code};
					delete $infos{$f}{before};
					%{$self->{function_metadata}{$sch}{$name}{$fn}{metadata}} = %{$infos{$f}};
					my $res_name = $f;
					$res_name =~ s/^([^\.]+)\.//;
					$f =~ s/^([^\.]+)\.//;
					if ($self->{package_as_schema}) {
						$res_name = $name . '.' . $res_name;
					} else {
						$res_name = $name . '_' . $res_name;
					}
					$res_name =~ s/"_"/_/g;
					$f =~ s/"//g;
					$self->{package_functions}{"\L$name\E"}{"\L$f\E"}{name}    = $self->quote_object_name($res_name);
					$self->{package_functions}{"\L$name\E"}{"\L$f\E"}{package} = $name;
				}
			}
		}
	}
}

sub _get_security_definer
{
	my ($self, $type) = @_;

	my %security = ();

	# This table does not exists before 10g
	return if ($self->{db_version} =~ /Release [89]/);

	# Retrieve security privilege per function defined in this database
	# Version of Oracle 10 does not have the OBJECT_TYPE column.
	my $str = "SELECT AUTHID,OBJECT_TYPE,OBJECT_NAME,OWNER FROM $self->{prefix}_PROCEDURES";
	if ($self->{db_version} =~ /Release 10/) {
		$str = "SELECT AUTHID,'ALL' AS OBJECT_TYPE,OBJECT_NAME,OWNER FROM $self->{prefix}_PROCEDURES";
	}
	if ($self->{schema}) {
		$str .= " WHERE OWNER = '$self->{schema}'";
	} else {
		$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	if ( $type && ($self->{db_version} !~ /Release 10/) ) {
		$str .= " AND OBJECT_TYPE='$type'";
	}
	$str .= " " . $self->limit_to_objects('FUNCTION|PROCEDURE|PACKAGE|TRIGGER', 'OBJECT_NAME|OBJECT_NAME|OBJECT_NAME|OBJECT_NAME');
	#$str .= " ORDER BY OBJECT_NAME";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		next if (!$row->[0]);
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[2] = "$row->[3].$row->[2]";
		}
		$security{$row->[2]}{security} = $row->[0];
		$security{$row->[2]}{owner} = $row->[3];
	}
	$sth->finish();

	return (\%security);
}

=head2 _get_identities

This function retrieve information about IDENTITY columns that must be
exported as PostgreSQL serial.

=cut

sub _get_identities
{
	my ($self) = @_;

	# Identity column appears in version 12 only
	return if ($self->{db_version} =~ /Release (8|9|10|11)/);

	# Retrieve all indexes 
	my $str = "SELECT OWNER, TABLE_NAME, COLUMN_NAME, GENERATION_TYPE, IDENTITY_OPTIONS FROM $self->{prefix}_TAB_IDENTITY_COLS";
	if (!$self->{schema}) {
		$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE OWNER = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('TABLE', 'TABLE_NAME');
	#$str .= " ORDER BY OWNER, TABLE_NAME, COLUMN_NAME";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %seqs = ();
	while (my $row = $sth->fetch)
	{
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[1] = "$row->[0].$row->[1]";
		}
		# GENERATION_TYPE can be ALWAYS, BY DEFAULT and BY DEFAULT ON NULL
		$seqs{$row->[1]}{$row->[2]}{generation} = $row->[3];
		# SEQUENCE options
		$seqs{$row->[1]}{$row->[2]}{options} = $row->[4];
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/(SCALE|EXTEND|SESSION)_FLAG: .//ig;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/KEEP_VALUE: .//is;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/(START WITH):/$1/;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/(INCREMENT BY):/$1/;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/MAX_VALUE:/MAXVALUE/;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/MIN_VALUE:/MINVALUE/;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/CYCLE_FLAG: N/NO CYCLE/;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/CYCLE_FLAG: Y/CYCLE/;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/CACHE_SIZE:/CACHE/;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/CACHE_SIZE:/CACHE/;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/ORDER_FLAG: .//;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/,//g;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/\s$//;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/CACHE\s+0/CACHE 1/;
		# For default values don't use option at all
		if ( $seqs{$row->[1]}{$row->[2]}{options} eq 'START WITH 1 INCREMENT BY 1 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NO CYCLE CACHE 20') {
			delete $seqs{$row->[1]}{$row->[2]}{options};
		}
		# Limit the sequence value to bigint max
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/MAXVALUE 9999999999999999999999999999/MAXVALUE 9223372036854775807/;
		$seqs{$row->[1]}{$row->[2]}{options} =~ s/\s+/ /g;
	}

	return %seqs;
}

=head2 _get_materialized_views

This function implements a mysql-native materialized views information.

Returns a hash of view names with the SQL queries they are based on.

=cut

sub _get_materialized_views
{
	my($self) = @_;

	# Retrieve all views
	my $str = "SELECT MVIEW_NAME,QUERY,UPDATABLE,REFRESH_MODE,REFRESH_METHOD,USE_NO_INDEX,REWRITE_ENABLED,BUILD_MODE,OWNER FROM $self->{prefix}_MVIEWS";
	if ($self->{db_version} =~ /Release 8/) {
		$str = "SELECT MVIEW_NAME,QUERY,UPDATABLE,REFRESH_MODE,REFRESH_METHOD,'',REWRITE_ENABLED,BUILD_MODE,OWNER FROM $self->{prefix}_MVIEWS";
	}
	if (!$self->{schema}) {
		$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE OWNER = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('MVIEW', 'MVIEW_NAME');
	#$str .= " ORDER BY MVIEW_NAME";
	my $sth = $self->{dbh}->prepare($str);
	if (not defined $sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	if (not $sth->execute(@{$self->{query_bind_params}})) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		return ();
	}

	my %data = ();
	while (my $row = $sth->fetch) {
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[8].$row->[0]";
		}
		$data{$row->[0]}{text} = $row->[1];
		$data{$row->[0]}{updatable} = ($row->[2] eq 'Y') ? 1 : 0;
		$data{$row->[0]}{refresh_mode} = $row->[3];
		$data{$row->[0]}{refresh_method} = $row->[4];
		$data{$row->[0]}{no_index} = ($row->[5] eq 'Y') ? 1 : 0;
		$data{$row->[0]}{rewritable} = ($row->[6] eq 'Y') ? 1 : 0;
		$data{$row->[0]}{build_mode} = $row->[7];
		$data{$row->[0]}{owner} = $row->[8];
	}

	return %data;
}

sub _get_materialized_view_names
{
	my($self) = @_;

	# Retrieve all views
	my $str = "SELECT MVIEW_NAME,OWNER FROM $self->{prefix}_MVIEWS";
	if (!$self->{schema}) {
		$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE OWNER = '$self->{schema}'";
	}
	$str .= $self->limit_to_objects('MVIEW', 'MVIEW_NAME');
	#$str .= " ORDER BY MVIEW_NAME";
	my $sth = $self->{dbh}->prepare($str);
	if (not defined $sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	if (not $sth->execute(@{$self->{query_bind_params}})) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}

	my @data = ();
	while (my $row = $sth->fetch) {
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

	# Retrieve all package information
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE = 'PACKAGE BODY'";
	if (!$self->{export_invalid}) {
		$str .= " AND STATUS='VALID'";
	} elsif ($self->{export_invalid} == 2) {
		$str .= " AND STATUS <> 'VALID'";
	}
	if ($owner) {
		$str .= " AND OWNER = '$owner'";
		$self->logit("Looking forward functions declaration in schema $owner.\n", 1) if (!$self->{quiet});
	} elsif (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
		$self->logit("Looking forward functions declaration in all schema.\n", 1) if (!$self->{quiet});
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
		$self->logit("Looking forward functions declaration in schema $self->{schema}.\n", 1) if (!$self->{quiet});
	}
	#$str .= " ORDER BY OBJECT_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @packages = ();
	while (my $row = $sth->fetch)
	{
		next if (grep(/^$row->[0]$/i, @packages));
		push(@packages, $row->[0]);
	}
	$sth->finish();

	# Get content of all packages definition
	my $sql = "SELECT NAME, OWNER, TYPE, TEXT FROM $self->{prefix}_SOURCE";
	if ($owner) {
		$sql .= " WHERE OWNER = '$owner'";
	} elsif (!$self->{schema}) {
		$sql .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$sql .= " WHERE OWNER = '$self->{schema}'";
	}
	$sql .= " AND TYPE <> 'PACKAGE'";
	$sql .= " ORDER BY OWNER, NAME, LINE";
	$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);
	my %function_metadata = ();
	my $has_pkg = $#packages;
	while (my $row = $sth->fetch)
	{
		next if ($has_pkg >= 0 && !grep(/^$row->[0]$/, @packages));
		$function_metadata{$row->[1]}{$row->[0]}{text} .= $row->[3];
	}
	$sth->finish();

	my @fct_done = ();
	push(@fct_done, @EXCLUDED_FUNCTION);
	foreach my $sch (sort keys %function_metadata)
	{
		next if ( ($owner && ($sch ne $owner)) || (!$owner && $self->{schema} && ($sch ne $self->{schema})) );
		foreach my $name (sort keys %{$function_metadata{$sch}})
		{
			$self->_remove_comments(\$function_metadata{$sch}{$name}{text}, 1);
			$self->{comment_values} = ();
			$function_metadata{$sch}{$name}{text} =~  s/\%ORA2PG_COMMENT\d+\%//gs;
			my %infos = $self->_lookup_package($function_metadata{$sch}{$name}{text});
			delete $function_metadata{$sch}{$name};
			foreach my $f (sort keys %infos)
			{
				next if (!$f);
				my $fn = lc($f);
				my $res_name = $f;
				if ($res_name =~ s/^([^\.]+)\.//) {
					next if (lc($1) ne lc($name));
				}
				if ($self->{package_as_schema}) {
					$res_name = $name . '.' . $res_name;
				} else {
					$res_name = $name . '_' . $res_name;
				}
				$res_name =~ s/"_"/_/g;
				$f =~ s/"//gs;
				if ($res_name)
				{
					$self->{package_functions}{"\L$name\E"}{"\L$f\E"}{name}    = $self->quote_object_name($res_name);
					$self->{package_functions}{"\L$name\E"}{"\L$f\E"}{package} = $name;
				}
			}
		}
	}
}

sub _get_procedures
{
	my ($self) = @_;

	# Retrieve all functions 
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE='PROCEDURE'";
	if (!$self->{export_invalid}) {
		$str .= " AND STATUS='VALID'";
	} elsif ($self->{export_invalid} == 2) {
		$str .= " AND STATUS <> 'VALID'";
	}
	if (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('PROCEDURE','OBJECT_NAME');
	#$str .= " ORDER BY OBJECT_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %procedures = ();
	my @fct_done = ();
	push(@fct_done, @EXCLUDED_FUNCTION);
	while (my $row = $sth->fetch) {
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
		next if (grep(/^$row->[0]$/i, @fct_done));
		push(@fct_done, $row->[0]);
		$procedures{"$row->[0]"}{owner} = $row->[1];
	}
	$sth->finish();

	my $sql = "SELECT NAME,OWNER,TEXT FROM $self->{prefix}_SOURCE";
	if (!$self->{schema}) {
		$sql .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$sql .= " WHERE OWNER = '$self->{schema}'";
	}
	$sql .= " " . $self->limit_to_objects('PROCEDURE','NAME');
	$sql .= " ORDER BY OWNER,NAME,LINE";
	$sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
		# Fix possible Malformed UTF-8 character
		$row->[2] = encode('UTF-8', $row->[2]) if ($self->{force_plsql_encoding});
		# Remove some bargage when migrating from 8i
		$row->[2] =~ s/\bAUTHID\s+[^\s]+\s+//is;
		if (exists $procedures{"$row->[0]"}) {
			$procedures{"$row->[0]"}{text} .= $row->[2];
		}
	}

	return \%procedures;
}

sub _get_packages
{
	my ($self) = @_;

	# Retrieve the list of packages
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE = 'PACKAGE'";
	if (!$self->{export_invalid}) {
		$str .= " AND STATUS='VALID'";
	} elsif ($self->{export_invalid} == 2) {
		$str .= " AND STATUS <> 'VALID'";
	}
	if (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('PACKAGE','OBJECT_NAME');
	#$str .= " ORDER BY OBJECT_NAME";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %packages = ();
	my @fct_done = ();
	while (my $row = $sth->fetch)
	{
		$self->logit("\tFound Package: $row->[0]\n", 1);
		next if (grep(/^$row->[0]$/, @fct_done));
		push(@fct_done, $row->[0]);
		# Get package definition first
		my $sql = "SELECT TEXT FROM $self->{prefix}_SOURCE WHERE OWNER='$row->[1]' AND NAME='$row->[0]' AND TYPE='PACKAGE' ORDER BY LINE";
		my $sth2 = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch)
		{
			$packages{$row->[0]}{desc} .= 'CREATE OR REPLACE ' if ($r->[0] =~ /^PACKAGE\s+/is);
			$packages{$row->[0]}{desc} .= $r->[0];
		}
		$sth2->finish();
		$packages{$row->[0]}{desc} .= "\n" if (exists $packages{$row->[0]});

		# Then package body code
		$sql = "SELECT TEXT FROM $self->{prefix}_SOURCE WHERE OWNER='$row->[1]' AND NAME='$row->[0]' AND TYPE='PACKAGE BODY' ORDER BY LINE";
		$sth2 = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch)
		{
			$packages{$row->[0]}{text} .= 'CREATE OR REPLACE ' if ($r->[0] =~ /^PACKAGE\s+/is);
			$packages{$row->[0]}{text} .= $r->[0];
		}
		$packages{$row->[0]}{owner} = $row->[1];
	}

	return \%packages;
}

sub _get_types
{
	my ($self, $name) = @_;

	# Retrieve all user defined types
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER,OBJECT_ID FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE='TYPE'";
	if (!$self->{export_invalid}) {
		$str .= " AND STATUS='VALID'";
	} elsif ($self->{export_invalid} == 2) {
		$str .= " AND STATUS <> 'VALID'";
	}
	if ($name) {
		$str .= " AND OBJECT_NAME='$name'";
	} else {
		$str .= " AND OBJECT_NAME NOT LIKE 'SYS_PLSQL_%'"; # found in export from 9i
	}
	$str .= " AND GENERATED='N'";
	if ($self->{schema}) {
		$str .= "AND OWNER='$self->{schema}' ";
	} else {
		$str .= "AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	if (!$name) {
		$str .= $self->limit_to_objects('TYPE', 'OBJECT_NAME');
	} else {
		@{$self->{query_bind_params}} = ();
	}
	#$str .= " ORDER BY OBJECT_NAME";

	# use a separeate connection
	my $local_dbh = _db_connection($self);

	my $sth = $local_dbh->prepare($str) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);
	$sth->execute(@{$self->{query_bind_params}}) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);

	my @types = ();
	my @fct_done = ();
	while (my $row = $sth->fetch)
	{
		next if ($row->[0] =~ /^(SDO_GEOMETRY|ST_|STGEOM_)/);
		#my $sql = "SELECT DBMS_METADATA.GET_DDL('TYPE','$row->[0]','$row->[1]') FROM DUAL";
		my $sql = "SELECT TEXT,LINE FROM $self->{prefix}_SOURCE WHERE OWNER='$row->[1]' AND NAME='$row->[0]' AND (TYPE='TYPE' OR TYPE='TYPE BODY') ORDER BY TYPE, LINE";
		if (!$self->{schema} && $self->{export_schema}) {
			$row->[0] = "$row->[1].$row->[0]";
		}
		$self->logit("\tFound Type: $row->[0]\n", 1);
		next if (grep(/^$row->[0]$/, @fct_done));
		push(@fct_done, $row->[0]);
		my %tmp = ();
		my $sth2 = $local_dbh->prepare($sql) or $self->logit("FATAL: " . $local_dbh->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch) {
			$tmp{code} .= $r->[0];
		}
		$sth2->finish();
		$tmp{name} = $row->[0];
		$tmp{owner} = $row->[1];
		$tmp{pos} = $row->[2];
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

	$local_dbh->disconnect() if ($local_dbh);

	return \@types;
}

sub _col_count
{
	my ($self, $table, $owner) = @_;

	my $condition = '';
	$condition .= "AND A.TABLE_NAME='$table' " if ($table);
	if ($owner) {
		$condition .= "AND A.OWNER='$owner' ";
	} else {
		$condition .= " AND A.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	if (!$table) {
		$condition .= $self->limit_to_objects('TABLE', 'A.TABLE_NAME');
	} else {
		@{$self->{query_bind_params}} = ();
	}

	my $sth = '';
	if ($self->{db_version} !~ /Release 8/) {
		$sth = $self->{dbh}->prepare(<<END);
SELECT A.OWNER, A.TABLE_NAME, COUNT(*)
FROM $self->{prefix}_TAB_COLUMNS A, $self->{prefix}_OBJECTS O WHERE A.OWNER=O.OWNER and A.TABLE_NAME=O.OBJECT_NAME and O.OBJECT_TYPE='TABLE' $condition
GROUP BY A.OWNER, A.TABLE_NAME
END
		if (!$sth) {
			$self->logit("FATAL: _col_count() " . $self->{dbh}->errstr . "\n", 0, 1);
		}
	} else {
		# an 8i database.
		$sth = $self->{dbh}->prepare(<<END);
SELECT A.OWNER, A.TABLE_NAME, COUNT(*)
FROM $self->{prefix}_TAB_COLUMNS A, $self->{prefix}_OBJECTS O WHERE A.OWNER=O.OWNER and A.TABLE_NAME=O.OBJECT_NAME and O.OBJECT_TYPE='TABLE' $condition
GROUP BY A.OWNER, A.TABLE_NAME
END
		if (!$sth) {
			$self->logit("FATAL: _col_count() " . $self->{dbh}->errstr . "\n", 0, 1);
		}
	}
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

=head2 auto_set_encoding

This function is used to find the PostgreSQL charset corresponding to the
Oracle NLS_LANG value

=cut

sub auto_set_encoding
{
	my $oracle_charset = shift;

	my %ENCODING = (
		"AL32UTF8" => "UTF8",
		"JA16EUC" => "EUC_JP",
		"JA16SJIS" => "EUC_JIS_2004",
		"ZHT32EUC" => "EUC_TW",
		"CL8ISO8859P5" => "ISO_8859_5",
		"AR8ISO8859P6" => "ISO_8859_6",
		"EL8ISO8859P7" => "ISO_8859_7",
		"IW8ISO8859P8" => "ISO_8859_8",
		"CL8KOI8R" => "KOI8R",
		"CL8KOI8U" => "KOI8U",
		"WE8ISO8859P1" => "LATIN1",
		"EE8ISO8859P2" => "LATIN2",
		"SE8ISO8859P3" => "LATIN3",
		"NEE8ISO8859P4"=> "LATIN4",
		"WE8ISO8859P9" => "LATIN5",
		"NE8ISO8859P10"=> "LATIN6",
		"BLT8ISO8859P13"=> "LATIN7",
		"CEL8ISO8859P14"=> "LATIN8",
		"WE8ISO8859P15" => "LATIN9",
		"RU8PC866" => "WIN866",
		"EE8MSWIN1250" => "WIN1250",
		"CL8MSWIN1251" => "WIN1251",
		"WE8MSWIN1252" => "WIN1252",
		"EL8MSWIN1253" => "WIN1253",
		"TR8MSWIN1254" => "WIN1254",
		"IW8MSWIN1255" => "WIN1255",
		"AR8MSWIN1256" => "WIN1256",
		"BLT8MSWIN1257"=> "WIN1257"
	);

	foreach my $k (keys %ENCODING) {
		return $ENCODING{$k} if (uc($oracle_charset) eq $k);
	}

	return '';
}

1;

