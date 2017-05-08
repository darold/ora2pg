package Ora2Pg::MySQL;

use vars qw($VERSION);
use strict;

use POSIX qw(locale_h);

#set locale to LC_NUMERIC C
setlocale(LC_NUMERIC,"C");


$VERSION = '18.1';

# These definitions can be overriden from configuration file
our %MYSQL_TYPE = (
	'TINYINT' => 'smallint', # 1 byte
	'SMALLINT' => 'smallint', # 2 bytes
	'MEDIUMINT' => 'integer', # 3 bytes
	'INT' => 'integer', # 4 bytes
	'BIGINT' => 'bigint', # 8 bytes
	'DECIMAL' => 'decimal',
	'DEC' => 'decimal',
	'NUMERIC' => 'numeric',
	'FIXED' => 'numeric',
	'FLOAT' => 'double precision',
	'REAL' => 'real',
	'DOUBLE PRECISION' => 'double precision',
	'DOUBLE' => 'double precision',
	'BOOLEAN' => 'boolean',
	'BOOL' => 'boolean',
	'CHAR' => 'char',
	'VARCHAR' => 'varchar',
	'TINYTEXT' => 'text',
	'TEXT' => 'text',
	'MEDIUMTEXT' => 'text',
	'LONGTEXT' => 'text',
	'VARBINARY' => 'bytea',
	'BINARY' => 'bytea',
	'TINYBLOB' => 'bytea',
	'BLOB' => 'bytea',
	'MEDIUMBLOB' => 'bytea',
	'LONGBLOB' => 'bytea',
	'ENUM' => 'text',
	'SET' => 'text',
	'DATE' => 'date',
	'DATETIME' => 'timestamp without time zone',
	'TIME' => 'time without time zone',
	'TIMESTAMP' => 'timestamp without time zone',
	'YEAR' => 'smallint',
	'MULTIPOLYGON' => 'geometry',
	'BIT' => 'bit varying',
);

sub _get_version
{
	my $self = shift;

	my $oraver = '';
	my $sql = "SELECT version()";

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

	my $sql = "SHOW DATABASES WHERE `Database` NOT IN ('information_schema', 'performance_schema');";

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

	my $sql = "SHOW VARIABLES LIKE 'character\\_set\\_%';";
        my $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
        $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	my $my_encoding = '';
	my $my_client_encoding = '';
	while ( my @row = $sth->fetchrow()) {
		if ($row[0] eq 'character_set_database') {
			$my_encoding = $row[1];
		} elsif ($row[0] eq 'character_set_client') {
			$my_client_encoding = $row[1];
		}
	}
	$sth->finish();

	my $my_timestamp_format = '';
	my $my_date_format = '';
	$sql = "SHOW VARIABLES LIKE '%\\_format';";
        $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
        $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		if ($row[0] eq 'datetime_format') {
			$my_timestamp_format = $row[1];
		} elsif ($row[0] eq 'date_format') {
			$my_date_format = $row[1];
		}
	}
	$sth->finish();

	#my $pg_encoding = auto_set_encoding($charset);
	my $pg_encoding = $my_encoding;

	return ($my_encoding, $my_client_encoding, $pg_encoding, $my_timestamp_format, $my_date_format);
}



=head2 _table_info

This function retrieves all MySQL tables information.

Returns a handle to a DB query statement.

=cut

sub _table_info
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

	my %tables_infos = ();
	my %comments = ();
	my $sql = "SELECT TABLE_NAME,TABLE_COMMENT,TABLE_TYPE,TABLE_ROWS,ROUND( ( data_length + index_length) / 1024 / 1024, 2 ) AS \"Total Size Mb\", AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = '$self->{schema}'";
	$sql .= $self->limit_to_objects('TABLE', 'TABLE_NAME');
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		$row->[2] =~ s/^BASE //;
		$comments{$row->[0]}{comment} = $row->[1];
		$comments{$row->[0]}{table_type} = $row->[2];
		$tables_infos{$row->[0]}{owner} = '';
		$tables_infos{$row->[0]}{num_rows} = $row->[3] || 0;
		$tables_infos{$row->[0]}{comment} =  $comments{$row->[0]}{comment} || '';
		$tables_infos{$row->[0]}{type} =  $comments{$row->[0]}{table_type} || '';
		$tables_infos{$row->[0]}{nested} = '';
		$tables_infos{$row->[0]}{size} = $row->[4] || 0;
		$tables_infos{$row->[0]}{tablespace} = 0;
		$tables_infos{$row->[0]}{auto_increment} = $row->[5] || 0;
		my $sth2 = $self->{dbh}->prepare("SELECT DISTINCT TABLESPACE_NAME FROM INFORMATION_SCHEMA.FILES WHERE TABLE_NAME = '$row->[0]' AND table_schema = '$self->{schema}'") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0);
		$sth2->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch) {
			$tables_infos{$row->[0]}{tablespace} = $r->[0];
			last;
		}
		$sth2->finish();
		# Get creation option unavailable in information_schema
		$sth2 = $self->{dbh}->prepare("SHOW CREATE TABLE $row->[0]") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0);
		$sth2->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch) {
			if ($r->[1] =~ /CONNECTION='([^']+)'/) {
				$tables_infos{$row->[0]}{connection} = $1;
			}
			last;
		}
		$sth2->finish();
	}
	$sth->finish();

	return %tables_infos;
}

sub _column_comments
{
	my ($self, $table) = @_;

	my $condition = '';

	my $sql = "SELECT COLUMN_NAME,COLUMN_COMMENT,TABLE_NAME,'' AS \"Owner\" FROM INFORMATION_SCHEMA.COLUMNS";
	if ($self->{schema}) {
		$sql .= " WHERE TABLE_SCHEMA='$self->{schema}' ";
	}
	$sql .= "AND TABLE_NAME='$table' " if ($table);
	$sql .= $self->limit_to_objects('TABLE','TABLE_NAME') if (!$table);

	my $sth = $self->{dbh}->prepare($sql) or $self->logit("WARNING only: " . $self->{dbh}->errstr . "\n", 0, 0);

	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %data = ();
	while (my $row = $sth->fetch) {
		$data{$row->[2]}{$row->[0]} = $row->[1];
	}
	return %data;
}

sub _column_info
{
	my ($self, $table, $owner, $objtype, $recurs) = @_;

	$objtype ||= 'TABLE';

	my $condition = '';
	if ($self->{schema}) {
		$condition .= "AND TABLE_SCHEMA='$self->{schema}' ";
	}
	$condition .= "AND TABLE_NAME='$table' " if ($table);
	$condition .= $self->limit_to_objects('TABLE', 'TABLE_NAME') if (!$table);
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

	my $sth = $self->{dbh}->prepare(<<END);
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_OCTET_LENGTH, TABLE_NAME, '' AS OWNER, ORDINAL_POSITION, COLUMN_TYPE, EXTRA
FROM INFORMATION_SCHEMA.COLUMNS
$condition
ORDER BY ORDINAL_POSITION
END
	if (!$sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	my $pos = 0;
	while (my $row = $sth->fetch) {
		$row->[2] = $row->[7] if $row->[1] =~ /char/i;
		$row->[10] = $pos;
		push(@{$data{"$row->[8]"}{"$row->[0]"}}, @$row);

		$pos++;
	}

	return %data;
}

sub _get_indexes
{
	my ($self, $table, $owner) = @_;

	my $condition = '';
	$condition = " FROM $self->{schema}" if ($self->{schema});
	$condition .= $self->limit_to_objects('TABLE|INDEX', "`Table`|`Key_name`") if (!$table);
	$condition =~ s/ AND / WHERE /;

	my %tables_infos = ();
	if ($table) {
		$tables_infos{$table} = 1;
	} else {
		%tables_infos = Ora2Pg::MySQL::_table_info($self);
	}
	my %data = ();
	my %unique = ();
	my %idx_type = ();
	my %index_tablespace = ();

	# Retrieve all indexes for the given table
	foreach my $t (keys %tables_infos) {
		my $sth = $self->{dbh}->prepare("SHOW INDEX FROM $t $condition;") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

		my $i = 1;
		while (my $row = $sth->fetch) {

			next if ($row->[2] eq 'PRIMARY');
		#Table : The name of the table.
		#Non_unique : 0 if the index cannot contain duplicates, 1 if it can.
		#Key_name : The name of the index. If the index is the primary key, the name is always PRIMARY.
		#Seq_in_index : The column sequence number in the index, starting with 1.
		#Column_name : The column name.
		#Collation : How the column is sorted in the index. In MySQL, this can have values “A” (Ascending) or NULL (Not sorted).
		#Cardinality : An estimate of the number of unique values in the index.
		#Sub_part : The number of indexed characters if the column is only partly indexed, NULL if the entire column is indexed.
		#Packed : Indicates how the key is packed. NULL if it is not.
		#Null : Contains YES if the column may contain NULL values and '' if not.
		#Index_type : The index method used (BTREE, FULLTEXT, HASH, RTREE).
		#Comment : Information about the index not described in its own column, such as disabled if the index is disabled. 
			my $idxname = $row->[2];
			$row->[1] = 'UNIQUE' if (!$row->[1]);
			$unique{$row->[0]}{$idxname} = $row->[1];
			# Set right label to spatial index
			if ($row->[10] =~ /SPATIAL/) {
				$row->[10] = 'SPATIAL_INDEX';
			}
			$idx_type{$row->[0]}{$idxname}{type_name} = $row->[10];
			# Save original column name
			my $colname = $row->[4];
			# Enclose with double quote if required
			$row->[4] = $self->quote_object_name($row->[4]);

			if ($self->{preserve_case}) {
				if (($row->[4] !~ /".*"/) && ($row->[4] !~ /\(.*\)/)) {
					$row->[4] =~ s/^/"/;
					$row->[4] =~ s/$/"/;
				}
			}
			push(@{$data{$row->[0]}{$idxname}}, $row->[4]);
			$index_tablespace{$row->[0]}{$idxname} = '';

		}
	}

	return \%unique, \%data, \%idx_type, \%index_tablespace;
}

sub _count_indexes
{
	my ($self, $table, $owner) = @_;

	my $condition = '';
	$condition = " FROM $self->{schema}" if ($self->{schema});
	$condition .= $self->limit_to_objects('TABLE|INDEX', "`Table`|`Key_name`") if (!$table);
	$condition =~ s/ AND / WHERE /;

	my %tables_infos = ();
	if ($table) {
		$tables_infos{$table} = 1;
	} else {
		%tables_infos = Ora2Pg::MySQL::_table_info($self);
	}
	my %data = ();

	# Retrieve all indexes for the given table
	foreach my $t (keys %tables_infos) {
		my $sth = $self->{dbh}->prepare("SHOW INDEX FROM $t $condition;") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

		my $i = 1;
		while (my $row = $sth->fetch) {

		#Table : The name of the table.
		#Non_unique : 0 if the index cannot contain duplicates, 1 if it can.
		#Key_name : The name of the index. If the index is the primary key, the name is always PRIMARY.
		#Seq_in_index : The column sequence number in the index, starting with 1.
		#Column_name : The column name.
		#Collation : How the column is sorted in the index. In MySQL, this can have values “A” (Ascending) or NULL (Not sorted).
		#Cardinality : An estimate of the number of unique values in the index.
		#Sub_part : The number of indexed characters if the column is only partly indexed, NULL if the entire column is indexed.
		#Packed : Indicates how the key is packed. NULL if it is not.
		#Null : Contains YES if the column may contain NULL values and '' if not.
		#Index_type : The index method used (BTREE, FULLTEXT, HASH, RTREE).
		#Comment : Information about the index not described in its own column, such as disabled if the index is disabled. 
			push(@{$data{$row->[0]}{$row->[2]}}, $row->[4]);

		}
	}

	return \%data;
}


sub _foreign_key
{
        my ($self, $table, $owner) = @_;

        my $condition = '';
        $condition .= "AND A.TABLE_NAME='$table' " if ($table);
        $condition .= "AND A.CONSTRAINT_SCHEMA='$self->{schema}' " if ($self->{schema});

        my $deferrable = $self->{fkey_deferrable} ? "'DEFERRABLE' AS DEFERRABLE" : "DEFERRABLE";
	my $sql = "SELECT DISTINCT A.COLUMN_NAME,A.ORDINAL_POSITION,A.TABLE_NAME,A.REFERENCED_TABLE_NAME,A.REFERENCED_COLUMN_NAME,A.POSITION_IN_UNIQUE_CONSTRAINT,A.CONSTRAINT_NAME,A.REFERENCED_TABLE_SCHEMA,B.MATCH_OPTION,B.UPDATE_RULE,B.DELETE_RULE FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS A INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS B ON A.CONSTRAINT_NAME = B.CONSTRAINT_NAME WHERE A.REFERENCED_COLUMN_NAME IS NOT NULL $condition ORDER BY A.ORDINAL_POSITION,A.POSITION_IN_UNIQUE_CONSTRAINT";
        my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);
        my @cons_columns = ();
	my $i = 1;
        my %data = ();
        my %link = ();
        while (my $r = $sth->fetch) {
		my $key_name = $r->[2] . '_' . $r->[0] . '_fk' . $i;
		if ($r->[6] ne 'PRIMARY') {
			$key_name = uc($r->[6]);
		}
		if ($self->{schema} && (lc($r->[7]) ne lc($self->{schema}))) {
			print STDERR "WARNING: Foreign key $r->[2].$r->[0] point to an other database: $r->[7].$r->[3].$r->[4], please fix it.\n";
		}
		push(@{$link{$r->[2]}{$key_name}{local}}, $r->[0]);
		push(@{$link{$r->[2]}{$key_name}{remote}{$r->[3]}}, $r->[4]);
		$r->[8] = 'SIMPLE'; # See pathetical documentation of mysql
		# SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,$deferrable,DEFERRED,R_OWNER,TABLE_NAME,OWNER,UPDATE_RULE
                push(@{$data{$r->[2]}}, [ ($key_name, $key_name, $r->[8], $r->[10], 'DEFERRABLE', 'Y', '', $r->[2], '', $r->[9]) ]);
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
	my $str = "SELECT TABLE_NAME,VIEW_DEFINITION,CHECK_OPTION,IS_UPDATABLE,DEFINER,SECURITY_TYPE FROM INFORMATION_SCHEMA.VIEWS $condition";
	$str .= $self->limit_to_objects('VIEW', 'TABLE_NAME');
	$str .= " ORDER BY TABLE_NAME";
	$str =~ s/ AND / WHERE /;

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %ordered_view = ();
	my %data = ();
	while (my $row = $sth->fetch) {
		$row->[1] =~ s/`$self->{schema}`\.//g;
		$row->[1] =~ s/`([^\s`,]+)`/$1/g;
		$row->[1] =~ s/`/"/g;
		$data{$row->[0]}{text} = $row->[1];
		$data{$row->[0]}{owner} = '';
		$data{$row->[0]}{comment} = '';
		$data{$row->[0]}{check_option} = $row->[2];
		$data{$row->[0]}{updatable} = $row->[3];
		$data{$row->[0]}{definer} = $row->[4];
		$data{$row->[0]}{security} = $row->[5];
	}
	return %data;
}

sub _get_triggers
{
	my($self) = @_;

	# Retrieve all indexes 
	# TRIGGER_CATALOG            | varchar(512)  | NO   |     |         |       |
	# TRIGGER_SCHEMA             | varchar(64)   | NO   |     |         |       |
	# TRIGGER_NAME               | varchar(64)   | NO   |     |         |       |
	# EVENT_MANIPULATION         | varchar(6)    | NO   |     |         |       |
	# EVENT_OBJECT_CATALOG       | varchar(512)  | NO   |     |         |       |
	# EVENT_OBJECT_SCHEMA        | varchar(64)   | NO   |     |         |       |
	# EVENT_OBJECT_TABLE         | varchar(64)   | NO   |     |         |       |
	# ACTION_ORDER               | bigint(4)     | NO   |     | 0       |       |
	# ACTION_CONDITION           | longtext      | YES  |     | NULL    |       |
	# ACTION_STATEMENT           | longtext      | NO   |     | NULL    |       |
	# ACTION_ORIENTATION         | varchar(9)    | NO   |     |         |       |
	# ACTION_TIMING              | varchar(6)    | NO   |     |         |       |
	# ACTION_REFERENCE_OLD_TABLE | varchar(64)   | YES  |     | NULL    |       |
	# ACTION_REFERENCE_NEW_TABLE | varchar(64)   | YES  |     | NULL    |       |
	# ACTION_REFERENCE_OLD_ROW   | varchar(3)    | NO   |     |         |       |
	# ACTION_REFERENCE_NEW_ROW   | varchar(3)    | NO   |     |         |       |
	# CREATED                    | datetime      | YES  |     | NULL    |       |
	# SQL_MODE                   | varchar(8192) | NO   |     |         |       |
	# DEFINER                    | varchar(77)   | NO   |     |         |       |
	# CHARACTER_SET_CLIENT       | varchar(32)   | NO   |     |         |       |
	# COLLATION_CONNECTION       | varchar(32)   | NO   |     |         |       |
	# DATABASE_COLLATION         | varchar(32)   | NO   |     |         |       |

	my $str = "SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_STATEMENT, '' AS WHEN_CLAUSE, '' AS DESCRIPTION, ACTION_ORIENTATION FROM INFORMATION_SCHEMA.TRIGGERS";
	if ($self->{schema}) {
		$str .= " AND TRIGGER_SCHEMA = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('TABLE|VIEW|TRIGGER','EVENT_OBJECT_TABLE|EVENT_OBJECT_TABLE|TRIGGER_NAME');
	$str =~ s/ AND / WHERE /;

	$str .= " ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @triggers = ();
	while (my $row = $sth->fetch) {
		$row->[7] = 'FOR EACH '. $row->[7];
		push(@triggers, [ @$row ]);
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

	# CONSTRAINT_CATALOG | varchar(512) | NO   |     |         |       |
	# CONSTRAINT_SCHEMA  | varchar(64)  | NO   |     |         |       |
	# CONSTRAINT_NAME    | varchar(64)  | NO   |     |         |       |
	# TABLE_SCHEMA       | varchar(64)  | NO   |     |         |       |
	# TABLE_NAME         | varchar(64)  | NO   |     |         |       |
	# CONSTRAINT_TYPE    | varchar(64)  | NO   |     |         |       |

	my $condition = '';
	$condition = " FROM $self->{schema}" if ($self->{schema});
	$condition .= $self->limit_to_objects('TABLE|INDEX', "`Table`|`Key_name`") if (!$table);
	$condition =~ s/ AND / WHERE /;

	my %tables_infos = ();
	if ($table) {
		$tables_infos{$table} = 1;
	} else {
		%tables_infos = Ora2Pg::MySQL::_table_info($self);
	}
	# Retrieve all indexes for the given table
	foreach my $t (keys %tables_infos) {
		my $sth = $self->{dbh}->prepare("SHOW INDEX FROM $t $condition;") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

		my $i = 1;
		while (my $row = $sth->fetch) {
			# Exclude non unique constraints
			next if ($row->[1]);
		#Table : The name of the table.
		#Non_unique : 0 if the index cannot contain duplicates, 1 if it can.
		#Key_name : The name of the index. If the index is the primary key, the name is always PRIMARY.
		#Seq_in_index : The column sequence number in the index, starting with 1.
		#Column_name : The column name.
		#Collation : How the column is sorted in the index. In MySQL, this can have values “A” (Ascending) or NULL (Not sorted).
		#Cardinality : An estimate of the number of unique values in the index.
		#Sub_part : The number of indexed characters if the column is only partly indexed, NULL if the entire column is indexed.
		#Packed : Indicates how the key is packed. NULL if it is not.
		#Null : Contains YES if the column may contain NULL values and '' if not.
		#Index_type : The index method used (BTREE, FULLTEXT, HASH, RTREE).
		#Comment : Information about the index not described in its own column, such as disabled if the index is disabled. 

			my $idxname = $row->[0] . '_idx' . $i;
			if ($row->[2] ne 'PRIMARY') {
				$idxname = $row->[2];
			}
			my $type = 'P';
			$type = 'U' if ($row->[2] ne 'PRIMARY');
			next if (!grep(/^'$type'$/, @accepted_constraint_types));
			my $generated = 0;
			$generated = 'GENERATED NAME' if ($row->[2] ne 'PRIMARY');
			if (!exists $result{$row->[0]}{$idxname}) {
				my %constraint = (type => $type, 'generated' => $generated, 'index_name' => $idxname, columns => [ ($row->[4]) ] );
				$result{$row->[0]}{$idxname} = \%constraint if ($row->[4]);
				$i++ if ($row->[2] ne 'PRIMARY');
			} else {
				push(@{$result{$row->[0]}{$idxname}->{columns}}, $row->[4]);
			}
		}
	}
	return %result;
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

	my $str = "SELECT ROUTINE_NAME,ROUTINE_DEFINITION,DATA_TYPE,ROUTINE_BODY,EXTERNAL_LANGUAGE,SECURITY_TYPE,IS_DETERMINISTIC FROM INFORMATION_SCHEMA.ROUTINES";
	if ($self->{schema}) {
		$str .= " AND ROUTINE_SCHEMA = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_objects('FUNCTION','ROUTINE_NAME');
	$str =~ s/ AND / WHERE /;
	$str .= " ORDER BY ROUTINE_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %functions = ();
	while (my $row = $sth->fetch) {

		my $kind = 'FUNCTION';
		if (!$row->[2]) {
			$kind = 'PROCEDURE';
		}
		next if ( ($kind ne $self->{type}) && ($self->{type} ne 'SHOW_REPORT') );
		my $sth2 = $self->{dbh}->prepare("SHOW CREATE $kind $row->[0]") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch) {
			$functions{"$row->[0]"}{text} = $r->[2];
			last;
		}
		$sth2->finish();
		if ($self->{plsql_pgsql} || ($self->{type} eq 'SHOW_REPORT')) {
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
	my ($self, $fctname) = @_;

        my %fct_detail = ();
	$fct_detail{name} = $fctname;
        $fct_detail{func_ret_type} = 'OPAQUE';

	# Split data into declarative and code part
	while ($self->{functions}{$fctname}{definition} =~ s/\s*DECLARE ([^;]+;)//im) {
		$fct_detail{declare} .= "$1\n";
	}
	$fct_detail{code} = $self->{functions}{$fctname}{definition};
	# Remove any label before the main block
	$fct_detail{code} =~ s/^[^\s\:]+:\s*BEGIN/BEGIN/;
	# Remove first BEGIN
	$fct_detail{code} =~ s/BEGIN//;

	return if (!$fct_detail{code});

	if ($self->{functions}{$fctname}{return}) {
		$fct_detail{hasreturn} = 1;
		$fct_detail{func_ret_type} = $self->_sql_type($self->{functions}{$fctname}{return});
	}
	$fct_detail{language} = $self->{functions}{$fctname}{language};
	$fct_detail{immutable} = 1 if ($self->{functions}{$fctname}{immutable} eq 'YES');
	$fct_detail{security} = $self->{functions}{$fctname}{security};

	# Extract arguments from the create statement
	$self->{functions}{$fctname}{text} =~ /FUNCTION \`$fctname\`(\(.*?\)) RETURNS/i;
	$fct_detail{args} = $1 || '';

	if ($self->{functions}{$fctname}{text} =~ /RETURNS (.*?) CHARSET/) {
		$fct_detail{func_ret_type} = $self->_sql_type($1);
	}

	# Now convert types
	$fct_detail{args} = replace_sql_type($fct_detail{args}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});
	$fct_detail{declare} = replace_sql_type($fct_detail{declare}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});

	# Replace PL/SQL code into PL/PGSQL similar code
	$fct_detail{declare} = Ora2Pg::PLSQL::mysql_to_plpgsql($self, $fct_detail{declare});
	if ($fct_detail{code}) {
		$fct_detail{code} = Ora2Pg::PLSQL::mysql_to_plpgsql($self, "BEGIN".$fct_detail{code});
	}

	return %fct_detail;
}

sub _lookup_procedure
{
	my ($self, $fctname) = @_;

        my %fct_detail = ();
	$fct_detail{name} = $fctname;
        $fct_detail{func_ret_type} = 'OPAQUE';

	# Split data into declarative and code part
	while ($self->{procedures}{$fctname}{definition} =~ s/\s*DECLARE\s+([^;]+;)//is) {
		$fct_detail{declare} .= "$1\n";
	}
	$fct_detail{code} = $self->{procedures}{$fctname}{definition};
	# Remove any label before the main block
	$fct_detail{code} =~ s/^[^\s\:]+:\s*BEGIN/BEGIN/;
	# Remove first BEGIN
	$fct_detail{code} =~ s/^BEGIN//;

	return if (!$fct_detail{code});

	$fct_detail{language} = $self->{procedures}{$fctname}{language};
	$fct_detail{immutable} = 1 if ($self->{procedures}{$fctname}{immutable} eq 'YES');
	$fct_detail{security} = $self->{procedures}{$fctname}{security};

	# Extract arguments from the create statement
	$self->{procedures}{$fctname}{text} =~ /PROCEDURE \`$fctname\`(\(.*?)BEGIN/is;
	$fct_detail{args} = $1 || '';
	$fct_detail{args} =~ s/\)[^\)]+$/\)/;

	# Procedure that have out parameters are functions with PG
	if ($fct_detail{args} =~ /\b(OUT|INOUT)\b/) {
		# set return type to empty to avoid returning void later
		$fct_detail{func_ret_type} = ' ';
	}

	# Now convert types
	$fct_detail{args} = replace_sql_type($fct_detail{args}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});
	$fct_detail{declare} = replace_sql_type($fct_detail{declare}, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});

	# Replace PL/SQL code into PL/PGSQL similar code
	$fct_detail{declare} = Ora2Pg::PLSQL::mysql_to_plpgsql($self, $fct_detail{declare});
	if ($fct_detail{code}) {
		$fct_detail{code} = Ora2Pg::PLSQL::mysql_to_plpgsql($self, "BEGIN".$fct_detail{code});
	}

	return %fct_detail;
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
	$str .= " " . $self->limit_to_objects('FUNCTION','ROUTINE_NAME');
	$str =~ s/ AND / WHERE /;
	$str .= " ORDER BY ROUTINE_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @functions = ();
	while (my $row = $sth->fetch) {
		push(@functions, $row->[0]);
	}
	$sth->finish();

	return @functions;
}



sub _sql_type
{
        my ($self, $type, $len, $precision, $scale) = @_;

	my $data_type = '';
	
	# Simplify timestamp type
	$type =~ s/TIMESTAMP\(\d+\)/TIMESTAMP/i;
	$type =~ s/TIME\(\d+\)/TIME/i;
	$type =~ s/DATE\(\d+\)/DATE/i;
	# Remove BINARY from CHAR(n) BINARY, TEXT(n) BINARY, VARCHAR(n) BINARY ...
	$type =~ s/(CHAR|TEXT)(\(\d+\)) BINARY/$1$2/i;
	$type =~ s/(CHAR|TEXT) BINARY/$1/i;

        # Overide the length
        $len = $precision if ( ((uc($type) eq 'NUMBER') || (uc($type) eq 'BIT')) && $precision );
        if (exists $MYSQL_TYPE{uc($type)}) {
		$type = uc($type); # Force uppercase
		if ($len) {
			if ( ($type eq "CHAR") || ($type =~ /VARCHAR/) ) {
				# Type CHAR have default length set to 1
				# Type VARCHAR(2) must have a specified length
				$len = 1 if (!$len && ($type eq "CHAR"));
                		return "$MYSQL_TYPE{$type}($len)";
			} elsif ($type eq 'BIT') {
				if ($precision) {
					return "$MYSQL_TYPE{$type}($precision)";
				} else {
					return $MYSQL_TYPE{$type};
				}
			} elsif ($type =~ /^(TINYINT|SMALLINT|MEDIUMINT|INTEGER|BIGINT|INT|REAL|DOUBLE|FLOAT|DECIMAL|NUMERIC)$/i) {
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
						return $MYSQL_TYPE{$type};
					}
				} else {
					if ($precision) {
						if ($self->{pg_numeric_type}) {
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
			return $MYSQL_TYPE{$type};
		} else {
			return $MYSQL_TYPE{$type};
		}
        }

        return $type;
}

sub replace_sql_type
{
        my ($str, $pg_numeric_type, $default_numeric, $pg_integer_typei, %data_type) = @_;

	$str =~ s/with local time zone/with time zone/igs;
	$str =~ s/([A-Z])ORA2PG_COMMENT/$1 ORA2PG_COMMENT/igs;

	# Remove any reference to UNSIGNED AND ZEROFILL
	$str =~ s/\b(UNSIGNED|ZEROFILL)\b//gis;
	# Remove BINARY from CHAR(n) BINARY and VARCHAR(n) BINARY
	$str =~ s/(CHAR|TEXT)(\(\d+\)) BINARY/$1$2/gis;
	$str =~ s/(CHAR|TEXT) BINARY/$1/gis;

	# Replace type with precision
	my $mysqltype_regex = join('|', keys %MYSQL_TYPE);
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
			$str =~ s/\b$type\b\s*\([^\)]+\)/$MYSQL_TYPE{$type}\%\|$len\%\|\%/is;
		} elsif ($precision && ($type =~ /(BIT|TINYINT|SMALLINT|MEDIUMINT|INTEGER|BIGINT|INT|REAL|DOUBLE|FLOAT|DECIMAL|NUMERIC)/)) {
			if (!$scale) {
				$str =~ s/\b$type\b\s*\([^\)]+\)/$MYSQL_TYPE{$type}\%\|$precision\%\|\%/is;
			} else {
				$str =~ s/\b$type\b\s*\([^\)]+\)/$MYSQL_TYPE{$type}\%\|$args\%\|\%/is;
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
	foreach my $type (sort { length($b) <=> length($a) } keys %MYSQL_TYPE) {
		# Keep enum as declared, we are not in table definition
		next if (uc($type) eq 'ENUM');
		while ($str =~ s/\b$type\b/%%RECOVER_TYPE$i%%/is) {
			$recover_type{$i} = $MYSQL_TYPE{$type};
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
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

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
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

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

This function implements an MySQL-native partitions information.
Return two hash ref with partition details and partition default.
=cut

sub _get_partitions
{
	my($self) = @_;

	# Retrieve all partitions.
	my $str = qq{
SELECT TABLE_NAME, PARTITION_ORDINAL_POSITION, PARTITION_NAME, PARTITION_DESCRIPTION, TABLESPACE_NAME, PARTITION_METHOD, PARTITION_EXPRESSION
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE PARTITION_NAME IS NOT NULL AND SUBPARTITION_NAME IS NULL AND (PARTITION_METHOD = 'RANGE' OR PARTITION_METHOD = 'LIST')
};
	$str .= $self->limit_to_objects('TABLE|PARTITION', 'TABLE_NAME|PARTITION_NAME');
	if ($self->{schema}) {
		$str .= "\tAND TABLE_SCHEMA ='$self->{schema}'\n";
	}
	$str .= "ORDER BY TABLE_NAME,PARTITION_ORDINAL_POSITION\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %parts = ();
	my %default = ();
	while (my $row = $sth->fetch) {
		if ( ($row->[3] eq 'MAXVALUE') || ($row->[3] eq 'DEFAULT')) {
			$default{$row->[0]} = $row->[2];
			next;
		}
		my $i = $#{$parts{$row->[0]}{$row->[1]}{$row->[2]}} + 1;
		push(@{$parts{$row->[0]}{$row->[1]}{$row->[2]}}, { 'type' => $row->[5], 'value' => $row->[3], 'column' => $row->[6], 'colpos' => $i, 'tablespace' => $row->[4], 'owner' => ''});
		$self->logit(".",1);
	}
	$sth->finish;
	$self->logit("\n", 1);

	return \%parts, \%default;
}

=head2 _get_subpartitions

This function implements a MySQL subpartitions information.
Return two hash ref with partition details and partition default.
=cut

sub _get_subpartitions
{
	my($self) = @_;

	# Retrieve all partitions.
	my $str = qq{
SELECT TABLE_NAME, SUBPARTITION_ORDINAL_POSITION, SUBPARTITION_NAME, PARTITION_DESCRIPTION, TABLESPACE_NAME, SUBPARTITION_METHOD, SUBPARTITION_EXPRESSION
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE SUBPARTITION_NAME IS NOT NULL AND SUBPARTITION_EXPRESSION IS NOT NULL AND (SUBPARTITION_METHOD = 'RANGE' OR SUBPARTITION_METHOD = 'LIST')
};
	$str .= $self->limit_to_objects('TABLE|PARTITION', 'TABLE_NAME|PARTITION_NAME');
	if ($self->{schema}) {
		$str .= " AND TABLE_SCHEMA ='$self->{schema}'\n";
	}
	$str .= " ORDER BY TABLE_NAME,PARTITION_ORDINAL_POSITION\n";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %subparts = ();
	my %default = ();
	while (my $row = $sth->fetch) {
		if ( ($row->[3] eq 'MAXVALUE') || ($row->[3] eq 'DEFAULT')) {
			$default{$row->[0]} = $row->[2];
			next;
		}
		my $i = $#{$subparts{$row->[0]}{$row->[1]}{$row->[2]}} + 1;
		push(@{$subparts{$row->[0]}{$row->[1]}{$row->[2]}}, { 'type' => $row->[5], 'value' => $row->[3], 'column' => $row->[6], 'colpos' => $i, 'tablespace' => $row->[4], 'owner' => ''});
		$self->logit(".",1);
	}
	$sth->finish;
	$self->logit("\n", 1);

	return \%subparts, \%default;
}

=head2 _get_partitions_list

This function implements a MySQL-native partitions information.
Return a hash of the partition table_name => type

=cut

sub _get_partitions_list
{
	my($self) = @_;

	# Retrieve all partitions.
	my $str = qq{
SELECT TABLE_NAME, PARTITION_ORDINAL_POSITION, PARTITION_NAME, PARTITION_DESCRIPTION, TABLESPACE_NAME, PARTITION_METHOD
FROM INFORMATION_SCHEMA.PARTITIONS WHERE SUBPARTITION_NAME IS NULL AND PARTITION_NAME IS NOT NULL
};
	$str .= $self->limit_to_objects('TABLE|PARTITION','TABLE_NAME|PARTITION_NAME');
	if ($self->{schema}) {
		$str .= " AND TABLE_SCHEMA ='$self->{schema}'";
	}
	$str .= " ORDER BY TABLE_NAME,PARTITION_NAME\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

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
	my($self) = @_;

	# Retrieve all partitions.
	my $str = qq{
SELECT TABLE_NAME, PARTITION_ORDINAL_POSITION, PARTITION_NAME, PARTITION_DESCRIPTION, TABLESPACE_NAME, PARTITION_METHOD
FROM INFORMATION_SCHEMA.PARTITIONS WHERE SUBPARTITION_NAME IS NULL AND PARTITION_NAME IS NOT NULL
};
	$str .= $self->limit_to_objects('TABLE|PARTITION','TABLE_NAME|PARTITION_NAME');
	if ($self->{schema}) {
		$str .= " AND TABLE_SCHEMA ='$self->{schema}'";
	}
	$str .= " ORDER BY TABLE_NAME,PARTITION_NAME\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch) {
		$parts{$row->[0]}++ if ($row->[2]);
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
	foreach my $t (@{$infos{TABLE}}) {
		$sth = $self->{dbh}->prepare("SHOW INDEX FROM $t->{name} FROM $self->{schema}") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my @row = $sth->fetchrow()) {
			next if ($row[2] eq 'PRIMARY');
			push(@{$infos{INDEX}}, { ( name => $row[2], invalid => 0) });
		}
	}
	# FUNCTION
	$sql = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE DATA_TYPE IS NOT NULL AND ROUTINE_SCHEMA = '$self->{schema}'";
	$sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{FUNCTION}}, { ( name => $row[0], invalid => 0) });
	}
	$sth->finish();
	# PROCEDURE
	$sql = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE DATA_TYPE IS NULL AND ROUTINE_SCHEMA = '$self->{schema}'";
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
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
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
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
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
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
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
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
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

This function retrieves the size of the MySQL database in MB

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

	my $sql = qq{
SELECT TABLE_NAME, sum(DATA_LENGTH + INDEX_LENGTH)/1024/1024 AS TSize
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='$self->{schema}'
};

	$sql .= $self->limit_to_objects('TABLE', 'TABLE_NAME');
	$sql .= " GROUP BY TABLE_NAME ORDER BY tsize";
	$sql .= " LIMIT $self->{top_max}" if ($self->{top_max});

        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
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

	return;
}

sub _extract_sequence_info
{
	my ($self) = shift;

	return;
}

# MySQL does not have sequences but we count auto_increment as sequences
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

	my @seqs = ();
	my $sql = "SELECT TABLE_NAME, AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = '$self->{schema}'";
	$sql .= $self->limit_to_objects('TABLE', 'TABLE_NAME');
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		push(@seqs, $row->[0]) if ($row->[1]);
	}
	$sth->finish();

	return \@seqs;
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
	$condition .= $self->limit_to_objects('TABLE', 'TABLE_NAME') if (!$table);
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

	my $sth = $self->{dbh}->prepare(<<END);
SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT, TABLE_NAME
FROM INFORMATION_SCHEMA.COLUMNS
$condition
ORDER BY ORDINAL_POSITION
END
	if (!$sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		$data{$row->[3]}{"$row->[0]"}{nullable} = $row->[1];
		$data{$row->[3]}{"$row->[0]"}{default} = $row->[2];
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
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %triggers = ();
	while (my $row = $sth->fetch) {
		push(@{$triggers{$row->[1]}}, $row->[0]);
	}

	return %triggers;
}

1;

