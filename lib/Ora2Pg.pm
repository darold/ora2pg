package Ora2Pg;
#------------------------------------------------------------------------------
# Project  : Oracle to PostgreSQL database schema converter
# Name     : Ora2Pg.pm
# Language : Perl
# Authors  : Gilles Darold, gilles _AT_ darold _DOT_ net
# Copyright: Copyright (c) 2000-2014 : Gilles Darold - All rights reserved -
# Function : Main module used to export Oracle database schema to PostgreSQL
# Usage    : See documentation in this file with perldoc.
#------------------------------------------------------------------------------
#
#        This program is free software: you can redistribute it and/or modify
#        it under the terms of the GNU General Public License as published by
#        the Free Software Foundation, either version 3 of the License, or
#        any later version.
# 
#        This program is distributed in the hope that it will be useful,
#        but WITHOUT ANY WARRANTY; without even the implied warranty of
#        MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#        GNU General Public License for more details.
# 
#        You should have received a copy of the GNU General Public License
#        along with this program. If not, see < http://www.gnu.org/licenses/ >.
# 
#------------------------------------------------------------------------------

use vars qw($VERSION $PSQL %AConfig);
use Carp qw(confess);
use DBI;
use POSIX qw(locale_h _exit :sys_wait_h);
use IO::File;
use Config;
use Time::HiRes qw/usleep/;
use Fcntl qw/ :flock /;
use IO::Handle;
use IO::Pipe;
use File::Basename;

#set locale to LC_NUMERIC C
setlocale(LC_NUMERIC,"C");

$VERSION = '13.0';
$PSQL = $ENV{PLSQL} || 'psql';

$| = 1;

our %RUNNING_PIDS = ();
# Multiprocess communication pipe
our $pipe = undef;

# Minimized the footprint on disc, so that more rows fit on a data page,
# which is the most important factor for speed. 
our %TYPALIGN = (
	'bool' => 0, 'boolean' => 0, 'bytea' => 4, 'char' => 0, 'name' => 0,
	'int8' => 8, 'int2' => 2, 'int4' => 4, 'text' => 4, 'oid' => 4, 'json' => 4,
	'xml' => 4, 'point' => 8, 'lseg' => 8, 'path' => 8, 'box' => 8,
	'polygon' => 8, 'line' => 8, 'float4' => 4, 'float8' => 8,
	'abstime' => 4, 'reltime' => 4, 'tinterval' => 4, 'circle' => 8,
	'money' => 8, 'macaddr' => 4, 'inet' => 4, 'cidr' => 4, 'bpchar' => 4,
	'varchar' => 4, 'date' => 4, 'time' => 8, 'timestamp' => 8,
	'timestamptz' => 8, 'interval' => 8, 'timetz' => 8, 'bit' => 4,
	'varbit' => 4, 'numeric' => 4, 'uuid' => 0, 'timestamp with time zone' => 8,
	'character varying' => 0, 'timestamp without time zone' => 8,
	'double precision' => 8, 'smallint' => 2, 'integer' => 4, 'bigint' => 8,
	'decimal' => '4', 'real' => 4, 'smallserial' => 2, 'serial' => 4,
	'bigserial' => 8
);

# These definitions can be overriden from configuration file
our %TYPE = (
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
	# The RAW type is presented as hexadecimal characters. The
	# contents are treated as binary data. Limit of 2000 bytes
	# PG type text should match all needs or if you want you could
	# use blob (large object)
	'RAW' => 'bytea',
	'ROWID' => 'oid',
	'FLOAT' => 'double precision',
	'DEC' => 'decimal',
	'DECIMAL' => 'decimal',
	'DOUBLE PRECISION' => 'double precision',
	'INT' => 'integer',
	'INTEGER' => 'integer',
	'BINARY_INTEGER' => 'integer',
	'PLS_INTEGER' => 'integer',
	'REAL' => 'real',
	'SMALLINT' => 'smallint',
	'BINARY_FLOAT' => 'double precision',
	'BINARY_DOUBLE' => 'double precision',
	'TIMESTAMP' => 'timestamp',
	'BOOLEAN' => 'boolean',
	'INTERVAL' => 'interval',
	'XMLTYPE' => 'xml',
	'TIMESTAMP WITH TIME ZONE' => 'timestamp with time zone',
	'TIMESTAMP WITH LOCAL TIME ZONE' => 'timestamp with time zone',
	'SDO_GEOMETRY' => 'geometry',
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

our %INDEX_TYPE = (
	'NORMAL' => 'b-tree',
	'NORMAL/REV' => 'reversed b-tree',
	'FUNCTION-BASED NORMAL' => 'function based b-tree',
	'FUNCTION-BASED NORMAL/REV' => 'function based reversed b-tree',
	'BITMAP' => 'bitmap',
	'BITMAP JOIN' => 'bitmap join',
	'FUNCTION-BASED BITMAP' => 'function based bitmap',
	'FUNCTION-BASED BITMAP JOIN' => 'function based bitmap join',
	'CLUSTER' => 'cluster',
	'DOMAIN' => 'domain',
	'IOT - TOP' => 'IOT',
);

our @KEYWORDS = qw(
	ALL ANALYSE ANALYZE AND ANY ARRAY AS ASC ASYMMETRIC BOTH CASE CAST
	CHECK COLLATE COLLATION COLUMN CONCURRENTLY CONSTRAINT CREATE CROSS
	CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_SCHEMA CURRENT_TIME
	CURRENT_TIMESTAMP CURRENT_USER DEFAULT DEFERRABLE DESC DISTINCT DO ELSE
	END EXCEPT FALSE FETCH FOR FOREIGN FREEZE FROM FULL GRANT GROUP HAVING
	ILIKE IN INITIALLY INTERSECT INTO IS ISNULL JOIN LEADING LEFT LIKE
	LIMIT LOCALTIME LOCALTIMESTAMP NATURAL NOT NOTNULL NULL OFFSET ON ONLY
	OR ORDER OUTER OVER OVERLAPS PLACING PRIMARY REFERENCES RETURNING RIGHT
	SELECT SESSION_USER SIMILAR SOME SYMMETRIC TABLE THEN TO TRAILING TRUE
	UNION UNIQUE USER USING VARIADIC VERBOSE WHEN WHERE WINDOW WITH
);

our %BOOLEAN_MAP = (
	'yes' => 't',
	'no' => 'f',
	'y' => 't',
	'n' => 'f',
	'1' => 't',
	'0' => 'f',
	'true' => 't',
	'false' => 'f',
	'enabled'=> 't',
	'disabled'=> 'f',
);

our @GRANTS = (
	'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
	'REFERENCES', 'TRIGGER', 'USAGE', 'CREATE', 'CONNECT',
	'TEMPORARY', 'TEMP', 'USAGE', 'ALL', 'ALL PRIVILEGES',
	'EXECUTE'
);

$SIG{'CHLD'} = 'DEFAULT';

####
# method used to fork as many child as wanted
##
sub spawn
{
	my $coderef = shift;

	unless (@_ == 0 && $coderef && ref($coderef) eq 'CODE') {
		print "usage: spawn CODEREF";
		exit 0;
	}

	my $pid;
	if (!defined($pid = fork)) {
		print STDERR "Error: cannot fork: $!\n";
		return;
	} elsif ($pid) {
		$RUNNING_PIDS{$pid} = $pid;
		return; # the parent
	}
	# the child -- go spawn
	$< = $>;
	$( = $); # suid progs only

	exit &$coderef();
}

# With multiprocess we need to wait all childs
sub wait_child
{
        my $sig = shift;
        print STDERR "Received terminating signal ($sig).\n";
	if ($^O !~ /MSWin32|dos/i) {
		1 while wait != -1;
		$SIG{INT} = \&wait_child;
		$SIG{TERM} = \&wait_child;
	}
        _exit(0);
}
$SIG{INT} = \&wait_child;
$SIG{TERM} = \&wait_child;

=head1 PUBLIC METHODS

=head2 new HASH_OPTIONS

Creates a new Ora2Pg object.

The only required option is:

    - config : Path to the configuration file (required).

All directives found in the configuration file can be overwritten in the
instance call by passing them in lowercase as arguments.

=cut

sub new
{
	my ($class, %options) = @_;

	# This create an OO perl object
	my $self = {};
	bless ($self, $class);

	# Initialize this object
	$self->_init(%options);
	
	# Return the instance
	return($self);
}



=head2 export_schema FILENAME

Print SQL data output to a file name or
to STDOUT if no file name is specified.

=cut

sub export_schema
{
	my $self = shift;

	# Create default export file where things will be written with the dump() method
	# First remove it if the output file already exists
	if (not defined $self->{fhout}) {
		$self->remove_export_file();
		$self->create_export_file();
	} else {
		$self->logit("FATAL: method export_schema() could not be called several time.\n",0,1);
	}

	foreach my $t (@{$self->{export_type}}) {
		next if ($t =~ /^SHOW_/i);
		$self->{type} = $t;
		# Return data as string
		$self->_get_sql_data();
	}

}


=head2 open_export_file FILENAME

Open a file handle to a given filename.

=cut

sub open_export_file
{
	my ($self, $outfile, $noprefix) = @_;

	my $filehdl = undef;

	if ($outfile) {
		if ($self->{input_file} && ($outfile eq $self->{input_file})) {
			$self->logit("FATAL: input file is the same as output file: $outfile, can not overwrite it.\n",0,1);
		}
		if ($self->{output_dir} && !$noprefix) {
			$outfile = $self->{output_dir} . '/' . $outfile;
		}
		# If user request data compression
		if ($outfile =~ /\.gz$/i) {
			eval("use Compress::Zlib;");
			$self->{compress} = 'Zlib';
			$filehdl = gzopen("$outfile", "wb") or $self->logit("FATAL: Can't create deflation file $outfile\n",0,1);
		} elsif ($outfile =~ /\.bz2$/i) {
			$self->logit("Error: can't run bzip2\n",0,1) if (!-x $self->{bzip2});
			$self->{compress} = 'Bzip2';
			$filehdl = new IO::File;
			$filehdl->open("|$self->{bzip2} --stdout >$outfile") or $self->logit("FATAL: Can't open pipe to $self->{bzip2} --stdout >$outfile: $!\n", 0,1);
		} else {
			$filehdl = new IO::File;
			$filehdl->open(">$outfile") or $self->logit("FATAL: Can't open $outfile: $!\n", 0, 1);
			binmode($filehdl, $self->{'binmode'});
		}
		$filehdl->autoflush(1) if (defined $filehdl && !$self->{compress});
	}

	return $filehdl;
}

=head2 create_export_file FILENAME

Set output file and open a file handle on it,
will use STDOUT if no file name is specified.

=cut

sub create_export_file
{
	my ($self, $outfile) = @_;


	# Init with configuration OUTPUT filename
	$outfile ||= $self->{output};
	if ($self->{input_file} && ($outfile eq $self->{input_file})) {
		$self->logit("FATAL: input file is the same as output file: $outfile, can not overwrite it.\n",0,1);
	}
	if ($outfile) {
		if ($self->{output_dir} && $outfile) {
			$outfile = $self->{output_dir} . "/" . $outfile;
		}
		# Send output to the specified file
		if ($outfile =~ /\.gz$/) {
			eval("use Compress::Zlib;");
			$self->{compress} = 'Zlib';
			$self->{fhout} = gzopen($outfile, "wb") or $self->logit("FATAL: Can't create deflation file $outfile\n", 0, 1);
		} elsif ($outfile =~ /\.bz2$/) {
			$self->logit("FATAL: can't run bzip2\n",0,1) if (!-x $self->{bzip2});
			$self->{compress} = 'Bzip2';
			$self->{fhout} = new IO::File;
			$self->{fhout}->open("|$self->{bzip2} --stdout >$outfile") or $self->logit("FATAL: Can't open pipe to $self->{bzip2} --stdout >$outfile: $!\n", 0, 1);
		} else {
			$self->{fhout} = new IO::File;
			$self->{fhout}->open(">>$outfile") or $self->logit("FATAL: Can't open $outfile: $!\n", 0, 1);
			binmode($self->{fhout},$self->{'binmode'});
		}
		if ( $self->{compress} && (($self->{jobs} > 1) || ($self->{oracle_copies} > 1)) ) {
			die "FATAL: you can't use compressed output with parallel dump\n";
		}
	}

}

sub remove_export_file
{
	my ($self, $outfile) = @_;


	# Init with configuration OUTPUT filename
	$outfile ||= $self->{output};
	if ($self->{input_file} && ($outfile eq $self->{input_file})) {
		$self->logit("FATAL: input file is the same as output file: $outfile, can not overwrite it.\n",0,1);
	}
	if ($outfile) {
		if ($self->{output_dir} && $outfile) {
			$outfile = $self->{output_dir} . "/" . $outfile;
		}
		unlink($outfile);
	}

}




=head2 append_export_file FILENAME

Open a file handle to a given filename to append data.

=cut

sub append_export_file
{
	my ($self, $outfile, $noprefix) = @_;

	my $filehdl = undef;

	if ($outfile) {
		if ($self->{output_dir} && !$noprefix) {
			$outfile = $self->{output_dir} . '/' . $outfile;
		}
		# If user request data compression
		if ($self->{compress}) {
			die "FATAL: you can't use compressed output with parallel dump\n";
		} else {
			$filehdl = new IO::File;
			$filehdl->open(">>$outfile") or $self->logit("FATAL: Can't open $outfile: $!\n", 0, 1);
			binmode($filehdl, $self->{'binmode'});
			$filehdl->autoflush(1);
		}
	}

	return $filehdl;
}



=head2 close_export_file FILEHANDLE

Close a file handle.

=cut

sub close_export_file
{
	my ($self, $filehdl) = @_;


	return if (!defined $filehdl);

	if ($self->{output} =~ /\.gz$/) {
		$filehdl->gzclose();
	} else {
		$filehdl->close();
	}

}

=head2 modify_struct TABLE_NAME ARRAYOF_FIELDNAME

Modify the table structure during the export. Only the specified columns
will be exported. 

=cut

sub modify_struct
{
	my ($self, $table, @fields) = @_;

	if (!$self->{preserve_case}) {
		map { $_ = lc($_) } @fields;
		$table = lc($table);
	}

	push(@{$self->{modify}{$table}}, @fields);

}

=head2 quote_reserved_words

Return a quoted object named if it is a PostgreSQL reserved word

=cut

sub quote_reserved_words
{
	my ($self, $obj_name) = @_;

	if ($self->{use_reserved_words}) {
		if ($obj_name && grep(/^$obj_name$/i, @KEYWORDS)) {
			return '"' . $obj_name . '"';
		}
	}
	if (!$self->{preserve_case}) {
		$obj_name = lc($obj_name);
	}

	return $obj_name;
}

=head2 is_reserved_words

Return 1 if the given object name is a PostgreSQL reserved word

=cut

sub is_reserved_words
{
	my ($obj_name) = @_;

	if ($obj_name && grep(/^$obj_name$/i, @KEYWORDS)) {
		return 1;
	}
	return 0;
}


=head2 replace_tables HASH

Modify table names during the export.

=cut

sub replace_tables
{
	my ($self, %tables) = @_;

	foreach my $t (keys %tables) {
		$self->{replaced_tables}{"\L$t\E"} = $tables{$t};
	}

}

=head2 replace_cols HASH

Modify column names during the export.

=cut

sub replace_cols
{
	my ($self, %cols) = @_;

	foreach my $t (keys %cols) {
		foreach my $c (keys %{$cols{$t}}) {
			$self->{replaced_cols}{"\L$t\E"}{"\L$c\E"} = $cols{$t}{$c};
		}
	}

}

=head2 set_where_clause HASH

Add a WHERE clause during data export on specific tables or on all tables

=cut

sub set_where_clause
{
	my ($self, $global, %table_clause) = @_;

	$self->{global_where} = $global;
	foreach my $t (keys %table_clause) {
		$self->{where}{"\L$t\E"} = $table_clause{$t};
	}

}



#### Private subroutines ####

=head1 PRIVATE METHODS

=head2 _init HASH_OPTIONS

Initialize an Ora2Pg object instance with a connexion to the
Oracle database.

=cut

sub _init
{
	my ($self, %options) = @_;

	# Read configuration file
	$self->read_config($options{config}) if ($options{config});

	# Those are needed by DBI
	$ENV{ORACLE_HOME} = $AConfig{'ORACLE_HOME'} if ($AConfig{'ORACLE_HOME'});
	$ENV{NLS_LANG} = $AConfig{'NLS_LANG'} if ($AConfig{'NLS_LANG'});

	# Init arrays
	$self->{default_tablespaces} = ();
	$self->{limited} = ();
	$self->{excluded} = ();
	$self->{view_as_table} = ();
	$self->{modify} = ();
	$self->{replaced_tables} = ();
	$self->{replaced_cols} = ();
	$self->{replace_as_boolean} = ();
	$self->{ora_boolean_values} = ();
	$self->{null_equal_empty} = 1;
	$self->{estimate_cost} = 0;
	$self->{where} = ();
	$self->{ora_reserved_words} = (); 
	$self->{defined_pk} = ();
	$self->{allow_partition} = ();

	# Init PostgreSQL DB handle
	$self->{dbhdest} = undef;
	$self->{idxcomment} = 0;
	$self->{standard_conforming_strings} = 1;
	$self->{allow_code_break} = 1;
	$self->{create_schema} = 1;
	$self->{external_table} = ();

	# Used to precise if we need to prefix partition tablename with main tablename
	$self->{prefix_partition} = 0;

	# Use to preserve the data export type with geometry objects
	$self->{local_type} = '';

	# Shall we log on error during data import or abort.
	$self->{log_on_error} = 0;

	# Initialyze following configuration file
	foreach my $k (sort keys %AConfig) {
		if (lc($k) eq 'allow') {
			$self->{limited} = $AConfig{ALLOW};
		} elsif (lc($k) eq 'exclude') {
			$self->{excluded} = $AConfig{EXCLUDE};
		} elsif (lc($k) eq 'view_as_table') {
			$self->{view_as_table} = $AConfig{VIEW_AS_TABLE};
		} else {
			$self->{lc($k)} = $AConfig{$k};
		}
	}
	# Exclude table generated by partition logging
	push(@{$self->{excluded}}, 'MLOG\$_.*', 'RUPD\$_.*');

	# Set default system user/schema to not export
	push(@{$self->{sysusers}}, 'SYSTEM','SYS','DBSNMP','OUTLN','PERFSTAT','CTXSYS','XDB','WMSYS','SYSMAN','SQLTXPLAIN','MDSYS','EXFSYS','ORDSYS','DMSYS','OLAPSYS','FLOWS_020100','FLOWS_FILES','TSMSYS','WKSYS','FLOWS_030000');

	# Set default tablespace to exclude when using USE_TABLESPACE
	push(@{$self->{default_tablespaces}}, 'TEMP', 'USERS','SYSTEM');

	# Default boolean values
	foreach my $k (keys %BOOLEAN_MAP) {
		$self->{ora_boolean_values}{lc($k)} = $BOOLEAN_MAP{$k};
	}
	# additional boolean values given from config file
	foreach my $k (keys %{$self->{boolean_values}}) {
		$self->{ora_boolean_values}{lc($k)} = $AConfig{BOOLEAN_VALUES}{$k};
	}

	# Set transaction isolation level
	if ($self->{transaction} eq 'readonly') {
		$self->{transaction} = 'SET TRANSACTION READ ONLY';
	} elsif ($self->{transaction} eq 'readwrite') {
		$self->{transaction} = 'SET TRANSACTION READ WRITE';
	} elsif ($self->{transaction} eq 'committed') {
		$self->{transaction} = 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED';
	} else {
		$self->{transaction} = 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE';
	}

	# Set default cost unit value to 5 minutes
	$self->{cost_unit_value} ||= 5;

	# Defined if column order must be optimized
	$self->{reordering_columns} ||= 0;

	# Initialize suffix that may be added to the index name
	$self->{indexes_suffix} ||= '';

	# Disable synchronous commit for pg data load
	$self->{synchronous_commit} ||= 0;

	# Autodetect spatial type
	$self->{autodetect_spatial_type} ||= 0;

	# Overwrite configuration with all given parameters
	# and try to preserve backward compatibility
	foreach my $k (keys %options) {
		if ($options{allow} && (lc($k) eq 'allow')) {
			$self->{limited} = ();
			push(@{$self->{limited}}, split(/[\s\t;,]+/, $options{allow}) );
		# preserve backward compatibility
		} elsif ($options{tables} && (lc($k) eq 'tables')) {
			$self->{limited} = ();
			push(@{$self->{limited}}, split(/[\s\t;,]+/, $options{tables}) );
		} elsif ($options{exclude} && (lc($k) eq 'exclude')) {
			$self->{excluded} = ();
			push(@{$self->{excluded}}, split(/[\s\t;,]+/, $options{exclude}) );
		} elsif ($options{view_as_table} && (lc($k) eq 'view_as_table')) {
			$self->{view_as_table} = ();
			push(@{$self->{view_as_table}}, split(/[\s\t;,]+/, $options{view_as_table}) );
		} elsif ($options{datasource} && (lc($k) eq 'datasource')) {
			$self->{oracle_dsn} = $options{datasource};
		} elsif ($options{user} && (lc($k) eq 'user')) {
			$self->{oracle_user} = $options{user};
		} elsif ($options{password} && (lc($k) eq 'password')) {
			$self->{oracle_pwd} = $options{password};
		} elsif ($options{$k} && $options{$k} ne '') {
			$self->{"\L$k\E"} = $options{$k};
		}
	}

	if ($AConfig{'DEBUG'} == 1) {
		$self->{debug} = 1;
	}
	# Set default XML data extract method
	if (not defined $self->{xml_pretty} || ($self->{xml_pretty} != 0)) {
		$self->{xml_pretty} = 1;
	}
	if (!$self->{fdw_server}) {
		$self->{fdw_server} = 'orcl';
	}

	# Log file handle
	$self->{fhlog} = undef;
	if ($self->{logfile}) {
		$self->{fhlog} = new IO::File;
		$self->{fhlog}->open(">>$self->{logfile}") or $self->logit("FATAL: can't log to $self->{logfile}, $!\n", 0, 1);
	}

	# Autoconvert SRID
	if (not defined $self->{convert_srid} || ($self->{convert_srid} != 0)) {
		$self->{convert_srid} = 1;
	}

	# Free some memory
	%options = ();
	%AConfig = ();

	# Multiprocess init
	$self->{jobs} ||= 1;
	$self->{child_count}  = 0;
	# backward compatibility
	if ($self->{thread_count}) {
		$self->{jobs} = $self->{thread_count} || 1;
	}
	$self->{has_utf8_fct} = 1;
	eval { utf8::valid("test utf8 function"); };
	if ($@) {
		# Old perl install doesn't include these functions
		$self->{has_utf8_fct} = 0;
	}

	# Multiple Oracle connection
	$self->{oracle_copies} ||= 0;
	$self->{ora_conn_count} = 0;
	$self->{data_limit} ||= 10000;
	$self->{disable_partition} ||= 0;

	# Set user defined data type translation
	if ($self->{data_type}) {
		my @transl = split(/[,;]/, $self->{data_type});
		foreach my $t (@transl) {
			my ($typ, $val) = split(/:/, $t);
			$typ =~ s/^\s+//;
			$typ =~ s/\s+$//;
			$val =~ s/^\s+//;
			$val =~ s/\s+$//;
			$TYPE{$typ} = $val if ($val);
		}
	}

	# Set some default
	$self->{global_where} ||= '';
	$self->{prefix} = 'DBA';
	if ($self->{user_grants}) {
		$self->{prefix} = 'ALL';
	}
	$self->{bzip2} ||= '/usr/bin/bzip2';
	$self->{default_numeric} ||= 'bigint';
	$self->{type_of_type} = ();
	$self->{dump_as_html} ||= 0;
	$self->{top_max} ||= 10;

	# backward compatibility
	if ($self->{disable_table_triggers}) {
		$self->{disable_triggers} = $self->{disable_table_triggers};
	}
	$self->{binmode} ||= ':raw';
	$self->{binmode} =~ s/^://;
	$self->{binmode} = ':' . lc($self->{binmode});
	# Set some default values
	if ($self->{enable_microsecond} eq '') {
		$self->{enable_microsecond} = 1;
	} 
	if ($self->{external_to_fdw} eq '') {
		$self->{external_to_fdw} = 1;
	}
	if ($self->{pg_supports_insteadof} eq '') {
		$self->{pg_supports_insteadof} = 1;
	}
	if ($self->{pg_supports_mview} eq '') {
		$self->{pg_supports_mview} = 1;
	}
	$self->{pg_supports_checkoption} ||= 0;

	# Backward compatibility with LongTrunkOk with typo
	if ($self->{longtrunkok} && not defined $self->{longtruncok}) {
		$self->{longtruncok} = $self->{longtrunkok};
	}
	$self->{longtruncok} = 0 if (not defined $self->{longtruncok});
	$self->{longreadlen} ||= (1024*1024);
	# Backward compatibility with PG_NUMERIC_TYPE alone
	$self->{pg_integer_type} = 1 if (not defined $self->{pg_integer_type});
	# Backward compatibility with CASE_SENSITIVE
	$self->{preserve_case} = $self->{case_sensitive} if (defined $self->{case_sensitive} && not defined $self->{preserve_case});

	if (($self->{standard_conforming_strings} =~ /^off$/i) || ($self->{standard_conforming_strings} == 0)) {
		$self->{standard_conforming_strings} = 0;
	} else {
		$self->{standard_conforming_strings} = 1;
	}
	$self->{compile_schema} ||= 0;
	$self->{export_invalid} ||= 0;
	$self->{use_reserved_words} ||= 0;
	$self->{pkey_in_create} ||= 0;
	# Should we add SET ON_ERROR_STOP to generated SQL files
	$self->{stop_on_error} = 1 if (not defined $self->{stop_on_error});
	# Force foreign keys to be created initialy deferred if export type
	# is TABLE or to set constraint deferred with data export types/
	$self->{defer_fkey} ||= 0;

	# Allow multiple or chained extraction export type
	$self->{export_type} = ();
	if ($self->{type}) {
		@{$self->{export_type}} = split(/[\s\t,;]+/, $self->{type});
		# Assume backward comaptibility with DATA replacement by INSERT
		map { s/^DATA$/INSERT/; } @{$self->{export_type}};
	} else {
		push(@{$self->{export_type}}, 'TABLE');
	}
	# If you decide to autorewrite PLSQL code, this load the dedicated
	# Perl module
	$self->{plsql_pgsql} = 1 if ($self->{estimate_cost});
	if ($self->{plsql_pgsql}) {
		use Ora2Pg::PLSQL;
	}

	$self->{fhout} = undef;
	$self->{compress} = '';
	$self->{pkgcost} = 0;
	$self->{total_pkgcost} = 0;

	if ($^O =~ /MSWin32|dos/i) {
		if ( ($self->{oracle_copies} > 1) || ($self->{jobs} > 1) ) {
			$self->logit("WARNING: multiprocess is not supported under that kind of OS.\n", 0);
			$self->logit("If you need full speed at data export, please use Linux instead.\n", 0);
		}
		$self->{oracle_copies} = 0;
		$self->{jobs} = 0;
	}

	if (!$self->{input_file}) {
		# Connect the database
		$self->{dbh} = $self->_oracle_connection();

		# Auto detect character set
		if ($self->_init_oracle_connection($self->{dbh})) {
			$self->{dbh}->disconnect();
			$self->{dbh} = $self->_oracle_connection(1);
			$self->_init_oracle_connection($self->{dbh}, 1);
		}

		# Compile again all objects in the schema
		if ($self->{compile_schema}) {
			if ($self->{debug} && $self->{compile_schema}) {
				$self->logit("Force Oracle to compile schema before code extraction\n", 1);
			}
			$self->_compile_schema($self->{dbh}, uc($self->{compile_schema}));
		}

	} else {
		$self->{plsql_pgsql} = 1;
		if (grep(/^$self->{type}$/, 'TABLE', 'SEQUENCE', 'GRANT', 'TABLESPACE', 'VIEW', 'TRIGGER', 'QUERY', 'FUNCTION','PROCEDURE','PACKAGE','TYPE')) {
			$self->export_schema();
		} else {
			$self->logit("FATAL: bad export type using input file option\n", 0, 1);
		}
		return;
	}

	# Get the Oracle version
	$self->{db_version} = $self->_get_version() if (!$self->{input_file});

	# Retreive all table informations
        foreach my $t (@{$self->{export_type}}) {
                $self->{type} = $t;
		if (($self->{type} eq 'TABLE') || ($self->{type} eq 'FDW') || ($self->{type} eq 'INSERT') || ($self->{type} eq 'COPY') || ($self->{type} eq 'KETTLE')) {
			$self->{plsql_pgsql} = 1;
			$self->_tables();
		} elsif ($self->{type} eq 'VIEW') {
			$self->_views();
		} elsif ($self->{type} eq 'GRANT') {
			$self->_grants();
		} elsif ($self->{type} eq 'SEQUENCE') {
			$self->_sequences();
		} elsif ($self->{type} eq 'TRIGGER') {
			$self->_triggers();
		} elsif ($self->{type} eq 'FUNCTION') {
			$self->_functions();
		} elsif ($self->{type} eq 'PROCEDURE') {
			$self->_procedures();
		} elsif ($self->{type} eq 'PACKAGE') {
			$self->_packages();
		} elsif ($self->{type} eq 'TYPE') {
			$self->_types();
		} elsif ($self->{type} eq 'TABLESPACE') {
			$self->_tablespaces();
		} elsif ($self->{type} eq 'PARTITION') {
			$self->_partitions();
		} elsif ($self->{type} eq 'MVIEW') {
			$self->_materialized_views();
		} elsif (($self->{type} eq 'SHOW_REPORT') || ($self->{type} eq 'SHOW_VERSION') || ($self->{type} eq 'SHOW_SCHEMA') || ($self->{type} eq 'SHOW_TABLE') || ($self->{type} eq 'SHOW_COLUMN') || ($self->{type} eq 'SHOW_ENCODING')) {
			$self->_show_infos($self->{type});
			$self->{dbh}->disconnect() if ($self->{dbh}); 
			exit 0;
		} else {
			warn "type option must be TABLE, VIEW, GRANT, SEQUENCE, TRIGGER, PACKAGE, FUNCTION, PROCEDURE, PARTITION, TYPE, INSERT, COPY, TABLESPACE, SHOW_REPORT, SHOW_VERSION, SHOW_SCHEMA, SHOW_TABLE, SHOW_COLUMN, SHOW_ENCODING, FDW, MVIEW, QUERY, KETTLE\n";
		}
		# Mofify export structure if required
		if ($self->{type} =~ /^(INSERT|COPY)$/) {
			for my $t (keys %{$self->{'modify_struct'}}) {
				$self->modify_struct($t, @{$self->{'modify_struct'}{$t}});
			}
		}
		$self->replace_tables(%{$self->{'replace_tables'}});
		$self->replace_cols(%{$self->{'replace_cols'}});
		$self->set_where_clause($self->{"global_where"}, %{$self->{'where'}});
	}

	if ( ($self->{type} eq 'INSERT') || ($self->{type} eq 'COPY') || ($self->{type} eq 'KETTLE') ) {
		if ( ($self->{type} eq 'KETTLE') && !$self->{pg_dsn} ) {
			$self->logit("FATAL: PostgreSQL connection datasource must be defined with KETTLE export.\n", 0, 1);
		} elsif ($self->{type} ne 'KETTLE') {
			$self->{dbhdest} = $self->_send_to_pgdb() if ($self->{pg_dsn} && !$self->{dbhdest});
		}
	}

	# Disconnect from the database
	$self->{dbh}->disconnect() if ($self->{dbh});

}


sub _oracle_connection
{
	my ($self, $quiet) = @_;

	$self->logit("Trying to connect to database: $self->{oracle_dsn}\n", 1) if (!$quiet);

	my $dbh = DBI->connect($self->{oracle_dsn}, $self->{oracle_user}, $self->{oracle_pwd}, {ora_envhp => 0, LongReadLen=>$self->{longreadlen}, LongTruncOk=>$self->{longtruncok} });

	# Fix a problem when exporting type LONG and LOB
	$dbh->{'LongReadLen'} = $self->{longreadlen};
	$dbh->{'LongTruncOk'} = $self->{longtruncok};

	# Check for connection failure
	if (!$dbh) {
		$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
	}

	# Use consistent reads for concurrent dumping...
	$dbh->begin_work || $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	if ($self->{debug} && !$quiet) {
		$self->logit("Isolation level: $self->{transaction}\n", 1);
	}
	my $sth = $dbh->prepare($self->{transaction}) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	$sth->finish;
	undef $sth;

	return $dbh;
}

# use to set encoding
sub _init_oracle_connection
{
	my ($self, $dbh, $quiet) = @_;

	my $must_reconnect = 0;

	# Auto detect character set
	if ($self->{debug} && !$quiet) {
		$self->logit("Auto detecting Oracle character set and the corresponding PostgreSQL client encoding to use.\n", 1);
	}
	my $encoding = $self->_get_encoding($dbh);
	if (!$self->{nls_lang}) {
		$self->{nls_lang} = $encoding;
		$ENV{NLS_LANG} = $self->{nls_lang};
		if ($self->{debug} && !$quiet) {
			$self->logit("\tUsing Oracle character set: $self->{nls_lang}.\n", 1);
		}
		$must_reconnect = 1;
	} else {
		$ENV{NLS_LANG} = $self->{nls_lang};
		if ($self->{debug} && !$quiet) {
			$self->logit("\tUsing the character set given in NLS_LANG configuration directive ($self->{nls_lang}).\n", 1);
		}
	}

	if (!$self->{client_encoding}) {
		if ($self->{'binmode'} =~ /utf8/i) {
			$self->{client_encoding} = 'UTF8';
			if ($self->{debug} && !$quiet) {
				$self->logit("\tUsing PostgreSQL client encoding forced to UTF8 as BINMODE configuration directive has been set to $self->{binmode}.\n", 1);
			}
		} else {
			$self->{client_encoding} = &auto_set_encoding($encoding);
			if ($self->{debug} && !$quiet) {
				$self->logit("\tUsing PostgreSQL client encoding: $self->{client_encoding}.\n", 1);
			}
		}
	} else {
		if ($self->{debug} && !$quiet) {
			$self->logit("\tUsing PostgreSQL client encoding given in CLIENT_ENCODING configuration directive ($self->{client_encoding}).\n", 1);
		}
	}

	return $must_reconnect;
}


# We provide a DESTROY method so that the autoloader doesn't
# bother trying to find it. We also close the DB connexion
sub DESTROY
{
	my $self = shift;

	#$self->{dbh}->disconnect() if ($self->{dbh});

}


=head2 _send_to_pgdb DEST_DATASRC DEST_USER DEST_PASSWD

Open a DB handle to a PostgreSQL database

=cut

sub _send_to_pgdb
{
	my ($self, $destsrc, $destuser, $destpasswd) = @_;

	eval("use DBD::Pg qw(:pg_types);");

	# Init with configuration options if no parameters
	$destsrc ||= $self->{pg_dsn};
	$destuser ||= $self->{pg_user};
	$destpasswd ||= $self->{pg_pwd};

        # Then connect the destination database
        my $dbhdest = DBI->connect($destsrc, $destuser, $destpasswd);

	$destsrc =~ /dbname=([^;]*)/;
	$self->{dbname} = $1;
	$destsrc =~ /host=([^;]*)/;
	$self->{dbhost} = $1;
	$self->{dbhost} = 'localhost' if (!$self->{dbhost});
	$destsrc =~ /port=([^;]*)/;
	$self->{dbport} = $1;
	$self->{dbport} = 5432 if (!$self->{dbport});
	$self->{dbuser} = $destuser;
	$self->{dbpwd} = $destpasswd;

        # Check for connection failure
        if (!$dbhdest) {
		$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
	}

	return $dbhdest;
}

# Backward Compatibility
sub send_to_pgdb
{
	return &_send_to_pgdb(@_);

}

=head2 _grants

This function is used to retrieve all privilege information.

It extracts all Oracle's ROLES to convert them to Postgres groups (or roles)
and searches all users associated to these roles.

=cut

sub _grants
{
	my ($self) = @_;

	$self->logit("Retrieving users/roles/grants information...\n", 1);
	($self->{grants}, $self->{roles}) = $self->_get_privilege();
}


=head2 _sequences

This function is used to retrieve all sequences information.

=cut

sub _sequences
{
	my ($self) = @_;

	$self->logit("Retrieving sequences information...\n", 1);
	$self->{sequences} = $self->_get_sequences();

}


=head2 _triggers

This function is used to retrieve all triggers information.

=cut

sub _triggers
{
	my ($self) = @_;

	$self->logit("Retrieving triggers information...\n", 1);
	$self->{triggers} = $self->_get_triggers();

}


=head2 _functions

This function is used to retrieve all functions information.

=cut

sub _functions
{
	my $self = shift;

	$self->logit("Retrieving functions information...\n", 1);
	$self->{functions} = $self->_get_functions();

}

=head2 _procedures

This function is used to retrieve all procedures information.

=cut

sub _procedures
{
	my $self = shift;

	$self->logit("Retrieving procedures information...\n", 1);
	$self->{procedures} = $self->_get_procedures();
}


=head2 _packages

This function is used to retrieve all packages information.

=cut

sub _packages
{
	my ($self) = @_;

	$self->logit("Retrieving packages information...\n", 1);
	$self->{packages} = $self->_get_packages();

}


=head2 _types

This function is used to retrieve all custom types information.

=cut

sub _types
{
	my ($self) = @_;

	$self->logit("Retrieving user defined types information...\n", 1);
	$self->{types} = $self->_get_types();

}

=head2 _tables

This function is used to retrieve all table information.

Sets the main hash of the database structure $self->{tables}.
Keys are the names of all tables retrieved from the current
database. Each table information is composed of an array associated
to the table_info key as array reference. In other way:

    $self->{tables}{$class_name}{table_info} = [(OWNER,TYPE,COMMENT,NUMROW)];

DBI TYPE can be TABLE, VIEW, SYSTEM TABLE, GLOBAL TEMPORARY, LOCAL TEMPORARY,
ALIAS, SYNONYM or a data source specific type identifier. This only extracts
the TABLE type.

It also gets the following information in the DBI object to affect the
main hash of the database structure :

    $self->{tables}{$class_name}{field_name} = $sth->{NAME};
    $self->{tables}{$class_name}{field_type} = $sth->{TYPE};

It also calls these other private subroutines to affect the main hash
of the database structure :

    @{$self->{tables}{$class_name}{column_info}} = $self->_column_info($class_name, $owner);
    %{$self->{tables}{$class_name}{unique_key}}  = $self->_unique_key($class_name, $owner);
    @{$self->{tables}{$class_name}{foreign_key}} = $self->_foreign_key($class_name, $owner);
    %{$self->{tables}{$class_name}{check_constraint}}  = $self->_check_constraint($class_name, $owner);

=cut

sub _tables
{
	my ($self, $nodetail) = @_;

	# Get all tables information specified by the DBI method table_info
	$self->logit("Retrieving table information...\n", 1);

	# Retrieve tables informations
	my %tables_infos = $self->_table_info();

	# Get detailed informations on each tables
	if (!$nodetail) {
		# Retrieve all column's details
		my %columns_infos = $self->_column_info('',$self->{schema});
		foreach my $tb (keys %columns_infos) {
			next if (!exists $tables_infos{$tb});
			foreach my $c (keys %{$columns_infos{$tb}}) {
				push(@{$self->{tables}{$tb}{column_info}{$c}}, @{$columns_infos{$tb}{$c}});
			}
		}
		%columns_infos = ();

		# Retrieve comment of each columns
		my %columns_comments = $self->_column_comments('',$self->{schema});
		foreach my $tb (keys %columns_comments) {
			next if (!exists $tables_infos{$tb});
			foreach my $c (keys %{$columns_comments{$tb}}) {
				$self->{tables}{$tb}{column_comments}{$c} = $columns_comments{$tb}{$c};
			}
		}

		# Extract foreign keys informations
		if (!$self->{skip_fkeys}) {
			my ($foreign_link, $foreign_key) = $self->_foreign_key('',$self->{schema});
			foreach my $tb (keys %{$foreign_link}) {
				next if (!exists $tables_infos{$tb});
				%{$self->{tables}{$tb}{foreign_link}} =  %{$foreign_link->{$tb}};
			}
			foreach my $tb (keys %{$foreign_key}) {
				next if (!exists $tables_infos{$tb});
				push(@{$self->{tables}{$tb}{foreign_key}}, @{$foreign_key->{$tb}});
			}
		}
	}

	# Retrieve all unique keys informations
	my %unique_keys = $self->_unique_key('',$self->{schema});
	foreach my $tb (keys %unique_keys) {
		next if (!exists $tables_infos{$tb});
		foreach my $c (keys %{$unique_keys{$tb}}) {
			$self->{tables}{$tb}{unique_key}{$c} = $unique_keys{$tb}{$c};
		}
	}
	%unique_keys = ();

	# Retrieve check constraints
	if (!$self->{skip_checks}) {
		my %check_constraints = $self->_check_constraint('',$self->{schema});
		foreach my $tb (keys %check_constraints) {
			next if (!exists $tables_infos{$tb});
			%{$self->{tables}{$tb}{check_constraint}} = ( %{$check_constraints{$tb}});
		}
	}

	# Retrieve all indexes informations
	if (!$self->{skip_indices} && !$self->{skip_indexes}) {
		my ($uniqueness, $indexes, $idx_type, $idx_tbsp) = $self->_get_indexes('',$self->{schema});
		foreach my $tb (keys %{$uniqueness}) {
			next if (!exists $tables_infos{$tb});
			%{$self->{tables}{$tb}{uniqueness}} = %{$uniqueness->{$tb}};
		}
		foreach my $tb (keys %{$indexes}) {
			next if (!exists $tables_infos{$tb});
			%{$self->{tables}{$tb}{indexes}} = %{$indexes->{$tb}};
		}
		foreach my $tb (keys %{$idx_type}) {
			next if (!exists $tables_infos{$tb});
			%{$self->{tables}{$tb}{idx_type}} = %{$idx_type->{$tb}};
		}
		foreach my $tb (keys %{$idx_tbsp}) {
			next if (!exists $tables_infos{$tb});
			%{$self->{tables}{$tb}{idx_tbsp}} = %{$idx_tbsp->{$tb}};
		}
	}

	my @done = ();
	my $id = 0;
	# Set the table information for each class found
	my $i = 1;
	my $num_total_table = scalar keys %tables_infos;
	foreach my $t (sort keys %tables_infos) {

		# forget or not this object if it is in the exclude or allow lists.
		if ($self->{tables}{$t}{table_info}{type} ne 'view') {
			next if ($self->skip_this_object('TABLE', $t));
		}
		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i, $num_total_table, 25, '=', 'tables', "scanning table $t" );
		}

		if (grep(/^$t$/, @done)) {
			$self->logit("Duplicate entry found: $t\n", 1);
		} else {
			push(@done, $t);
		} 
		$self->logit("[$i] Scanning table $t ($tables_infos{$t}{num_rows} rows)...\n", 1);
		
		# Check of uniqueness of the table
		if (exists $self->{tables}{$t}{field_name}) {
			$self->logit("Warning duplicate table $t, maybe a SYNONYM ? Skipped.\n", 1);
			next;
		}
		# Try to respect order specified in the TABLES limited extraction array
		$self->{tables}{$t}{internal_id} = 0;
		if ($#{$self->{limited}} >= 0) {
			for (my $j = 0; $j <= $#{$self->{limited}}; $j++) {
				if (uc($self->{limited}->[$j]) eq uc($t)) {
					$self->{tables}{$t}{internal_id} = $j;
					last;
				}
			}
		}

		# usually TYPE,COMMENT,NUMROW
		$self->{tables}{$t}{table_info}{type} = $tables_infos{$t}{type};
		$self->{tables}{$t}{table_info}{comment} = $tables_infos{$t}{comment};
		$self->{tables}{$t}{table_info}{num_rows} = $tables_infos{$t}{num_rows};
		$self->{tables}{$t}{table_info}{owner} = $tables_infos{$t}{owner};
		$self->{tables}{$t}{table_info}{tablespace} = $tables_infos{$t}{tablespace};

		# Set the fields information
		my $query = "SELECT * FROM \"$tables_infos{$t}{owner}\".\"$t\" WHERE 1=0";
		my $sth = $self->{dbh}->prepare($query);
		if (!defined($sth)) {
			warn "Can't prepare statement: $DBI::errstr";
			next;
		}
		$sth->execute;
		if ($sth->err) {
			warn "Can't execute statement: $DBI::errstr";
			next;
		}
		$self->{tables}{$t}{type} = 'table';
		$self->{tables}{$t}{field_name} = $sth->{NAME};
		$self->{tables}{$t}{field_type} = $sth->{TYPE};
		$i++;
	}

	if (!$self->{quiet} && !$self->{debug}) {
		print STDERR $self->progress_bar($i - 1, $num_total_table, 25, '=', 'tables', 'end of scanning.'), "\n";
	}
 
	# Try to search requested TABLE names in the VIEW names if not found in
	# real TABLE names
	if ($#{$self->{view_as_table}} >= 0) {
		my %view_infos = $self->_get_views();
		foreach my $view (sort keys %view_infos) {
			# Set the table information for each class found
			# Jump to desired extraction
			next if (!grep($view =~ /^$_$/i, @{$self->{view_as_table}}));
			$self->logit("Scanning view $view to export as table...\n", 1);

			$self->{tables}{$view}{type} = 'view';
			$self->{tables}{$view}{text} = $view_infos{$view};
			$self->{tables}{$view}{alias}= $view_infos{$view}{alias};
			my $realview = $view;
			if ($view !~ /"/) {
				$realview = "\"$view\"";
			}
			if ($self->{schema}) {
				$realview = "\"$self->{schema}\".$realview";
			}
			# Set the fields information
			my $sth = $self->{dbh}->prepare("SELECT * FROM $realview WHERE 1=0");
			if (!defined($sth)) {
				warn "Can't prepare statement: $DBI::errstr";
				next;
			}
			$sth->execute;
			if ($sth->err) {
				warn "Can't execute statement: $DBI::errstr";
				next;
			}
			$self->{tables}{$view}{field_name} = $sth->{NAME};
			$self->{tables}{$view}{field_type} = $sth->{TYPE};
			my %columns_infos = $self->_column_info($view, $self->{schema});
			foreach my $tb (keys %columns_infos) {
				next if ($tb ne $view);
				foreach my $c (keys %{$columns_infos{$tb}}) {
					push(@{$self->{tables}{$view}{column_info}{$c}}, @{$columns_infos{$tb}{$c}});
				}
			}
		}
	}

	# Look at external tables
	if ($self->{db_version} !~ /Release 8/) {
		%{$self->{external_table}} = $self->_get_external_tables();
	}

}

sub _split_table_definition
{
	my $str = shift();

	my $ct = '';
	my @parts = split(/([\(\)])/, $str);
	my $def = '';
	my $param = '';
	my $i = 0;
	for (; $i <= $#parts; $i++) {
		$ct++ if ($parts[$i] =~ /\(/);
		$ct-- if ($parts[$i] =~ /\)/);
		if ( ($def ne '') && ($ct == 0) ) {
			last;
		}
		$def .= $parts[$i] if ($def || ($parts[$i] ne '('));
	}
	$i++;
	for (; $i <= $#parts; $i++) {
		$param .= $parts[$i];
	}

	$def =~ s/[\t\s]+/ /g;
	$param =~ s/[\t\s]+/ /g;

	return ($def, $param);
}

sub _get_plsql_code
{
	my $str = shift();

	my $ct = '';
	my @parts = split(/(BEGIN|DECLARE|END[\t\s]*(?!IF|LOOP|CASE|INTO|FROM|,)[^;\s\t]*[\t\s]*;)/, $str);
	my $code = '';
	my $other = '';
	my $i = 0;
	for (; $i <= $#parts; $i++) {
		$ct++ if ($parts[$i] =~ /\bBEGIN\b/);
		$ct-- if ($parts[$i] =~ /END[\t\s]*(?!IF|LOOP|CASE|INTO|FROM|,)[^;\s\t]*[\t\s]*;/);
		if ( ($ct ne '') && ($ct == 0) ) {
			$code .= $parts[$i];
			last;
		}
		$code .= $parts[$i];
	}
	$i++;
	for (; $i <= $#parts; $i++) {
		$other .= $parts[$i];
	}

	$code =~ s/[\t\s]+/ /g;
	$other =~ s/[\t\s]+/ /g;

	return ($code, $other);
}


sub _parse_constraint
{
	my ($self, $tb_name, $cur_col_name, $c) = @_;

	if ($c =~ /^([^\s]+) (UNIQUE|PRIMARY KEY)\s*\(([^\)]+)\)/i) {
		my $tp = 'U';
		$tp = 'P' if ($2 eq 'PRIMARY KEY');
		$self->{tables}{$tb_name}{unique_key}{$1} = { (
			type => $tp, 'generated' => 0, 'index_name' => $1,
			columns => ()
		) };
		push(@{$self->{tables}{$tb_name}{unique_key}{$1}{columns}}, split(/\s*,\s*/, $3));
	} elsif ($c =~ /^([^\s]+) CHECK\s*\(([^\)]+)\)/i) {
		my %tmp = ($1 => $2);
		$self->{tables}{$tb_name}{check_constraint}{constraint}{$1} = $2;
	} elsif ($c =~ /^([^\s]+) FOREIGN KEY (\([^\)]+\))?\s*REFERENCES ([^\(]+)\(([^\)]+)\)/i) {
		my $c_name = $1;
		if ($2) {
			$cur_col_name = $2;
		}
		my $f_tb_name = $3;
		my @col_list = split(/,/, $4);
		$c_name =~ s/"//g;
		$f_tb_name =~ s/"//g;
		$cur_col_name =~ s/[\("\)]//g;
		map { s/"//g; } @col_list;
		if (!$self->{export_schema}) {
			$f_tb_name =~ s/^[^\.]+\.//;
			$f_tb_name =~ s/^[^\.]+\.//;
			map { s/^[^\.]+\.//; } @col_list;
		}
		push(@{$self->{tables}{$tb_name}{foreign_link}{"\U$c_name\E"}{local}}, $cur_col_name);
		push(@{$self->{tables}{$tb_name}{foreign_link}{"\U$c_name\E"}{remote}{$f_tb_name}}, @col_list);
		my $deferrable = '';
		$deferrable = 'DEFERRABLE' if ($c =~ /DEFERRABLE/);
		my $deferred = '';
		$deferred = 'DEFERRED' if ($c =~ /INITIALLY DEFERRED/);
		# CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,$deferrable,DEFERRED,R_OWNER,TABLE_NAME,OWNER
		push(@{$self->{tables}{$tb_name}{foreign_key}}, [ ($c_name,'','','',$deferrable,$deferred,'',$tb_name,'') ]);
	}
}

sub _get_dml_from_file
{
	my $self = shift;

	# Load file in a single string
	if (not open(INFILE, $self->{input_file})) {
		die "FATAL: can't read file $self->{input_file}, $!\n";
	}
	my $content = '';
	while (my $l = <INFILE>) {
		chomp($l);
		$l =~ s/\r//g;
		$l =~ s/\t+/ /g;
		$content =~ s/\-\-.*//;
		next if (!$l);
		$content .= $l . ' ';
	}
	close(INFILE);

	$content =~ s/\/\*(.*?)\*\// /g;
	$content =~ s/CREATE\s+OR\s+REPLACE/CREATE/g;

	return $content;
}

sub read_schema_from_file
{
	my $self = shift;

	# Load file in a single string
	my $content = $self->_get_dml_from_file();

	my $tid = 0; 

	# Remove potential dynamic table creation before parsing
	while ($content =~ s/'(TRUNCATE|CREATE)\s+(GLOBAL|UNIQUE)?\s*(TEMPORARY)?\s*(TABLE|INDEX)([^']+)'//i) {};
	while ($content =~ s/'ALTER\s+TABLE\s*([^']+)'//i) {};

	while ($content =~ s/TRUNCATE TABLE\s+([^;]+);//i) {
		my $tb_name = $1;
		$tb_name =~ s/"//g;
		if (!exists $self->{tables}{$tb_name}{table_info}{type}) {
			$self->{tables}{$tb_name}{table_info}{type} = 'TABLE';
			$self->{tables}{$tb_name}{table_info}{num_rows} = 0;
			$tid++;
			$self->{tables}{$tb_name}{internal_id} = $tid;
		}
		$self->{tables}{$tb_name}{truncate_table} = 1;
	}

	while ($content =~ s/CREATE\s+(GLOBAL)?\s*(TEMPORARY)?\s*TABLE[\s]+([^\s]+)\s+AS\s+([^;]+);//i) {
		my $tb_name = $3;
		$tb_name =~ s/"//g;
		my $tb_def = $4;
		$tb_def =~ s/\s+/ /g;
		$self->{tables}{$tb_name}{table_info}{type} = 'TEMPORARY ' if ($2);
		$self->{tables}{$tb_name}{table_info}{type} .= 'TABLE';
		$self->{tables}{$tb_name}{table_info}{num_rows} = 0;
		$tid++;
		$self->{tables}{$tb_name}{internal_id} = $tid;
		$self->{tables}{$tb_name}{table_as} = $tb_def;
	}

	while ($content =~ s/CREATE\s+(GLOBAL)?\s*(TEMPORARY)?\s*TABLE[\s]+([^\s\(]+)\s*([^;]+);//i) {
		my $tb_name = $3;
		my $tb_def  = $4;
		my $tb_param  = '';
		$tb_name =~ s/"//g;
		$self->{tables}{$tb_name}{table_info}{type} = 'TEMPORARY ' if ($2);
		$self->{tables}{$tb_name}{table_info}{type} .= 'TABLE';
		$self->{tables}{$tb_name}{table_info}{num_rows} = 0;
		$tid++;
		$self->{tables}{$tb_name}{internal_id} = $tid;

		($tb_def, $tb_param) = &_split_table_definition($tb_def);
		my @column_defs = split(/\s*,\s*/, $tb_def);
		map { s/^\s+//; s/\s+$//; } @column_defs;
		# Fix split on scale comma, for example NUMBER(9,4)
		for (my $i = 0; $i <= $#column_defs; $i++) {
			if ($column_defs[$i] =~ /^\d+/) {
				$column_defs[$i-1] .= ",$column_defs[$i]";
				$column_defs[$i] = '';
			}
		}
		# Fix split on multicolumn's constraints, ex: UNIQUE (last_name,first_name) 
		for (my $i = $#column_defs; $i >= 0; $i--) {
			if ( ($column_defs[$i] !~ /\s/) || ($column_defs[$i] =~ /^[^\(]+\) REFERENCES/i) || ($column_defs[$i] =~ /^[^\(]+\) USING INDEX/ii)) {
				$column_defs[$i-1] .= ",$column_defs[$i]";
				$column_defs[$i] = '';
			}
		}
		my $pos = 0;
		my $cur_c_name = '';
		foreach my $c (@column_defs) {
			next if (!$c);
			# Remove things that are not possible with postgres
			$c =~ s/(PRIMARY KEY.*)NOT NULL/$1/i;
			# Rewrite some parts for easiest/generic parsing
			$c =~ s/^(PRIMARY KEY|UNIQUE)/CONSTRAINT ora2pg_ukey_$tb_name $1/i;
			$c =~ s/^CHECK\b/CONSTRAINT ora2pg_ckey_$tb_name CHECK/i;
			$c =~ s/^FOREIGN KEY/CONSTRAINT ora2pg_fkey_$tb_name FOREIGN KEY/i;
			# Get column name
			if ($c =~ s/^\s*([^\s]+)\s*//) {
				my $c_name = $1;
				$c_name =~ s/"//g;
				# Retrieve all columns information
				if (uc($c_name) ne 'CONSTRAINT') {
					$cur_c_name = $c_name;
					my $c_type = '';
					if ($c =~ s/^([^\s\(]+)\s*//) {
						$c_type = $1;
					} else {
						next;
					}
					my $c_length = '';
					my $c_scale = '';
					if ($c =~ s/^\(([^\)]+)\)\s*//) {
						$c_length = $1;
						if ($c_length =~ s/\s*,\s*(\d+)\s*//) {
							$c_scale = $1;
						}
					}
					my $c_nullable = 1;
					if ($c =~ s/CONSTRAINT\s*([^\s]+)?\s*NOT NULL//) {
						$c_nullable = 0;
					} elsif ($c =~ s/NOT NULL//) {
						$c_nullable = 0;
					}

					if (($c =~ s/(UNIQUE|PRIMARY KEY)\s*\(([^\)]+)\)//i) || ($c =~ s/(UNIQUE|PRIMARY KEY)\s*//i)) {
						my $pk_name = 'ora2pg_ukey_' . $c_name; 
						my $cols = $c_name;
						if ($2) {
							$cols = $2;
						}
						$self->_parse_constraint($tb_name, $c_name, "$pk_name $1 ($cols)");

					} elsif ( ($c =~ s/CONSTRAINT\s([^\s]+)\sCHECK\s*\(([^\)]+)\)//i) || ($c =~ s/CHECK\s*\(([^\)]+)\)//i) ) {
						my $pk_name = 'ora2pg_ckey_' . $c_name; 
						my $chk_search = $1;
						if ($2) {
							$pk_name = $1;
							$chk_search = $2;
						}
						$self->_parse_constraint($tb_name, $c_name, "$pk_name CHECK ($chk_search)");

					} elsif ($c =~ s/REFERENCES\s+([^\(]+)\(([^\)]+)\)//i) {

						my $pk_name = 'ora2pg_fkey_' . $c_name; 
						my $chk_search = $1 . "($2)";
						$chk_search =~ s/\s+//g;
						$self->_parse_constraint($tb_name, $c_name, "$pk_name FOREIGN KEY ($c_name) REFERENCES $chk_search");
					}

					my $c_default = '';
					if ($c =~ s/DEFAULT\s+([^\s]+)\s*//) {
						if (!$self->{plsql_pgsql}) {
							$c_default = $1;
						} else {
							$c_default = Ora2Pg::PLSQL::plsql_to_plpgsql($1, $self->{allow_code_break},$self->{null_equal_empty});
						}
					}
					#COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE, DATA_DEFAULT, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH, TABLE_NAME, OWNER
					push(@{$self->{tables}{$tb_name}{column_info}{$c_name}}, ($c_name, $c_type, $c_length, $c_nullable, $c_default, $c_length, $c_scale, $c_length, $tb_name, '', $pos));
				} else {
					$self->_parse_constraint($tb_name, $cur_c_name, $c);
				}
			}
			$pos++;
		}
		map {s/^/\t/; s/$/,\n/; } @column_defs;
		# look for storage information
		if ($tb_param =~ /TABLESPACE[\s]+([^\s]+)/i) {
			$self->{tables}{$tb_name}{table_info}{tablespace} = $1;
			$self->{tables}{$tb_name}{table_info}{tablespace} =~ s/"//g;
		}
		if ($tb_param =~ /PCTFREE\s+(\d+)/i) {
			$self->{tables}{$tb_name}{table_info}{fillfactor} = $1;
		}
		if ($tb_param =~ /\bNOLOGGING\b/i) {
			$self->{tables}{$tb_name}{table_info}{nologging} = 1;
		}

	}

	while ($content =~ s/ALTER\s+TABLE[\s]+([^\s]+)\s+([^;]+);//i) {
		my $tb_name = $1;
		$tb_name =~ s/"//g;
		my $tb_def = $2;
		$tb_def =~ s/\s+/ /g;
		$tb_def =~ s/\s*USING INDEX.*//g;
		if (!exists $self->{tables}{$tb_name}{table_info}{type}) {
			$self->{tables}{$tb_name}{table_info}{type} = 'TABLE';
			$self->{tables}{$tb_name}{table_info}{num_rows} = 0;
			$tid++;
			$self->{tables}{$tb_name}{internal_id} = $tid;
		}
		push(@{$self->{tables}{$tb_name}{alter_table}}, $tb_def);
	}

	while ($content =~ s/CREATE\s+(UNIQUE)?\s*INDEX\s+([^\s]+)\s+ON\s+([^\s\(]+)\s*\(([^;]+);//i) {
		my $is_unique = $1;
		my $idx_name = $2;
		$idx_name =~ s/"//g;
		my $tb_name = $3;
		$tb_name =~ s/\s+/ /g;
		my $idx_def = $4;
		$idx_def =~ s/\s+/ /g;
		$idx_def =~ s/\s*nologging//i;
		$idx_def =~ s/STORAGE\s*\([^\)]+\)\s*//i;
		$idx_def =~ s/COMPRESS(\s+\d+)?\s*//i;
		# look for storage information
		if ($idx_def =~ s/TABLESPACE\s*([^\s]+)\s*//i) {
			$self->{tables}{$tb_name}{idx_tbsp}{$idx_name} = $1;
			$self->{tables}{$tb_name}{idx_tbsp}{$idx_name} =~ s/"//g;
		}
		if ($idx_def =~ s/ONLINE\s*//i) {
			$self->{tables}{$tb_name}{concurrently}{$idx_name} = 1;
		}
		if ($idx_def =~ s/INDEXTYPE\s+IS\s+.*SPATIAL_INDEX//i) {
			$self->{tables}{$tb_name}{spatial}{$idx_name} = 1;
		}
		$idx_def =~ s/\)[^\)]*$//;
		$self->{tables}{$tb_name}{uniqueness}{$idx_name} = $is_unique || '';
                $idx_def =~ s/SYS_EXTRACT_UTC[\s\t]*\(([^\)]+)\)/$1/isg;
		push(@{$self->{tables}{$tb_name}{indexes}{$idx_name}}, $idx_def);
		$self->{tables}{$tb_name}{idx_type}{$idx_name}{type} = 'NORMAL';
		if ($idx_def =~ /\(/) {
			$self->{tables}{$tb_name}{idx_type}{$idx_name}{type} = 'FUNCTION-BASED';
		}

		if (!exists $self->{tables}{$tb_name}{table_info}{type}) {
			$self->{tables}{$tb_name}{table_info}{type} = 'TABLE';
			$self->{tables}{$tb_name}{table_info}{num_rows} = 0;
			$tid++;
			$self->{tables}{$tb_name}{internal_id} = $tid;
		}

	}
	# Extract comments
	$self->read_comment_from_file();
}

sub read_comment_from_file
{
	my $self = shift;

	# Load file in a single string
	my $content = $self->_get_dml_from_file();

	my $tid = 0; 

	while ($content =~ s/COMMENT\s+ON\s+TABLE\s+([^\s]+)\s*IS\s*'([^;]+);//i) {
		my $tb_name = $1;
		my $tb_comment = $2;
		$tb_name =~ s/"//g;
		$tb_comment =~ s/'$//g;
		if (exists $self->{tables}{$tb_name}) {
			$self->{tables}{$tb_name}{table_info}{comment} = $tb_comment;
		}
	}

	while ($content =~ s/COMMENT\s+ON\s+COLUMN\s+([^\s]+)\s*IS\s*'([^;]+);//i) {
		my $tb_name = $1;
		my $tb_comment = $2;
		$tb_name =~ s/"//g;
		$tb_comment =~ s/'$//g;
		if ($tb_name =~ s/\.([^\.]+)$//) {
			if (exists $self->{tables}{$tb_name}) {
					$self->{tables}{$tb_name}{column_comments}{"\L$1\E"} = $tb_comment;
			} elsif (exists $self->{views}{$tb_name}) {
					$self->{views}{$tb_name}{column_comments}{"\L$1\E"} = $tb_comment;
			}
		}
	}

}


sub read_view_from_file
{
	my $self = shift;

	# Load file in a single string
	my $content = $self->_get_dml_from_file();

	my $tid = 0; 

	$content =~ s/CREATE\s+NO\s+FORCE\s+VIEW/CREATE VIEW/g;
	$content =~ s/CREATE\s+FORCE\s+VIEW/CREATE VIEW/g;
	$content =~ s/CREATE VIEW[\s]+([^\s]+)\s+OF\s+(.*?)\s+AS\s+/CREATE VIEW $1 AS /g;
	# Views with aliases
	while ($content =~ s/CREATE\sVIEW[\s]+([^\s]+)\s*\((.*?)\)\s+AS\s+([^;]+);//i) {
		my $v_name = $1;
		$v_name =~ s/"//g;
		my $v_alias = $2;
		my $v_def = $3;
		$v_def =~ s/\s+/ /g;
		$tid++;
	        $self->{views}{$v_name}{text} = $v_def;
		# Remove constraint
		while ($v_alias =~ s/(,[^,\(]+\(.*)$//) {};
		my @aliases = split(/\s*,\s*/, $v_alias);
		foreach (@aliases) {
			my @tmp = split(/\s+/);
			push(@{$self->{views}{$v_name}{alias}}, \@tmp);
		}
	}
	# Standard views
	while ($content =~ s/CREATE\sVIEW[\s]+([^\s]+)\s+AS\s+([^;]+);//i) {
		my $v_name = $1;
		$v_name =~ s/"//g;
		my $v_def = $2;
		$v_def =~ s/\s+/ /g;
		$tid++;
	        $self->{views}{$v_name}{text} = $v_def;
	}

	# Extract comments
	$self->read_comment_from_file();
}

sub read_grant_from_file
{
	my $self = shift;

	# Load file in a single string
	my $content = $self->_get_dml_from_file();

	my $tid = 0; 

	# Extract grant information
	while ($content =~ s/GRANT\s+(.*?)\s+ON\s+([^\s]+)\s+TO\s+([^;]+)(\s+WITH GRANT OPTION)?;//i) {
		my $g_priv = $1;
		my $g_name = $2;
		$g_name =~ s/"//g;
		my $g_user = $3;
		my $g_option = $4;
		$g_priv =~ s/\s+//g;
		$tid++;
		$self->{grants}{$g_name}{type} = '';
		push(@{$self->{grants}{$g_name}{privilege}{$g_user}}, split(/,/, $g_priv));
		if ($g_priv =~ /EXECUTE/) {
			$self->{grants}{$table}{type} = 'PACKAGE BODY';
		} else {
			$self->{grants}{$table}{type} = 'TABLE';
		}
	}
}

sub read_trigger_from_file
{
	my $self = shift;

	# Load file in a single string
	my $content = $self->_get_dml_from_file();

	my $tid = 0; 

	my $doloop = 1;
	do {
		if ($content =~ s/CREATE\s+TRIGGER\s+([^\s]+)\s+(BEFORE|AFTER|INSTEAD\s+OF)\s+(.*?)\s+ON\s+([^\s]+)\s+(.*)//i) {
			my $t_name = $1;
			$t_name =~ s/"//g;
			my $t_pos = $2;
			my $t_event = $3;
			my $tb_name = $4;
			my $trigger = $5;
			my $t_type = '';
			if ($trigger =~ s/^\s*(FOR\s+EACH\s+)(ROW|STATEMENT)\s*//i) {
				$t_type = $1 . $2;
			}
			my $t_when_cond = '';
			if ($trigger =~ s/^\s*WHEN\s+(.*?)\s+((?:BEGIN|DECLARE|CALL).*)//i) {
				$t_when_cond = $1;
				$trigger = $2;
				if ($trigger =~ /^(BEGIN|DECLARE)/) {
					($trigger, $content) = &_get_plsql_code($trigger);
				} else {
					$trigger =~ s/([^;]+;)[\t\s]*(.*)/$1/;
					$content = $2;
				}
			} else {
				if ($trigger =~ /^(BEGIN|DECLARE)/) {
					($trigger, $content) = &_get_plsql_code($trigger);
				}
			}
			$tid++;
			# TRIGGER_NAME, TRIGGER_TYPE, TRIGGERING_EVENT, TABLE_NAME, TRIGGER_BODY, WHEN_CLAUSE, DESCRIPTION,ACTION_TYPE
			$trigger =~ s/END\s[^\s]+$/END/is;
			push(@{$self->{triggers}}, [($t_name, $t_pos, $t_event, $tb_name, $trigger, $t_when_cond, '', $t_type)]);

		} else {
			$doloop = 0;
		}
	} while ($doloop);

}

sub read_sequence_from_file
{
	my $self = shift;

	# Load file in a single string
	my $content = $self->_get_dml_from_file();

	my $tid = 0; 

	# Sequences 
	while ($content =~ s/CREATE\s+SEQUENCE[\s]+([^\s]+)\s*([^;]+);//i) {
		my $s_name = $1;
		$s_name =~ s/"//g;
		my $s_def = $2;
		$s_def =~ s/\s+/ /g;
		$tid++;
		my @seq_info = ();

		# SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, LAST_NUMBER, CACHE_SIZE, CYCLE_FLAG, SEQUENCE_OWNER FROM $self->{prefix}_SEQUENCES";
		push(@seq_info, $s_name);
		if ($s_def =~ /MINVALUE\s+(\d+)/i) {
			push(@seq_info, $1);
		} else {
			push(@seq_info, '');
		}
		if ($s_def =~ /MAXVALUE\s+(\d+)/i) {
			push(@seq_info, $1);
		} else {
			push(@seq_info, '');
		}
		if ($s_def =~ /INCREMENT\s*(?:BY)?\s+(\d+)/i) {
			push(@seq_info, $1);
		} else {
			push(@seq_info, 1);
		}
		if ($s_def =~ /START\s+WITH\s+(\d+)/i) {
			push(@seq_info, $1);
		} else {
			push(@seq_info, '');
		}
		if ($s_def =~ /CACHE\s+(\d+)/i) {
			push(@seq_info, $1);
		} else {
			push(@seq_info, '');
		}
		if ($s_def =~ /NOCYCLE/i) {
			push(@seq_info, 'NO');
		} else {
			push(@seq_info, 'YES');
		}
		if ($s_name =~ /^([^\.]+)\./i) {
			push(@seq_info, $1);
		} else {
			push(@seq_info, '');
		}
		push(@{$self->{sequences}}, \@seq_info);
	}
}

sub read_tablespace_from_file
{
	my $self = shift;

	# Load file in a single string
	my $content = $self->_get_dml_from_file();

	my $tid = 0; 

	# tablespace
	while ($content =~ s/CREATE\s+TABLESPACE[\s]+([^\s]+)\s*DATAFILE\s*([^;]+);//i) {
		my $t_name = $1;
		$t_name =~ s/"//g;
		my $t_def = $2;
		$t_def =~ s/\s+/ /g;
		$tid++;
		# get path
		if ($t_def =~ /'([^\']+)'/) {
			my $t_path = dirname($1);
			# TYPE - TABLESPACE_NAME - FILEPATH - OBJECT_NAME
			@{$self->{tablespaces}{TABLE}{$t_name}{$t_path}} = ();
		}

	}
}





=head2 _views

This function is used to retrieve all views information.

Sets the main hash of the views definition $self->{views}.
Keys are the names of all views retrieved from the current
database and values are the text definitions of the views.

It then sets the main hash as follows:

    # Definition of the view
    $self->{views}{$table}{text} = $view_infos{$table};

=cut

sub _views
{
	my ($self) = @_;

	# Get all views information
	$self->logit("Retrieving views information...\n", 1);
	my %view_infos = $self->_get_views();

	my $i = 1;
	foreach my $view (sort keys %view_infos) {
		$self->logit("[$i] Scanning $view...\n", 1);
		$self->{views}{$view}{text} = $view_infos{$view};
                # Retrieve also aliases from views
                $self->{views}{$view}{alias}= $view_infos{$view}{alias};
		$i++;
	}

}

=head2 _materialized_views

This function is used to retrieve all materialized views information.

Sets the main hash of the views definition $self->{materialized_views}.
Keys are the names of all materialized views retrieved from the current
database and values are the text definitions of the views.

It then sets the main hash as follows:

    # Definition of the matérialized view
    $self->{materialized_views}{text} = $view_infos{$view};

=cut

sub _materialized_views
{
	my ($self) = @_;

	# Get all views information
	$self->logit("Retrieving materialized views information...\n", 1);
	my %view_infos = $self->_get_materialized_views();

	my $i = 1;
	foreach my $table (sort keys %view_infos) {
		$self->logit("[$i] Scanning $table...\n", 1);
		$self->{materialized_views}{$table}{text} = $view_infos{$table}{text};
		$self->{materialized_views}{$table}{updatable}= $view_infos{$table}{updatable};
		$self->{materialized_views}{$table}{refresh_mode}= $view_infos{$table}{refresh_mode};
		$self->{materialized_views}{$table}{refresh_method}= $view_infos{$table}{refresh_method};
		$self->{materialized_views}{$table}{no_index}= $view_infos{$table}{no_index};
		$self->{materialized_views}{$table}{rewritable}= $view_infos{$table}{rewritable};
		$self->{materialized_views}{$table}{build_mode}= $view_infos{$table}{build_mode};
		$i++;
	}

}

=head2 _tablespaces

This function is used to retrieve all Oracle Tablespaces information.

Sets the main hash $self->{tablespaces}.

=cut

sub _tablespaces
{
	my ($self) = @_;

	$self->logit("Retrieving tablespaces information...\n", 1);
	$self->{tablespaces} = $self->_get_tablespaces();

}

=head2 _partitions

This function is used to retrieve all Oracle partition information.

Sets the main hash $self->{partition}.

=cut

sub _partitions
{
	my ($self) = @_;

	$self->logit("Retrieving partitions information...\n", 1);
	($self->{partitions}, $self->{partitions_default}) = $self->_get_partitions();

}


sub get_replaced_tbname
{
	my ($self, $tmptb) = @_;

	if (exists $self->{replaced_tables}{"\L$tmptb\E"} && $self->{replaced_tables}{"\L$tmptb\E"}) {
		$self->logit("\tReplacing table $tmptb as " . $self->{replaced_tables}{lc($tmptb)} . "...\n", 1);
		$tmptb = $self->{replaced_tables}{lc($tmptb)};
	}
	if (!$self->{preserve_case}) {
		$tmptb = lc($tmptb);
		$tmptb =~ s/"//g;
	} elsif ($tmptb !~ /"/) {
		$tmptb = '"' . $tmptb . '"';
	}
	$tmptb = $self->quote_reserved_words($tmptb);

	return $tmptb; 
}

=head2 _get_sql_data

Returns a string containing the PostgreSQL compatible SQL Schema
definition.

=cut

sub _get_sql_data
{
	my ($self, $outfile) = @_;

	my $sql_header = "-- Generated by Ora2Pg, the Oracle database Schema converter, version $VERSION\n";
	$sql_header .= "-- Copyright 2000-2014 Gilles DAROLD. All rights reserved.\n";
	$sql_header .= "-- DATASOURCE: $self->{oracle_dsn}\n\n";
	if ($self->{client_encoding}) {
		$sql_header .= "SET client_encoding TO '\U$self->{client_encoding}\E';\n\n";
	}
	if ($self->{type} ne 'TABLE') {
		$sql_header .= $self->set_search_path();
	}
	$sql_header .= "\\set ON_ERROR_STOP ON\n\n" if ($self->{stop_on_error});

	my $sql_output = "";

	# Process view only
	if ($self->{type} eq 'VIEW') {
		$self->logit("Add views definition...\n", 1);
		# Read DML from file if any
		if ($self->{input_file}) {
			$self->read_view_from_file();
		}
		my $nothing = 0;
		$self->dump($sql_header);
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		my $i = 1;
		my $num_total_view = scalar keys %{$self->{views}};
		foreach my $view (sort { $a cmp $b } keys %{$self->{views}}) {
			$self->logit("\tAdding view $view...\n", 1);
			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($i, $num_total_view, 25, '=', 'views', "generating $view" );
			}
			my $fhdl = undef;
			if ($self->{file_per_table}) {
				my $file_name = "$dirprefix${view}_$self->{output}";
				$file_name =~ s/\.(gz|bz2)$//;
				$self->dump("\\i $file_name\n");
				$self->logit("Dumping to one file per view : ${view}_$self->{output}\n", 1);
				$fhdl = $self->open_export_file("${view}_$self->{output}");
			}
			if ($self->{pg_supports_checkoption} && ($self->{views}{$view}{text} =~ /\bCHECK\s+OPTION\b/i)) {
				$self->{views}{$view}{text} =~ s/\s*\bWITH\b\s+.*$/ WITH CHECK OPTION/s;
			} else {	
				$self->{views}{$view}{text} =~ s/\s*\bWITH\b\s+.*$//s;
			}
			$self->{views}{$view}{text} = $self->_format_view($self->{views}{$view}{text});
			if (!@{$self->{views}{$view}{alias}}) {
				if (!$self->{preserve_case}) {
					$sql_output .= "CREATE OR REPLACE VIEW \L$view\E AS ";
				} else {
					$sql_output .= "CREATE OR REPLACE VIEW \"$view\" AS ";
				}
				$sql_output .= $self->{views}{$view}{text} . ";\n\n";
			} else {
				if (!$self->{preserve_case}) {
					$sql_output .= "CREATE OR REPLACE VIEW \L$view\E (";
				} else {
					$sql_output .= "CREATE OR REPLACE VIEW \"$view\" (";
				}
				my $count = 0;
				foreach my $d (@{$self->{views}{$view}{alias}}) {
					if ($count == 0) {
						$count = 1;
					} else {
						$sql_output .= ", ";
					}
					if (!$self->{preserve_case}) {
						$sql_output .= "\L$d->[0]\E";
					} else {
						$sql_output .= "\"$d->[0]\"";
					}
				}
				if (!$self->{preserve_case}) {
					if ($self->{views}{$view}{text} =~ /SELECT[^\s\t]*(.*?)\bFROM\b/is) {
						my $clause = $1;
						$clause =~ s/"([^"]+)"/"\L$1\E"/gs;
						$self->{views}{$view}{text} =~ s/SELECT[^\s\t]*(.*?)\bFROM\b/SELECT $clause FROM/is;
					}
				}
				$sql_output .= ") AS " . $self->{views}{$view}{text} . ";\n\n";
			}
			if ($self->{file_per_table}) {
				$self->dump($sql_header . $sql_output, $fhdl);
				$self->close_export_file($fhdl);
				$sql_output = '';
			}
			$nothing++;
			$i++;

			# Add comments on columns
			if (!$self->{disable_comment}) {
				foreach $f (keys %{$self->{views}{$view}{column_comments}}) {
					next unless $self->{views}{$view}{column_comments}{$f};
					$self->{views}{$view}{column_comments}{$f} =~ s/'/''/gs;
					if (!$self->{preserve_case}) {
						$sql_output .= "COMMENT ON COLUMN $view.$f IS E'" . $self->{views}{$view}{column_comments}{$f} .  "';\n";
					} else {
						my $vname = $view;
						$vname =~ s/\./"."/;
						$sql_output .= "COMMENT ON COLUMN \"$vname\".\"$f\" IS E'" . $self->{views}{$view}{column_comments}{$f} .  "';\n";
					}
				}
			}

		}

		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $num_total_view, 25, '=', 'views', 'end of output.'), "\n";
		}

		if (!$nothing) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		} else {
			$sql_output .= "\n";
		}

		$self->dump($sql_output);

		return;
	}

	# Process materialized view only
	if ($self->{type} eq 'MVIEW') {
		$self->logit("Add materialized views definition...\n", 1);
		my $nothing = 0;
		$self->dump($sql_header) if ($self->{file_per_table} && !$self->{pg_dsn});
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		if ($self->{plsql_pgsql} && !$self->{pg_supports_mview}) {
			my $sqlout = qq{
$sql_header

CREATE TABLE materialized_views (
        mview_name text NOT NULL PRIMARY KEY,
        view_name text NOT NULL,
        iname text,
        last_refresh TIMESTAMP WITH TIME ZONE
);

CREATE OR REPLACE FUNCTION create_materialized_view(text, text, text)
RETURNS VOID
AS \$\$
DECLARE
    mview ALIAS FOR \$1; -- name of the materialized view to create
    vname ALIAS FOR \$2; -- name of the related view
    iname ALIAS FOR \$3; -- name of the colum of mview to used as unique key
    entry materialized_views%ROWTYPE;
BEGIN
    EXECUTE 'SELECT * FROM materialized_views WHERE mview_name = ' || quote_literal(mview) || '' INTO entry;
    IF entry.iname IS NOT NULL THEN
        RAISE EXCEPTION 'Materialized view % already exist.', mview;
    END IF;

    EXECUTE 'REVOKE ALL ON ' || quote_ident(vname) || ' FROM PUBLIC';
    EXECUTE 'GRANT SELECT ON ' || quote_ident(vname) || ' TO PUBLIC';
    EXECUTE 'CREATE TABLE ' || quote_ident(mview) || ' AS SELECT * FROM ' || quote_ident(vname);
    EXECUTE 'REVOKE ALL ON ' || quote_ident(mview) || ' FROM PUBLIC';
    EXECUTE 'GRANT SELECT ON ' || quote_ident(mview) || ' TO PUBLIC';
    INSERT INTO materialized_views (mview_name, view_name, iname, last_refresh)
    VALUES (
	quote_literal(mview), 
	quote_literal(vname),
	quote_literal(iname),
	CURRENT_TIMESTAMP
    );
    IF iname IS NOT NULL THEN
        EXECUTE 'CREATE INDEX ' || quote_ident(mview) || '_' || quote_ident(iname)  || '_idx ON ' || quote_ident(mview) || '(' || quote_ident(iname) || ')';
    END IF;

    RETURN;
END
\$\$
SECURITY DEFINER
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drop_materialized_view(text) RETURNS VOID
AS
\$\$
DECLARE
    mview ALIAS FOR \$1;
    entry materialized_views%ROWTYPE;
BEGIN
    EXECUTE 'SELECT * FROM materialized_views WHERE mview_name = ''' || quote_literal(mview) || '''' INTO entry;
    IF entry.iname IS NULL THEN
        RAISE EXCEPTION 'Materialized view % does not exist.', mview;
    END IF;

    IF entry.iname IS NOT NULL THEN
        EXECUTE 'DROP INDEX ' || quote_ident(mview) || '_' || entry.iname  || '_idx';
    END IF;
    EXECUTE 'DROP TABLE ' || quote_ident(mview);
    EXECUTE 'DELETE FROM materialized_views WHERE mview_name=''' || quote_literal(mview) || '''';

    RETURN;
END
\$\$
SECURITY DEFINER
LANGUAGE plpgsql ;

CREATE OR REPLACE FUNCTION refresh_full_materialized_view(text) RETURNS VOID
AS \$\$
DECLARE
    mview ALIAS FOR \$1;
    entry materialized_views%ROWTYPE;
BEGIN
    EXECUTE 'SELECT * FROM materialized_views WHERE mview_name = ''' || quote_literal(mview) || '''' INTO entry;
    IF entry.iname IS NULL THEN
        RAISE EXCEPTION 'Materialized view % does not exist.', mview;
    END IF;

    IF entry.iname IS NOT NULL THEN
        EXECUTE 'DROP INDEX ' || quote_ident(mview) || '_' || entry.iname  || '_idx';
    END IF;
    EXECUTE 'TRUNCATE ' || quote_ident(mview);
    EXECUTE 'INSERT INTO ' || quote_ident(mview) || ' SELECT * FROM ' || entry.view_name;
    EXECUTE 'UPDATE materialized_views SET last_refresh=CURRENT_TIMESTAMP WHERE mview_name=''' || quote_literal(mview) || '''';

    IF entry.iname IS NOT NULL THEN
        EXECUTE 'CREATE INDEX ' || quote_ident(mview) || '_' || entry.iname  || '_idx ON ' || quote_ident(mview) || '(' || entry.iname || ')';
    END IF;

    RETURN;
END
\$\$
SECURITY DEFINER
LANGUAGE plpgsql ;

};
			$self->dump($sqlout);
		}
                my $i = 1;
                my $num_total_mview = scalar keys %{$self->{materialized_views}};
		foreach my $view (sort { $a cmp $b } keys %{$self->{materialized_views}}) {
			$self->logit("\tAdding materialized view $view...\n", 1);
			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($i, $num_total_mview, 25, '=', 'materialized views', "generating $view" );
			}
			my $fhdl = undef;
			if ($self->{file_per_table} && !$self->{pg_dsn}) {
				my $file_name = "$dirprefix${view}_$self->{output}";
				$file_name =~ s/\.(gz|bz2)$//;
				$self->dump("\\i $file_name\n");
				$self->logit("Dumping to one file per materialized view : ${view}_$self->{output}\n", 1);
				$fhdl = $self->open_export_file("${view}_$self->{output}");
			}
			if (!$self->{plsql_pgsql}) {
				$sql_output .= "CREATE MATERIALIZED VIEW $view\n";
				$sql_output .= "BUILD $self->{materialized_views}{$view}{build_mode}\n";
				$sql_output .= "REFRESH $self->{materialized_views}{$view}{refresh_method} ON $self->{materialized_views}{$view}{refresh_mode}\n";
				$sql_output .= "ENABLE QUERY REWRITE" if ($self->{materialized_views}{$view}{rewritable});
				$sql_output .= "AS $self->{materialized_views}{$view}{text}";
				$sql_output .= " USING INDEX" if ($self->{materialized_views}{$view}{no_index});
				$sql_output .= " USING NO INDEX" if (!$self->{materialized_views}{$view}{no_index});
				$sql_output .= ";\n\n";
			} else {
				$self->{materialized_views}{$view}{text} = $self->_format_view($self->{materialized_views}{$view}{text});
				if (!$self->{preserve_case}) {
					$self->{materialized_views}{$view}{text} =~ s/"//gs;
				}
				$self->{materialized_views}{$view}{text} =~ s/^PERFORM/SELECT/;
				if (!$self->{pg_supports_mview}) {
					$sql_output .= "CREATE VIEW \L$view\E_mview AS\n";
					$sql_output .= $self->{materialized_views}{$view}{text};
					$sql_output .= ";\n\n";
					$sql_output .= "SELECT create_materialized_view('\L$view\E','\L$view\E_mview', change with the name of the colum to used for the index);\n\n\n";
				} else {
					$sql_output .= "CREATE MATERIALIZED VIEW \L$view\E AS\n";
					$sql_output .= $self->{materialized_views}{$view}{text};
					if ($self->{materialized_views}{$view}{build_mode} eq 'DEFERRED') {
						$sql_output .= " WITH NO DATA";
					}
					$sql_output .= ";\n\n";
				}
			}

			if ($self->{file_per_table} && !$self->{pg_dsn}) {
				$self->dump($sql_header . $sql_output, $fhdl);
				$self->close_export_file($fhdl);
				$sql_output = '';
			}
			$nothing++;
			$i++;
		}
		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $num_total_mview, 25, '=', 'materialized views', 'end of output.'), "\n";
		}
		if (!$nothing) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}

		$self->dump($sql_output);

		return;
	}

	# Process grant only
	if ($self->{type} eq 'GRANT') {
		$self->logit("Add users/roles/grants privileges...\n", 1);
		my $grants = '';
		my $users = '';

		# Read DML from file if any
		if ($self->{input_file}) {
			$self->read_grant_from_file();
		}
		# Add privilege definition
		foreach my $table (sort {"$self->{grants}{$a}{type}.$a" cmp "$self->{grants}{$b}{type}.$b" } keys %{$self->{grants}}) {
			my $realtable = lc($table);
			my $obj = $self->{grants}{$table}{type} || 'TABLE';
			if ($self->{export_schema} &&  $self->{schema}) {
				if (!$self->{preserve_case}) {
					$realtable =  "\L$self->{schema}.$table\E";
				} else {
					$realtable =  "\"$self->{schema}\".\"$table\"";
				}
			} elsif ($self->{preserve_case}) {
				$realtable =  "\"$table\"";
			}
			$grants .= "-- Set priviledge on $self->{grants}{$table}{type} $table\n";

			if ($self->{grants}{$table}{type} ne 'PACKAGE BODY') {
				if ($self->{grants}{$table}{owner}) {
					if (grep(/^$self->{grants}{$table}{owner}$/, @{$self->{roles}{roles}})) {
						$grants .= "ALTER $obj $realtable OWNER TO ROLE $self->{grants}{$table}{owner};\n";
						$obj = '' if (!grep(/^$obj$/, 'FUNCTION', 'SEQUENCE','SCHEMA','TABLESPACE'));
						$grants .= "GRANT ALL ON $obj $realtable TO ROLE $self->{grants}{$table}{owner};\n";
					} else {
						$grants .= "ALTER $obj $realtable OWNER TO $self->{grants}{$table}{owner};\n";
						$obj = '' if (!grep(/^$obj$/, 'FUNCTION', 'SEQUENCE','SCHEMA','TABLESPACE'));
						$grants .= "GRANT ALL ON $obj $realtable TO $self->{grants}{$table}{owner};\n";
					}
				}
				if (grep(/^$self->{grants}{$table}{type}$/, 'FUNCTION', 'SEQUENCE','SCHEMA','TABLESPACE')) {
					$grants .= "REVOKE ALL ON $self->{grants}{$table}{type} $realtable FROM PUBLIC;\n";
				} else {
					$grants .= "REVOKE ALL ON $realtable FROM PUBLIC;\n";
				}
			} else {
				if ($self->{grants}{$table}{owner}) {
					if (grep(/^$self->{grants}{$table}{owner}$/, @{$self->{roles}{roles}})) {
						$grants .= "ALTER SCHEMA $realtable OWNER TO ROLE $self->{grants}{$table}{owner};\n";
						$grants .= "GRANT ALL ON SCHEMA $realtable TO ROLE $self->{grants}{$table}{owner};\n";
					} else {
						$grants .= "ALTER SCHEMA $realtable OWNER TO $self->{grants}{$table}{owner};\n";
						$grants .= "GRANT ALL ON SCHEMA $realtable TO $self->{grants}{$table}{owner};\n";
					}
				}
				$grants .= "REVOKE ALL ON SCHEMA $realtable FROM PUBLIC;\n";
			}
			foreach my $usr (sort keys %{$self->{grants}{$table}{privilege}}) {
				my $agrants = '';
				foreach my $g (@GRANTS) {
					$agrants .= "$g," if (grep(/^$g$/i, @{$self->{grants}{$table}{privilege}{$usr}}));
				}
				$agrants =~ s/,$//;
				if ($self->{grants}{$table}{type} ne 'PACKAGE BODY') {
					if (grep(/^$self->{grants}{$table}{type}$/, 'FUNCTION', 'SEQUENCE','SCHEMA','TABLESPACE')) {
						$grants .= "GRANT $agrants ON $obj $realtable TO $usr;\n";
					} else {
						$grants .= "GRANT $agrants ON $realtable TO $usr;\n";
					}
				} else {
						$grants .= "GRANT USAGE ON SCHEMA $realtable TO $usr;\n";
						$grants .= "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA $realtable TO $usr;\n";
				}
			}
			$grants .= "\n";
		}

		foreach my $r (@{$self->{roles}{owner}}, @{$self->{roles}{grantee}}) {
			my $secret = 'change_my_secret';
			if ($self->{gen_user_pwd}) {
				$secret = &randpattern("CccnCccn");
			}
			$sql_header .= "CREATE " . ($self->{roles}{type}{$r} ||'USER') . " $r";
			$sql_header .= " WITH PASSWORD '$secret'" if ($self->{roles}{password_required}{$r} ne 'NO');
			# It's difficult to parse all oracle privilege. So if one admin option is set we set all PG admin option.
			if (grep(/YES|1/, @{$self->{roles}{$r}{admin_option}})) {
				$sql_header .= " CREATEDB CREATEROLE CREATEUSER INHERIT";
			}
			if ($self->{roles}{type}{$r} eq 'USER') {
				$sql_header .= " LOGIN";
			}
			if (exists $self->{roles}{role}{$r}) {
				$users .= " IN ROLE " . join(',', @{$self->{roles}{role}{$r}});
			}
			$sql_header .= ";\n";
		}
		if (!$grants) {
			$grants = "-- Nothing found of type $self->{type}\n";
		}

		$sql_output .= "\n" . $grants . "\n";

		$self->dump($sql_header . $sql_output);

		return;
	}

	# Process sequences only
	if ($self->{type} eq 'SEQUENCE') {
		$self->logit("Add sequences definition...\n", 1);
		# Read DML from file if any
		if ($self->{input_file}) {
			$self->read_sequence_from_file();
		}
		my $i = 1;
		my $num_total_sequence = $#{$self->{sequences}} + 1;

		foreach my $seq (sort { $a->[0] cmp $b->[0] } @{$self->{sequences}}) {
			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($i, $num_total_sequence, 25, '=', 'sequences', "generating $seq->[0]" );
			}
			my $cache = 1;
			$cache = $seq->[5] if ($seq->[5]);
			my $cycle = '';
			$cycle = ' CYCLE' if ($seq->[6] eq 'Y');
			if (!$self->{preserve_case}) {
				$sql_output .= "CREATE SEQUENCE \L$seq->[0]\E INCREMENT $seq->[3]";
			} else {
				$sql_output .= "CREATE SEQUENCE \"$seq->[0]\" INCREMENT $seq->[3]";
			}
			if ($seq->[1] <= (-2**63-1)) {
				$sql_output .= " NO MINVALUE";
			} else {
				$sql_output .= " MINVALUE $seq->[1]";
			}
			if ($seq->[2] >= (2**63-1)) {
				$sql_output .= " NO MAXVALUE";
			} else {
				$sql_output .= " MAXVALUE $seq->[2]";
			}
			$sql_output .= " START $seq->[4] CACHE $cache$cycle;\n";

			if ($self->{force_owner}) {
				my $owner = $seq->[7];
				$owner = $self->{force_owner} if ($self->{force_owner} ne "1");
				if (!$self->{preserve_case}) {
					$sql_output .= "ALTER SEQUENCE \L$seq->[0]\E OWNER TO \L$owner\E;\n";
				} else {
					$sql_output .= "ALTER SEQUENCE \"$seq->[0]\" OWNER TO \"$owner\";\n";
				}
			}
			$i++;
		}
		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $num_total_sequence, 25, '=', 'sequences', 'end of output.'), "\n";
		}
		if (!$sql_output) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}

		$self->dump($sql_header . $sql_output);
		return;
	}

	# Process triggers only. PL/SQL code is pre-converted to PL/PGSQL following
	# the recommendation of Roberto Mello, see http://techdocs.postgresql.org/
	# Oracle's PL/SQL to PostgreSQL PL/pgSQL HOWTO  
	if ($self->{type} eq 'TRIGGER') {
		$self->logit("Add triggers definition...\n", 1);
		$self->dump($sql_header);
		# Read DML from file if any
		if ($self->{input_file}) {
			$self->read_trigger_from_file();
		}
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		my $nothing = 0;
                my $i = 1;      
                my $num_total_trigger = $#{$self->{triggers}} + 1;
		foreach my $trig (sort {$a->[0] cmp $b->[0]} @{$self->{triggers}}) {

			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($i, $num_total_trigger, 25, '=', 'triggers', "generating $trig->[0]" );
			}
			my $fhdl = undef;
			if ($self->{file_per_function} && !$self->{pg_dsn}) {
				$self->dump("\\i $dirprefix$trig->[0]_$self->{output}\n");
				$self->logit("Dumping to one file per trigger : $trig->[0]_$self->{output}\n", 1);
				$fhdl = $self->open_export_file("$trig->[0]_$self->{output}");
			}
			$trig->[1] =~ s/\s*EACH ROW//is;
			chomp($trig->[4]);
			$trig->[4] =~ s/[;\/]$//;
			$self->logit("\tDumping trigger $trig->[0] defined on table $trig->[3]...\n", 1);
			# Check if it's like a pg rule
			if (!$self->{pg_supports_insteadof} && $trig->[1] =~ /INSTEAD OF/) {
				if (!$self->{preserve_case}) {
					$sql_output .= "CREATE OR REPLACE RULE \L$trig->[0]\E AS\n\tON $trig->[2] TO \L$trig->[3]\E\n\tDO INSTEAD\n(\n\t$trig->[4]\n);\n\n";
				} else {
					$sql_output .= "CREATE OR REPLACE RULE \L$trig->[0]\E AS\n\tON $trig->[2] TO \"$trig->[3]\"\n\tDO INSTEAD\n(\n\t$trig->[4]\n);\n\n";
				}
			} else {
				# Replace direct call of a stored procedure in triggers
				if ($trig->[7] eq 'CALL') {
					if ($self->{plsql_pgsql}) {
						$trig->[4] = Ora2Pg::PLSQL::plsql_to_plpgsql($trig->[4], $self->{allow_code_break},$self->{null_equal_empty});
					}
					$trig->[4] = "BEGIN;\nSELECT $trig->[4];\nEND;";
				} else {
					if ($self->{plsql_pgsql}) {
						$trig->[4] = Ora2Pg::PLSQL::plsql_to_plpgsql($trig->[4], $self->{allow_code_break},$self->{null_equal_empty});
						$trig->[4] =~ s/\b(END[;]*)$/RETURN NEW;\n$1/igs;
					}
				}
				if (!$self->{preserve_case}) {
					$sql_output .= "DROP TRIGGER IF EXISTS \L$trig->[0]\E ON \L$trig->[3]\E CASCADE;\n";
				} else {
					$sql_output .= "DROP TRIGGER IF EXISTS \L$trig->[0]\E ON \"$trig->[3]\" CASCADE;\n";
				}
				if ($self->{pg_supports_when} && $trig->[5]) {
					$sql_output .= "CREATE OR REPLACE FUNCTION trigger_fct_\L$trig->[0]\E () RETURNS trigger AS \$BODY\$\n$trig->[4]\n\$BODY\$\n LANGUAGE 'plpgsql';\n\n";
					$trig->[6] =~ s/\n+$//s;
					$trig->[6] =~ s/^[^\.\s\t]+\.//;
					$sql_output .= "CREATE TRIGGER $trig->[6]\n";
					if ($trig->[5]) {
						if ($self->{plsql_pgsql}) {
							$trig->[5] = Ora2Pg::PLSQL::plsql_to_plpgsql($trig->[5], $self->{allow_code_break},$self->{null_equal_empty});
						}
						$sql_output .= "\tWHEN ($trig->[5])\n";
					}
					$sql_output .= "\tEXECUTE PROCEDURE trigger_fct_\L$trig->[0]\E();\n\n";
				} else {
					$sql_output .= "CREATE OR REPLACE FUNCTION trigger_fct_\L$trig->[0]\E () RETURNS trigger AS \$BODY\$\n$trig->[4]\n\$BODY\$\n LANGUAGE 'plpgsql';\n\n";
					$sql_output .= "CREATE TRIGGER \L$trig->[0]\E\n\t";
					my $statement = 0;
					$statement = 1 if ($trig->[1] =~ s/ STATEMENT//);
					if (!$self->{preserve_case}) {
						$sql_output .= "$trig->[1] $trig->[2] ON \L$trig->[3]\E ";
					} else {
						$sql_output .= "$trig->[1] $trig->[2] ON \"$trig->[3]\" ";
					}
					if ($statement) {
						$sql_output .= "FOR EACH STATEMENT\n";
					} else {
						$sql_output .= "FOR EACH ROW\n";
					}
					$sql_output .= "\tEXECUTE PROCEDURE trigger_fct_\L$trig->[0]\E();\n\n";
				}
			}
			if ($self->{file_per_function} && !$self->{pg_dsn}) {
				$self->dump($sql_header . $sql_output, $fhdl);
				$self->close_export_file($fhdl);
				$sql_output = '';
			}
			$nothing++;
			$i++;
		}
		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $num_total_trigger, 25, '=', 'triggers', 'end of output.'), "\n";
		}
		if (!$nothing) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}

		$self->dump($sql_output);
		return;
	}

	# Process queries only
	if ($self->{type} eq 'QUERY') {
		$self->logit("Parse queries definition...\n", 1);
		$self->dump($sql_header);
		my $nothing = 0;
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		#---------------------------------------------------------
		# Code to use to find queries parser issues, it load a file
		# containing the untouched SQL code from Oracle queries
		#---------------------------------------------------------
		if ($self->{input_file}) {
			$self->{functions} = ();
			$self->logit("Reading input code from file $self->{input_file}...\n", 1);
			open(IN, "$self->{input_file}");
			my @allqueries = <IN>;
			close(IN);
			my $query = 1;
			my $content = join('', @allqueries);
			@allqueries = ();
			$self->{idxcomment} = 0;
			my %comments = $self->_remove_comments(\$content);
			foreach my $l (split(/\n/, $content)) {
				chomp($l);
				next if ($l =~ /^[\s\t]*$/);
				if ($old_line) {
					$l = $old_line .= ' ' . $l;
					$old_line = '';
				}
				if ( ($l =~ s/^\/$/;/) || ($l =~ /;[\s\t]*$/) ) {
						$self->{queries}{$query} .= "$l\n";
						$query++;
				} else {
					$self->{queries}{$query} .= "$l\n";
				}
			}
			foreach my $q (keys %{$self->{queries}}) {
				if ($self->{queries}{$q} !~ /(SELECT|UPDATE|DELETE|INSERT)/is) {
					delete $self->{queries}{$q};
				} else {
					$self->_restore_comments(\$self->{queries}{$q}, \%comments);
				}
			}
		}
		#--------------------------------------------------------

		my $total_size = 0;
		my $cost_value = 0;
		foreach my $q (sort keys %{$self->{queries}}) {
			$self->{idxcomment} = 0;
			my %comments = $self->_remove_comments($self->{queries}{$q});
			$total_size += length($self->{queries}{$q});
			$self->logit("Dumping query $q...\n", 1);
			my $fhdl = undef;
			if ($self->{plsql_pgsql}) {
				my $sql_q = Ora2Pg::PLSQL::plsql_to_plpgsql($self->{queries}{$q}, $self->{allow_code_break},$self->{null_equal_empty}, $self->{type});
				$sql_output .= $sql_q;
				if ($self->{estimate_cost}) {
					my ($cost, %cost_detail) = Ora2Pg::PLSQL::estimate_cost($sql_q);
					$cost += $Ora2Pg::PLSQL::OBJECT_SCORE{'QUERY'};
					$cost_value += $cost;
					$self->logit("Estimed cost of query [ $q ]: $cost\n", 0);
				}
			} else {
				$sql_output .= $self->{queries}{$q};
			}
			$sql_output .= "\n\n";
			$self->_restore_comments(\$sql_output, \%comments);
			$nothing++;
		}
		if ($self->{estimate_cost}) {
			my @infos = ( "Total number of queries: ".(scalar keys %{$self->{queries}}).".",
				"Total size of queries code: $total_size bytes.",
				"Total estimated cost: $cost_value units, ".$self->_get_human_cost($cost_value)."."
			);
			$self->logit(join("\n", @infos) . "\n", 1);
			map { s/^/-- /; } @infos;
			$sql_output .= join("\n", @infos);
		}
		if (!$nothing) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}
		$self->dump($sql_output);
		$self->{queries} = ();
		return;
	}


	# Process functions only
	if ($self->{type} eq 'FUNCTION') {
		use constant SQL_DATATYPE => 2;
		$self->logit("Add functions definition...\n", 1);
		$self->dump($sql_header);
		my $nothing = 0;
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		#---------------------------------------------------------
		# Code to use to find function parser issues, it load a file
		# containing the untouched PL/SQL code from Oracle Function
		#---------------------------------------------------------
		if ($self->{input_file}) {
			$self->{functions} = ();
			$self->logit("Reading input code from file $self->{input_file}...\n", 1);
			open(IN, "$self->{input_file}");
			my @allfct = <IN>;
			close(IN);
			my $fcnm = '';
			my $old_line = '';
			foreach my $l (@allfct) {
				chomp($l);
				next if ($l =~ /^[\s\t]*$/);
				if ($old_line) {
					$l = $old_line .= ' ' . $l;
					$old_line = '';
				}
				if ($l =~ /^[\s\t]*CREATE OR REPLACE (FUNCTION|PROCEDURE)[\s\t]*$/i) {
					$old_line = $l;
					next;
				}
				$l =~ s/^[\s\t]*CREATE OR REPLACE (FUNCTION|PROCEDURE)/$1/is;
				$l =~ s/^[\s\t]*CREATE (FUNCTION|PROCEDURE)/$1/is;
				if ($l =~ /^(FUNCTION|PROCEDURE)[\s\t]+([^\s\(\t]+)/i) {
					$fcnm = $2;
				}
				next if (!$fcnm);
				$self->{functions}{$fcnm} .= "$l\n";
				if ($l =~ /^END[\s\t]+($fcnm)[\s\t]*;/i) {
					$fcnm = '';
				}
			}
		}
		#--------------------------------------------------------

		my $total_size = 0;
		my $cost_value = 0;
                my $i = 1;
                my $num_total_function = scalar keys %{$self->{functions}};
		my $fct_cost = '';
		foreach my $fct (sort keys %{$self->{functions}}) {

			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($i, $num_total_function, 25, '=', 'functions', "generating $fct" );
			}

			# forget or not this object if it is in the exclude or allow lists.
			$i++, next if ($self->skip_this_object('FUNCTION', $fct));
			$self->{idxcomment} = 0;
			my %comments = $self->_remove_comments(\$self->{functions}{$fct});
			$total_size += length($self->{functions}->{$fct});
			$self->logit("Dumping function $fct...\n", 1);
			my $fhdl = undef;
			if ($self->{file_per_function} && !$self->{pg_dsn}) {
				$self->dump("\\i $dirprefix${fct}_$self->{output}\n");
				$self->logit("Dumping to one file per function : ${fct}_$self->{output}\n", 1);
				$fhdl = $self->open_export_file("${fct}_$self->{output}");
			}
			if ($self->{plsql_pgsql}) {
				my $sql_f = $self->_convert_function($self->{functions}{$fct});
				$sql_output .= $sql_f . "\n\n";
				if ($self->{estimate_cost}) {
					my ($cost, %cost_detail) = Ora2Pg::PLSQL::estimate_cost($sql_f);
					$cost += $Ora2Pg::PLSQL::OBJECT_SCORE{'FUNCTION'};
					$cost_value += $cost;
					$self->logit("Function $fct estimated cost: $cost\n", 1);
					$sql_output .= "-- Function $fct estimated cost: $cost\n";
					$fct_cost .= "\t-- Function $fct total estimated cost: $cost\n";
					foreach (sort { $cost_detail{$b} <=> $cost_detail{$a} } keys %cost_detail) {
						next if (!$cost_detail{$_});
						$fct_cost .= "\t\t-- $_ => $cost_detail{$_}";
						$fct_cost .= " (cost: $Ora2Pg::PLSQL::UNCOVERED_SCORE{$_})" if ($Ora2Pg::PLSQL::UNCOVERED_SCORE{$_});
						$fct_cost .= "\n";
					}
				}
			} else {
				$sql_output .= $self->{functions}{$fct} . "\n\n";
			}
			$self->_restore_comments(\$sql_output, \%comments);
			if ($self->{file_per_function} && !$self->{pg_dsn}) {
				$self->dump($sql_header . $sql_output, $fhdl);
				$self->close_export_file($fhdl);
				$sql_output = '';
			}
			$nothing++;
			$i++;
		}
		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $num_total_function, 25, '=', 'functions', 'end of output.'), "\n";
		}
		if ($self->{estimate_cost}) {
			my @infos = ( "Total number of functions: ".(scalar keys %{$self->{functions}}).".",
				"Total size of function code: $total_size bytes.",
				"Total estimated cost: $cost_value units, ".$self->_get_human_cost($cost_value)."."
			);
			$self->logit(join("\n", @infos) . "\n", 1);
			map { s/^/-- /; } @infos;
			$sql_output .= "\n" .  join("\n", @infos);
			$sql_output .= "\n" . $fct_cost;
		}
		if (!$nothing) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}
		$self->dump($sql_output);
		$self->{functions} = ();
		return;
	}

	# Process procedures only
	if ($self->{type} eq 'PROCEDURE') {

		use constant SQL_DATATYPE => 2;
		$self->logit("Add procedures definition...\n", 1);
		my $nothing = 0;
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		$self->dump($sql_header);
		#---------------------------------------------------------
		# Code to use to find procedure parser issues, it load a file
		# containing the untouched PL/SQL code from Oracle Procedure
		#---------------------------------------------------------
		if ($self->{input_file}) {
			$self->{procedures} = ();
			$self->logit("Reading input code from file $self->{input_file}...\n", 1);
			open(IN, "$self->{input_file}");
			my @allfct = <IN>;
			close(IN);
			my $fcnm = '';
			my $old_line = '';
			foreach my $l (@allfct) {
				chomp($l);
				next if ($l =~ /^[\s\t]*$/);
				if ($old_line) {
					$l = $old_line .= ' ' . $l;
					$old_line = '';
				}
				if ($l =~ /^[\s\t]*CREATE OR REPLACE (FUNCTION|PROCEDURE)[\s\t]*$/i) {
					$old_line = $l;
					next;
				}
				$l =~ s/^[\s\t]*CREATE OR REPLACE (FUNCTION|PROCEDURE)/$1/i;
				$l =~ s/^[\s\t]*CREATE (FUNCTION|PROCEDURE)/$1/i;
				if ($l =~ /^(FUNCTION|PROCEDURE)[\s\t]+([^\s\(\t]+)/i) {
					$fcnm = $2;
				}
				next if (!$fcnm);
				$self->{procedures}{$fcnm} .= "$l\n";
				if ($l =~ /^END[\s\t]+($fcnm)[\s\t]*;/i) {
					$fcnm = '';
				}
			}
		}
		#--------------------------------------------------------
                my $total_size = 0;
                my $cost_value = 0;
		my $i = 1;
		my $num_total_procedure = scalar keys %{$self->{procedures}};
		my $fct_cost = '';
		foreach my $fct (sort keys %{$self->{procedures}}) {

			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($i, $num_total_procedure, 25, '=', 'procedures', "generating $fct" );
			}
			# forget or not this object if it is in the exclude or allow lists.
			$i++, next if ($self->skip_this_object('PROCEDURE', $fct));
			$self->{idxcomment} = 0;
			my %comments = $self->_remove_comments(\$self->{procedures}{$fct});
			$total_size += length($self->{procedures}->{$fct});

			$self->logit("Dumping procedure $fct...\n", 1);
			my $fhdl = undef;
			if ($self->{file_per_function} && !$self->{pg_dsn}) {
				$self->dump("\\i $dirprefix${fct}_$self->{output}\n");
				$self->logit("Dumping to one file per procedure : ${fct}_$self->{output}\n", 1);
				$fhdl = $self->open_export_file("${fct}_$self->{output}");
			}
			if ($self->{plsql_pgsql}) {
				my $sql_p = $self->_convert_function($self->{procedures}{$fct});
				$sql_output .= $sql_p . "\n\n";
				if ($self->{estimate_cost}) {
					my ($cost, %cost_detail) = Ora2Pg::PLSQL::estimate_cost($sql_p);
					$cost += $Ora2Pg::PLSQL::OBJECT_SCORE{'PROCEDURE'};
					$cost_value += $cost;
					$self->logit("Function $fct estimated cost: $cost\n", 1);
					$fct_cost .= "\t-- Function $fct total estimated cost: $cost\n";
					foreach (sort { $cost_detail{$b} <=> $cost_detail{$a} } keys %cost_detail) {
						next if (!$cost_detail{$_});
						$fct_cost .= "\t\t-- $_ => $cost_detail{$_}";
						$fct_cost .= " (cost: $Ora2Pg::PLSQL::UNCOVERED_SCORE{$_})" if ($Ora2Pg::PLSQL::UNCOVERED_SCORE{$_});
						$fct_cost .= "\n";
					}
				}
			} else {
				$sql_output .= $self->{procedures}{$fct} . "\n\n";
			}
			$self->_restore_comments(\$sql_output, \%comments);
			$sql_output .= $fct_cost;
			if ($self->{file_per_function} && !$self->{pg_dsn}) {
				$self->dump($sql_header . $sql_output, $fhdl);
				$self->close_export_file($fhdl);
				$sql_output = '';
			}
			$nothing++;
			$i++;
		}
		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $num_total_procedure, 25, '=', 'procedures', 'end of output.'), "\n";
		}
		if ($self->{estimate_cost}) {
			my @infos = ( "Total number of procedures: ".(scalar keys %{$self->{procedures}}).".",
				"Total size of procedures code: $total_size bytes.",
				"Total estimated cost: $cost_value units, ".$self->_get_human_cost($cost_value)."."
			);
			$self->logit(join("\n", @infos) . "\n", 1);
			map { s/^/-- /; } @infos;
			$sql_output .= "\n" .  join("\n", @infos);
			$sql_output .= "\n" . $fct_cost;
		}
		if (!$nothing) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}
		$self->dump($sql_output);
		$self->{procedures} = ();

		return;
	}

	# Process packages only
	if ($self->{type} eq 'PACKAGE') {
		$self->logit("Add packages definition...\n", 1);
		my $nothing = 0;
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		$self->dump($sql_header);

		#---------------------------------------------------------
		# Code to use to find package parser bugs, it load a file
		# containing the untouched PL/SQL code from Oracle Package
		#---------------------------------------------------------
		if ($self->{input_file}) {
			$self->{plsql_pgsql} = 1;
			$self->{packages} = ();
			$self->logit("Reading input code from file $self->{input_file}...\n", 1);
			sleep(1);
			open(IN, "$self->{input_file}");
			my @allpkg = <IN>;
			close(IN);
			my $pknm = '';
			my $before = '';
			my $old_line = '';
			foreach my $l (@allpkg) {
				chomp($l);
				next if ($l =~ /^[\s\t]*$/);
				if ($old_line) {
					$l = $old_line .= ' ' . $l;
					$old_line = '';
				}
				if ($l =~ /^(?:CREATE|CREATE OR REPLACE)?[\s\t]*PACKAGE[\s\t]*(?:BODY[\s\t]*)?$/i) {
					$old_line = $l;
					next;
				}
				if ($l =~ /^(?:CREATE|CREATE OR REPLACE)?[\s\t]*PACKAGE[\s\t]+(?:BODY[\s\t]+)?([^\t\s]+)[\s\t]*$/is) {
					$old_line = $l;
					next;
				}
				if ($l =~ /^(?:CREATE|CREATE OR REPLACE)?[\s\t]*PACKAGE[\s\t]+(?:BODY[\s\t]+)?([^\t\s]+)[\s\t]*(AS|IS)/is) {
					$pknm = lc($1);
				}
				if ($pknm) {
					$self->{packages}{$pknm} .= "$l\n";
				}
			}
		}
		#--------------------------------------------------------

		my $total_size = 0;
		my $total_size_no_comment = 0;
		my $number_fct = 0;
		my $i = 1;
		my $num_total_package = scalar keys %{$self->{packages}};
		foreach my $pkg (sort keys %{$self->{packages}}) {

			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($i, $num_total_package, 25, '=', 'packages', "generating $pkg" );
			}
			$i++, next if (!$self->{packages}{$pkg});
			my $pkgbody = '';
			my $fct_cost = '';
			if (!$self->{plsql_pgsql}) {
				$self->logit("Dumping package $pkg...\n", 1);
				if ($self->{plsql_pgsql} && $self->{file_per_function} && !$self->{pg_dsn}) {
					my $dir = lc("$dirprefix${pkg}");
					if (!-d "$dir") {
						if (not mkdir($dir)) {
							$self->logit("Fail creating directory package : $dir - $!\n", 1);
							next;
						} else {
							$self->logit("Creating directory package: $dir\n", 1);
						}
					}
				}
				$pkgbody = $self->{packages}{$pkg};
			} else {
				my @codes = split(/CREATE(?: OR REPLACE)? PACKAGE BODY/, $self->{packages}{$pkg});
				if ($self->{estimate_cost}) {
					$total_size += length($self->{packages}->{$pkg});
					foreach my $txt (@codes) {
						my %infos = $self->_lookup_package("CREATE OR REPLACE PACKAGE BODY$txt");
						foreach my $f (sort keys %infos) {
							next if (!$f);
							$self->{idxcomment} = 0;
							my %comments = $self->_remove_comments(\$infos{$f});
							$total_size_no_comment += (length($infos{$f}) - (17 * $self->{idxcomment}));
							my ($cost, %cost_detail) = Ora2Pg::PLSQL::estimate_cost($infos{$f});
							$self->_restore_comments(\$infos{$f}, \%comments);
							$cost += $Ora2Pg::PLSQL::OBJECT_SCORE{'FUNCTION'};
							$self->logit("Function $f estimated cost: $cost\n", 1);
							$cost_value += $cost;
							$number_fct++;
							$fct_cost .= "\t-- Function $f total estimated cost: $cost\n";
							foreach (sort { $cost_detail{$b} <=> $cost_detail{$a} } keys %cost_detail) {
								next if (!$cost_detail{$_});
								$fct_cost .= "\t\t-- $_ => $cost_detail{$_}";
								$fct_cost .= " (cost: $Ora2Pg::PLSQL::UNCOVERED_SCORE{$_})" if ($Ora2Pg::PLSQL::UNCOVERED_SCORE{$_});
								$fct_cost .= "\n";
							}
						}
						$cost_value += $Ora2Pg::PLSQL::OBJECT_SCORE{'PACKAGE BODY'};
					}
					$fct_cost .= "-- Total estimated cost for package $pkg: $cost_value\n";
				}
				foreach my $txt (@codes) {
					$pkgbody .= $self->_convert_package("CREATE OR REPLACE PACKAGE BODY$txt");
					$pkgbody =~ s/[\r\n]*END;[\t\s\r\n]*$//is;
					$pkgbody =~ s/([\r\n]*;)[\t\s\r\n]*$/$1/is;
				}
			}
			if ($self->{estimate_cost}) {
				$self->logit("Total size of package code: $total_size bytes.\n", 1);
				$self->logit("Total size of package code without comments: $total_size_no_comment bytes.\n", 1);
				$self->logit("Total number of functions found inside those packages: $number_fct.\n", 1);
				$self->logit("Total estimated cost: $cost_value units, " . $self->_get_human_cost($cost_value) . ".\n", 1);
			}
			if ($pkgbody && ($pkgbody =~ /[a-z]/is)) {
				$sql_output .= "-- Oracle package '$pkg' declaration, please edit to match PostgreSQL syntax.\n";
				$sql_output .= $pkgbody . "\n";
				$sql_output .= "-- End of Oracle package '$pkg' declaration\n\n";
				if ($self->{estimate_cost}) {
					$sql_output .= "-- Total size of package code: $total_size bytes.\n";
					$sql_output .= "-- Total size of package code without comments: $total_size_no_comment bytes.\n";
					$sql_output .= "-- Total number of functions found inside those packages: $number_fct.\n";
					$sql_output .= "-- Total estimated cost: $cost_value units, " . $self->_get_human_cost($cost_value) . ".\n";
					$sql_output .= "-- Detailed cost per function:\n" . $fct_cost;
				}
				$nothing++;
			}
			$self->{total_pkgcost} += ($number_fct*$Ora2Pg::PLSQL::OBJECT_SCORE{'FUNCTION'});
			$self->{total_pkgcost} += $Ora2Pg::PLSQL::OBJECT_SCORE{'PACKAGE BODY'};
			$i++;
		}
		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $num_total_package, 25, '=', 'packages', 'end of output.'), "\n";
		}
		if (!$nothing) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}
		$self->dump($sql_output);
		$self->{packages} = ();
		return;
	}

	# Process types only
	if ($self->{type} eq 'TYPE') {
		$self->logit("Add custom types definition...\n", 1);
		#---------------------------------------------------------
		# Code to use to find type parser issues, it load a file
		# containing the untouched PL/SQL code from Oracle type
		#---------------------------------------------------------
		if ($self->{input_file}) {
			$self->{types} = ();
			$self->logit("Reading input code from file $self->{input_file}...\n", 1);
			open(IN, "$self->{input_file}");
			my @alltype = <IN>;
			close(IN);
			my $typnm = '';
			my $code = '';
			foreach my $l (@alltype) {
				chomp($l);
				next if ($l =~ /^[\s\t]*$/);
				$l =~ s/^[\s\t]*CREATE OR REPLACE[\s\t]*//i;
				$l =~ s/^[\s\t]*CREATE[\s\t]*//i;
				$code .= $l . "\n";
				if ($code =~ /^TYPE[\s\t]+([^\s\(\t]+)/is) {
					$typnm = $1;
				}
				next if (!$typnm);
				if ($code =~ /;/s) {
					push(@{$self->{types}}, { ('name' => $typnm, 'code' => $code) });
					$typnm = '';
					$code = '';
				}
			}
		}
		#--------------------------------------------------------
		my $i = 1;
		foreach my $tpe (sort {length($a->{name}) <=> length($b->{name}) } @{$self->{types}}) {
			$self->logit("Dumping type $tpe->{name}...\n", 1);
			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($i, $#{$self->{types}}+1, 25, '=', 'types', "generating $tpe->{name}" );
			}
			if ($self->{plsql_pgsql}) {
				$tpe->{code} = $self->_convert_type($tpe->{code});
			} else {
				$tpe->{code} = "CREATE OR REPLACE $tpe->{code}\n";
			}
			$sql_output .= $tpe->{code} . "\n";
			$i++;
		}

		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $#{$self->{types}}+1, 25, '=', 'types', 'end of output.'), "\n";
		}
		if (!$sql_output) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}
		$self->dump($sql_header . $sql_output);
		return;
	}

	# Process TABLESPACE only
	if ($self->{type} eq 'TABLESPACE') {
		$self->logit("Add tablespaces definition...\n", 1);
		$sql_header .= "-- Oracle tablespaces export, please edit path to match your filesystem.\n";
		$sql_header .= "-- In PostgreSQl the path must be a directory and is expected to already exists\n";
		my $create_tb = '';
		my @done = ();
		# Read DML from file if any
		if ($self->{input_file}) {
			$self->read_tablespace_from_file();
		}
		my $dirprefix = '';
		foreach my $tb_type (sort keys %{$self->{tablespaces}}) {
			# TYPE - TABLESPACE_NAME - FILEPATH - OBJECT_NAME
			foreach my $tb_name (sort keys %{$self->{tablespaces}{$tb_type}}) {
				foreach my $tb_path (sort keys %{$self->{tablespaces}{$tb_type}{$tb_name}}) {
					# Replace Oracle tablespace filename
					my $loc = $tb_name;
					$tb_path =~ /^(.*)[^\\\/]+$/;
					$loc = $1 . $loc;
					if (!grep(/^$tb_name$/, @done)) {
						$create_tb .= "CREATE TABLESPACE \L$tb_name\E LOCATION '$loc';\n";
					}
					push(@done, $tb_name);
					foreach my $obj (@{$self->{tablespaces}{$tb_type}{$tb_name}{$tb_path}}) {
						next if ($self->{file_per_index} && !$self->{pg_dsn} && ($tb_type eq 'INDEX'));
						if (!$self->{preserve_case} || ($tb_type eq 'INDEX')) {
							$sql_output .= "ALTER $tb_type \L$obj\E SET TABLESPACE \L$tb_name\E;\n";
						} else {
							$sql_output .= "ALTER $tb_type \"$obj\" SET TABLESPACE \L$tb_name\E;\n";
						}
					}
				}
			}
		}

		if (!$sql_output) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}
		$self->dump($sql_header . "$create_tb\n" . $sql_output);

		
		if ($self->{file_per_index} && !$self->{pg_dsn}) {
			my $fhdl = undef;
			$self->logit("Dumping tablespace alter indexes to one separate file : TBSP_INDEXES_$self->{output}\n", 1);
			$fhdl = $self->open_export_file("TBSP_INDEXES_$self->{output}");
			$sql_output = '';
			foreach my $tb_type (sort keys %{$self->{tablespaces}}) {
				# TYPE - TABLESPACE_NAME - FILEPATH - OBJECT_NAME
				foreach my $tb_name (sort keys %{$self->{tablespaces}{$tb_type}}) {
					foreach my $tb_path (sort keys %{$self->{tablespaces}{$tb_type}{$tb_name}}) {
						# Replace Oracle tablespace filename
						my $loc = $tb_name;
						$tb_path =~ /^(.*)[^\\\/]+$/;
						$loc = $1 . $loc;
						foreach my $obj (@{$self->{tablespaces}{$tb_type}{$tb_name}{$tb_path}}) {
							next if ($tb_type eq 'TABLE');
							$sql_output .= "ALTER $tb_type \L$obj\E SET TABLESPACE \L$tb_name\E;\n";
						}
					}
				}
			}
			$sql_output = "-- Nothing found of type $self->{type}\n" if (!$sql_output);
			$self->dump($sql_header . $sql_output, $fhdl);
			$self->close_export_file($fhdl);
		}
		return;
	}

	# Export as Kettle XML file
	if ($self->{type} eq 'KETTLE') {

		# Remove external table from data export
		if (scalar keys %{$self->{external_table}} ) {
			foreach my $table (keys %{$self->{tables}}) {
				if ( grep(/^$table$/i, keys %{$self->{external_table}}) ) {
					delete $self->{tables}{$table};
				}
			}
		}

		# Ordering tables by name
		my @ordered_tables = sort { $a cmp $b } keys %{$self->{tables}};

		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		foreach my $table (@ordered_tables) {
			next if ($self->skip_this_object('TABLE', $table));
			$shell_commands .= $self->create_kettle_output($table, $dirprefix);
		}
		$self->dump("#!/bin/sh\n\n", $fhdl);
		$self->dump("KETTLE_TEMPLATE_PATH='.'\n\n", $fhdl);
		$self->dump($shell_commands, $fhdl);

		return;
	}


	# Extract data only
	if (($self->{type} eq 'INSERT') || ($self->{type} eq 'COPY')) {

		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});

		# Connect the Oracle database to gather information
		$self->{dbh} = $self->_oracle_connection();
		# $self->{dbh} = DBI->connect($self->{oracle_dsn}, $self->{oracle_user}, $self->{oracle_pwd},
		#		{ ora_envhp  => 0, LongReadLen => $self->{longreadlen}, LongTruncOk => $self->{longtruncok} });

		# Fix a problem when exporting type LONG and LOB
		$self->{dbh}->{'LongReadLen'} = $self->{longreadlen};
		$self->{dbh}->{'LongTruncOk'} = $self->{longtruncok};

		# Check for connection failure
		if (!$self->{dbh}) {
			$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
		}

		# Remove external table from data export
		if (scalar keys %{$self->{external_table}} ) {
			foreach my $table (keys %{$self->{tables}}) {
				if ( grep(/^$table$/i, keys %{$self->{external_table}}) ) {
					delete $self->{tables}{$table};
				}
			}
		}
		# Get partition information
		$self->_partitions() if (!$self->{disable_partition});

		# Ordering tables by name
		my @ordered_tables = sort { $a cmp $b } keys %{$self->{tables}};

		# Set SQL orders that should be in the file header
		# (before the COPY or INSERT commands)
		my $first_header = "$sql_header\n";
		# Add search path and constraint deferring
		my $search_path = $self->set_search_path();
		if (!$self->{pg_dsn}) {
			# Set search path
			if ($search_path) {
				$first_header .= $self->set_search_path() . "\n";
			}
			# Open transaction
			$first_header .= "BEGIN;\n";
			# Defer all constraints
			if ($self->{defer_fkey}) {
				$first_header .= "SET CONSTRAINTS ALL DEFERRED;\n\n";
			}
		} else {
			# Set search path
			if ($search_path) {
				$self->{dbhdest}->do($search_path) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
			}
			$self->{dbhdest}->do("BEGIN;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
			# Defer all constraints
			if ($self->{defer_fkey}) {
				$self->{dbhdest}->do("SET CONSTRAINTS ALL DEFERRED;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
			}
		}

		#### Defined all SQL commands that must be executed before and after data loading
		my $load_file = "\n";
		foreach my $table (@ordered_tables) {
			next if ($self->skip_this_object('TABLE', $table));

			# Rename table and double-quote it if required
			my $tmptb = $self->get_replaced_tbname($table);

			#### Set SQL commands that must be executed before data loading

			# Drop foreign keys if required
			if ($self->{drop_fkey}) {
				$self->logit("Dropping foreign keys of table $table...\n", 1);
				my @drop_all = $self->_drop_foreign_keys($table, @{$self->{tables}{$table}{foreign_key}});
				foreach my $str (@drop_all) {
					chomp($str);
					next if (!$str);
					if ($self->{pg_dsn}) {
						my $s = $self->{dbhdest}->do($str) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					} else {
						$first_header .= "$str\n";
					}
				}
			}

			# Drop indexes if required
			if ($self->{drop_indexes}) {
				$self->logit("Dropping indexes of table $table...\n", 1);
				my @drop_all = $self->_drop_indexes($table, %{$self->{tables}{$table}{indexes}}) . "\n";
				foreach my $str (@drop_all) {
					chomp($str);
					next if (!$str);
					if ($self->{pg_dsn}) {
						my $s = $self->{dbhdest}->do($str) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					} else {
						$first_header .= "$str\n";
					}
				}
			}

			# Disable triggers of current table if requested
			if ($self->{disable_triggers}) {
				my $trig_type = 'USER';
				$trig_type = 'ALL' if (uc($self->{disable_triggers}) eq 'ALL');
				if ($self->{pg_dsn}) {
					my $s = $self->{dbhdest}->do("ALTER TABLE $tmptb DISABLE TRIGGER $trig_type;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$first_header .=  "ALTER TABLE $tmptb DISABLE TRIGGER $trig_type;\n";
				}
			}

			#### Add external data file loading if file_per_table is enable
			if ($self->{file_per_table} && !$self->{pg_dsn}) {
				my $file_name = "$dirprefix${table}_$self->{output}";
				$file_name =~ s/\.(gz|bz2)$//;
				$load_file .=  "\\i $file_name\n";
			}

			# With partitioned table, load data direct from table partition
			if (exists $self->{partitions}{$table}) {
				foreach my $pos (sort {$self->{partitions}{$table}{$a} <=> $self->{partitions}{$table}{$b}} keys %{$self->{partitions}{$table}}) {
					foreach my $part_name (sort {$self->{partitions}{$table}{$pos}{$a}->{'colpos'} <=> $self->{partitions}{$table}{$pos}{$b}->{'colpos'}} keys %{$self->{partitions}{$table}{$pos}}) {
						$part_name = $table . '_' . $part_name if ($self->{prefix_partition});
						next if ($self->{allow_partition} && !grep($_ =~ /^$part_name$/i, @{$self->{allow_partition}}));

						if ($self->{file_per_table} && !$self->{pg_dsn}) {
							my $file_name = "$dirprefix${part_name}_$self->{output}";
							$file_name =~ s/\.(gz|bz2)$//;
							$load_file .=  "\\i $file_name\n";
						}
					}
				}
				# Now load content of the default partion table
				if ($self->{partitions_default}{$table}) {
					if (!$self->{allow_partition} || grep($_ =~ /^$self->{partitions_default}{$table}$/i, @{$self->{allow_partition}})) {
						if ($self->{file_per_table} && !$self->{pg_dsn}) {
							my $part_name = $self->{partitions_default}{$table};
							$part_name = $table . '_' . $part_name if ($self->{prefix_partition});
							my $file_name = "$dirprefix${part_name}_$self->{output}";
							$file_name =~ s/\.(gz|bz2)$//;
							$load_file .=  "\\i $file_name\n";
						}
					}
				}
			}
		}

		if (!$self->{pg_dsn}) {
			# Write header to file
			$self->dump($first_header);

			if ($self->{file_per_table}) {
				# Write file loader
				$self->dump($load_file);
			}
		}

		# Commit transaction with direct connection to avoid deadlocks
		if ($self->{pg_dsn}) {
			my $s = $self->{dbhdest}->do("COMMIT;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
		}

		####
		#### Proceed to data export
		####

		# Force datetime format
		$self->_datetime_format();
		# Force numeric format
		$self->_numeric_format();

		# Set total number of rows
		my $global_rows = 0;
		foreach my $table (keys %{$self->{tables}}) {
			$global_rows += $self->{tables}{$table}{table_info}{num_rows};
		}
		# Open a pipe for interprocess communication
		my $reader = new IO::Handle;
		my $writer = new IO::Handle;

		# Fork the logger process
		$pipe = IO::Pipe->new($reader, $writer);
		$writer->autoflush(1);
		if ( ($self->{jobs} > 1) || ($self->{oracle_copies} > 1) ) {
			$self->{dbh}->{InactiveDestroy} = 1;
			$self->{dbhdest}->{InactiveDestroy} = 1 if (defined $self->{dbhdest});
			spawn sub {
				$self->multiprocess_progressbar($global_rows);
			};
			$self->{dbh}->{InactiveDestroy} = 0;
			$self->{dbhdest}->{InactiveDestroy} = 0 if (defined $self->{dbhdest});
		}
		$dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});

		my $start_time = time();
		my $global_count = 0;

		foreach my $table (@ordered_tables) {

			next if ($self->skip_this_object('TABLE', $table));

			# Rename table and double-quote it if required
			my $tmptb = $self->get_replaced_tbname($table);

			if ($self->{file_per_table} && !$self->{pg_dsn}) {
				# Do not dump data again if the file already exists
				next if ($self->file_exists("$dirprefix${table}_$self->{output}"));
			}

			# Open output file
			$self->data_dump($sql_header, $table) if (!$self->{pg_dsn} && $self->{file_per_table});

			# Add table truncate order
			if ($self->{truncate_table}) {
				$self->logit("Truncating table $table...\n", 1);
				if ($self->{pg_dsn}) {
					my $s = $self->{dbhdest}->do("TRUNCATE TABLE $tmptb;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					if ($self->{file_per_table}) {
						$self->data_dump("TRUNCATE TABLE $tmptb;\n",  $table);
					} else {
						$self->dump("\nTRUNCATE TABLE $tmptb;\n");
					}
				}
			}

			# Set global count
			$global_count += $self->{tables}{$table}{table_info}{num_rows};

			# With partitioned table, load data direct from table partition
			if (exists $self->{partitions}{$table}) {
				foreach my $pos (sort {$self->{partitions}{$table}{$a} <=> $self->{partitions}{$table}{$b}} keys %{$self->{partitions}{$table}}) {
					foreach my $part_name (sort {$self->{partitions}{$table}{$pos}{$a}->{'colpos'} <=> $self->{partitions}{$table}{$pos}{$b}->{'colpos'}} keys %{$self->{partitions}{$table}{$pos}}) {
						my $tbpart_name = $part_name;
						$tbpart_name = $table . '_' . $part_name if ($self->{prefix_partition});
						next if ($self->{allow_partition} && !grep($_ =~ /^$tbpart_name$/i, @{$self->{allow_partition}}));

						if ($self->{file_per_table} && !$self->{pg_dsn}) {
							# Do not dump data again if the file already exists
							next if ($self->file_exists("$dirprefix${tbpart_name}_$self->{output}"));
						}
						$self->_dump_table($dirprefix, $sql_header, $table, $start_time, $global_rows, $part_name);
					}
				}
				# Now load content of the default partition table
				if ($self->{partitions_default}{$table}) {
					if (!$self->{allow_partition} || grep($_ =~ /^$self->{partitions_default}{$table}$/i, @{$self->{allow_partition}})) {
						if ($self->{file_per_table} && !$self->{pg_dsn}) {
							# Do not dump data again if the file already exists
							next if ($self->file_exists("$dirprefix$self->{partitions_default}{$table}_$self->{output}"));
						}
						$self->_dump_table($dirprefix, $sql_header, $table, $start_time, $global_rows, $self->{partitions_default}{$table});
					}
				}
			} else {
				$self->_dump_table($dirprefix, $sql_header, $table, $start_time, $global_rows);
			}

			# Close data file
			$self->close_export_file($self->{cfhout}) if (defined $self->{cfhout});
			$self->{cfhout} = undef;

			# Display total export position
			if ( ($self->{jobs} <= 1) && ($self->{oracle_copies} <= 1) ) {
				my $end_time = time();
				my $dt = $end_time - $start_time;
				$dt ||= 1;
				my $rps = sprintf("%.1f", $global_count / ($dt+.0001));
				print STDERR $self->progress_bar($global_count, $global_rows, 25, '=', 'rows', "on total data (avg: $rps recs/sec)");
				print STDERR "\n";
			}
		}

		# Wait for all child die
		if ($self->{oracle_copies} > 1) {
			# Wait for all child dies less the logger
			while (scalar keys %RUNNING_PIDS > 1) {
				my $kid = waitpid(-1, WNOHANG);
				if ($kid > 0) {
					delete $RUNNING_PIDS{$kid};
				}
				usleep(500000);
			}
			# Terminate the process logger
			foreach my $k (keys %RUNNING_PIDS) {
				kill(10, $k);
				%RUNNING_PIDS = ();
			}
			$self->{dbh}->disconnect() if ($self->{dbh});
			$self->{dbh} = $self->_oracle_connection();
			$self->_init_oracle_connection($self->{dbh});
		}

		# Commit transaction
		if ($self->{pg_dsn}) {
			my $s = $self->{dbhdest}->do("BEGIN;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);

		}

		# Remove function created to export external table
		if ($self->{bfile_found}) {
			$self->logit("Removing function ora2pg_get_bfilename() used to retrieve path from BFILE.\n", 1);
			my $bfile_function = "DROP FUNCTION IF EXISTS ora2pg_get_bfilename";
			my $sth2 = $self->{dbh}->do($bfile_function);
		}

		#### Set SQL commands that must be executed after data loading
		my $footer = '';
		foreach my $table (@ordered_tables) {
			next if ($self->skip_this_object('TABLE', $table));

			# Rename table and double-quote it if required
			my $tmptb = $self->get_replaced_tbname($table);


			# disable triggers of current table if requested
			if ($self->{disable_triggers}) {
				my $trig_type = 'USER';
				$trig_type = 'ALL' if (uc($self->{disable_triggers}) eq 'ALL');
				my $str = "ALTER TABLE $tmptb ENABLE TRIGGER $trig_type;";
				if ($self->{pg_dsn}) {
					my $s = $self->{dbhdest}->do($str) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$footer .= "$str\n";
				}
			}

			# Recreate all foreign keys of the concerned tables
			if ($self->{drop_fkey}) {
				my @create_all = ();
				$self->logit("Restoring foreign keys of table $table...\n", 1);
				push(@create_all, $self->_create_foreign_keys($table, @{$self->{tables}{$table}{foreign_key}}));
				foreach my $str (@create_all) {
					chomp($str);
					next if (!$str);
					if ($self->{pg_dsn}) {
						my $s = $self->{dbhdest}->do($str) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					} else {
						$footer .= "$str\n";
					}
				}
			}

			# Recreate all indexes
			if ($self->{drop_indexes}) {
				my @create_all = ();
				$self->logit("Restoring indexes of table $table...\n", 1);
				push(@create_all, $self->_create_indexes($table, %{$self->{tables}{$table}{indexes}}));
				if ($#create_all >= 0) {
					if ($self->{plsql_pgsql}) {
						for (my $i = 0; $i <= $#create_all; $i++) {
							$create_all[$i] = Ora2Pg::PLSQL::plsql_to_plpgsql($create_all[$i], $self->{allow_code_break},$self->{null_equal_empty});
						}
					}
					foreach my $str (@create_all) {
						chomp($str);
						next if (!$str);
						if ($self->{pg_dsn}) {
							my $s = $self->{dbhdest}->do($str) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
						} else {
							$footer .= "$str\n";
						}
					}
				}
			}
		}

		# Insert restart sequences orders
		if (($#ordered_tables >= 0) && !$self->{disable_sequence}) {
			$self->logit("Restarting sequences\n", 1);
			my @restart_sequence = $self->_extract_sequence_info();
			foreach my $str (@restart_sequence) {
				if ($self->{pg_dsn}) {
					my $s = $self->{dbhdest}->do($str) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$footer .= "$str\n";
				}
			}
		}

		# Commit transaction
		if ($self->{pg_dsn}) {
			my $s = $self->{dbhdest}->do("COMMIT;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
		} else {
			$footer .= "COMMIT;\n\n";
		}

		# Recreate constraint an indexes if required
		$self->dump("\n$footer") if (!$self->{pg_dsn} && $footer);

		# Disconnect from the database
		$self->{dbh}->disconnect() if ($self->{dbh});
		$self->{dbhdest}->disconnect() if ($self->{dbhdest});

		return;
	}

	# Process PARTITION only
	if ($self->{type} eq 'PARTITION') {
		$self->logit("Add partitions definition...\n", 1);
		$sql_header .= "-- Oracle partitions export.\n";
		$sql_header .= "-- Please take a look at the export to see if order and default table match your need\n";
		my $total_partition = 0;
		foreach my $table (sort keys %{$self->{partitions}}) {
			foreach my $pos (keys %{$self->{partitions}{$table}}) {
				foreach my $part (keys %{$self->{partitions}{$table}{$pos}}) {
					$total_partition++;
				}
			}
		}
		my $i = 1;
		foreach my $table (sort keys %{$self->{partitions}}) {
			my $function = qq{
CREATE OR REPLACE FUNCTION ${table}_insert_trigger()
RETURNS TRIGGER AS \$\$
BEGIN
};
			my $cond = 'IF';
			my $funct_cond = '';
			my %create_table = ();
			my $idx = 0;
			my $old_pos = '';
			my $old_part = '';
			foreach my $pos (sort {$a <=> $b} keys %{$self->{partitions}{$table}}) {
				foreach my $part (sort {$self->{partitions}{$table}{$pos}{$a}->{'colpos'} <=> $self->{partitions}{$table}{$pos}{$b}->{'colpos'}} keys %{$self->{partitions}{$table}{$pos}}) {
					if (!$self->{quiet} && !$self->{debug}) {
						print STDERR $self->progress_bar($i, $total_partition, 25, '=', 'partitions', "generating $part" );
					}
					my $tb_name = $part;
					$tb_name = $table . "_" . $part if ($self->{prefix_partition});
					$create_table{$table}{table} .= "CREATE TABLE $tb_name ( CHECK (\n";
					my @condition = ();
					for (my $i = 0; $i <= $#{$self->{partitions}{$table}{$pos}{$part}}; $i++) {
						if ($self->{partitions}{$table}{$pos}{$part}[$i]->{type} eq 'LIST') {
							$create_table{$table}{table} .= "\t$self->{partitions}{$table}{$pos}{$part}[$i]->{column} IN ($self->{partitions}{$table}{$pos}{$part}[$i]->{value})";
						} else {
							if ($old_part eq '') {
								$create_table{$table}{table} .= "\t$self->{partitions}{$table}{$pos}{$part}[$i]->{column} <= " . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$pos}{$part}[$i]->{value}, $self->{allow_code_break},$self->{null_equal_empty});
							} else {
								$create_table{$table}{table} .= "\t$self->{partitions}{$table}{$pos}{$part}[$i]->{column} > " . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$old_pos}{$old_part}[$i]->{value}, $self->{allow_code_break},$self->{null_equal_empty}) . " AND $self->{partitions}{$table}{$pos}{$part}[$i]->{column} <= " . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$pos}{$part}[$i]->{value}, $self->{allow_code_break},$self->{null_equal_empty});
							}
						}
						$create_table{$table}{table} .= " AND" if ($i < $#{$self->{partitions}{$table}{$pos}{$part}});
						$create_table{$table}{'index'} .= "CREATE INDEX ${tb_name}_$self->{partitions}{$table}{$pos}{$part}[$i]->{column} ON $tb_name ($self->{partitions}{$table}{$pos}{$part}[$i]->{column});\n";
						if ($self->{partitions}{$table}{$pos}{$part}[$i]->{type} eq 'LIST') {
							push(@condition, "NEW.$self->{partitions}{$table}{$pos}{$part}[$i]->{column} IN (" . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$pos}{$part}[$i]->{value}, $self->{allow_code_break}, $self->{null_equal_empty}) . ")");
						} else {
							push(@condition, "NEW.$self->{partitions}{$table}{$pos}{$part}[$i]->{column} <= " . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$pos}{$part}[$i]->{value}, $self->{allow_code_break},$self->{null_equal_empty}));
						}
					}
					$create_table{$table}{table} .= "\n) ) INHERITS ($table);\n";
					$funct_cond .= "\t$cond ( " . join(' AND ', @condition) . " ) THEN INSERT INTO $tb_name VALUES (NEW.*);\n";
					$cond = 'ELSIF';
					$old_part = $part;
					$i++;
				}
				$old_pos = $pos;
			}
			if (!$self->{partitions_default}{$table}) {
				$function .= $funct_cond . qq{
        ELSE
                INSERT INTO $table VALUES (NEW.*);
};
			} else {
				$function .= $funct_cond . qq{
        ELSE
                INSERT INTO ${table}_$self->{partitions_default}{$table} VALUES (NEW.*);
};
			}
			$function .= qq{
                -- Or if you prefer raising an exception
                -- RAISE EXCEPTION 'Value out of range. Fix the ${table}_insert_trigger() function!';
        END IF;
        RETURN NULL;
END;
\$\$
LANGUAGE plpgsql;
};

			$sql_output .= qq{
$create_table{$table}{table}
};
			$sql_output .= qq{
-- Create default table, where datas are inserted if no condition match
CREATE TABLE $self->{partitions_default}{$table} () INHERITS ($table);
} if ($self->{partitions_default}{$table});
			$sql_output .= qq{
-- Create indexes on each partition table
$create_table{$table}{'index'}

$function

CREATE TRIGGER insert_${table}_trigger
    BEFORE INSERT ON $table
    FOR EACH ROW EXECUTE PROCEDURE ${table}_insert_trigger();

-------------------------------------------------------------------------------
};
		}

		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $total_partition, 25, '=', 'partitions', 'end of output.'), "\n";
		}
		if (!$sql_output) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}
	
		$self->dump($sql_header . $sql_output);

		return;
	}

	# DATABASE DESIGN
	# Dump the database structure: tables, constraints, indexes, etc.
	if ($self->{export_schema} &&  $self->{schema}) {
		if ($self->{create_schema}) {
			if (!$self->{preserve_case}) {
				$sql_output .= "CREATE SCHEMA \L$self->{schema}\E;\n\n";
			} else {
				$sql_output .= "CREATE SCHEMA \"$self->{schema}\";\n\n";
			}
		}
	}
	$sql_output .= $self->set_search_path();

	# Read DML from file if any
	if ($self->{input_file}) {
		$self->read_schema_from_file();
	}

	my $constraints = '';
	if ($self->{file_per_constraint}) {
		$constraints .= $self->set_search_path();
	}
	my $indices = '';
	if ($self->{file_per_index}) {
		$indices .= $self->set_search_path();
	}

	# Find first the total number of tables
	my $num_total_table = 0;
	foreach my $table (keys %{$self->{tables}}) {
		if ($self->{tables}{$table}{type} ne 'view') {
			next if ($self->skip_this_object('TABLE', $table));
		}
		$num_total_table++;
	}

	# Dump all table/index/constraints SQL definitions
	my $ib = 1;
	foreach my $table (sort { $self->{tables}{$a}{internal_id} <=> $self->{tables}{$b}{internal_id} } keys %{$self->{tables}}) {

		if ($self->{tables}{$table}{type} ne 'view') {
			next if ($self->skip_this_object('TABLE', $table));
		}
		$self->logit("Dumping table $table...\n", 1);

		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($ib, $num_total_table, 25, '=', 'tables', "exporting $table" );
		}
		# Create FDW server if required
		if ($self->{external_to_fdw}) {
			if ( grep(/^$table$/i, keys %{$self->{external_table}}) ) {
					$sql_header .= "CREATE EXTENSION file_fdw;\n\n" if ($sql_header !~ /CREATE EXTENSION file_fdw;/is);
					$sql_header .= "CREATE SERVER \L$self->{external_table}{$table}{directory}\E FOREIGN DATA WRAPPER file_fdw;\n\n" if ($sql_header !~ /CREATE SERVER $self->{external_table}{$table}{directory} FOREIGN DATA WRAPPER file_fdw;/is);
			}
		}

		my $tbname = $self->get_replaced_tbname($table);
		my $foreign = '';
		if ( ($self->{type} eq 'FDW') || ($self->{external_to_fdw} && grep(/^$table$/i, keys %{$self->{external_table}})) ) {
			$foreign = ' FOREIGN';
		}
		my $obj_type = $self->{tables}{$table}{table_info}{type} || 'TABLE';
		if ( ($obj_type eq 'TABLE') && $self->{tables}{$table}{table_info}{nologging}) {
			$obj_type = 'UNLOGGED ' . $obj_type;
		}
		if (exists $self->{tables}{$table}{table_as}) {
			if ($self->{plsql_pgsql}) {
				$self->{tables}{$table}{table_as} = Ora2Pg::PLSQL::plsql_to_plpgsql($self->{tables}{$table}{table_as}, $self->{allow_code_break},$self->{null_equal_empty});
			}
			$sql_output .= "\nCREATE $obj_type $tbname AS $self->{tables}{$table}{table_as};\n";
			next;
		}
		if (exists $self->{tables}{$table}{truncate_table}) {
			$sql_output .= "\nTRUNCATE TABLE $tbname;\n";
		}
		if (exists $self->{tables}{$table}{column_info}) {
			$sql_output .= "\nCREATE$foreign $obj_type $tbname (\n";

			# Extract column information following the Oracle position order
			foreach my $k (sort { 
					if (!$self->{reordering_columns}) {
						$self->{tables}{$table}{column_info}{$a}[10] <=> $self->{tables}{$table}{column_info}{$b}[10];
					} else {
						my $tmpa = $self->{tables}{$table}{column_info}{$a};
						$tmpa->[2] =~ s/\D//g;
						my $typa = $self->_sql_type($tmpa->[1], $tmpa->[2], $tmpa->[5], $tmpa->[6]);
						$typa =~ s/\(.*//;
						my $tmpb = $self->{tables}{$table}{column_info}{$b};
						$tmpb->[2] =~ s/\D//g;
						my $typb = $self->_sql_type($tmpb->[1], $tmpb->[2], $tmpb->[5], $tmpb->[6]);
						$typb =~ s/\(.*//;
						$TYPALIGN{$typb} <=> $TYPALIGN{$typa};
					}
				} keys %{$self->{tables}{$table}{column_info}}) {

				my $f = $self->{tables}{$table}{column_info}{$k};
				$f->[2] =~ s/\D//g;
				my $type = $self->_sql_type($f->[1], $f->[2], $f->[5], $f->[6]);
				$type = "$f->[1], $f->[2]" if (!$type);
				# Change column names
				my $fname = $f->[0];
				if (exists $self->{replaced_cols}{"\L$table\E"}{"\L$fname\E"} && $self->{replaced_cols}{"\L$table\E"}{"\L$fname\E"}) {
					$self->logit("\tReplacing column \L$f->[0]\E as " . $self->{replaced_cols}{"\L$table\E"}{"\L$fname\E"} . "...\n", 1);
					$fname = $self->{replaced_cols}{"\L$table\E"}{"\L$fname\E"};
				}
				# Check if this column should be replaced by a boolean following table/column name
				if (grep(/^$f->[0]$/i, @{$self->{'replace_as_boolean'}{uc($table)}})) {
					$type = 'boolean';
				# Check if this column should be replaced by a boolean following type/precision
				} elsif (exists $self->{'replace_as_boolean'}{uc($f->[1])} && ($self->{'replace_as_boolean'}{uc($f->[1])}[0] == $f->[5])) {
					$type = 'boolean';
				}

				if ($f->[1] =~ /SDO_GEOMETRY/) {
					# Set the dimension
					my $suffix = '';
					if ($f->[11] == 3) {
						$suffix = 'Z';
					} elsif ($f->[11] == 4) {
						$suffix = 'ZM';
					}
					$f->[12] ||= 0;
					$type = "geometry($ORA2PG_SDO_GTYPE{$f->[12]}$suffix";
					if ($f->[13]) {
						$type .= ",$f->[13]";
					}
					$type .= ")";
					
				}

				$type = $self->{'modify_type'}{"\L$table\E"}{"\L$f->[0]\E"} if (exists $self->{'modify_type'}{"\L$table\E"}{"\L$f->[0]\E"});
				if (!$self->{preserve_case}) {
					$fname = $self->quote_reserved_words($fname);
					$sql_output .= "\t\L$fname\E $type";
				} else {
					$sql_output .= "\t\"$fname\" $type";
				}
				if (!$f->[3] || ($f->[3] eq 'N')) {
					push(@{$self->{tables}{$table}{check_constraint}{notnull}}, $f->[0]);
					$sql_output .= " NOT NULL";
				}
				if ($f->[4] ne "") {
					$f->[4] =~ s/^[\s\t]+//;
					$f->[4] =~ s/[\s\t]+$//;
					if ($self->{plsql_pgsql}) {
						$f->[4] = Ora2Pg::PLSQL::plsql_to_plpgsql($f->[4], $self->{allow_code_break},$self->{null_equal_empty});
					}
					if ($self->{type} ne 'FDW') {
						$sql_output .= " DEFAULT $f->[4]";
					}
				}
				$sql_output .= ",\n";
			}
			if ($self->{pkey_in_create}) {
				$sql_output .= $self->_get_primary_keys($table, $self->{tables}{$table}{unique_key});
			}
			$sql_output =~ s/,$//;
			if ( ($self->{type} ne 'FDW') && (!$self->{external_to_fdw} || !grep(/^$table$/i, keys %{$self->{external_table}})) ) {
				if ($self->{use_tablespace} && $self->{tables}{$table}{table_info}{tablespace} && !grep(/^$self->{tables}{$table}{table_info}{tablespace}$/i, @{$self->{default_tablespaces}})) {
					$sql_output .= ") TABLESPACE $self->{tables}{$table}{table_info}{tablespace};\n";
				} else {
					$sql_output .= ");\n";
				}
			} elsif ( grep(/^$table$/i, keys %{$self->{external_table}}) ) {
				$sql_output .= ") SERVER \L$self->{external_table}{$table}{directory}\E OPTIONS(filename '$self->{external_table}{$table}{directory_path}$self->{external_table}{$table}{location}', format 'csv', delimiter '$self->{external_table}{$table}{delimiter}');\n";
			} else {
				my $schem = "schema '$self->{schema}'," if ($self->{schema});
				$sql_output .= ") SERVER $self->{fdw_server} OPTIONS($schem table '$table');\n";
			}
		}

		# Add comments on table
		if (!$self->{disable_comment} && $self->{tables}{$table}{table_info}{comment}) {
			$self->{tables}{$table}{table_info}{comment} =~ s/'/''/gs;
			$sql_output .= "COMMENT ON TABLE $tbname IS E'$self->{tables}{$table}{table_info}{comment}';\n";
		}

		# Add comments on columns
		if (!$self->{disable_comment}) {
			foreach $f (keys %{$self->{tables}{$table}{column_comments}}) {
				next unless $self->{tables}{$table}{column_comments}{$f};
				$self->{tables}{$table}{column_comments}{$f} =~ s/'/''/gs;
				# Change column names
				my $fname = $f;
				if (exists $self->{replaced_cols}{"\L$table\E"}{lc($fname)} && $self->{replaced_cols}{"\L$table\E"}{lc($fname)}) {
					$self->logit("\tReplacing column $f as " . $self->{replaced_cols}{"\L$table\E"}{lc($fname)} . "...\n", 1);
					$fname = $self->{replaced_cols}{"\L$table\E"}{lc($fname)};
				}
				if (!$self->{preserve_case}) {
					$sql_output .= "COMMENT ON COLUMN $tbname.$fname IS E'" . $self->{tables}{$table}{column_comments}{$f} .  "';\n";
				} else {
					$sql_output .= "COMMENT ON COLUMN $tbname.\"$fname\" IS E'" . $self->{tables}{$table}{column_comments}{$f} .  "';\n";
				}

			}
		}

		# Change ownership
		if ($self->{force_owner}) {
			my $owner = $self->{tables}{$table}{table_info}{owner};
			$owner = $self->{force_owner} if ($self->{force_owner} ne "1");
			$sql_output .= "ALTER $self->{tables}{$table}{table_info}{type} $tbname OWNER TO $owner;\n";
		}
		if (exists $self->{tables}{$table}{alter_table}) {
			$obj_type =~ s/UNLOGGED //;
			foreach (@{$self->{tables}{$table}{alter_table}}) {
				$sql_output .= "\nALTER $obj_type $tbname $_;\n";
			}
		}
		if ($self->{type} ne 'FDW') {
			# Set the unique (and primary) key definition 
			$constraints .= $self->_create_unique_keys($table, $self->{tables}{$table}{unique_key});
			# Set the check constraint definition 
			$constraints .= $self->_create_check_constraint($table, $self->{tables}{$table}{check_constraint},$self->{tables}{$table}{field_name});
			if (!$self->{file_per_constraint}) {
				$sql_output .= $constraints;
				$constraints = '';
			}

			# Set the index definition
			$indices .= $self->_create_indexes($table, %{$self->{tables}{$table}{indexes}}) . "\n";
			if ($self->{plsql_pgsql}) {
				$indices = Ora2Pg::PLSQL::plsql_to_plpgsql($indices, $self->{allow_code_break},$self->{null_equal_empty});
			}
			if (!$self->{file_per_index}) {
				$sql_output .= $indices;
				$indices = '';
			}
		}
		$ib++;
	}
	if (!$self->{quiet} && !$self->{debug}) {
		print STDERR $self->progress_bar($ib - 1, $num_total_table, 25, '=', 'tables', 'end of table export.'), "\n";
	}

	if ($self->{file_per_index} && ($self->{type} ne 'FDW')) {
		my $fhdl = undef;
		$self->logit("Dumping indexes to one separate file : INDEXES_$self->{output}\n", 1);
		$fhdl = $self->open_export_file("INDEXES_$self->{output}");
		$indices = "-- Nothing found of type indexes\n" if (!$indices);
		$self->dump($sql_header . $indices, $fhdl);
		$self->close_export_file($fhdl);
		$indices = '';
	}

	# Dumping foreign key constraints
	foreach my $table (keys %{$self->{tables}}) {
		next if ($#{$self->{tables}{$table}{foreign_key}} < 0);
		$self->logit("Dumping RI $table...\n", 1);
		# Add constraint definition
		if ($self->{type} ne 'FDW') {
			my $create_all = $self->_create_foreign_keys($table, @{$self->{tables}{$table}{foreign_key}});
			if ($create_all) {
				if ($self->{file_per_constraint}) {
					$constraints .= $create_all;
				} else {
					$sql_output .= $create_all;
				}
			}
		}
	}

	if ($self->{file_per_constraint} && ($self->{type} ne 'FDW')) {
		my $fhdl = undef;
		$self->logit("Dumping constraints to one separate file : CONSTRAINTS_$self->{output}\n", 1);
		$fhdl = $self->open_export_file("CONSTRAINTS_$self->{output}");
		$constraints = "-- Nothing found of type constraints\n" if (!$constraints);
		$self->dump($sql_header . $constraints, $fhdl);
		$self->close_export_file($fhdl);
		$constraints = '';
	}

	if (!$sql_output) {
		$sql_output = "-- Nothing found of type TABLE\n";
	}

	$self->dump($sql_header . $sql_output);
}

sub file_exists
{
	my ($self, $file) = @_;

	if ($self->{file_per_table} && !$self->{pg_dsn}) {
		if (-e "$file") {
			$self->logit("WARNING: Skipping dumping data to file $file, file already exists.\n", 0);
			return 1;
		}
	}
	return 0;
}

####
# dump table content
####
sub _dump_table
{
	my ($self, $dirprefix, $sql_header, $table, $start_time, $global_rows, $part_name) = @_;

	my @cmd_head = ();
	my @cmd_foot = ();

	# Set search path
	my $search_path = $self->set_search_path();
	if ($search_path) {
		push(@cmd_head,$search_path);
	}

	# Rename table and double-quote it if required
	my $tmptb = '';

	# Prefix partition name with tablename
	if ($part_name && $self->{prefix_partition}) {
		$tmptb = $self->get_replaced_tbname($table . '_' . $part_name);
	} else {
		$tmptb = $self->get_replaced_tbname($part_name || $table);
	}


	# Build the header of the query
	my @tt = ();
	my @stt = ();
	my @nn = ();
	my $col_list = '';

	# Extract column information following the Oracle position order
	my @fname = ();
	foreach my $i ( 0 .. $#{$self->{tables}{$table}{field_name}} ) {
		my $fieldname = ${$self->{tables}{$table}{field_name}}[$i];
		if (!$self->{preserve_case}) {
			if (exists $self->{modify}{"\L$table\E"}) {
				next if (!grep(/^$fieldname$/i, @{$self->{modify}{"\L$table\E"}}));
			}
		} else {
			if (exists $self->{modify}{"$table"}) {
				next if (!grep(/^$fieldname$/i, @{$self->{modify}{"$table"}}));
			}
		}
		if (!$self->{preserve_case}) {
			push(@fname, lc($fieldname));
		} else {
			push(@fname, $fieldname);
		}

		my $f = $self->{tables}{$table}{column_info}{$fieldname};
		$f->[2] =~ s/\D//g;

		if ($f->[1] =~ /GEOMETRY/i) {
			$self->{local_type} = $self->{type} if (!$self->{local_type});
			$self->{type} = 'INSERT';
		}

		my $type = $self->_sql_type($f->[1], $f->[2], $f->[5], $f->[6]);
		$type = "$f->[1], $f->[2]" if (!$type);
		push(@stt, uc($f->[1]));
		push(@tt, $type);
		push(@nn,  $self->{tables}{$table}{column_info}{$fieldname});
		# Change column names
		my $colname = $f->[0];
		if ($self->{replaced_cols}{lc($table)}{lc($f->[0])}) {
			$self->logit("\tReplacing column $f->[0] as " . $self->{replaced_cols}{lc($table)}{lc($f->[0])} . "...\n", 1);
			$colname = $self->{replaced_cols}{lc($table)}{lc($f->[0])};
		}
		if (!$self->{preserve_case}) {
			$colname = $self->quote_reserved_words($colname);
			$col_list .= "\L$colname\E,";
		} else {
			$col_list .= "\"$colname\",";
		}
	}
	$col_list =~ s/,$//;

	my $s_out = "INSERT INTO $tmptb ($col_list";
	if ($self->{type} eq 'COPY') {
		$s_out = "\nCOPY $tmptb ($col_list";
	}

	if ($self->{type} eq 'COPY') {
		$s_out .= ") FROM STDIN;\n";
	} else {
		$s_out .= ") VALUES (";
	}

	my $sprep = '';
	if ($self->{pg_dsn}) {
		if ($self->{type} ne 'COPY') {
			$s_out .= '?,' foreach (@fname);
			$s_out =~ s/,$//;
			$s_out .= ")";
			$sprep = $s_out;
		}
	}

	# Extract all data from the current table
	$self->ask_for_data($table, \@cmd_head, \@cmd_foot, $s_out, \@nn, \@tt, $sprep, \@stt, $part_name);

	$self->{type} = $self->{local_type} if ($self->{local_type});
	$self->{local_type} = '';
}

=head2 _column_comments

This function return comments associated to columns

=cut
sub _column_comments
{
	my ($self, $table, $owner) = @_;

	my $condition = '';
	$condition .= "AND TABLE_NAME='$table' " if ($table);
	$condition .= "AND OWNER='$owner' " if ($owner);
	$condition .= $self->limit_to_tables() if (!$table);
	$condition =~ s/^AND/WHERE/;

	$owner = "AND OWNER='$owner' " if ($owner);
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("WARNING only: " . $self->{dbh}->errstr . "\n", 0, 0);
SELECT COLUMN_NAME,COMMENTS,TABLE_NAME,OWNER
FROM $self->{prefix}_COL_COMMENTS $condition
END

	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %data = ();
	while (my $row = $sth->fetch) {
		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('TABLE', $row->[2]));
		$data{$row->[2]}{$row->[0]} = $row->[1];
	}

	return %data;
}


=head2 _create_indexes

This function return SQL code to create indexes of a table

=cut
sub _create_indexes
{
	my ($self, $table, %indexes) = @_;

	my $tbsaved = $table;
	$table = $self->get_replaced_tbname($table);
	my @out = ();
	# Set the index definition
	foreach my $idx (keys %indexes) {

		# Cluster, domain, bitmap join, reversed and IOT indexes will not be exported at all
		next if ($self->{tables}{$tbsaved}{idx_type}{$idx}{type} =~ /JOIN|IOT|CLUSTER|REV/i);
		next if ($self->{tables}{$tbsaved}{idx_type}{$idx}{type} =~ /DOMAIN/i && $self->{tables}{$tbsaved}{idx_type}{$idx}{type_name} !~ /SPATIAL_INDEX/);
		map { if ($_ !~ /\(.*\)/) { s/^/"/; s/$/"/; } } @{$indexes{$idx}};
		if (exists $self->{replaced_cols}{"\L$tbsaved\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}) {
			foreach my $c (keys %{$self->{replaced_cols}{"\L$tbsaved\E"}}) {
				map { s/"$c"/"$self->{replaced_cols}{"\L$tbsaved\E"}{$c}"/i } @{$indexes{$idx}};
			}
		}
		map { s/"//gs } @{$indexes{$idx}};
		if (!$self->{preserve_case}) {
			map { $_ = $self->quote_reserved_words($_) } @{$indexes{$idx}};
		} else {
			map { if ($_ !~ /\(.*\)/) { s/^/"/; s/$/"/; } } @{$indexes{$idx}};
		}
		# Add parentheses to index column definition when a space is found
		for (my $i = 0; $i <= $#{$indexes{$idx}}; $i++) {
			if ($indexes{$idx}->[$i] =~ /\s/) {
				$indexes{$idx}->[$i] = '(' . $indexes{$idx}->[$i] . ')';
			}
		}
		my $columns = join(',', @{$indexes{$idx}});
		my $colscompare = $columns;
		$colscompare =~ s/"//gs;
		my $columnlist = '';
		my $skip_index_creation = 0;
		foreach my $consname (keys %{$self->{tables}{$tbsaved}{unique_key}}) {
			my $constype =  $self->{tables}{$tbsaved}{unique_key}->{$consname}{type};
			next if (($constype ne 'P') && ($constype ne 'U'));
			my @conscols = @{$self->{tables}{$tbsaved}{unique_key}->{$consname}{columns}};
			for (my $i = 0; $i <= $#conscols; $i++) {
				# Change column names
				if (exists $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"}) {
					$conscols[$i] = $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"};
				}
			}
			$columnlist = join(',', @conscols);
			$columnlist =~ s/"//gs;
			if (lc($columnlist) eq lc($colscompare)) {
				$skip_index_creation = 1;
				last;
			}
		}

		# Do not create the index if there already a constraint on the same column list
		# the index will be automatically created by PostgreSQL at constraint import time.
		if (!$skip_index_creation) {
			my $unique = '';
			$unique = ' UNIQUE' if ($self->{tables}{$tbsaved}{uniqueness}{$idx} eq 'UNIQUE');
			my $str = '';
			my $concurrently = '';
			if ($self->{tables}{$tbsaved}{concurrently}{$idx}) {
				$concurrently = ' CONCURRENTLY';
			}
			$columns = lc($columns) if (!$self->{preserve_case});
			$columns =~ s/^\((.*)\)$/$1/;
			if ($self->{tables}{$tbsaved}{idx_type}{$idx}{type_name} !~ /SPATIAL_INDEX/) {
				$str .= "CREATE$unique INDEX$concurrently \L$idx$self->{indexes_suffix}\E ON $table ($columns)";
			} else {
				$str .= "CREATE$unique INDEX$concurrently \L$idx$self->{indexes_suffix}\E ON $table USING gist($columns)";
			}
			if ($self->{use_tablespace} && $self->{tables}{$tbsaved}{idx_tbsp}{$idx} && !grep(/^$self->{tables}{$tbsaved}{idx_tbsp}{$idx}$/i, @{$self->{default_tablespaces}})) {
				$str .= " TABLESPACE $self->{tables}{$tbsaved}{idx_tbsp}{$idx}";
			}
			$str .= ";";
			push(@out, $str);
		}
	}

	return wantarray ? @out : join("\n", @out);

}

=head2 _drop_indexes

This function return SQL code to drop indexes of a table

=cut
sub _drop_indexes
{
	my ($self, $table, %indexes) = @_;

	my $tbsaved = $table;
	$table = $self->{replaced_tables}{"\L$table\E"};

	my @out = ();
	# Set the index definition
	foreach my $idx (keys %indexes) {
		# Cluster, domain, bitmap join, reversed and IOT indexes will not be exported at all
		next if ($self->{tables}{$table}{idx_type}{$idx}{type} =~ /JOIN|IOT|CLUSTER|REV/i);
		next if ($self->{tables}{$table}{idx_type}{$idx}{type} =~ /DOMAIN/i && $self->{tables}{$table}{idx_type}{$idx}{type_name} !~ /SPATIAL_INDEX/);

		map { if ($_ !~ /\(.*\)/) { s/^/"/; s/$/"/; } } @{$indexes{$idx}};
		if (exists $self->{replaced_cols}{"\L$tbsaved\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}) {
			foreach my $c (keys %{$self->{replaced_cols}{"\L$tbsaved\E"}}) {
				map { s/"$c"/"$self->{replaced_cols}{"\L$tbsaved\E"}{$c}"/i } @{$indexes{$idx}};
			}
		}
		map { s/"//gs } @{$indexes{$idx}};
		if (!$self->{preserve_case}) {
			map { $_ = $self->quote_reserved_words($_) } @{$indexes{$idx}};
		} else {
			map { if ($_ !~ /\(.*\)/) { s/^/"/; s/$/"/; } } @{$indexes{$idx}};
		}
		my $columns = join(',', @{$indexes{$idx}});
		my $colscompare = $columns;
		$colscompare =~ s/"//gs;
		my $columnlist = '';
		my $skip_index_creation = 0;
		foreach my $consname (keys %{$self->{tables}{$table}{unique_key}}) {
			my $constype =   $self->{tables}{$table}{unique_key}->{$consname}{type};
			next if (($constype ne 'P') && ($constype ne 'U'));
			my @conscols = @{$self->{tables}{$table}{unique_key}->{$consname}{columns}};
			for (my $i = 0; $i <= $#conscols; $i++) {
				# Change column names
				if (exists $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"}) {
					$conscols[$i] = $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"};
				}
			}
			$columnlist = join(',', @conscols);
			$columnlist =~ s/"//gs;
			if (lc($columnlist) eq lc($colscompare)) {
				$skip_index_creation = 1;
				last;
			}
		}

		# Do not create the index if there already a constraint on the same column list
		# the index will be automatically created by PostgreSQL at constraint import time.
		if (!$skip_index_creation) {
			push(@out, "DROP INDEX IF EXISTS \L$idx$self->{indexes_suffix}\E;");
		}
	}

	return wantarray ? @out : join("\n", @out);
}

=head2 _exportable_indexes

This function return the indexes that will be exported

=cut
sub _exportable_indexes
{
	my ($self, $table, %indexes) = @_;

	my @out = ();
	# Set the index definition
	foreach my $idx (keys %indexes) {

		map { if ($_ !~ /\(.*\)/) { s/^/"/; s/$/"/; } } @{$indexes{$idx}};
		map { s/"//gs } @{$indexes{$idx}};
		my $columns = join(',', @{$indexes{$idx}});
		my $colscompare = $columns;
		my $columnlist = '';
		my $skip_index_creation = 0;
		foreach my $consname (keys %{$self->{tables}{$table}{unique_key}}) {
			my $constype =  $self->{tables}{$table}{unique_key}->{$consname}{type};
			next if (($constype ne 'P') && ($constype ne 'U'));
			my @conscols = @{$self->{tables}{$table}{unique_key}->{$consname}{columns}};
			$columnlist = join(',', @conscols);
			$columnlist =~ s/"//gs;
			if (lc($columnlist) eq lc($colscompare)) {
				$skip_index_creation = 1;
				last;
			}
		}

		# The index will not be created
		if (!$skip_index_creation) {
			push(@out, $idx);
		}
	}

	return @out;
}


=head2 _get_primary_keys

This function return SQL code to add primary keys of a create table definition

=cut
sub _get_primary_keys
{
	my ($self, $table, $unique_key) = @_;

	my $out = '';

	# Set the unique (and primary) key definition 
	foreach my $consname (keys %$unique_key) {
		next if ($self->{pkey_in_create} && ($unique_key->{$consname}{type} ne 'P'));
		my $constype =   $unique_key->{$consname}{type};
		my $constgen =   $unique_key->{$consname}{generated};
		my $index_name = $unique_key->{$consname}{index_name};
		my @conscols = @{$unique_key->{$consname}{columns}};
		my %constypenames = ('U' => 'UNIQUE', 'P' => 'PRIMARY KEY');
		my $constypename = $constypenames{$constype};
		for (my $i = 0; $i <= $#conscols; $i++) {
			# Change column names
			if (exists $self->{replaced_cols}{"\L$table\E"}{"\L$conscols[$i]\E"} && $self->{replaced_cols}{"\L$table\E"}{"\L$conscols[$i]\E"}) {
				$conscols[$i] = $self->{replaced_cols}{"\L$table\E"}{"\L$conscols[$i]\E"};
			}
		}
		map { s/"//gs } @conscols;
		if (!$self->{preserve_case}) {
			map { $_ = $self->quote_reserved_words($_) } @conscols;
		} else {
			map { s/^/"/; s/$/"/; } @conscols;
		}
		my $columnlist = join(',', @conscols);
		if (!$self->{preserve_case}) {
			$columnlist = lc($columnlist);
		}
		if ($columnlist) {
			if ($self->{pkey_in_create}) {
				if (!$self->{keep_pkey_names} || ($constgen eq 'GENERATED NAME')) {
					$out .= "\tPRIMARY KEY ($columnlist)";
				} else {
					$out .= "\tCONSTRAINT \L$consname\E PRIMARY KEY ($columnlist)";
				}
				if ($self->{use_tablespace} && $self->{tables}{$table}{idx_tbsp}{$index_name} && !grep(/^$self->{tables}{$table}{idx_tbsp}{$index_name}$/i, @{$self->{default_tablespaces}})) {
					$out .= " USING INDEX TABLESPACE $self->{tables}{$table}{idx_tbsp}{$index_name}";
				}
				$out .= ",\n";
			}
		}
	}
	$out =~ s/,$//s;

	return $out;
}


=head2 _create_unique_keys

This function return SQL code to create unique and primary keys of a table

=cut
sub _create_unique_keys
{
	my ($self, $table, $unique_key) = @_;

	my $out = '';

	my $tbsaved = $table;
	$table = $self->get_replaced_tbname($table);

	# Set the unique (and primary) key definition 
	foreach my $consname (keys %$unique_key) {
		next if ($self->{pkey_in_create} && ($unique_key->{$consname}{type} eq 'P'));
		my $constype =   $unique_key->{$consname}{type};
		my $constgen =   $unique_key->{$consname}{generated};
		my $index_name = $unique_key->{$consname}{index_name};
		my @conscols = @{$unique_key->{$consname}{columns}};
		my %constypenames = ('U' => 'UNIQUE', 'P' => 'PRIMARY KEY');
		my $constypename = $constypenames{$constype};
		for (my $i = 0; $i <= $#conscols; $i++) {
			# Change column names
			if (exists $self->{replaced_cols}{"$tbsaved"}{"\L$conscols[$i]\E"} && $self->{replaced_cols}{"$tbsaved"}{"\L$conscols[$i]\E"}) {
				$conscols[$i] = $self->{replaced_cols}{"$tbsaved"}{"\L$conscols[$i]\E"};
			}
		}
		map { s/"//gs } @conscols;
		if (!$self->{preserve_case}) {
			map { $_ = $self->quote_reserved_words($_) } @conscols;
		} else {
			map { s/^/"/; s/$/"/; } @conscols;
		}
		my $columnlist = join(',', @conscols);
		if (!$self->{preserve_case}) {
			$columnlist = lc($columnlist);
		}
		if ($columnlist) {
			if (!$self->{keep_pkey_names} || ($constgen eq 'GENERATED NAME')) {
				$out .= "ALTER TABLE $table ADD $constypename ($columnlist)";
			} else {
				$out .= "ALTER TABLE $table ADD CONSTRAINT \L$consname\E $constypename ($columnlist)";
			}
			if ($self->{use_tablespace} && $self->{tables}{$tbsaved}{idx_tbsp}{$index_name} && !grep(/^$self->{tables}{$tbsaved}{idx_tbsp}{$index_name}$/i, @{$self->{default_tablespaces}})) {
				$out .= " USING INDEX TABLESPACE $self->{tables}{$tbsaved}{idx_tbsp}{$index_name}";
			}
			$out .= ";\n";
		}
	}
	return $out;
}

=head2 _create_check_constraint

This function return SQL code to create the check constraints of a table

=cut
sub _create_check_constraint
{
	my ($self, $table, $check_constraint, $field_name) = @_;

	my $tbsaved = $table;
	$table = $self->get_replaced_tbname($table);

	my $out = '';
	# Set the check constraint definition 
	foreach my $k (keys %{$check_constraint->{constraint}}) {
		my $chkconstraint = $check_constraint->{constraint}->{$k};
		next if (!$chkconstraint);
		my $skip_create = 0;
		if (exists $check_constraint->{notnull}) {
			foreach my $col (@{$check_constraint->{notnull}}) {
				$skip_create = 1, last if (lc($chkconstraint) eq lc("\"$col\" IS NOT NULL"));
			}
		}
		if (!$skip_create) {
			if (exists $self->{replaced_cols}{"$tbsaved"} && $self->{replaced_cols}{"$tbsaved"}) {
				foreach my $c (keys %{$self->{replaced_cols}{"$tbsaved"}}) {
					$chkconstraint =~ s/"$c"/"$self->{replaced_cols}{"$tbsaved"}{$c}"/gsi;
					$chkconstraint =~ s/\b$c\b/$self->{replaced_cols}{"$tbsaved"}{$c}/gsi;
				}
			}
			if ($self->{plsql_pgsql}) {
				$chkconstraint = Ora2Pg::PLSQL::plsql_to_plpgsql($chkconstraint, $self->{allow_code_break},$self->{null_equal_empty});
			}
			if (!$self->{preserve_case}) {
				foreach my $c (@$field_name) {
					# Force lower case
					my $ret = $self->quote_reserved_words($c);
					$chkconstraint =~ s/"$c"/\L$ret\E/igs;
				}
				$k = lc($k);
			}
			$out .= "ALTER TABLE $table ADD CONSTRAINT $k CHECK ($chkconstraint);\n";
		}
	}

	return $out;
}

=head2 _create_foreign_keys

This function return SQL code to create the foreign keys of a table

=cut
sub _create_foreign_keys
{
	my ($self, $table, @foreign_key) = @_;

	my @out = ();

	my $tbsaved = $table;
	$table = $self->get_replaced_tbname($table);
	# Do not export foreign keys on excluded tables
	return if ($self->skip_this_object('TABLE', $table)); 

	# Add constraint definition
	my @done = ();
	foreach my $h (@foreign_key) {

		next if (grep(/^$h->[0]$/, @done));
		$h->[0] = uc($h->[0]);
		foreach my $desttable (keys %{$self->{tables}{$tbsaved}{foreign_link}{$h->[0]}{remote}}) {

			# Do not export foreign key to table that are not exported
			next if ($self->skip_this_object('TABLE', $desttable));
			my $str = '';
			push(@done, $h->[0]);
			map { $_ = '"' . $_ . '"' } @{$self->{tables}{$tbsaved}{foreign_link}{$h->[0]}{local}};
			map { $_ = '"' . $_ . '"' } @{$self->{tables}{$tbsaved}{foreign_link}{$h->[0]}{remote}{$desttable}};
			my $subsdesttable = $self->get_replaced_tbname($desttable);
			my @lfkeys = ();
			push(@lfkeys, @{$self->{tables}{$tbsaved}{foreign_link}{$h->[0]}{local}});
			if (exists $self->{replaced_cols}{"\L$tbsaved\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}) {
				foreach my $c (keys %{$self->{replaced_cols}{"\L$tbsaved\E"}}) {
					map { s/"$c"/"$self->{replaced_cols}{"\L$tbsaved\E"}{$c}"/i } @lfkeys;
				}
			}
			my @rfkeys = ();
			push(@rfkeys, @{$self->{tables}{$tbsaved}{foreign_link}{$h->[0]}{remote}{$desttable}});
			if (exists $self->{replaced_cols}{"\L$desttable\E"} && $self->{replaced_cols}{"\L$desttable\E"}) {
				foreach my $c (keys %{$self->{replaced_cols}{"\L$desttable\E"}}) {
					map { s/"$c"/"$self->{replaced_cols}{"\L$desttable\E"}{$c}"/i } @rfkeys;
				}
			}
			if ($self->{preserve_case}) {
				map { s/["]+/"/g; } @rfkeys;
				map { s/["]+/"/g; } @lfkeys;
			} else {
				map { s/["]+//g; } @rfkeys;
				map { s/["]+//g; } @lfkeys;
			}
			if (!$self->{preserve_case}) {
				map { $_ = $self->quote_reserved_words($_) } @lfkeys;
				map { $_ = $self->quote_reserved_words($_) } @rfkeys;
			}
			if (!$self->{preserve_case}) {
					$h->[0] = lc($h->[0]);
			}
			$str .= "ALTER TABLE $table ADD CONSTRAINT $h->[0] FOREIGN KEY (" . join(',', @lfkeys) . ") REFERENCES $subsdesttable(" . join(',', @rfkeys) . ")";
			$str .= " MATCH $h->[2]" if ($h->[2]);
			$str .= " ON DELETE $h->[3]" if ($h->[3]);
			# if DEFER_FKEY is enabled, force constraint to be
			# deferrable and defer it initially.
			$str .= (($self->{'defer_fkey'} ) ? ' DEFERRABLE' : " $h->[4]") if ($h->[4]);
			$str .= " INITIALLY " . ( ($self->{'defer_fkey'} ) ? 'DEFERRED' : $h->[5] ) . ";\n";
			push(@out, $str);
		}
	}

	return wantarray ? @out : join("\n", @out);
}

=head2 _drop_foreign_keys

This function return SQL code to the foreign keys of a table

=cut
sub _drop_foreign_keys
{
	my ($self, $table, @foreign_key) = @_;

	my @out = ();

	$table = $self->get_replaced_tbname($table);

	# Add constraint definition
	my @done = ();
	foreach my $h (@foreign_key) {
		next if (grep(/^$h->[0]$/, @done));
		push(@done, $h->[0]);
		my $str = '';
		$h->[0] = lc($h->[0]) if (!$self->{preserve_case});
		$str .= "ALTER TABLE $table DROP CONSTRAINT IF EXISTS $h->[0];";
		push(@out, $str);
	}

	return wantarray ? @out : join("\n", @out);
}


=head2 _extract_sequence_info

This function retrieves the last value returned from the sequences in the
Oracle database. The result is a SQL script assigning the new start values
to the sequences found in the Oracle database.

=cut
sub _extract_sequence_info
{
	my $self = shift;

	my $sql = "SELECT DISTINCT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, CYCLE_FLAG, ORDER_FLAG, CACHE_SIZE, LAST_NUMBER FROM $self->{prefix}_SEQUENCES";
	if ($self->{schema}) {
		$sql .= " WHERE SEQUENCE_OWNER='$self->{schema}'";
	} else {
		$sql .= " WHERE SEQUENCE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	my @script = ();

	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr ."\n", 0, 1);
	$sth->execute() or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $seq_info = $sth->fetchrow_hashref) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('SEQUENCE', $seq_info->{SEQUENCE_NAME}));

		my $nextvalue = $seq_info->{LAST_NUMBER} + $seq_info->{INCREMENT_BY};
		my $alter ="ALTER SEQUENCE IF EXISTS \L$seq_info->{SEQUENCE_NAME}\E RESTART WITH $nextvalue;";
		if ($self->{preserve_case}) {
			$alter = "ALTER SEQUENCE IF EXISTS \"$seq_info->{SEQUENCE_NAME}\" RESTART WITH $nextvalue;";
		}
		push(@script, $alter);
		$self->logit("Extracted sequence information for sequence \"$seq_info->{SEQUENCE_NAME}\"\n", 1);
	}
	$sth->finish();

	return @script;

}


=head2 _howto_get_data TABLE

This function implements an Oracle-native data extraction.

Returns the SQL query to use to retrieve data

=cut

sub _howto_get_data
{
	my ($self, $table, $name, $type, $src_type, $part_name) = @_;

	# Fix a problem when the table need to be prefixed by the schema
	my $realtable = $table;
	if ($self->{schema}) {
		$realtable = "\U$self->{schema}.$realtable\E";
	}
	my $alias = 'a';
	my $str = "SELECT ";
	my $extraStr = "";
	my $dateformat = 'YYYY-MM-DD HH24:MI:SS';
	my $timeformat = $dateformat;
	if ($self->{enable_microsecond}) {
		$timeformat = 'YYYY-MM-DD HH24:MI:SS.FF';
	}
	my $bfile_found = 0;
	my $timeformat_tz = $timeformat . ' TZH:TZM';
	for my $k (0 .. $#{$name}) {
		if ($name->[$k]->[0] !~ /"/) {
			$name->[$k]->[0] = '"' . $name->[$k]->[0] . '"';
		}
		if ( $src_type->[$k] =~ /date/i) {
			$str .= "to_char($name->[$k]->[0], '$dateformat'),";
		} elsif ( $src_type->[$k] =~ /timestamp.*with time zone/i) {
			$str .= "to_char($name->[$k]->[0], '$timeformat_tz'),";
		} elsif ( $src_type->[$k] =~ /timestamp/i) {
			$str .= "to_char($name->[$k]->[0], '$timeformat'),";
		# Only extract the path to the bfile, if dest type is bytea the bfile should be exported.
		} elsif ( ($src_type->[$k] =~ /bfile/i) && ($type->[$k] =~ /text/i) ) {
			$str .= "ora2pg_get_bfilename($name->[$k]->[0]),";
			$self->{bfile_found} = 1;
		} elsif ( $src_type->[$k] =~ /xmltype/i) {
			if ($self->{xml_pretty}) {
				$str .= "$alias.$name->[$k]->[0].extract('/').getStringVal(),";
			} else {
				$str .= "$alias.$name->[$k]->[0].extract('/').getClobVal(),";
			}
		} elsif ( $src_type->[$k] =~ /geometry/i) {
			my $spatial_sysref = "t.$name->[$k]->[0].SDO_SRID";
			if ($self->{convert_srid}) {
				$spatial_sysref = "sdo_cs.map_oracle_srid_to_epsg(t.$name->[$k]->[0].SDO_SRID)";
			}
			if ($self->{type} eq 'INSERT') {
				$str .= "'ST_GeomFromText('''||SDO_UTIL.TO_WKTGEOMETRY($name->[$k]->[0])||''','||$spatial_sysref||')',";
			} else {
				# Need to find a solution here. Copy want the spatial object to be gserialized.
				# Maybe the SC04 package can be used. Need more work
				#$str .= "ST_AsEWKB(ST_GeomFromEWKT('SRID=' || $spatial_sysref || ';' || SDO_UTIL.TO_WKTGEOMETRY($name->[$k]->[0])))"
			}
		} else {
			$str .= "$name->[$k]->[0],";
		}
	}
	$str =~ s/,$//;

	# If we have a BFILE we need to create a function
	if ($self->{bfile_found}) {
		$self->logit("Creating function ora2pg_get_bfilename( p_bfile IN BFILE ) to retrieve path from BFILE.\n", 1);
		my $bfile_function = qq{
CREATE OR REPLACE FUNCTION ora2pg_get_bfilename( p_bfile IN BFILE ) RETURN 
VARCHAR2
  AS
    l_dir   VARCHAR2(4000);
    l_fname VARCHAR2(4000);
    l_path  VARCHAR2(4000);
  BEGIN
    IF p_bfile IS NULL
    THEN RETURN NULL;
    ELSE
      dbms_lob.FILEGETNAME( p_bfile, l_dir, l_fname );
      SELECT directory_path INTO l_path FROM all_directories WHERE directory_name = l_dir;
      l_dir := rtrim(l_path,'/');
      RETURN l_dir || '/' || l_fname;
  END IF;
  END;
};
		my $sth2 = $self->{dbh}->do($bfile_function);
	}

	# Fix a problem when using data_limit AND where clause
	if (exists $self->{where}{"\L$table\E"} && $self->{where}{"\L$table\E"}) {
		$extraStr .= ' AND (' . $self->{where}{"\L$table\E"} . ')';
	} elsif ($self->{global_where}) {
		$extraStr .= ' AND (' . $self->{global_where} . ')';
	}
	if ($part_name) {
		$alias = "PARTITION($part_name)";
	} else {
		$alias = 't';
	}
	$str .= " FROM $realtable $alias";
	if (exists $self->{where}{"\L$table\E"} && $self->{where}{"\L$table\E"}) {
		if ($str =~ / WHERE /) {
			$str .= ' AND ';
		} else {
			$str .= ' WHERE ';
		}
		$str .= '(' . $self->{where}{"\L$table\E"} . ')';
		$self->logit("\tApplying WHERE clause on table: " . $self->{where}{"\L$table\E"} . "\n", 1);
	} elsif ($self->{global_where}) {
		if ($str =~ / WHERE /) {
			$str .= ' AND ';
		} else {
			$str .= ' WHERE ';
		}
		$str .= '(' . $self->{global_where} . ')';
		$self->logit("\tApplying WHERE global clause: " . $self->{global_where} . "\n", 1);
	}

	if ( ($self->{oracle_copies} > 1) && $self->{defined_pk}{"\L$table\E"} ) {
		if ($str =~ / WHERE /) {
			$str .= " AND ABS(MOD(" . $self->{defined_pk}{"\L$table\E"} . ", $self->{oracle_copies})) = ?";
		} else {
			$str .= " WHERE ABS(MOD(" . $self->{defined_pk}{"\L$table\E"} . ", $self->{oracle_copies})) = ?";
		}
	}

	return $str;
}


=head2 _sql_type INTERNAL_TYPE LENGTH PRECISION SCALE

This function returns the PostgreSQL data type corresponding to the
Oracle data type.

=cut

sub _sql_type
{
        my ($self, $type, $len, $precision, $scale) = @_;

	my $data_type = '';

	# Simplify timestamp type
	$type =~ s/TIMESTAMP\(\d+\)/TIMESTAMP/i;

        # Overide the length
        $len = $precision if ( ($type eq 'NUMBER') && $precision );

        if (exists $TYPE{uc($type)}) {
		$type = uc($type); # Force uppercase
		if ($len) {

			if ( ($type eq "CHAR") || ($type =~ /VARCHAR/) ) {
				# Type CHAR have default length set to 1
				# Type VARCHAR(2) must have a specified length
				$len = 1 if (!$len && ($type eq "CHAR"));
                		return "$TYPE{$type}($len)";
			} elsif ($type eq "NUMBER") {
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
					} elsif ($self->{pg_integer_type}) {
						# Most of the time interger should be enought?
						return $self->{default_numeric} || 'bigint';
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
			return "$TYPE{$type}";
		} else {
			if (($type eq 'NUMBER') && $self->{pg_integer_type}) {
				return $self->{default_numeric};
			} else {
				return $TYPE{$type};
			}
		}
        }

        return $type;
}


=head2 _column_info TABLE OWNER

This function implements an Oracle-native column information.

Returns a list of array references containing the following information
elements for each column the specified table

[(
  column name,
  column type,
  column length,
  nullable column,
  default value
  ...
)]

=cut

sub _column_info
{
	my ($self, $table, $owner, $recurs) = @_;

	my $condition = '';
	$condition .= "AND TABLE_NAME='$table' " if ($table);
	$condition .= "AND OWNER='$owner' " if ($owner);
	$condition .= $self->limit_to_tables() if (!$table);
	$condition =~ s/^AND/WHERE/;

	my $sth = '';
	if ($self->{db_version} !~ /Release 8/) {
		$sth = $self->{dbh}->prepare(<<END);
SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE, DATA_DEFAULT, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH, TABLE_NAME, OWNER
FROM $self->{prefix}_TAB_COLUMNS $condition
ORDER BY COLUMN_ID
END
		if (!$sth) {
			my $ret = $self->{dbh}->err;
			if (!$recurs && ($ret == 942) && ($self->{prefix} eq 'DBA')) {
				$self->logit("HINT: Please activate USER_GRANTS or connect using a user with DBA privilege.\n");
				$self->{prefix} = 'ALL';
				return $self->_column_info($table, $owner, 1);
			}
			$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		}
	} else {
		# an 8i database.
		$sth = $self->{dbh}->prepare(<<END);
SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE, DATA_DEFAULT, DATA_PRECISION, DATA_SCALE, DATA_LENGTH, TABLE_NAME, OWNER
FROM $self->{prefix}_TAB_COLUMNS $condition
ORDER BY COLUMN_ID
END
		if (!$sth) {
			my $ret = $self->{dbh}->err;
			if (!$recurs && ($ret == 942) && ($self->{prefix} eq 'DBA')) {
				$self->logit("HINT: Please activate USER_GRANTS or connect using a user with DBA privilege.\n");
				$self->{prefix} = 'ALL';
				return $self->_column_info($table, $owner, 1);
			}
			$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		}
	}
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my $spatial_query =  'SELECT DISTINCT c.%s.SDO_GTYPE FROM %s c';
	my $spatial_sysref = 'SELECT DISTINCT c.%s.SDO_SRID FROM %s c';
	if ($self->{convert_srid}) {
		$spatial_sysref = 'SELECT DISTINCT sdo_cs.map_oracle_srid_to_epsg(c.%s.SDO_SRID) FROM %s c';
	}

	my %data = ();
	my $pos = 0;
	while (my $row = $sth->fetch) {
		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('TABLE', $row->[-2]));
		if ($#{$row} == 9) {
			$row->[2] = $row->[7] if $row->[1] =~ /char/i;
		}
		# check if this is a spatial column
		my @geom_inf = ();
		if ($row->[1] eq 'SDO_GEOMETRY') {
			# Set dimension and type of the spatial column
			if ($self->{autodetect_spatial_type}) {
				# Get spatial information
				my $squery = sprintf($spatial_query, $row->[0], $row->[-2]);
				my $sth2 = $self->{dbh}->prepare($squery);
				if (!$sth2) {
					$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
				}
				$sth2->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
				my @result = ();
				my @dims = ();
				while (my $r = $sth2->fetch) {
					$r->[0] =~s /^(\d)00//;
					push(@result, $r->[0]);
					push(@dims, $1) if (!grep(/^$1/, @dims));
				}
				$sth2->finish();
				if ($#result == 0) {
					push(@geom_inf, $dims[0], $result[0]);
				} elsif ($#dims == 0) {
					push(@geom_inf, $dims[0], 0);
				} else {
					push(@geom_inf, 0, 0);
				}
			} else {
				push(@geom_inf, 0, 0);
			}
			# Get the SRID of the column
			my $squery = sprintf($spatial_sysref, $row->[0], $row->[-2]);
			my $sth2 = $self->{dbh}->prepare($squery);
			if (!$sth2) {
				$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			}
			$sth2->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			my @result = ();
			while (my $r = $sth2->fetch) {
				push(@result, $r->[0]) if ($r->[0] =~ /\d+/);
			}
			$sth2->finish();
			if ($#result == 0) {
				push(@geom_inf, $result[0]);
			} else {
				push(@geom_inf, 0);
			}
		}
		push(@{$data{$row->[-2]}{$row->[0]}}, (@$row, $pos, @geom_inf));

		$pos++;
	}

	return %data;	
}

=head2 _unique_key TABLE OWNER

This function implements an Oracle-native unique (including primary)
key column information.

Returns a hash of hashes in the following form:
    ( owner => table => constraintname => (type => 'PRIMARY',
                         columns => ('a', 'b', 'c')),
      owner => table => constraintname => (type => 'UNIQUE',
                         columns => ('b', 'c', 'd')),
      etc.
    )

=cut

sub _unique_key
{
	my($self, $table, $owner) = @_;

	my %result = ();
        my @accepted_constraint_types = ();
        push @accepted_constraint_types, "'P'" unless($self->{skip_pkeys});
        push @accepted_constraint_types, "'U'" unless($self->{skip_ukeys});
        return %result unless(@accepted_constraint_types);

        my $cons_types = '('. join(',', @accepted_constraint_types) .')';

	my $sql = "SELECT DISTINCT COLUMN_NAME,POSITION,CONSTRAINT_NAME FROM $self->{prefix}_CONS_COLUMNS";
	$sql .=  " WHERE OWNER='$owner'" if ($owner);
	$sql .=  " ORDER BY POSITION";
	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $sth->errstr . "\n", 0, 1);
	my @cons_columns = ();
	while (my $r = $sth->fetch) {
		push(@cons_columns, [ @$r ]);
	}
	$sth->finish;

	my $condition = '';
	$condition .= "AND TABLE_NAME='$table' " if ($table);
	$condition .= "AND OWNER='$owner' " if ($owner);
	$condition .= $self->limit_to_tables() if (!$table);

	if ($self->{db_version} !~ /Release 8/) {
		$sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,DEFERRABLE,DEFERRED,R_OWNER,CONSTRAINT_TYPE,GENERATED,TABLE_NAME,OWNER,INDEX_NAME
FROM $self->{prefix}_CONSTRAINTS
WHERE CONSTRAINT_TYPE IN $cons_types
AND STATUS='ENABLED'
$condition
END
	} else {
		$sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,DEFERRABLE,DEFERRED,R_OWNER,CONSTRAINT_TYPE,GENERATED,TABLE_NAME,OWNER
FROM $self->{prefix}_CONSTRAINTS
WHERE CONSTRAINT_TYPE IN $cons_types
AND STATUS='ENABLED'
$condition
END
	}
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch) {
		if ($self->{db_version} =~ /Release 8/) {
			push(@$row, '');
		}
		my %constraint = (type => $row->[7], 'generated' => $row->[8], 'index_name' => $row->[11], columns => ());
		foreach my $r (@cons_columns) {
			# Skip constraints on system internal columns
			next if ($r->[0] =~ /^SYS_NC/i);
			if ($r->[2] eq $row->[0]) {
				push(@{$constraint{'columns'}}, $r->[0]);
			}
		}
		if ($#{$constraint{'columns'}} >= 0) {
			$result{$row->[9]}{$row->[0]} = \%constraint;
		}
	}
	return %result;

}

=head2 _check_constraint TABLE OWNER

This function implements an Oracle-native check constraint
information.

Returns a hash of lists of all column names defined as check constraints
for the specified table and constraint name.

=cut

sub _check_constraint
{
	my($self, $table, $owner) = @_;

	my $condition = '';
	$condition .= "AND TABLE_NAME='$table' " if ($table);
	$condition .= "AND OWNER='$owner' " if ($owner);
	$condition .= $self->limit_to_tables() if (!$table);

	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,DEFERRABLE,DEFERRED,R_OWNER,TABLE_NAME,OWNER
FROM $self->{prefix}_CONSTRAINTS
WHERE CONSTRAINT_TYPE='C' $condition
AND STATUS='ENABLED'
END

	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		$data{$row->[7]}{constraint}{$row->[0]} = $row->[2];
	}

	return %data;
}


=head2 _foreign_key TABLE OWNER

This function implements an Oracle-native foreign key reference
information.

Returns a list of hash of hash of array references. Ouf! Nothing very difficult.
The first hash is composed of all foreign key names. The second hash has just
two keys known as 'local' and 'remote' corresponding to the local table where
the foreign key is defined and the remote table referenced by the key.

The foreign key name is composed as follows:

    'local_table_name->remote_table_name'

Foreign key data consists in two arrays representing at the same index for the
local field and the remote field where the first one refers to the second one.
Just like this:

    @{$link{$fkey_name}{local}} = @local_columns;
    @{$link{$fkey_name}{remote}} = @remote_columns;

=cut

sub _foreign_key
{
	my ($self, $table, $owner) = @_;

	my $condition = '';
	$condition .= "AND TABLE_NAME='$table' " if ($table);
	$condition .= "AND OWNER='$owner' " if ($owner);
	$condition .= $self->limit_to_tables() if (!$table);

	my $sql = "SELECT DISTINCT COLUMN_NAME,POSITION,TABLE_NAME,OWNER,CONSTRAINT_NAME FROM $self->{prefix}_CONS_COLUMNS";
	$sql .= " WHERE OWNER='$owner'" if ($owner);
	$sql .= " ORDER BY POSITION";
	my $sth2 = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
	my @cons_columns = ();
	while (my $r = $sth2->fetch) {
		push(@cons_columns, [ @$r ]);
	}

	my $deferrable = $self->{fkey_deferrable} ? "'DEFERRABLE' AS DEFERRABLE" : "DEFERRABLE";
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,$deferrable,DEFERRED,R_OWNER,TABLE_NAME,OWNER
FROM $self->{prefix}_CONSTRAINTS
WHERE CONSTRAINT_TYPE='R' $condition
AND STATUS='ENABLED'
END
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	my %link = ();
	my @tab_done = ();
	while (my $row = $sth->fetch) {
		next if (grep(/^$row->[7]$row->[0]$/, @tab_done));
		push(@{$data{$row->[7]}}, [ @$row ]);
		push(@tab_done, "$row->[7]$row->[0]");
		my @done = ();
		foreach my $r (@cons_columns) {
			# Skip it if tablename and owner are not the same
			next if (($r->[2] ne $row->[7]) && ($r->[3] ne $row->[8]));
			# If the names of the constraints are the same set the local column of the foreign keys
			if ($r->[4] eq $row->[0]) {
				if (!grep(/^$r->[2]$r->[0]$/, @done)) {
					push(@{$link{$row->[7]}{$row->[0]}{local}}, $r->[0]);
					push(@done, "$r->[2]$r->[0]");
				}
			}
		}
		@done = ();

		foreach my $r (@cons_columns) {
			# Skip it if tablename and owner are not the same
			next if (($r->[2] ne $row->[7]) && ($r->[3] ne $row->[8]));
			# If the names of the constraints are the same as the unique constraint definition for
			# the referenced table set the remote part of the foreign keys
			if ($r->[4] eq $row->[1]) {
				if (!grep(/^$r->[2]$r->[0]$/, @done)) {
					push(@{$link{$row->[7]}{$row->[0]}{remote}{$r->[2]}}, $r->[0]);
					push(@done, "$r->[2]$r->[0]");
				}
			}
		}
	}

	return \%link, \%data;
}

=head2 _get_privilege

This function implements an Oracle-native obkect priviledge information.

Returns a hash of all privilede.

=cut

sub _get_privilege
{
	my($self) = @_;

	my %privs = ();
	my %roles = ();

	# Retrieve all privilege per table defined in this database
	my $str = "SELECT b.GRANTEE,b.OWNER,b.TABLE_NAME,b.PRIVILEGE,a.OBJECT_TYPE FROM DBA_TAB_PRIVS b, DBA_OBJECTS a";
	if ($self->{schema}) {
		$str .= " WHERE b.GRANTOR = '$self->{schema}'";
	} else {
		$str .= " WHERE b.GRANTOR NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$str .= " AND b.TABLE_NAME=a.OBJECT_NAME AND a.OWNER=b.GRANTOR";
	$str .= " " . $self->limit_to_tables('b.TABLE_NAME');
	
	if (!$self->{export_invalid}) {
		$str .= " AND a.STATUS='VALID'";
	}
	$str .= " ORDER BY b.TABLE_NAME, b.GRANTEE";

	my $error = "\n\nFATAL: You must be connected as an oracle dba user to retrieved grants\n\n";
	my $sth = $self->{dbh}->prepare($str) or $self->logit($error . "FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		$privs{$row->[2]}{type} = $row->[4];
		$privs{$row->[2]}{owner} = $row->[1] if (!$privs{$row->[2]}{owner});
		push(@{$privs{$row->[2]}{privilege}{$row->[0]}}, $row->[3]);
		push(@{$roles{owner}}, $row->[1]) if (!grep(/^$row->[1]$/, @{$roles{owner}}));
		push(@{$roles{grantee}}, $row->[0]) if (!grep(/^$row->[0]$/, @{$roles{grantee}}));
	}
	$sth->finish();

	# Retrieve all privilege per column table defined in this database
	$str = "SELECT b.GRANTEE,b.OWNER,b.TABLE_NAME,b.PRIVILEGE,b.COLUMN_NAME FROM DBA_COL_PRIVS b";
	if (!$self->{export_invalid}) {
		$str .= ", DBA_OBJECTS a";
	}
	if ($self->{schema}) {
		$str .= " WHERE b.GRANTOR = '$self->{schema}'";
	} else {
		$str .= " WHERE b.GRANTOR NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	if (!$self->{export_invalid}) {
		$str .= " AND a.STATUS='VALID' AND b.TABLE_NAME=a.OBJECT_NAME AND a.OWNER=b.GRANTOR";
	}
	$str .= " " . $self->limit_to_tables('b.TABLE_NAME');

	$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
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
		$str = "SELECT PRIVILEGE,ADMIN_OPTION FROM DBA_SYS_PRIVS WHERE GRANTEE = '$r' ORDER BY PRIVILEGE";
		$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			push(@{$roles{admin}{$r}{privilege}}, $row->[0]);
			push(@{$roles{admin}{$r}{admin_option}}, $row->[1]);
		}
		$sth->finish();
	}
	# Now try to find if it's a user or a role 
	foreach my $u (@done) {
		$str = "SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE = '$u'";
		$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			push(@{$roles{role}{$u}}, $row->[0]);
		}
		$str = "SELECT USERNAME FROM DBA_USERS WHERE USERNAME = '$u'";
		$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			$roles{type}{$u} = 'USER';
		}
		next if  $roles{type}{$u};
		$str = "SELECT ROLE,PASSWORD_REQUIRED FROM DBA_ROLES WHERE ROLE='$u'";
		$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			$roles{type}{$u} = 'ROLE';
			$roles{password_required}{$u} = $row->[1];
		}
		$sth->finish();
	}

	return (\%privs, \%roles);
}



=head2 _get_indexes TABLE OWNER

This function implements an Oracle-native indexes information.

Returns a hash of an array containing all unique indexes and a hash of
array of all indexe names which are not primary keys for the specified table.

=cut

sub _get_indexes
{
	my ($self, $table, $owner) = @_;

	my $idxowner = '';
	if ($owner) {
		$idxowner = "AND IC.TABLE_OWNER = '$owner'";
	}
	my $sub_owner = '';
	if ($owner) {
		$sub_owner = "AND OWNER=$self->{prefix}_INDEXES.TABLE_OWNER";
	}

	my $condition = '';
	$condition .= "AND $self->{prefix}_IND_COLUMNS.TABLE_NAME='$table' " if ($table);
	$condition .= "AND $self->{prefix}_IND_COLUMNS.INDEX_OWNER='$owner' AND $self->{prefix}_INDEXES.OWNER='$owner' " if ($owner);
	$condition .= $self->limit_to_tables("$self->{prefix}_IND_COLUMNS.TABLE_NAME") if (!$table);

	# Retrieve all indexes 
	my $sth = '';
	if ($self->{db_version} !~ /Release 8/) {
		$sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT DISTINCT $self->{prefix}_IND_COLUMNS.INDEX_NAME,$self->{prefix}_IND_COLUMNS.COLUMN_NAME,$self->{prefix}_INDEXES.UNIQUENESS,$self->{prefix}_IND_COLUMNS.COLUMN_POSITION,$self->{prefix}_INDEXES.INDEX_TYPE,$self->{prefix}_INDEXES.TABLE_TYPE,$self->{prefix}_INDEXES.GENERATED,$self->{prefix}_INDEXES.JOIN_INDEX,$self->{prefix}_IND_COLUMNS.TABLE_NAME,$self->{prefix}_IND_COLUMNS.INDEX_OWNER,$self->{prefix}_INDEXES.TABLESPACE_NAME,$self->{prefix}_INDEXES.ITYP_NAME
FROM $self->{prefix}_IND_COLUMNS
JOIN $self->{prefix}_INDEXES ON ($self->{prefix}_INDEXES.INDEX_NAME=$self->{prefix}_IND_COLUMNS.INDEX_NAME)
WHERE $self->{prefix}_INDEXES.GENERATED <> 'Y' AND $self->{prefix}_INDEXES.TEMPORARY <> 'Y' $condition
ORDER BY $self->{prefix}_IND_COLUMNS.COLUMN_POSITION
END
	} else {
		# an 8i database.
		$sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT DISTINCT $self->{prefix}_IND_COLUMNS.INDEX_NAME,$self->{prefix}_IND_COLUMNS.COLUMN_NAME,$self->{prefix}_INDEXES.UNIQUENESS,$self->{prefix}_IND_COLUMNS.COLUMN_POSITION,$self->{prefix}_INDEXES.INDEX_TYPE,$self->{prefix}_INDEXES.TABLE_TYPE,$self->{prefix}_INDEXES.GENERATED,$self->{prefix}_IND_COLUMNS.TABLE_NAME,$self->{prefix}_IND_COLUMNS.INDEX_OWNER,$self->{prefix}_INDEXES.TABLESPACE_NAME,$self->{prefix}_INDEXES.ITYP_NAME
FROM $self->{prefix}_IND_COLUMNS, $self->{prefix}_INDEXES
WHERE $self->{prefix}_INDEXES.INDEX_NAME=$self->{prefix}_IND_COLUMNS.INDEX_NAME $condition
AND $self->{prefix}_INDEXES.GENERATED <> 'Y'
AND $self->{prefix}_INDEXES.TEMPORARY <> 'Y'
ORDER BY $self->{prefix}_IND_COLUMNS.COLUMN_POSITION
END
	}

	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my $idxnc = qq{SELECT IE.COLUMN_EXPRESSION FROM $self->{prefix}_IND_EXPRESSIONS IE, $self->{prefix}_IND_COLUMNS IC
WHERE  IE.INDEX_OWNER = IC.INDEX_OWNER
AND    IE.INDEX_NAME = IC.INDEX_NAME
AND    IE.TABLE_OWNER = IC.TABLE_OWNER
AND    IE.TABLE_NAME = IC.TABLE_NAME
AND    IE.COLUMN_POSITION = IC.COLUMN_POSITION
AND    IC.COLUMN_NAME = ?
AND    IE.TABLE_NAME = ?
$idxowner
};
	my $sth2 = $self->{dbh}->prepare($idxnc);
	my %data = ();
	my %unique = ();
	my %idx_type = ();
	while (my $row = $sth->fetch) {
		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('INDEX', $row->[0]));
		$unique{$row->[-4]}{$row->[0]} = $row->[2];
		if (($#{$row} > 6) && ($row->[7] eq 'Y')) {
			$idx_type{$row->[-4]}{$row->[0]}{type} = $row->[4] . ' JOIN';
		} else {
			$idx_type{$row->[-4]}{$row->[0]}{type} = $row->[4];
		}
		if ($row->[-1] =~ /SPATIAL_INDEX/) {
			$idx_type{$row->[-4]}{$row->[0]}{type_name} = $row->[-1];
		}
		# Replace function based index type
		if ($row->[1] =~ /^SYS_NC/i) {
			$sth2->execute($row->[1],$row->[-3]) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			my $nc = $sth2->fetch();
			$row->[1] = $nc->[0];
		}
		$row->[1] =~ s/SYS_EXTRACT_UTC[\s\t]*\(([^\)]+)\)/$1/isg;
		push(@{$data{$row->[-4]}{$row->[0]}}, $row->[1]);
		$index_tablespace{$row->[-4]}{$row->[0]} = $row->[-2];
	}

	return \%unique, \%data, \%idx_type, \%index_tablespace;
}


=head2 _get_sequences

This function implements an Oracle-native sequences information.

Returns a hash of an array of sequence names with MIN_VALUE, MAX_VALUE,
INCREMENT and LAST_NUMBER for the specified table.

=cut

sub _get_sequences
{
	my($self) = @_;

	# Retrieve all indexes 
	my $str = "SELECT DISTINCT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, LAST_NUMBER, CACHE_SIZE, CYCLE_FLAG, SEQUENCE_OWNER FROM $self->{prefix}_SEQUENCES";
	if (!$self->{schema}) {
		$str .= " WHERE SEQUENCE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE SEQUENCE_OWNER = '$self->{schema}'";
	}
	$str .= " ORDER BY SEQUENCE_NAME";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @seqs = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('SEQUENCE', $row->[0]));

		push(@seqs, [ @$row ]);
	}

	return \@seqs;
}

=head2 _get_external_tables

This function implements an Oracle-native external tables information.

Returns a hash of external tables names with the file they are based on.

=cut

sub _get_external_tables
{
	my($self) = @_;

	# Retrieve all database link from dba_db_links table
	my $str = "SELECT a.*,b.DIRECTORY_PATH,c.LOCATION FROM $self->{prefix}_EXTERNAL_TABLES a, $self->{prefix}_DIRECTORIES b, $self->{prefix}_EXTERNAL_LOCATIONS c";
	if (!$self->{schema}) {
		$str .= " WHERE a.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE a.OWNER = '$self->{schema}'";
	}
	$str .= " AND a.DEFAULT_DIRECTORY_NAME = b.DIRECTORY_NAME AND a.TABLE_NAME=c.TABLE_NAME AND a.DEFAULT_DIRECTORY_NAME=c.DIRECTORY_NAME AND a.OWNER=c.OWNER";
	$str .= " ORDER BY a.TABLE_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	
	my %data = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('TABLE', $row->[1]));

		$data{$row->[1]}{directory} = $row->[5];
		$data{$row->[1]}{directory_path} = $row->[10];
		if ($data{$row->[1]}{directory_path} =~ /([\/\\])/) {
			$data{$row->[1]}{directory_path} .= $1 if ($data{$row->[1]}{directory_path} !~ /$1$/); 
		}
		$data{$row->[1]}{location} = $row->[11];
		$data{$row->[1]}{delimiter} = ',';
		if ($row->[8] =~ /FIELDS TERMINATED BY '(.)'/) {
			$data{$row->[1]}{delimiter} = $1;
		}
	}
	$sth->finish();

	return %data;
}


=head2 _get_dblink

This function implements an Oracle-native database link information.

Returns a hash of dblink names with the connection they are based on.

=cut


sub _get_dblink
{
	my($self) = @_;

	# Retrieve all database link from dba_db_links table
	my $str = "SELECT OWNER,DB_LINK,USERNAME,HOST,CREATED FROM $self->{prefix}_db_links";
	if (!$self->{schema}) {
		$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE OWNER = '$self->{schema}'";
	}
	$str .= " ORDER BY DB_LINK";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		$data{$row->[1]}{owner} = $row->[0];
		$data{$row->[1]}{username} = $row->[2];
		$data{$row->[1]}{host} = $row->[3];
	}

	return %data;
}

=head2 _get_job

This function implements an Oracle-native job information.

Returns a hash of job number with the connection they are based on.

=cut


sub _get_job
{
	my($self) = @_;

	# Retrieve all database job from user_jobs table
	my $str = "SELECT JOB,WHAT,INTERVAL FROM $self->{prefix}_jobs";
	if (!$self->{schema}) {
		$str .= " WHERE SCHEMA_USER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE SCHEMA_USER = '$self->{schema}'";
	}
	$str .= " ORDER BY JOB";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		$data{$row->[0]}{what} = $row->[1];
		$data{$row->[0]}{interval} = $row->[2];
	}

	return %data;
}


=head2 _get_views

This function implements an Oracle-native views information.

Returns a hash of view names with the SQL queries they are based on.

=cut

sub _get_views
{
	my($self) = @_;

	# Retrieve all views
	my $str = "SELECT VIEW_NAME,TEXT FROM $self->{prefix}_VIEWS v";
	if (!$self->{export_invalid}) {
		$str .= ", $self->{prefix}_OBJECTS a";
	}

	if (!$self->{schema}) {
		$str .= " WHERE v.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE v.OWNER = '$self->{schema}'";
	}
	if (!$self->{export_invalid}) {
		$str .= " AND a.OBJECT_TYPE='VIEW' AND a.STATUS='VALID' AND v.VIEW_NAME=a.OBJECT_NAME AND a.OWNER=v.OWNER";
	}
	$str .= " ORDER BY v.VIEW_NAME";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		if (!grep($row->[0] =~ /^$_$/, @{$self->{view_as_table}})) {
			next if ($self->skip_this_object('VIEW', $row->[0]));
		}

		$data{$row->[0]} = $row->[1];
		@{$data{$row->[0]}{alias}} = $self->_alias_info ($row->[0]);
	}

	return %data;
}

=head2 _get_materialized_views

This function implements an Oracle-native materialized views information.

Returns a hash of view names with the SQL queries they are based on.

=cut

sub _get_materialized_views
{
	my($self) = @_;

	# Retrieve all views
	my $str = "SELECT MVIEW_NAME,QUERY,UPDATABLE,REFRESH_MODE,REFRESH_METHOD,USE_NO_INDEX,REWRITE_ENABLED,BUILD_MODE FROM $self->{prefix}_MVIEWS";
	if ($self->{db_version} =~ /Release 8/) {
		$str = "SELECT MVIEW_NAME,QUERY,UPDATABLE,REFRESH_MODE,REFRESH_METHOD,'',REWRITE_ENABLED,BUILD_MODE FROM $self->{prefix}_MVIEWS";
	}
	if (!$self->{schema}) {
		$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE OWNER = '$self->{schema}'";
	}
	$str .= " ORDER BY MVIEW_NAME";
	my $sth = $self->{dbh}->prepare($str);
	if (not defined $sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	if (not $sth->execute) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		return ();
	}

	my %data = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('MVIEW', $row->[0]));

		$data{$row->[0]}{text} = $row->[1];
		$data{$row->[0]}{updatable} = ($row->[2] eq 'Y') ? 1 : 0;
		$data{$row->[0]}{refresh_mode} = $row->[3];
		$data{$row->[0]}{refresh_method} = $row->[4];
		$data{$row->[0]}{no_index} = ($row->[5] eq 'Y') ? 1 : 0;
		$data{$row->[0]}{rewritable} = ($row->[6] eq 'Y') ? 1 : 0;
		$data{$row->[0]}{build_mode} = $row->[7];
	}

	return %data;
}

sub _get_materialized_view_names
{
	my($self) = @_;

	# Retrieve all views
	my $str = "SELECT MVIEW_NAME FROM $self->{prefix}_MVIEWS";
	if (!$self->{schema}) {
		$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE OWNER = '$self->{schema}'";
	}
	$str .= " ORDER BY MVIEW_NAME";
	my $sth = $self->{dbh}->prepare($str);
	if (not defined $sth) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	if (not $sth->execute) {
		$self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}

	my @data = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('MVIEW', $row->[0]));
		push(@data, uc($row->[0]));
	}

	return @data;
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

	my $str = "SELECT COLUMN_NAME, COLUMN_ID FROM $self->{prefix}_TAB_COLUMNS WHERE TABLE_NAME='$view'";
	if ($self->{schema}) {
		$str .= " AND OWNER = '$self->{schema}'";
	} else {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$str .= " ORDER BY COLUMN_ID ASC";
        my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        my $data = $sth->fetchall_arrayref();
	$self->logit("View $view column aliases:\n", 1);
	foreach my $d (@$data) {
		$self->logit("\t$d->[0] =>  column id:$d->[1]\n", 1);
	}

        return @$data; 

}

=head2 _get_triggers

This function implements an Oracle-native triggers information. 

Returns an array of refarray of all triggers information.

=cut

sub _get_triggers
{
	my($self) = @_;

	# Retrieve all indexes 
	my $str = "SELECT TRIGGER_NAME, TRIGGER_TYPE, TRIGGERING_EVENT, TABLE_NAME, TRIGGER_BODY, WHEN_CLAUSE, DESCRIPTION,ACTION_TYPE FROM $self->{prefix}_TRIGGERS WHERE STATUS='ENABLED'";
	if (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
	}
	$str .= " " . $self->limit_to_tables();

	$str .= " ORDER BY TABLE_NAME, TRIGGER_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @triggers = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('TRIGGER', $row->[0]));

		push(@triggers, [ @$row ]);
	}

	return \@triggers;
}


=head2 _get_functions

This function implements an Oracle-native functions information.

Returns a hash of all function names with their PLSQL code.

=cut

sub _get_functions
{
	my $self = shift;

	# Retrieve all functions 
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE='FUNCTION'";
	$str .= " AND STATUS='VALID'" if (!$self->{export_invalid});
	if (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
	}
	$str .= " ORDER BY OBJECT_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %functions = ();
	my @fct_done = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('FUNCTION', $row->[0]));

		next if (grep(/^$row->[0]$/, @fct_done));
		push(@fct_done, $row->[0]);
		my $sql = "SELECT TEXT FROM $self->{prefix}_SOURCE WHERE OWNER='$row->[1]' AND NAME='$row->[0]' ORDER BY LINE";
		my $sth2 = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch) {
			$functions{"$row->[0]"} .= $r->[0];
		}
	}

	return \%functions;
}

=head2 _get_procedures

This procedure implements an Oracle-native procedures information.

Returns a hash of all procedure names with their PLSQL code.

=cut

sub _get_procedures
{
	my $self = shift;

	# Retrieve all procedures 
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE='PROCEDURE'";
	$str .= " AND STATUS='VALID'" if (!$self->{export_invalid});
	if (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
	}
	$str .= " ORDER BY OBJECT_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %procedures = ();
	my @fct_done = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('PROCEDURE', $row->[0]));

		next if (grep(/^$row->[0]$/, @fct_done));
		push(@fct_done, $row->[0]);
		my $sql = "SELECT TEXT FROM $self->{prefix}_SOURCE WHERE OWNER='$row->[1]' AND NAME='$row->[0]' ORDER BY LINE";
		my $sth2 = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch) {
			$procedures{"$row->[0]"} .= $r->[0];
		}
	}

	return \%procedures;
}


=head2 _get_packages

This function implements an Oracle-native packages information.

Returns a hash of all package names with their PLSQL code.

=cut

sub _get_packages
{
	my ($self) = @_;

	# Retrieve all indexes 
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE LIKE 'PACKAGE%'";
	$str .= " AND STATUS='VALID'" if (!$self->{export_invalid});
	if (!$self->{schema}) {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
	}
	$str .= " ORDER BY OBJECT_NAME";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %packages = ();
	my @fct_done = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('PACKAGE', $row->[0]));

		$self->logit("\tFound Package: $row->[0]\n", 1);
		next if (grep(/^$row->[0]$/, @fct_done));
		push(@fct_done, $row->[0]);
		my $sql = "SELECT TEXT FROM $self->{prefix}_SOURCE WHERE OWNER='$row->[1]' AND NAME='$row->[0]' AND (TYPE='PACKAGE' OR TYPE='PACKAGE BODY') ORDER BY TYPE, LINE";
		my $sth2 = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch) {
			$packages{"$row->[0]"} .= $r->[0];
		}
	}

	return \%packages;
}

=head2 _get_types

This function implements an Oracle custom types information.

Returns a hash of all type names with their code.

=cut

sub _get_types
{
	my ($self, $name) = @_;

	# Retrieve all user defined types
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE='TYPE'";
	$str .= " AND STATUS='VALID'" if (!$self->{export_invalid});
	$str .= " AND OBJECT_NAME='$name'" if ($name);
	$str .= " AND GENERATED='N'";
	if (!$self->{schema}) {
		# We need to remove SYSTEM from the exclusion list
		shift(@{$self->{sysusers}});
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " AND OWNER = '$self->{schema}'";
	}
	$str .= " ORDER BY OBJECT_NAME";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @types = ();
	my @fct_done = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('TYPE', $row->[0]));

		$self->logit("\tFound Type: $row->[0]\n", 1);
		next if (grep(/^$row->[0]$/, @fct_done));
		push(@fct_done, $row->[0]);
		my %tmp = ();
		my $sql = "SELECT TEXT FROM $self->{prefix}_SOURCE WHERE OWNER='$row->[1]' AND NAME='$row->[0]' AND (TYPE='TYPE' OR TYPE='TYPE BODY') ORDER BY TYPE, LINE";
		my $sth2 = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch) {
			$tmp{code} .= $r->[0];
		}
		$tmp{name} .= $row->[0];
		push(@types, \%tmp);
	}

	return \@types;
}

=head2 _table_info

This function retrieves all Oracle-native tables information.

Returns a handle to a DB query statement.

=cut

sub _table_info
{
	my $self = shift;

	my @mviews = $self->_get_materialized_view_names();

	my $owner = '';
	if ($self->{schema}) {
		$owner .= "AND OWNER='$self->{schema}' ";
	} else {
            $owner .= "AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	$owner .= $self->limit_to_tables();
	$owner =~ s/^AND/WHERE/;

	my %comments = ();
	my $sql = "SELECT TABLE_NAME,COMMENTS,TABLE_TYPE FROM ALL_TAB_COMMENTS $owner";
	my $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('TABLE', $row->[0]));
		next if (grep(/^$row->[0]$/i, @mviews));
		$comments{$row->[0]}{comment} = $row->[1];
		$comments{$row->[0]}{table_type} = $row->[2];
	}
	$sth->finish();

	$sql = "SELECT OWNER,TABLE_NAME,NVL(num_rows,1) NUMBER_ROWS,TABLESPACE_NAME FROM ALL_TABLES $owner";
        $sql .= " ORDER BY OWNER, TABLE_NAME";
        $sth = $self->{dbh}->prepare( $sql ) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my %tables_infos = ();
	while (my $row = $sth->fetch) {
		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('TABLE', $row->[1]));
		next if (grep(/^$row->[1]$/i, @mviews));
		$tables_infos{$row->[1]}{owner} = $row->[0] || '';
		$tables_infos{$row->[1]}{num_rows} = $row->[2] || 0;
		$tables_infos{$row->[1]}{tablespace} = $row->[3] || 0;
		$tables_infos{$row->[1]}{comment} =  $comments{$row->[1]}{comment} || '';
		$tables_infos{$row->[1]}{type} =  $comments{$row->[1]}{table_type} || '';
	}
	$sth->finish();

	return %tables_infos;
}


=head2 _get_tablespaces

This function implements an Oracle-native tablespaces information.

Returns a hash of an array of tablespace names with their system file path.

=cut

sub _get_tablespaces
{
	my($self) = @_;

	# Retrieve all object with tablespaces.
my $str = qq{
SELECT a.SEGMENT_NAME,a.TABLESPACE_NAME,a.SEGMENT_TYPE,c.FILE_NAME, a.OWNER
FROM DBA_SEGMENTS a, $self->{prefix}_OBJECTS b, DBA_DATA_FILES c
WHERE a.SEGMENT_TYPE IN ('INDEX', 'TABLE')
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
	$str .= " ORDER BY TABLESPACE_NAME";
	my $error = "\n\nFATAL: You must be connected as an oracle dba user to retrieved tablespaces\n\n";
	my $sth = $self->{dbh}->prepare($str) or $self->logit($error . "FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %tbs = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('TABLESPACE', $row->[1]));

		# TYPE - TABLESPACE_NAME - FILEPATH - OBJECT_NAME
		push(@{$tbs{$row->[2]}{$row->[1]}{$row->[3]}}, $row->[0]);
		$self->logit(".",1);
	}
	$sth->finish;
	$self->logit("\n", 1);

	return \%tbs;
}

=head2 _get_partitions

This function implements an Oracle-native partitions information.
Return two hash ref with partition details and partition default.
=cut

sub _get_partitions
{
	my($self) = @_;

	# Retrieve all partitions.
	my $str = qq{
SELECT
	a.table_name,
	a.partition_position,
	a.partition_name,
	a.high_value,
	a.tablespace_name,
	b.partitioning_type,
	c.name,
	c.column_name,
	c.column_position
FROM $self->{prefix}_tab_partitions a, $self->{prefix}_part_tables b, $self->{prefix}_part_key_columns c
WHERE
	a.table_name = b.table_name AND
	(b.partitioning_type = 'RANGE' OR b.partitioning_type = 'LIST')
	AND a.table_name = c.name
};

	if ($self->{prefix} ne 'USER') {
		if ($self->{schema}) {
			$str .= "\tAND a.table_owner ='$self->{schema}'\n";
		} else {
			$str .= "\tAND a.table_owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "')\n";
		}
	}
	$str .= "ORDER BY a.table_name,a.partition_position,c.column_position\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	my %default = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('PARTITION', $row->[0]));

		if ( ($row->[3] eq 'MAXVALUE') || ($row->[3] eq 'DEFAULT')) {
			$default{$row->[0]} = $row->[2];
			next;
		}
		push(@{$parts{$row->[0]}{$row->[1]}{$row->[2]}}, { 'type' => $row->[5], 'value' => $row->[3], 'column' => $row->[7], 'colpos' => $row->[8], 'tablespace' => $row->[4] });
		$self->logit(".",1);
	}
	$sth->finish;
	$self->logit("\n", 1);

	return \%parts, \%default;
}

=head2 _get_synonyms

This function implements an Oracle-native synonym information.

=cut

sub _get_synonyms
{
	my($self) = @_;

	# Retrieve all synonym
	my $str = "SELECT SYNONYM_NAME,TABLE_OWNER,TABLE_NAME,DB_LINK FROM $self->{prefix}_SYNONYMS";
	if ($self->{schema}) {
		$str .= "\tWHERE owner ='$self->{schema}' AND table_owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	} else {
		$str .= "\tWHERE owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "') AND table_owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ";
	}
	$str .= $self->limit_to_tables();
	$str .= " ORDER BY SYNONYM_NAME\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %synonyms = ();
	while (my $row = $sth->fetch) {
		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('SYNONYM', $row->[0]));
		$synonyms{$row->[0]}{owner} = $row->[1];
		$synonyms{$row->[0]}{table} = $row->[2];
		$synonyms{$row->[0]}{dblink} = $row->[3];
	}
	$sth->finish;

	return %synonyms;
}


=head2 _get_partitions_list

This function implements an Oracle-native partitions information.
Return a hash of the partition table_name => type
=cut

sub _get_partitions_list
{
	my($self) = @_;

	# Retrieve all partitions.
	my $str = qq{
SELECT
	a.table_name,
	a.partition_position,
	a.partition_name,
	a.high_value,
	a.tablespace_name,
	b.partitioning_type
FROM $self->{prefix}_tab_partitions a, $self->{prefix}_part_tables b
WHERE a.table_name = b.table_name
};

	if ($self->{prefix} ne 'USER') {
		if ($self->{schema}) {
			$str .= "\tAND a.table_owner ='$self->{schema}'\n";
		} else {
			$str .= "\tAND a.table_owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "')\n";
		}
	}
	$str .= "ORDER BY a.table_name\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	while (my $row = $sth->fetch) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('PARTITION', $row->[2]));
		$parts{$row->[5]}++;
	}
	$sth->finish;

	return %parts;
}

sub _get_custom_types
{
        my $str = uc(shift);

	my %all_types = %TYPE;
	$all_types{'DOUBLE'} = $all_types{'DOUBLE PRECISION'};
	delete $all_types{'DOUBLE PRECISION'};
	my @types_found = ();
	while ($str =~ s/(\w+)//s) {
		if (exists $all_types{$1}) {
			push(@types_found, $all_types{$1});
		}
	}
        return @types_found;
}

sub format_data_row
{
	my ($self, $row, $data_types, $action, $src_data_types, $custom_types, $table) = @_;

	for (my $idx = 0; $idx < scalar(@$data_types); $idx++) {
		my $data_type = $data_types->[$idx] || '';
		if ($row->[$idx] =~ /^ARRAY\(0x/) {
			my @type_col = ();
			for (my $i = 0;  $i <= $#{$row->[$idx]}; $i++) {
				push(@type_col, $self->format_data_type($row->[$idx][$i], $custom_types->{$data_type}[$i], $action, $table));
			}
			if ($action eq 'COPY') {
				$row->[$idx] =  "(" . join(',', @type_col) . ")";
			} else {
				$row->[$idx] =  "ROW(" . join(',', @type_col) . ")";
			}
		} else {
			$row->[$idx] = $self->format_data_type($row->[$idx], $data_type, $action, '', $src_data_types->[$idx]);
		}
	}
}

sub format_data_type
{
	my ($self, $col, $data_type, $action, $table, $src_type) = @_;

	# Preparing data for output
	if ($action ne 'COPY') {
		if (!defined $col) {
			$col = 'NULL';
		} elsif ($data_type eq 'bytea') {
			$col = escape_bytea($col);
			if (!$self->{standard_conforming_strings}) {
				$col = "'$col'";
			} else {
				$col = "E'$col'";
			}
			# RAW data type is returned in hex
			$col = "decode($col, 'hex')" if ($src_type eq 'RAW');
		} elsif ($data_type =~ /(char|text|xml)/) {
			if (!$self->{standard_conforming_strings}) {
				$col =~ s/'/''/gs; # double single quote
				$col =~ s/\\/\\\\/g;
				$col =~ s/\0//gs;
				$col = "'$col'";
			} else {
				$col =~ s/\0//gs;
				$col =~ s/\\/\\\\/g;
				$col =~ s/'/\\'/gs; # escape single quote
				$col = "E'$col'";
			}
		} elsif ($data_type =~ /(date|time)/) {
			if ($col =~ /^0000-00-00/) {
				$col = 'NULL';
			} elsif ($col =~ /^(\d+-\d+-\d+ \d+:\d+:\d+)\.$/) {
				$col = "'$1'";
			} else {
				$col = "'$col'";
			}
		} elsif ($data_type eq 'boolean') {
			if (exists $self->{ora_boolean_values}{lc($col)}) {
				$col = "'" . $self->{ora_boolean_values}{lc($col)} . "'";
			}
		} else {
			# covered now by the call to _numeric_format()
			# $col =~ s/,/\./;
			$col =~ s/\~/inf/;
			$col = 'NULL' if ($col eq '');
		}
	} else {
		if (!defined $col) {
			$col = '\N';
		} elsif ($data_type eq 'boolean') {
			if (exists $self->{ora_boolean_values}{lc($col)}) {
				$col = $self->{ora_boolean_values}{lc($col)};
			}
		} elsif ($data_type !~ /(char|date|time|text|bytea|xml)/) {
			# covered now by the call to _numeric_format()
			#$col =~ s/,/\./;
			$col =~ s/\~/inf/;
			$col = '\N' if ($col eq '');
		} elsif ($data_type eq 'bytea') {
			$col = escape_bytea($col);
			# RAW data type is returned in hex
			$col = '\\\\x' . $col if ($src_type eq 'RAW');
		} elsif ($data_type !~ /(date|time)/) {
			if ($self->{has_utf8_fct}) {
				utf8::encode($col) if (!utf8::valid($col));
			}
			$col =~ s/\0//gs;
			$col =~ s/\\/\\\\/g;
			$col =~ s/\r/\\r/g;
			$col =~ s/\n/\\n/g;
			$col =~ s/\t/\\t/g;
			if (!$self->{noescape}) {
				$col =~ s/\f/\\f/gs;
				$col =~ s/([\1-\10])/sprintf("\\%03o", ord($1))/egs;
				$col =~ s/([\13-\14])/sprintf("\\%03o", ord($1))/egs;
				$col =~ s/([\16-\37])/sprintf("\\%03o", ord($1))/egs;
			}
		} elsif ($data_type =~ /(date|time)/) {
			if ($col =~ /^0000-00-00/) {
				$col = '\N';
			} elsif ($col =~ /^(\d+-\d+-\d+ \d+:\d+:\d+)\.$/) {
				$col = $1;
			}
		}
	}
	return $col;
}


sub format_data
{
	my ($self, $rows, $data_types, $action, $src_data_types, $custom_types, $table) = @_;

	foreach my $row (@$rows) {
		$self->format_data_row($row,$data_types,$action, $src_data_types, $custom_types, $table);
	}
}

=head2 dump

This function dump data to the right output (gzip file, file or stdout).

=cut

sub dump
{
	my ($self, $data, $fh) = @_;

	if (!$self->{compress}) {
		if (defined $fh) {
			$fh->print($data);
		} elsif ($self->{fhout}) {
			$self->{fhout}->print($data);
		} else {
			print $data;
		}
	} elsif ($self->{compress} eq 'Zlib') {
		if (not defined $fh) {
			$self->{fhout}->gzwrite($data) or $self->logit("FATAL: error writing compressed data\n", 0, 1);
		} else {
			$fh->gzwrite($data) or $self->logit("FATAL: error writing compressed data\n", 0, 1);
		}
	} else {
		 $self->{fhout}->print($data);
	}

}


=head2 data_dump

This function dump data to the right output (gzip file, file or stdout) in multiprocess safety.
File is open and locked before writind data, it is closed at end.

=cut

sub data_dump
{
	my ($self, $data, $rname) = @_;

	my $dirprefix = '';
	$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});

	my $filename = $self->{output};
	if ($self->{file_per_table}) {
		$self->logit("Dumping data from $rname to file: $dirprefix${rname}_$self->{output}\n", 1);
		$filename = "${rname}_$self->{output}";
	}
	if ( ($self->{jobs} > 1) || ($self->{oracle_copies} > 1) ) {
		$self->{fhout}->close() if (defined $self->{fhout} && !$self->{file_per_table} && !$self->{pg_dsn});
		my $fh = $self->append_export_file($filename);
		flock($fh, 2) || die "FATAL: can't lock file $dirprefix$filename\n";
		$fh->print($data);
		$self->close_export_file($fh);
		$self->logit("Written " . length($data) . " bytes to $dirprefix$filename\n", 1);
		# Reopen default output file
		$self->create_export_file() if (defined $self->{fhout} && !$self->{file_per_table} && !$self->{pg_dsn});
	} elsif ($self->{file_per_table}) {
		$self->{cfhout} = $self->open_export_file($filename) if (!defined $self->{cfhout});
		if ($self->{compress} eq 'Zlib') {
			$self->{cfhout}->gzwrite($data) or $self->logit("FATAL: error writing compressed data\n", 0, 1);
		} else {
			$self->{cfhout}->print($data);
		}
	} else {
		$self->dump($data);
	}

}

=head2 read_config

This function read the specified configuration file.

=cut

sub read_config
{
	my ($self, $file) = @_;

	my $fh = new IO::File;
	$fh->open($file) or $self->logit("FATAL: can't read configuration file $file, $!\n", 0, 1);
	while (my $l = <$fh>) {
		chomp($l);
		$l =~ s/\r//gs;
		$l =~ s/^[\s\t]*\#.*$//g;
		next if (!$l || ($l =~ /^[\s\t]+$/));
		$l =~ s/^\s*//; $l =~ s/\s*$//;
		my ($var, $val) = split(/[\s\t]+/, $l, 2);
		$var = uc($var);
                if ($var eq 'IMPORT') {
			if ($val) {
				$self->logit("Importing $val...\n", 1);
				$self->read_config($val);
				$self->logit("Done importing $val.\n",1);
			}
		} elsif ($var =~ /^SKIP/) {
			if ($val) {
				$self->logit("No extraction of \L$val\E\n",1);
				my @skip = split(/[\s\t;,]+/, $val);
				foreach my $s (@skip) {
					$s = 'indexes' if ($s =~ /^indices$/i);
					$AConfig{"skip_\L$s\E"} = 1;
				}
			}
		} elsif (!grep(/^$var$/, 'TABLES', 'ALLOW', 'MODIFY_STRUCT', 'REPLACE_TABLES', 'REPLACE_COLS', 'WHERE', 'EXCLUDE','VIEW_AS_TABLE','ORA_RESERVED_WORDS','SYSUSERS','REPLACE_AS_BOOLEAN','BOOLEAN_VALUES','MODIFY_TYPE','DEFINED_PK', 'ALLOW_PARTITION')) {
			$AConfig{$var} = $val;
		} elsif ( ($var eq 'TABLES') || ($var eq 'ALLOW') || ($var eq 'EXCLUDE') || ($var eq 'VIEW_AS_TABLE') || ($var eq 'ALLOW_PARTITION') ) {
			$var = 'ALLOW' if ($var eq 'TABLES');
			push(@{$AConfig{$var}}, split(/[\s\t;,]+/, $val) );
		} elsif ( $var eq 'SYSUSERS' ) {
			push(@{$AConfig{$var}}, split(/[\s\t;,]+/, $val) );
		} elsif ( $var eq 'ORA_RESERVED_WORDS' ) {
			push(@{$AConfig{$var}}, split(/[\s\t;,]+/, $val) );
		} elsif ($var eq 'MODIFY_STRUCT') {
			while ($val =~ s/([^\(\s\t]+)[\t\s]*\(([^\)]+)\)[\t\s]*//) {
				my $table = $1;
				my $fields = $2;
				$fields =~ s/^\s+//;
				$fields =~ s/\s+$//;
				push(@{$AConfig{$var}{$table}}, split(/[\s,]+/, $fields) );
			}
		} elsif ($var eq 'MODIFY_TYPE') {
			$val =~ s/\\,/#NOSEP#/gs;
			$val =~ s/\\\s/#NOSPC#/gs;
			my @modif_type = split(/[\s,;\t]+/, $val);
			foreach my $r (@modif_type) { 
				$r =~ s/#NOSEP#/,/gs;
				$r =~ s/#NOSPC#/ /gs;
				my ($table, $col, $type) = split(/:/, lc($r));
				$AConfig{$var}{$table}{$col} = $type;
			}
		} elsif ($var eq 'REPLACE_COLS') {
			while ($val =~ s/([^\(\s\t]+)\s*\(([^\)]+)\)\s*//) {
				my $table = $1;
				my $fields = $2;
				$fields =~ s/^\s+//;
				$fields =~ s/\s+$//;
				my @rel = split(/[\t\s,]+/, $fields);
				foreach my $r (@rel) {
					my ($old, $new) = split(/:/, $r);
					$AConfig{$var}{$table}{$old} = $new;
				}
			}
		} elsif ($var eq 'REPLACE_TABLES') {
			my @replace_tables = split(/[\s,;\t]+/, $val);
			foreach my $r (@replace_tables) { 
				my ($old, $new) = split(/:/, $r);
				$AConfig{$var}{$old} = $new;
			}
		} elsif ($var eq 'REPLACE_AS_BOOLEAN') {
			my @replace_boolean = split(/[\s,;\t]+/, $val);
			foreach my $r (@replace_boolean) { 
				my ($table, $col) = split(/:/, $r);
				push(@{$AConfig{$var}{uc($table)}}, uc($col));
			}
		} elsif ($var eq 'BOOLEAN_VALUES') {
			my @replace_boolean = split(/[\s,;\t]+/, $val);
			foreach my $r (@replace_boolean) { 
				my ($yes, $no) = split(/:/, $r);
				$AConfig{$var}{lc($yes)} = 't';
				$AConfig{$var}{lc($no)} = 'f';
			}
		} elsif ($var eq 'DEFINED_PK') {
			my @defined_pk = split(/[\s,;\t]+/, $val);
			foreach my $r (@defined_pk) { 
				my ($table, $col) = split(/:/, lc($r));
				$AConfig{$var}{lc($table)} = $col;
			}
		} elsif ($var eq 'WHERE') {
			while ($val =~ s/([^\[\s\t]+)[\t\s]*\[([^\]]+)\][\s\t]*//) {
				my $table = $1;
				my $where = $2;
				$where =~ s/^\s+//;
				$where =~ s/\s+$//;
				$AConfig{$var}{$table} = $where;
			}
			if ($val) {
				$AConfig{"GLOBAL_WHERE"} = $val;
			}
		}
	}
	$fh->close();
}

sub _extract_functions
{
	my ($self, $content) = @_;

	my @lines = split(/\n/s, $content);
	my @functions = ('');
	my $before = '';
	my $fcname =  '';
	for (my $i = 0; $i <= $#lines; $i++) { 
		if ($lines[$i] =~ /^[\t\s]*(FUNCTION|PROCEDURE)[\t\s]+([a-z0-9_\-"]+)(.*)/i) {
			$fcname = $2;
			if ($before) {
				push(@functions, "$before\n");
				$functions[-1] .= "FUNCTION $2 $3\n";
			} else {
				push(@functions, "FUNCTION $fcname $3\n");
			}
			$before = '';
		} elsif ($fcname) {
			$functions[-1] .= "$lines[$i]\n";
		} else {
			$before .= "$lines[$i]\n";
		}
		$fcname = '' if ($lines[$i] =~ /^[\t\s]*END[\t\s]+$fcname\b/i);
	}
	#push(@functions, "$before\n") if ($before);

	map { s/END[\s\t]+(?!IF|LOOP|CASE|INTO|FROM|,)[a-z0-9_]+[\s\t]*;/END;/igs; } @functions;

	return @functions;
}

=head2 _convert_package

This function is used to rewrite Oracle PACKAGE code to
PostgreSQL SCHEMA. Called only if PLSQL_PGSQL configuration directive
is set to 1.

=cut

sub _convert_package
{
	my ($self, $plsql) = @_;

	my $dirprefix = '';
	$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
	my $content = '';
	if ($plsql =~ /PACKAGE[\s\t]+BODY[\s\t]*([^\s\t]+)[\s\t]*(AS|IS)[\s\t]*(.*)/is) {
		my $pname = $1;
		my $type = $2;
		$content = $3;
		$pname =~ s/"//g;
		$self->logit("Dumping package $pname...\n", 1);
		if ($self->{file_per_function} && !$self->{pg_dsn}) {
			my $dir = lc("$dirprefix$pname");
			if (!-d "$dir") {
				if (not mkdir($dir)) {
					$self->logit("Fail creating directory package : $dir - $!\n", 1);
					next;
				} else {
					$self->logit("Creating directory package: $dir\n", 1);
				}
			}
		}
		$self->{idxcomment} = 0;
		$content =~ s/END[^;]*;$//is;
		my %comments = $self->_remove_comments(\$content);
		my @functions = $self->_extract_functions($content);
		if (!$self->{preserve_case}) {
			$content = "-- PostgreSQL does not recognize PACKAGES, using SCHEMA instead.\n";
			$content .= "DROP SCHEMA IF EXISTS $pname CASCADE;\n";
			$content .= "CREATE SCHEMA $pname;\n";
		} else {
			$content = "-- PostgreSQL does not recognize PACKAGES, using SCHEMA instead.\n";
			$content .= "DROP SCHEMA IF EXISTS \"$pname\" CASCADE;\n";
			$content .= "CREATE SCHEMA \"$pname\";\n";
		}
		$self->{pkgcost} = 0;
		foreach my $f (@functions) {
			$content .= $self->_convert_function($f, $pname, \%comments);
		}
		$self->_restore_comments(\$content, \%comments);
		if ($self->{estimate_cost}) {
			$self->{total_pkgcost} += $self->{pkgcost} || 0;
		}
	}

	return $content;
}

=head2 _restore_comments

This function is used to restore comments into SQL code previously
remove for easy parsing

=cut

sub _restore_comments
{
	my ($self, $content, $comments) = @_;

	foreach my $k (keys %$comments) {
		$$content =~ s/$k[\n]?/$comments->{$k}\n/s;
	}

}

=head2 _remove_comments

This function is used to remove comments from SQL code
to allow easy parsing

=cut

sub _remove_comments
{
	my ($self, $content) = @_;

	my %comments = ();

	while ($$content =~ s/(\/\*(.*?)\*\/)/ORA2PG_COMMENT$self->{idxcomment}\%/s) {
		$comments{"ORA2PG_COMMENT$self->{idxcomment}%"} = $1;
		$self->{idxcomment}++;
	}
	while ($$content =~ s/(\'[^\'\n\r]+\b(PROCEDURE|FUNCTION)[\t\s]+[^\'\n\r]+\')/ORA2PG_COMMENT$self->{idxcomment}\%/is) {
		$comments{"ORA2PG_COMMENT$self->{idxcomment}%"} = $1;
		$self->{idxcomment}++;
	}
	my @lines = split(/\n/, $$content);
	for (my $j = 0; $j <= $#lines; $j++) {
		if ($lines[$j] =~ s/([\s\t]*\-\-.*)$/ORA2PG_COMMENT$self->{idxcomment}\%/) {
			$comments{"ORA2PG_COMMENT$self->{idxcomment}%"} = $1;
			chomp($comments{"ORA2PG_COMMENT$self->{idxcomment}%"});
			$self->{idxcomment}++;
		}
	}
	$$content = join("\n", @lines);

	return %comments;
}

=head2 _convert_function

This function is used to rewrite Oracle FUNCTION code to
PostgreSQL. Called only if PLSQL_PGSQL configuration directive               
is set to 1.

=cut

sub _convert_function
{
	my ($self, $plsql, $pname, $hrefcomments) = @_;

	my $func_before = '';
	my $func_name = '';
	my $func_args = '';
	my $func_ret_type = 'OPAQUE';
	my $hasreturn = 0;
	my $immutable = '';
	my $setof = '';

	my $dirprefix = '';
	$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});

	# Split data into declarative and code part
	my ($func_declare, $func_code) = split(/\bBEGIN\b/i,$plsql,2);
	if ( $func_declare =~ s/(.*?)\b(FUNCTION|PROCEDURE)[\s\t]+([^\s\t\(]+)[\s\t]*(\([^\)]*\)|[\s\t]*)//is ) {
		$func_before = $1;
		$func_name = $3;
		$func_args = $4;
		my $clause = '';
		my $code = '';
		$func_name =~ s/"//g;

		# forget or not this object if it is in the exclude or allow lists.
		return if (($self->{type} =~ /(FUNCTION|PROCEDURE)/) && $self->skip_this_object('FUNCTION', $func_name));

		$immutable = 1 if ($func_declare =~ s/\bDETERMINISTIC\b//is);
		$setof = 1 if ($func_declare =~ s/\bPIPELINED\b//is);
		if ($func_declare =~ s/(.*?)RETURN[\s\t]+self[\s\t]+AS RESULT IS//is) {
			$func_args .= $1;
			$hasreturn = 1;
			$func_ret_type = 'OPAQUE';
		} elsif ($func_declare =~ s/(.*?)RETURN[\s\t]+([^\s\t]+)//is) {
			$func_args .= $1;
			$hasreturn = 1;
			$func_ret_type = $self->_sql_type($2) || 'OPAQUE';
		}
		if ($func_declare =~ s/(.*?)(USING|AS|IS)//is) {
			$func_args .= $1 if (!$hasreturn);
			$clause = $2;
		}
		# rewrite argument syntax
		# Replace alternate syntax for default value
		$func_args =~ s/:=/DEFAULT/igs;
		# NOCOPY not supported
		$func_args =~ s/[\s\t]*NOCOPY//s;
		# IN OUT should be INOUT
		$func_args =~ s/IN[\s\t]+OUT/INOUT/s;

		# Now convert types
		$func_args = Ora2Pg::PLSQL::replace_sql_type($func_args, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});

		#$func_declare = $self->_convert_declare($func_declare);
		$func_declare = Ora2Pg::PLSQL::replace_sql_type($func_declare, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});
		# Replace PL/SQL code into PL/PGSQL similar code
		$func_declare = Ora2Pg::PLSQL::plsql_to_plpgsql($func_declare, $self->{allow_code_break},$self->{null_equal_empty});
		if ($func_code) {
			$func_code = Ora2Pg::PLSQL::plsql_to_plpgsql("BEGIN".$func_code, $self->{allow_code_break},$self->{null_equal_empty});
		}
	} else {
		return $plsql;
	}
	if ($func_code) {
		if ($self->{preserve_case}) {
			$func_name= "\"$func_name\"";
			$func_name = "\"$pname\"" . '.' . $func_name if ($pname);
		} else {
			$func_name= lc($func_name);
			$func_name = $pname . '.' . $func_name if ($pname);
		}
		$func_args = '()' if (!$func_args);
		my $function = "\nCREATE OR REPLACE FUNCTION $func_name $func_args";
		if (!$pname && $self->{export_schema} && $self->{schema}) {
			if (!$self->{preserve_case}) {
				$function = "\nCREATE OR REPLACE FUNCTION $self->{schema}\.$func_name $func_args";
				$self->logit("Parsing function $self->{schema}\.$func_name...\n", 1);
			} else {
				$function = "\nCREATE OR REPLACE FUNCTION \"$self->{schema}\"\.$func_name $func_args";
				$self->logit("Parsing function \"$self->{schema}\"\.$func_name...\n", 1);
			}
		} else {
			$self->logit("Parsing function $func_name...\n", 1);
		}
		$setof = ' SETOF' if ($setof);
		if ($hasreturn) {
			$function .= " RETURNS$setof $func_ret_type AS \$body\$\n";
		} else {
			my @nout = $func_args =~ /\bOUT /ig;
			my @ninout = $func_args =~ /\bINOUT /ig;
			if ($#nout > 0) {
				$function .= " RETURNS$setof RECORD AS \$body\$\n";
			} elsif ($#nout == 0) {
				$func_args =~ /[\s\t]*OUT[\s\t]+([A-Z0-9_\$\%\.]+)[\s\t\),]*/i;
				$function .= " RETURNS$setof $1 AS \$body\$\n";
			} elsif ($#ninout == 0) {
				$func_args =~ /[\s\t]*INOUT[\s\t]+([A-Z0-9_\$\%\.]+)[\s\t\),]*/i;
				$function .= " RETURNS$setof $1 AS \$body\$\n";
			} else {
				$function .= " RETURNS VOID AS \$body\$\n";
			}
		}
		$func_declare = '' if ($func_declare !~ /[a-z]/is);
		$function .= "DECLARE\n$func_declare\n" if ($func_declare);
		$function .= $func_code;
		$immutable = ' IMMUTABLE' if ($immutable);
		$function .= "\n\$body\$\nLANGUAGE PLPGSQL$immutable;\n";
#		my $fct_cost = '';
#		if ($self->{estimate_cost}) {
#			$func_name =~ s/"//g;
#			my ($cost, %cost_detail) = Ora2Pg::PLSQL::estimate_cost($function);
#			if ($cost && ($self->{type} ne 'PACKAGE')) {
#				$function .= "-- Porting cost of function \L$func_name\E: $cost\n";
#				foreach (sort { $cost_detail{$b} <=> $cost_detail{$a} } keys %cost_detail) {
#					next if (!$cost_detail{$_});
#					$function .= "\t-- $_ => $cost_detail{$_}";
#					$function .= " (cost: $Ora2Pg::PLSQL::UNCOVERED_SCORE{$_})" if ($Ora2Pg::PLSQL::UNCOVERED_SCORE{$_});
#					$function .= "\n";
#				}
#			}
#			$self->{pkgcost} += ($cost || 0);
#		}
		$function = "\n$func_before$function";

		if ($pname && $self->{file_per_function} && !$self->{pg_dsn}) {
			$func_name =~ s/^"*$pname"*\.//i;
			$func_name =~ s/"//g; # Remove case sensitivity quoting
			$self->logit("\tDumping to one file per function: $dirprefix\L$pname/$func_name\E_$self->{output}\n", 1);
			my $sql_header = "-- Generated by Ora2Pg, the Oracle database Schema converter, version $VERSION\n";
			$sql_header .= "-- Copyright 2000-2014 Gilles DAROLD. All rights reserved.\n";
			$sql_header .= "-- DATASOURCE: $self->{oracle_dsn}\n\n";
			if ($self->{client_encoding}) {
				$sql_header .= "SET client_encoding TO '\U$self->{client_encoding}\E';\n";
			}
			$sql_header .= $self->set_search_path();

			my $fhdl = $self->open_export_file("$dirprefix\L$pname/$func_name\E_$self->{output}", 1);
			$self->_restore_comments(\$function, $hrefcomments);
			$self->dump($sql_header . $function, $fhdl);
			$self->close_export_file($fhdl);
			$function = "\\i $dirprefix\L$pname/$func_name\E_$self->{output}\n";
		}
		return $function;
	}
	return $func_declare;
}

=head2 _convert_declare

This function is used to rewrite Oracle FUNCTION declaration code
to PostgreSQL. Called only if PLSQL_PGSQL configuration directive
is set to 1.

=cut

sub _convert_declare
{
	my ($self, $declare) = @_;

	$declare =~ s/[\s\t]+$//s;

	return if (!$declare);

	my @allwithcomments = split(/(ORA2PG_COMMENT\d+\%\n*)/s, $declare);
	for (my $i = 0; $i <= $#allwithcomments; $i++) {
		next if ($allwithcomments[$i] =~ /ORA2PG_COMMENT/);
		my @pg_declare = ();
		foreach my $tmp_var (split(/;/,$allwithcomments[$i])) {
			# Not cursor declaration
			if ($tmp_var !~ /\bcursor\b/is) {
				# Extract default assignment
				my $tmp_assign = '';
				if ($tmp_var =~ s/[\s\t]*(:=|DEFAULT)(.*)$//is) {
					$tmp_assign = " $1$2";
				}
				# Extract variable name and type
				my $tmp_pref = '';
				my $tmp_name = '';
				my $tmp_type = '';
				if ($tmp_var =~ /([\s\t]*)([^\s\t]+)[\s\t]+(.*?)$/s) {
					$tmp_pref = $1;
					$tmp_name = $2;
					$tmp_type = $3;
					$tmp_type =~ s/[\s\t]+//gs;
					if ($tmp_type =~ /([^\(]+)\(([^\)]+)\)/) {
						my $type_name = $1;
						my ($prec, $scale) = split(/,/, $2);
						$scale ||= 0;
						my $len = $prec;
						$prec = 0 if (!$scale);
						$len =~ s/\D//g;
						$tmp_type = $self->_sql_type($type_name,$len,$prec,$scale);
					} else {
						$tmp_type = $self->_sql_type($tmp_type);
					}
					push(@pg_declare, "$tmp_pref$tmp_name $tmp_type$tmp_assign;");
				}
			} else {
				push(@pg_declare, "$tmp_var;");
			}
		}
		$allwithcomments[$i] = join("", @pg_declare);
	}

	return join("", @allwithcomments);
}


=head2 _format_view

This function is used to rewrite Oracle VIEW declaration code
to PostgreSQL.

=cut

sub _format_view
{
	my ($self, $sqlstr) = @_;


	my @tbs = ();
	# Retrieve all tbs names used in view if possible
	if ($sqlstr =~ /\bFROM\b(.*)/is) {
		my $tmp = $1;
		$tmp =~ s/[\r\n\t]+/ /gs;
		$tmp =~ s/\bWHERE.*//is;
		# Remove all SQL reserved words of FROM STATEMENT
		$tmp =~ s/(LEFT|RIGHT|INNER|OUTER|NATURAL|CROSS|JOIN|\(|\))//igs;
		# Remove all ON join, if any
		$tmp =~ s/\bON\b[A-Z_\.\s\t]*=[A-Z_\.\s\t]*//igs;
		# Sub , with whitespace
		$tmp =~ s/,/ /g;
		if ($tmp =~ /[\(\)]/) {
			my @tmp_tbs = split(/\s+/, $tmp);
			foreach my $p (@tmp_tbs) {
				 push(@tbs, $p) if ($p =~ /^[A-Z_0-9\$]+$/i);
			}
		}
	}
	foreach my $tb (@tbs) {
		next if (!$tb);
		my $regextb = $tb;
		$regextb =~ s/\$/\\\$/g;
		if (!$self->{preserve_case}) {
			# Escape column name
			$sqlstr =~ s/["']*\b$regextb\b["']*\.["']*([A-Z_0-9\$]+)["']*(,?)/\L$tb\E.\L$1\E$2/igs;
			# Escape table name
			$sqlstr =~ s/(^=\s?)["']*\b$regextb\b["']*/\L$tb\E/igs;
			# Escape AS names
			#$sqlstr =~ s/(\bAS[\s\t]*)["']*([A-Z_0-9]+)["']*/$1\L$2\E/igs;
		} else {
			# Escape column name
			$sqlstr =~ s/["']*\b${regextb}["']*\.["']*([A-Z_0-9\$]+)["']*(,?)/"$tb"."$1"$2/igs;
			# Escape table name
			$sqlstr =~ s/(^=\s?)["']*\b$regextb\b["']*/"$tb"/igs;
			# Escape AS names
			#$sqlstr =~ s/(\bAS[\s\t]*)["']*([A-Z_0-9]+)["']*/$1"$2"/igs;
			if ($tb =~ /(.*)\.(.*)/) {
				my $prefx = $1;
				my $sufx = $2;
				$sqlstr =~ s/"$regextb"/"$prefx"\."$sufx/g;
			}
		}
	}
	if ($self->{plsql_pgsql}) {
		$sqlstr = Ora2Pg::PLSQL::plsql_to_plpgsql($sqlstr, $self->{allow_code_break},$self->{null_equal_empty}, $self->{type});
	}

	return $sqlstr;
}

=head2 randpattern

This function is used to replace the use of perl module String::Random
and is simply a cut & paste from this module.

=cut

sub randpattern
{
	my $patt = shift;

	my $string = '';

	my @upper=("A".."Z");
	my @lower=("a".."z");
	my @digit=("0".."9");
	my %patterns = (
	    'C' => [ @upper ],
	    'c' => [ @lower ],
	    'n' => [ @digit ],
	);
	for my $ch (split(//, $patt)) {
		if (exists $patterns{$ch}) {
			$string .= $patterns{$ch}->[int(rand(scalar(@{$patterns{$ch}})))];
		} else {
			$string .= $ch;
		}
	}

	return $string;
}

=head2 logit

This function log information to STDOUT or to a logfile
following a debug level. If critical is set, it dies after
writing to log.

=cut

sub logit
{
	my ($self, $message, $level, $critical) = @_;

	$level ||= 0;

	if ($self->{debug} >= $level) {
		if (defined $self->{fhlog}) {
			$self->{fhlog}->print($message);
		} else {
			print $message;
		}
	}
	if ($critical) {
		if ($self->{debug} < $level) {
			if (defined $self->{fhlog}) {
				$self->{fhlog}->print($message);
			} else {
				print "$message\n";
			}
		}
		$self->{fhlog}->close() if (defined $self->{fhlog});
		$self->{dbh}->disconnect() if ($self->{dbh});
		$self->{dbhdest}->disconnect() if ($self->{dbhdest});
		die "Aborting export...\n";
	}
}

=head2 _convert_type

This function is used to rewrite Oracle PACKAGE code to
PostgreSQL SCHEMA. Called only if PLSQL_PGSQL configuration directive
is set to 1.

=cut

sub _convert_type
{
	my ($self, $plsql) = @_;

	my $unsupported = "-- Unsupported, please edit to match PostgreSQL syntax\n";
	my $content = '';
	if ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+(IS|AS)[\t\s]*TABLE[\t\s]*OF[\t\s]+(.*)/is) {
		my $type_name = $1;
		my $type_of = $3;
		$type_of =~ s/[\t\s\r\n]*NOT[\t\s]+NULL//s;
		$type_of =~ s/[\t\s\r\n]*;$//s;
		$type_of =~ s/^[\t\s\r\n]+//s;
		if ($type_of !~ /[\t\s\r\n]/s) { 
			$self->{type_of_type}{'Nested Tables'}++;
			$content = "CREATE TYPE \L$type_name\E AS (\L$type_name\E $type_of\[\]);\n";
		} else {
			$self->{type_of_type}{'Associative Arrays'}++;
			$self->logit("WARNING: this kind of Nested Tables are not supported, skipping type $1\n", 1);
			return "${unsupported}CREATE OR REPLACE $plsql";
		}
	} elsif ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+(AS|IS)[\t\s]*OBJECT[\t\s]*\((.*?)(TYPE BODY.*)/is) {
		$self->{type_of_type}{'Type Boby'}++;
		$self->logit("WARNING: TYPE BODY are not supported, skipping type $1\n", 1);
		return "${unsupported}CREATE OR REPLACE $plsql";
	} elsif ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+(AS|IS)[\t\s]*OBJECT[\t\s]*\((.*)\)([^\)]*)/is) {
		my $type_name = $1;
		my $description = $3;
		my $notfinal = $4;
		$notfinal =~ s/[\s\t\r\n]+/ /gs;
		if ($description =~ /[\s\t]*(MAP MEMBER|MEMBER|CONSTRUCTOR)[\t\s]+(FUNCTION|PROCEDURE).*/is) {
			$self->{type_of_type}{'Type with member method'}++;
			$self->logit("WARNING: TYPE with CONSTRUCTOR and MEMBER FUNCTION are not supported, skipping type $type_name\n", 1);
			return "${unsupported}CREATE OR REPLACE $plsql";
		}
		$description =~ s/^[\s\t\r\n]+//s;
		my $declar = Ora2Pg::PLSQL::replace_sql_type($description, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});
		$type_name =~ s/"//g;
		$type_name = $self->get_replaced_tbname($type_name);
		if ($notfinal =~ /FINAL/is) {
			$content = "-- Inherited types are not supported in PostgreSQL, replacing with inherited table\n";
			$content .= qq{CREATE TABLE $type_name (
$declar
);
};
			$self->{type_of_type}{'Type inherited'}++;
		} else {
			$content = qq{
CREATE TYPE $type_name AS (
$declar
);
};
			$self->{type_of_type}{'Object type'}++;
		}
	} elsif ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+UNDER[\t\s]*([^\t\s]+)[\t\s]+\((.*)\)([^\)]*)/is) {
		my $type_name = $1;
		my $type_inherit = $2;
		my $description = $3;
		if ($description =~ /[\s\t]*(MAP MEMBER|MEMBER|CONSTRUCTOR)[\t\s]+(FUNCTION|PROCEDURE).*/is) {
			$self->logit("WARNING: TYPE with CONSTRUCTOR and MEMBER FUNCTION are not supported, skipping type $type_name\n", 1);
			$self->{type_of_type}{'Type with member method'}++;
			return "${unsupported}CREATE OR REPLACE $plsql";
		}
		$description =~ s/^[\s\t\r\n]+//s;
		my $declar = Ora2Pg::PLSQL::replace_sql_type($description, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});
		$type_name =~ s/"//g;
		$type_name = $self->get_replaced_tbname($type_name);
		$content = qq{
CREATE TABLE $type_name (
$declar
) INHERITS (\L$type_inherit\E);
};
		$self->{type_of_type}{'Subtype'}++;
	} elsif ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+(AS|IS)[\t\s]*(VARRAY|VARYING ARRAY)[\t\s]*\((\d+)\)[\t\s]*OF[\t\s]*(.*)/is) {
		my $type_name = $1;
		my $size = $4;
		my $tbname = $5;
		$type_name =~ s/"//g;
		$tbname =~ s/;//g;
		chomp($tbname);
		my $declar = Ora2Pg::PLSQL::replace_sql_type($tbname, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});
		$content = qq{
CREATE TYPE \L$type_name\E AS ($type_name $declar\[$size\]);
};
		$self->{type_of_type}{Varrays}++;
	} else {
		$self->{type_of_type}{Unknown}++;
		$plsql =~ s/;$//s;
		$content = "${unsupported}CREATE OR REPLACE $plsql;"
	}
	return $content;
}

sub ask_for_data
{
	my ($self, $table, $cmd_head, $cmd_foot, $s_out, $nn, $tt, $sprep, $stt, $part_name) = @_;

	# Build SQL query to retrieve data from this table
	if (!$part_name) {
		$self->logit("Looking how to retrieve data from $table...\n", 1);
	} else {
		$self->logit("Looking how to retrieve data from $table partition $part_name...\n", 1);
	}
	my $query = $self->_howto_get_data($table, $nn, $tt, $stt, $part_name);

	# Check for boolean rewritting
	for (my $i = 0; $i <= $#{$nn}; $i++) {
		my $colname = $nn->[$i]->[0];
		$colname =~ s/"//g;
		# Check if this column should be replaced by a boolean following table/column name
		if (grep(/^$colname$/i, @{$self->{'replace_as_boolean'}{uc($table)}})) {
			$tt->[$i] = 'boolean';
		# Check if this column should be replaced by a boolean following type/precision
		} elsif (exists $self->{'replace_as_boolean'}{uc($nn->[$i]->[1])} && ($self->{'replace_as_boolean'}{uc($nn->[$i]->[1])}[0] == $nn->[$i]->[5])) {
			$tt->[$i] = 'boolean';
		}
	}

	# check if destination column type must be changed
	for (my $i = 0; $i <= $#{$nn}; $i++) {
		my $colname = $nn->[$i]->[0];
		$colname =~ s/"//g;
		$tt->[$i] = $self->{'modify_type'}{"\L$table\E"}{"\L$colname\E"} if (exists $self->{'modify_type'}{"\L$table\E"}{"\L$colname\E"});
	}

	if ( ($self->{oracle_copies} > 1) && $self->{defined_pk}{"\L$table\E"} ) {
		$self->{ora_conn_count} = 0;
		while ($self->{ora_conn_count} < $self->{oracle_copies}) {
			spawn sub {
				$self->logit("Creating new connection to Oracle database...\n", 1);
				$self->_extract_data($query, $table, $cmd_head, $cmd_foot, $s_out, $nn, $tt, $sprep, $stt, $part_name, $self->{ora_conn_count});
			};
			$self->{ora_conn_count}++;
		}
		# Wait for oracle connection terminaison
		while ($self->{ora_conn_count} > 0) {
			my $kid = waitpid(-1, WNOHANG);
			if ($kid > 0) {
				$self->{ora_conn_count}--;
				delete $RUNNING_PIDS{$kid};
			}
			usleep(500000);
		}

	} else {
		$self->_extract_data($query, $table, $cmd_head, $cmd_foot, $s_out, $nn, $tt, $sprep, $stt, $part_name);
	}
}

sub _extract_data
{
	my ($self, $query, $table, $cmd_head, $cmd_foot, $s_out, $nn, $tt, $sprep, $stt, $part_name, $proc) = @_;

	$0 = 'ora2pg - querying Oracle';

	my %user_type = ();
	for (my $idx = 0; $idx < scalar(@$tt); $idx++) {
		my $data_type = $tt->[$idx] || '';
		my $custom_type = '';
		if (!exists $TYPE{$stt->[$idx]}) {
			$custom_type = $self->_get_types($stt->[$idx]);
			foreach my $tpe (sort {length($a->{name}) <=> length($b->{name}) } @{$custom_type}) {
				$self->logit("Looking inside custom type $tpe->{name} to extract values...\n", 1);
				push(@{$user_type{$data_type}}, &_get_custom_types($tpe->{code}));
			}
		}
	}

	my $rname = $part_name || $table;
	my $dbh;
	my $sth;
	if ( ($self->{oracle_copies} > 1) && $self->{defined_pk}{"\L$table\E"} ) {

		$dbh = $self->{dbh}->clone();

		$self->{dbh}->{InactiveDestroy} = 1;
		$self->{dbh} = undef;
		if (defined $self->{dbhdest}) {
			$self->{dbhdest}->{InactiveDestroy} = 1;
			$self->{dbhdest} = undef;
		}

		# Set row cache size
		$dbh->{RowCacheSize} = int($self->{data_limit}/10);

		# prepare the query before execution
		$sth = $dbh->prepare($query,{ora_auto_lob => 1,ora_exe_mode=>OCI_STMT_SCROLLABLE_READONLY, ora_check_sql => 1}) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
		my $r = $sth->{NAME};

		# Extract data now by chunk of DATA_LIMIT and send them to a dedicated job
		$self->logit("Fetching all data from $rname tuples...\n", 1);
		if (defined $proc) {
			$sth->execute($proc) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
		} else {
			$sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
		}

	} else {

		# Set row cache size
		$self->{dbh}->{RowCacheSize} = int($self->{data_limit}/10);

		# prepare the query before execution
		$sth = $self->{dbh}->prepare($query,{ora_auto_lob => 1,ora_exe_mode=>OCI_STMT_SCROLLABLE_READONLY, ora_check_sql => 1}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		my $r = $sth->{NAME};

		# Extract data now by chunk of DATA_LIMIT and send them to a dedicated job
		$self->logit("Fetching all data from $rname tuples...\n", 1);
		if (defined $proc) {
			$sth->execute($proc) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		} else {
			$sth->execute() or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		}
	}
	my $start_time   = time();
	my $total_record = 0;
	my $total_row = $self->{tables}{$table}{table_info}{num_rows};
	my $nrows = 0;
	while ( my $rows = $sth->fetchall_arrayref(undef,$self->{data_limit})) {

		$nrows =  @$rows;
		$total_record += $nrows;
		if ( ($self->{jobs} > 1) || ($self->{oracle_copies} > 1) ) {
			while ($self->{child_count} >= $self->{jobs}) {
				my $kid = waitpid(-1, WNOHANG);
				if ($kid > 0) {
					$self->{child_count}--;
					delete $RUNNING_PIDS{$kid};
				}
				usleep(50000);
			}
			# The parent's connection should not be closed when $dbh is destroyed
			$self->{dbh}->{InactiveDestroy} = 1;
			$self->{dbhdest}->{InactiveDestroy} = 1 if (defined $self->{dbhdest});
			$dbh->{InactiveDestroy} = 1 if (defined $dbh);

			spawn sub {
				$self->_dump_to_pg($dbh, $rows, $table, $cmd_head, $cmd_foot, $s_out, $tt, $sprep, $stt, $start_time, $part_name, $total_record, %user_type);
			};
			$self->{child_count}++;
			$self->{dbh}->{InactiveDestroy} = 0;
			$self->{dbhdest}->{InactiveDestroy} = 0 if (defined $self->{dbhdest});
			$dbh->{InactiveDestroy} = 0 if (defined $dbh);
		} else {
			$self->_dump_to_pg($dbh, $rows, $table, $cmd_head, $cmd_foot, $s_out, $tt, $sprep, $stt, $start_time, $part_name, $total_record, %user_type);
		}
	}
	$sth->finish();

	if ( ($self->{jobs} <= 1) && ($self->{oracle_copies} <= 1) ) {
		print STDERR "\n";
	}

	# Wait for all child end
	while ($self->{child_count} > 0) {
		my $kid = waitpid(-1, WNOHANG);
		if ($kid > 0) {
			$self->{child_count}--;
			delete $RUNNING_PIDS{$kid};
		}
		usleep(500000);
	}

	$dbh->disconnect() if (defined $dbh);

	return;
}

sub log_error_copy
{
	my ($self, $table, $s_out, $rows) = @_;

	my $outfile = '';
	if ($self->{output_dir} && !$noprefix) {
		$outfile = $self->{output_dir} . '/';
	}
	$outfile .= $table . '_error.log';

	open(OUTERROR, ">>$outfile") or $self->logit("FATAL: can not write to $outfile, $!\n", 0, 1);
	binmode(OUTERROR, $self->{'binmode'});
	print OUTERROR "$s_out";
	foreach my $row (@$rows) {
		print OUTERROR join("\t", @$row), "\n";
	}
	print OUTERROR "\\.\n";
	close(OUTERROR);

}

sub log_error_insert
{
	my ($self, $table, $sql_out) = @_;

	my $outfile = '';
	if ($self->{output_dir} && !$noprefix) {
		$outfile = $self->{output_dir} . '/';
	}
	$outfile .= $table . '_error.log';

	open(OUTERROR, ">>$outfile") or $self->logit("FATAL: can not write to $outfile, $!\n", 0, 1);
	binmode(OUTERROR, $self->{'binmode'});
	print OUTERROR "$sql_out\n";
	close(OUTERROR);

}


sub _dump_to_pg
{
	my ($self, $dbh, $rows, $table, $cmd_head, $cmd_foot, $s_out, $tt, $sprep, $stt, $start_time, $part_name, $glob_total_record, %user_type) = @_;

	$0 = 'ora2pg - sending to PostgreSQL';

	if ( ($self->{jobs} > 1) || ($self->{oracle_copies} > 1) ) {
		$pipe->writer();
	}
	# Open a connection to the postgreSQL database if required
	my $rname = $part_name || $table;

	# Connect to PostgreSQL if direct import is enabled
	my $dbhdest = undef;
	if ($self->{pg_dsn}) {
		$dbhdest = $self->_send_to_pgdb();
		$self->logit("Dumping data from table $rname into PostgreSQL...\n", 1);
		$self->logit("Disabling synchronous commit when writing to PostgreSQL...\n", 1);
		if (!$self->{synchronous_commit}) {
			my $s = $dbhdest->do("SET synchronous_commit TO off") or $self->logit("FATAL: " . $dbhdest->errstr . "\n", 0, 1);
		}
	}

	# Build header of the file
	my $h_towrite = '';
	foreach my $cmd (@$cmd_head) {
		if ($self->{pg_dsn}) {
			my $s = $dbhdest->do("$cmd") or $self->logit("FATAL: " . $dbhdest->errstr . "\n", 0, 1);
		} else {
			$h_towrite .= "$cmd\n";
		}
	}

	# Build footer of the file
	my $e_towrite = '';
	foreach my $cmd (@$cmd_foot) {
		if ($self->{pg_dsn}) {
			my $s = $dbhdest->do("$cmd") or $self->logit("FATAL: " . $dbhdest->errstr . "\n", 0, 1);
		} else {
			$e_towrite .= "$cmd\n";
		}
	}

	# Preparing data for output
	if (!$sprep) {
		$self->logit("DEBUG: Formatting bulk of $self->{data_limit} data for PostgreSQL.\n", 1);
		$self->format_data($rows, $tt, $self->{type}, $stt, \%user_type, $table);
	}

	# Add COPY header to the output
	my $sql_out = $s_out;

	# Creating output
	$self->logit("DEBUG: Creating output for $self->{data_limit} tuples\n", 1);
	if ($self->{type} eq 'COPY') {
		if ($self->{pg_dsn}) {
			$sql_out =~ s/;$//;
			$self->logit("DEBUG: Sending COPY bulk output directly to PostgreSQL backend\n", 1);
			my $s = $dbhdest->do($sql_out) or $self->logit("FATAL: " . $dbhdest->errstr . "\n", 0, 1);
			$sql_out = '';
			my $skip_end = 0;
			foreach my $row (@$rows) {
				unless($dbhdest->pg_putcopydata(join("\t", @$row) . "\n")) {
					if ($self->{log_on_error}) {
						$self->logit("ERROR (log error enabled): " . $dbhdest->errstr . "\n", 0, 0);
						$self->log_error_copy($table, $s_out, $rows);
						$skip_end = 1;
						last;
					} else {
						$self->logit("FATAL: " . $dbhdest->errstr . "\n", 0, 1);
					}
				}
			}
			unless ($dbhdest->pg_putcopyend()) {
				if ($self->{log_on_error}) {
					$self->logit("ERROR (log error enabled): " . $dbhdest->errstr . "\n", 0, 0);
					$self->log_error_copy($table, $s_out, $rows) if (!$skip_end);
				} else {
					$self->logit("FATAL: " . $dbhdest->errstr . "\n", 0, 1);
				}
			}
		} else {
			# then add data to the output
			map { $sql_out .= join("\t", @$_) . "\n"; } @$rows;
			$sql_out .= "\\.\n";
		}
	} elsif (!$sprep) {
		$sql_out = '';
		foreach my $row (@$rows) {
			$sql_out .= $s_out;
			$sql_out .= join(',', @$row) . ");\n";
		}
	}

	# Insert data if we are in online processing mode
	if ($self->{pg_dsn}) {
		if ($self->{type} ne 'COPY') {
			if (!$sprep) {
				$self->logit("DEBUG: Sending INSERT output directly to PostgreSQL backend\n", 1);
				unless($dbhdest->do($sql_out)) {
					if ($self->{log_on_error}) {
						$self->logit("WARNING (log error enabled): " . $dbhdest->errstr . "\n", 0, 0);
						$self->log_error_insert($table, $sql_out);
					} else {
						$self->logit("FATAL: " . $dbhdest->errstr . "\n", 0, 1);
					}
				}
			} else {
				my $ps = $dbhdest->prepare($sprep) or $self->logit("FATAL: " . $dbhdest->errstr . "\n", 0, 1);
				for (my $i = 0; $i <= $#{$tt}; $i++) {
					if ($tt->[$i] eq 'bytea') {
						$ps->bind_param($i+1, undef, { pg_type => DBD::Pg::PG_BYTEA });
					}
				}
				$self->logit("DEBUG: Sending INSERT bulk output directly to PostgreSQL backend\n", 1);
				foreach my $row (@$rows) {
					unless ($ps->execute(@$row) ) {
						if ($self->{log_on_error}) {
							$self->logit("ERROR (log error enabled): " . $ps->errstr . "\n", 0, 0);
							$s_out =~ s/\([,\?]+\)/\(/;
							$self->format_data_row($row,$tt,'INSERT', $stt, \%user_type, $table);
							$self->log_error_insert($table, $s_out . join(',', @$row) . ");\n");
						} else {
							$self->logit("FATAL: " . $ps->errstr . "\n", 0, 1);
						}
					}
				}
				$ps->finish();
			}
		}
	} else {
		if ($part_name && $self->{prefix_partition})  {
			$part_name = $table . '_' . $part_name;
		}
		$self->data_dump($h_towrite . $sql_out . $e_towrite, $part_name || $table);
	}

	my $total_row = $self->{tables}{$table}{table_info}{num_rows};
	my $tt_record = @$rows;
	$dbhdest->disconnect() if ($dbhdest);

	my $end_time = time();
	my $dt = $end_time - $start_time;
	$dt ||= 1;
	my $rps = sprintf("%2.1f", $tt_record / ($dt+.0001));
	if (!$self->{quiet} && !$self->{debug}) {
		if ( ($self->{jobs} > 1) || ($self->{oracle_copies} > 1) ) {
			$pipe->print("$tt_record $table $total_row $start_time\n");
		} else {
			$rps = sprintf("%2.1f", $glob_total_record / ($dt+.0001));
			print STDERR $self->progress_bar($glob_total_record, $total_row, 25, '=', 'rows', "Table $table ($rps recs/sec)");
		}
	} elsif ($self->{debug}) {
		$self->logit("Extracted records from table $table: $tt_record ($rps recs/sec)\n", 1);
	}

}

# Global array, to store the converted values
my @bytea_array;
sub build_escape_bytea
{
	foreach my $tmp (0..255)
	{
		my $out;
		if ($tmp >= 32 and $tmp <= 126) {
			if ($tmp == 92) {
				$out = '\\\\134';
			} elsif ($tmp == 39) {
				$out = '\\\\047';
			} else {
				$out = chr($tmp);
			}
		} else { 
			$out = sprintf('\\\\%03o',$tmp);
		}
		$bytea_array[$tmp] = $out;
	}
}

=head2 escape_bytea

This function return an escaped bytea entry for Pg.

=cut


sub escape_bytea
{
	my $data = shift;

	# In this function, we use the array built by build_escape_bytea
	my @array= unpack("C*", $data);
	foreach my $elt (@array) {
		$elt = $bytea_array[$elt];
	}
	return join('', @array);
}

=head2 _show_infos

This function display a list of schema, table or column only to stdout.

=cut

sub _show_infos
{
	my ($self, $type) = @_;

	if ($type eq 'SHOW_ENCODING') {
		$self->logit("Showing Oracle encoding...\n", 1);
		my $encoding = $self->_get_encoding($self->{dbh});
		$self->logit("NLS_LANG $encoding\n", 0);
		$self->logit("CLIENT_ENCODING $self->{client_encoding}\n", 0);
	} elsif ($type eq 'SHOW_VERSION') {
		$self->logit("Showing Oracle Version...\n", 1);
		$self->logit("$self->{db_version}\n", 0);
	} elsif ($type eq 'SHOW_REPORT') {
		$self->logit("Reporting Oracle Content...\n", 1);
		# Get Oracle database version and size
		my $ver = $self->_get_version();
		my $size = $self->_get_database_size();
		# Get the list of all database objects
		my %objects = $self->_get_objects();
		# Determining how many non automatiques indexes will be exported
		my %all_indexes = ();
		$self->{skip_fkeys} = $self->{skip_indices} = $self->{skip_indexes} = $self->{skip_checks} = 0;
		$self->{view_as_table} = ();
		# Extract all tables informations
		$self->_tables();
		my $total_index = 0;
		my $total_table_objects = 0;
		my $total_index_objects = 0;
		foreach my $table (sort keys %{$self->{tables}}) {
			# forget or not this object if it is in the exclude or allow lists.
			next if ($self->skip_this_object('TABLE', $table));
			$total_table_objects++;
			push(@exported_indexes, $self->_exportable_indexes($table, %{$self->{tables}{$table}{indexes}}));
			$total_index_objects += scalar keys %{$self->{tables}{$table}{indexes}};
			foreach my $idx (sort keys %{$self->{tables}{$table}{idx_type}}) {
				next if (!grep(/^$idx$/i, @exported_indexes));
				my $typ = $self->{tables}{$table}{idx_type}{$idx}{type};
				push(@{$all_indexes{$typ}}, $idx);
				$total_index++;
			}
		}
		# Convert Oracle user defined type to PostgreSQL
		$self->_types();
		foreach my $tpe (sort {length($a->{name}) <=> length($b->{name}) } @{$self->{types}}) {
			$self->_convert_type($tpe->{code});
		}
		# Get definition of Oracle Database Link
		my %dblink = $self->_get_dblink();
		$objects{'DATABASE LINK'} = scalar keys %dblink;	
		# Get definition of Oracle Jobs
		my %jobs = $self->_get_job();
		$objects{'JOB'} = scalar keys %jobs;	

		# Look at all database objects to compute report
		my %report_info = ();
		$report_info{'Version'} = $ver || 'Unknown';
		$report_info{'Schema'} = $self->{schema} || '';
		$report_info{'Size'} = $size || 'Unknown';
		my $i = 1;
		my $num_total_obj = scalar keys %objects;
		foreach my $typ (sort keys %objects) {
			$i++, next if ($typ eq 'PACKAGE'); # Package are scanned with PACKAGE BODY not PACKAGE objects
			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($i, $num_total_obj, 25, '=', 'objects types', "inspecting object $typ" );
			}
			$report_info{'Objects'}{$typ}{'number'} = 0;
			$report_info{'Objects'}{$typ}{'invalid'} = 0;
			if (!grep(/^$typ$/, 'DATABASE LINK', 'JOB', 'TABLE', 'INDEX')) {
				for (my $i = 0; $i <= $#{$objects{$typ}}; $i++) {
					$report_info{'Objects'}{$typ}{'number'}++;
					$report_info{'Objects'}{$typ}{'invalid'}++ if ($objects{$typ}[$i]->{invalid});
				}
			} elsif ($typ eq 'TABLE') {
				$report_info{'Objects'}{$typ}{'number'} = $total_table_objects;
			} elsif ($typ eq 'INDEX') {
				$report_info{'Objects'}{$typ}{'number'} = $total_index_objects;
			} else {
				$report_info{'Objects'}{$typ}{'number'} = $objects{$typ};
			}
			$report_info{'total_object_invalid'} += $report_info{'Objects'}{$typ}{'invalid'};
			$report_info{'total_object_number'} += $report_info{'Objects'}{$typ}{'number'};
			if ($report_info{'Objects'}{$typ}{'number'} > 0) {
				$report_info{'Objects'}{$typ}{'real_number'} = ($report_info{'Objects'}{$typ}{'number'} - $report_info{'Objects'}{$typ}{'invalid'});
				$report_info{'Objects'}{$typ}{'real_number'} = $report_info{'Objects'}{$typ}{'number'} if ($self->{export_invalid});
			}
			if ($self->{estimate_cost}) {
				$report_info{'Objects'}{$typ}{'cost_value'} = ($report_info{'Objects'}{$typ}{'real_number'}*$Ora2Pg::PLSQL::OBJECT_SCORE{$typ});
				$report_info{'Objects'}{$typ}{'cost_value'} = 288 if (($typ eq 'TABLE') && ($report_info{'Objects'}{$typ}{'cost_value'} > 288));
				$report_info{'Objects'}{$typ}{'cost_value'} = 288 if (($typ eq 'INDEX') && ($report_info{'Objects'}{$typ}{'cost_value'} > 288));
				$report_info{'Objects'}{$typ}{'cost_value'} = 96 if (($typ eq 'TABLE PARTITION') && ($report_info{'Objects'}{$typ}{'cost_value'} > 96));
			}
			if ($typ eq 'INDEX') {
				my $bitmap = 0;
				foreach my $t (sort keys %INDEX_TYPE) {
					my $len = ($#{$all_indexes{$t}}+1);
					$report_info{'Objects'}{$typ}{'detail'} .= "\L$len $INDEX_TYPE{$t} index(es)\E\n" if ($len);
					if ($self->{estimate_cost} && $len && ( ($t =~ /FUNCTION.*NORMAL/) || ($t eq 'FUNCTION-BASED BITMAP') ) ) {
						$report_info{'Objects'}{$typ}{'cost_value'} += ($len * $Ora2Pg::PLSQL::OBJECT_SCORE{'FUNCTION-BASED-INDEX'});
					}
					if ($self->{estimate_cost} && $len && ($t =~ /REV/)) {
						$report_info{'Objects'}{$typ}{'cost_value'} += ($len * $Ora2Pg::PLSQL::OBJECT_SCORE{'REV-INDEX'});
					}
				}
				$report_info{'Objects'}{$typ}{'cost_value'} += ($Ora2Pg::PLSQL::OBJECT_SCORE{$typ}*$total_index) if ($self->{estimate_cost});
				$report_info{'Objects'}{$typ}{'comment'} = "$total_index index(es) are concerned by the export, others are automatically generated and will do so on PostgreSQL. Bitmap index(es) will be exported as b-tree index(es) if any. Cluster, domain, bitmap join and IOT indexes will not be exported at all. Reverse indexes are not exported too, you may use a trigram-based index (see pg_trgm) or a reverse() function based index and search. Use 'varchar_pattern_ops', 'text_pattern_ops' or 'bpchar_pattern_ops' operators in your indexes to improve search with the LIKE operator respectively into varchar, text or char columns.";
			} elsif ($typ eq 'MATERIALIZED VIEW') {
				$report_info{'Objects'}{$typ}{'comment'}= "All materialized view will be exported as snapshot materialized views, they are only updated when fully refreshed.";
			} elsif ($typ eq 'TABLE') {
				my $exttb = scalar keys %{$self->{external_table}};
				if ($exttb) {
					if (!$self->{external_to_fdw}) {
						$report_info{'Objects'}{$typ}{'comment'} = "$exttb external table(s) will be exported as standard table. See EXTERNAL_TO_FDW configuration directive to export as file_fdw foreign tables or use COPY in your code if you just want to load data from external files.";
					} else {
						$report_info{'Objects'}{$typ}{'comment'} = "$exttb external table(s) will be exported as file_fdw foreign table. See EXTERNAL_TO_FDW configuration directive to export as standard table or use COPY in your code if you just want to load data from external files.";
					}
				}

				my %table_detail = ();
				my $virt_column = 0;
				my @done = ();
				my $id = 0;
				my $total_check = 0;
				my $total_row_num = 0;
				# Set the table information for each class found
				foreach my $t (sort keys %{$self->{tables}}) {

					# forget or not this object if it is in the exclude or allow lists.
					next if ($self->skip_this_object('TABLE', $t));

					# Set the total number of rows
					$total_row_num += $self->{tables}{$t}{table_info}{num_rows};

					# Look at reserved words if tablename is found
					if (&is_reserved_words($t)) {
						$table_detail{'reserved words in table name'}++;
					}
					# Get fields informations
					foreach my $k (sort {$self->{tables}{$t}{column_info}{$a}[10] <=> $self->{tables}{$t}{column_info}{$a}[10]} keys %{$self->{tables}{$t}{column_info}}) {
						if (&is_reserved_words($self->{tables}{$t}{column_info}{$k}[0])) {
							$table_detail{'reserved words in column name'}++;
						}
						$self->{tables}{$t}{column_info}{$k}[1] =~ s/TIMESTAMP\(\d+\)/TIMESTAMP/i;
						if (!exists $TYPE{uc($self->{tables}{$t}{column_info}{$k}[1])}) {
							$table_detail{'unknow types'}++;
						}
						if ( (uc($self->{tables}{$t}{column_info}{$k}[1]) eq 'NUMBER') && ($self->{tables}{$t}{column_info}{$k}[2] eq '') ) {
							$table_detail{'numbers with no precision'}++;
						}
						if ( $TYPE{uc($self->{tables}{$t}{column_info}{$k}[1])} eq 'bytea' ) {
							$table_detail{'binary columns'}++;
						}
					}
					# Get check constraints information related to this table
					my @constraints = $self->_lookup_check_constraint($t, $self->{tables}{$t}{check_constraint},$self->{tables}{$t}{field_name}, 1);
					$total_check += ($#constraints + 1);
					if ($self->{estimate_cost} && ($#constraints >= 0)) {
						$report_info{'Objects'}{$typ}{'cost_value'} += (($#constraints + 1) * $Ora2Pg::PLSQL::OBJECT_SCORE{'CHECK'});
					}
				}
				$report_info{'Objects'}{$typ}{'comment'} .= " $total_check check constraint(s)." if ($total_check);
				foreach my $d (sort keys %table_detail) {
					$report_info{'Objects'}{$typ}{'detail'} .= "\L$table_detail{$d} $d\E\n";
				}
				$report_info{'Objects'}{$typ}{'detail'} .= "Total number of rows: $total_row_num\n";
				$report_info{'Objects'}{$typ}{'detail'} .= "Top $self->{top_max} of tables sorted by number of rows:\n";
				my $j = 1;
				foreach my $t (sort {$self->{tables}{$b}{table_info}{num_rows} <=> $self->{tables}{$a}{table_info}{num_rows}} keys %{$self->{tables}}) {
					$report_info{'Objects'}{$typ}{'detail'} .= "\L$t\E has $self->{tables}{$t}{table_info}{num_rows} rows\n";
					$j++;
					last if ($j > $self->{top_max});
				}
				if ($self->{prefix} eq 'DBA') {
					$report_info{'Objects'}{$typ}{'detail'} .= "Top $self->{top_max} of largest tables:\n";
					$i = 1;
					my %largest_table = $self->_get_largest_tables();
					foreach my $t (sort { $largest_table{$b} <=> $largest_table{$a} } keys %largest_table) {
						$report_info{'Objects'}{$typ}{'detail'} .= "\L$t\E: $largest_table{$t} MB ($self->{tables}{$t}{table_info}{num_rows} rows)\n";
						$i++;
					}
				}
				$comment = "Nothing particular." if (!$comment);
				$report_info{'Objects'}{$typ}{'cost_value'} =~ s/(\.\d).*$/$1/;
			} elsif ($typ eq 'TYPE') {
				my $total_type = 0;
				foreach my $t (sort keys %{$self->{type_of_type}}) {
					$total_type++ if (!grep(/^$t$/, 'Associative Arrays','Type Boby','Type with member method'));
					$report_info{'Objects'}{$typ}{'detail'} .= "\L$self->{type_of_type}{$t} $t\E\n" if ($self->{type_of_type}{$t});
				}
				$report_info{'Objects'}{$typ}{'cost_value'} = ($Ora2Pg::PLSQL::OBJECT_SCORE{$typ}*$total_type) if ($self->{estimate_cost});
				$report_info{'Objects'}{$typ}{'comment'} = "$total_type type(s) are concerned by the export, others are not supported. Note that Type inherited and Subtype are converted as table, type inheritance is not supported.";
			} elsif ($typ eq 'TYPE BODY') {
				$report_info{'Objects'}{$typ}{'comment'} = "Export of type with member method are not supported, they will not be exported.";
			} elsif ($typ eq 'TRIGGER') {
				my $triggers = $self->_get_triggers();
				my $total_size = 0;
				foreach my $trig (@{$triggers}) {
					$total_size += length($trig->[4]);
					if ($self->{estimate_cost}) {
						my ($cost, %cost_detail) = Ora2Pg::PLSQL::estimate_cost($trig->[4]);
						$report_info{'Objects'}{$typ}{'cost_value'} += $cost;
						$report_info{'Objects'}{$typ}{'detail'} .= "\L$trig->[0]: $cost\E\n";
					}
				}
				$report_info{'Objects'}{$typ}{'comment'} = "Total size of trigger code: $total_size bytes.";
			} elsif ($typ eq 'SEQUENCE') {
				$report_info{'Objects'}{$typ}{'comment'} = "Sequences are fully supported, but all call to sequence_name.NEXTVAL or sequence_name.CURRVAL will be transformed into NEXTVAL('sequence_name') or CURRVAL('sequence_name').";
			} elsif ($typ eq 'FUNCTION') {
				my $functions = $self->_get_functions();
				my $total_size = 0;
				foreach my $fct (keys %{$functions}) {
					$total_size += length($functions->{$fct});
					if ($self->{estimate_cost}) {
						my ($cost, %cost_detail) = Ora2Pg::PLSQL::estimate_cost($functions->{$fct});
						$report_info{'Objects'}{$typ}{'cost_value'} += $cost;
						$report_info{'Objects'}{$typ}{'detail'} .= "\L$fct: $cost\E\n";
						$report_info{full_function_details}{"\L$fct\E"}{count} = $cost;
						foreach my $d (sort { $cost_detail{$b} <=> $cost_detail{$a} } keys %cost_detail) {
							next if (!$cost_detail{$d});
							$report_info{full_function_details}{"\L$fct\E"}{info} .= "\t$d => $cost_detail{$d}";
							$report_info{full_function_details}{"\L$fct\E"}{info} .= " (cost: $Ora2Pg::PLSQL::UNCOVERED_SCORE{$d})" if ($Ora2Pg::PLSQL::UNCOVERED_SCORE{$d});
							$report_info{full_function_details}{"\L$fct\E"}{info} .= "\n";
						}
					}
				}
				$report_info{'Objects'}{$typ}{'comment'} = "Total size of function code: $total_size bytes.";
			} elsif ($typ eq 'PROCEDURE') {
				my $procedures = $self->_get_procedures();
				my $total_size = 0;
				foreach my $proc (keys %{$procedures}) {
					$total_size += length($procedures->{$proc});
					if ($self->{estimate_cost}) {
						my ($cost, %cost_detail) = Ora2Pg::PLSQL::estimate_cost($procedures->{$proc});
						$report_info{'Objects'}{$typ}{'cost_value'} += $cost;
						$report_info{'Objects'}{$typ}{'detail'} .= "\L$proc: $cost\E\n";
						$report_info{full_function_details}{"\L$proc\E"}{count} = $cost;
						foreach my $d (sort { $cost_detail{$b} <=> $cost_detail{$a} } keys %cost_detail) {
							next if (!$cost_detail{$d});
							$report_info{full_function_details}{"\L$proc\E"}{info} .= "\t$d => $cost_detail{$d}";
							$report_info{full_function_details}{"\L$proc\E"}{info} .= " (cost: $Ora2Pg::PLSQL::UNCOVERED_SCORE{$d})" if ($Ora2Pg::PLSQL::UNCOVERED_SCORE{$d});
							$report_info{full_function_details}{"\L$proc\E"}{info} .= "\n";
						}
					}
				}
				$report_info{'Objects'}{$typ}{'comment'} = "Total size of procedure code: $total_size bytes.";
			} elsif ($typ eq 'PACKAGE BODY') {
				my $packages = $self->_get_packages();
				my $total_size = 0;
				my $number_fct = 0;
				my $number_pkg = 0;
				foreach my $pkg (keys %{$packages}) {
					next if (!$packages->{$pkg});
					$number_pkg++;
					$total_size += length($packages->{$pkg});
					my @codes = split(/CREATE(?: OR REPLACE)? PACKAGE BODY/, $packages->{$pkg});
					foreach my $txt (@codes) {
						my %infos = $self->_lookup_package("CREATE OR REPLACE PACKAGE BODY$txt");
						foreach my $f (sort keys %infos) {
							next if (!$f);
							if ($self->{estimate_cost}) {
								my ($cost, %cost_detail) = Ora2Pg::PLSQL::estimate_cost($infos{$f});
								$report_info{'Objects'}{$typ}{'cost_value'} += $cost;
								$report_info{'Objects'}{$typ}{'detail'} .= "\L$f: $cost\E\n";
								$report_info{full_function_details}{"\L$f\E"}{count} = $cost;
								foreach my $d (sort { $cost_detail{$b} <=> $cost_detail{$a} } keys %cost_detail) {
									next if (!$cost_detail{$d});
									$report_info{full_function_details}{"\L$f\E"}{info} .= "\t$d => $cost_detail{$d}";
									$report_info{full_function_details}{"\L$f\E"}{info} .= " (cost: $Ora2Pg::PLSQL::UNCOVERED_SCORE{$d})" if ($Ora2Pg::PLSQL::UNCOVERED_SCORE{$d});
									$report_info{full_function_details}{"\L$f\E"}{info} .= "\n";
								}
							}
							$number_fct++;
						}
					}
				}
				if ($self->{estimate_cost}) {
					$report_info{'Objects'}{$typ}{'cost_value'} += ($number_fct*$Ora2Pg::PLSQL::OBJECT_SCORE{'FUNCTION'});
					$report_info{'Objects'}{$typ}{'cost_value'} += ($number_pkg*$Ora2Pg::PLSQL::OBJECT_SCORE{'PACKAGE BODY'});
				}
				$report_info{'Objects'}{$typ}{'comment'} = "Total size of package code: $total_size bytes. Number of procedures and functions found inside those packages: $number_fct.";
			} elsif ($typ eq 'SYNONYM') {
				my %synonyms = $self->_get_synonyms();
				foreach my $t (sort keys %synonyms) {
					if ($synonyms{$t}{dblink}) {
						$report_info{'Objects'}{$typ}{'detail'} .= "\L$t\E is a link to $synonyms{$t}{dblink}";
						$report_info{'Objects'}{$typ}{'detail'} .= " ($synonyms{$t}{owner}.$synonyms{$t}{table})" if ($synonyms{$t}{table});
						$report_info{'Objects'}{$typ}{'detail'} .= "\n";
					} else {
						$report_info{'Objects'}{$typ}{'detail'} .= "\L$t\E is an alias to $synonyms{$t}{owner}.$synonyms{$t}{table}\n";
					}
				}
				$report_info{'Objects'}{$typ}{'comment'} = "SYNONYM are not exported at all. An usual workaround is to use View instead or set the PostgreSQL search_path in your session to access object outside the current schema.";
			} elsif ($typ eq 'INDEX PARTITION') {
				$report_info{'Objects'}{$typ}{'comment'} = "Only local indexes partition are exported, they are build on the column used for the partitioning.";
			} elsif ($typ eq 'TABLE PARTITION') {
				my %partitions = $self->_get_partitions_list();
				foreach my $t (sort keys %partitions) {
					$report_info{'Objects'}{$typ}{'detail'} .= "\L$partitions{$t} $t\E partitions\n";
				}
				$report_info{'Objects'}{$typ}{'comment'} = "Partitions are exported using table inheritance and check constraint. Hash partitions are not supported by PostgreSQL and will not be exported.";
			} elsif ($typ eq 'CLUSTER') {
				$report_info{'Objects'}{$typ}{'comment'} = "Clusters are not supported by PostgreSQL and will not be exported.";
			} elsif ($typ eq 'VIEW') {
				$report_info{'Objects'}{$typ}{'comment'} = "Views are fully supported, but if you have updatable views you will need to use INSTEAD OF triggers.";
			} elsif ($typ eq 'DATABASE LINK') {
				$report_info{'Objects'}{$typ}{'comment'} = "Database links will not be exported. You may try the dblink perl contrib module or use the SQL/MED PostgreSQL features with the different Foreign Data Wrapper (FDW) extentions.";
				if ($self->{estimate_cost}) {
					$report_info{'Objects'}{$typ}{'cost_value'} = ($Ora2Pg::PLSQL::OBJECT_SCORE{'DATABASE LINK'}*$objects{$typ});
				}
			} elsif ($typ eq 'JOB') {
				$report_info{'Objects'}{$typ}{'comment'} = "Job are not exported. You may set external cron job with them.";
				if ($self->{estimate_cost}) {
					$report_info{'Objects'}{$typ}{'cost_value'} = ($Ora2Pg::PLSQL::OBJECT_SCORE{'JOB'}*$objects{$typ});
				}
			}
			$report_info{'total_cost_value'} += $report_info{'Objects'}{$typ}{'cost_value'};
			$i++;
		}
		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $num_total_obj, 25, '=', 'objects types', 'end of objects auditing.'), "\n";
		}

		# Display report in the requested format
		$self->_show_report(%report_info);

	} elsif ($type eq 'SHOW_SCHEMA') {
		# Get all tables information specified by the DBI method table_info
		$self->logit("Showing all schema...\n", 1);
		my $sth = $self->_schema_list()  or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while ( my @row = $sth->fetchrow()) {
			my $warning = '';
			if (&is_reserved_words($row[0])) {
				$warning = " (Warning: '$row[0]' is a reserved word in PostgreSQL)";
			}
			$self->logit("SCHEMA $row[0]$warning\n", 0);
		}
		$sth->finish();
	} elsif ( ($type eq 'SHOW_TABLE') || ($type eq 'SHOW_COLUMN') ) {
		# Get all tables information specified by the DBI method table_info
		$self->logit("Showing table information...\n", 1);

		# Retrieve tables informations
		my %tables_infos = $self->_table_info();

		# Retrieve all columns information
		my %columns_infos = $self->_column_info('',$self->{schema});
		foreach my $tb (keys %columns_infos) {
			foreach my $c (keys %{$columns_infos{$tb}}) {
				push(@{$self->{tables}{$tb}{column_info}{$c}}, @{$columns_infos{$tb}{$c}});
			}
		}
		%columns_infos = ();

		my @done = ();
		my $id = 0;
		# Set the table information for each class found
		my $i = 1;
		my $total_row_num = 0;
		foreach my $t (sort keys %tables_infos) {

			# forget or not this object if it is in the exclude or allow lists.
			if ($tables_infos{$t}{type} ne 'VIEW') {
				next if ($self->skip_this_object('TABLE', $t));
			}

			# Jump to desired extraction
			if (grep(/^$t$/, @done)) {
				$self->logit("Duplicate entry found: $t\n", 1);
				next;
			} else {
				push(@done, $t);
			}
			my $warning = '';
			if (&is_reserved_words($t)) {
				$warning = " (Warning: '$t' is a reserved word in PostgreSQL)";
			}

			$total_row_num += $tables_infos{$t}{num_rows};

			# Show table information
			$self->logit("[$i] TABLE $t ($tables_infos{$t}{num_rows} rows)$warning\n", 0);

			# Set the fields information
			if ($type eq 'SHOW_COLUMN') {

				# Collect column's details for the current table with attempt to preserve column declaration order
				foreach my $k (sort { 
						if (!$self->{reordering_columns}) {
							$self->{tables}{$t}{column_info}{$a}[10] <=> $self->{tables}{$t}{column_info}{$b}[10];
						} else {
							my $tmpa = $self->{tables}{$t}{column_info}{$a};
							$tmpa->[2] =~ s/\D//g;
							my $typa = $self->_sql_type($tmpa->[1], $tmpa->[2], $tmpa->[5], $tmpa->[6]);
							$typa =~ s/\(.*//;
							my $tmpb = $self->{tables}{$t}{column_info}{$b};
							$tmpb->[2] =~ s/\D//g;
							my $typb = $self->_sql_type($tmpb->[1], $tmpb->[2], $tmpb->[5], $tmpb->[6]);
							$typb =~ s/\(.*//;
							$TYPALIGN{$typb} <=> $TYPALIGN{$typa};
						}
					} keys %{$self->{tables}{$t}{column_info}}) {
					# COLUMN_NAME,DATA_TYPE,DATA_LENGTH,NULLABLE,DATA_DEFAULT,DATA_PRECISION,DATA_SCALE,CHAR_LENGTH,TABLE_NAME,OWNER,POSITION,SDO_DIM,SDO_GTYPE,SRID
					my $d = $self->{tables}{$t}{column_info}{$k};
					$d->[2] =~ s/\D//g;
					my $type = $self->_sql_type($d->[1], $d->[2], $d->[5], $d->[6]);
					$type = "$d->[1], $d->[2]" if (!$type);
					$type = $self->{'modify_type'}{"\L$t\E"}{"\L$k\E"} if (exists $self->{'modify_type'}{"\L$t\E"}{"\L$k\E"});
					my $align = '';
					my $len = $d->[2];
					if (($d->[1] =~ /char/i) && ($d->[7] > $d->[2])) {
						$d->[2] = $d->[7];
					}
					$self->logit("\t$d->[0] : $d->[1]");
					if ($d->[1] !~ /SDO_GEOMETRY/) {
						if ($d->[2] && !$d->[5]) {
							$self->logit("($d->[2])");
						} elsif ($d->[5] && ($d->[1] =~ /NUMBER/i) ) {
							$self->logit("($d->[5]");
							$self->logit(",$d->[6]") if ($d->[6]);
							$self->logit(")");
						}
						$warning = '';
						if ($self->{reordering_columns}) {
							my $typ = $type;
							$typ =~ s/\(.*//;
							$align = " - typalign: $TYPALIGN{$typ}";
						}
					} else {
						# Set the dimension
						my $suffix = '';
						if ($d->[11] == 3) {
							$suffix = 'Z';
						} elsif ($d->[11] == 4) {
							$suffix = 'ZM';
						}
						$d->[12] ||= 0;
						$type = "geometry($ORA2PG_SDO_GTYPE{$d->[12]}$suffix";
						if ($d->[13]) {
							$type .= ",$d->[13]";
						}
						$type .= ")";
						
					}
					if (&is_reserved_words($d->[0])) {
						$warning = " (Warning: '$d->[0]' is a reserved word in PostgreSQL)";
					}
					# Check if this column should be replaced by a boolean following table/column name
					if (grep(/^$d->[0]$/i, @{$self->{'replace_as_boolean'}{$t}})) {
						$type = 'boolean';
					# Check if this column should be replaced by a boolean following type/precision
					} elsif (exists $self->{'replace_as_boolean'}{uc($d->[1])} && ($self->{'replace_as_boolean'}{uc($d->[1])}[0] == $d->[5])) {
						$type = 'boolean';
					}
					$self->logit(" => $type$warning$align\n");
				}
			}
			$i++;
		}
		$self->logit("----------------------------------------------------------\n", 0);
		$self->logit("Total number of rows: $total_row_num\n\n", 0);
		$self->logit("Top $self->{top_max} of tables sorted by number of rows:\n", 0);
		$i = 1;
		foreach my $t (sort {$tables_infos{$b}{num_rows} <=> $tables_infos{$a}{num_rows}} keys %tables_infos) {
			$self->logit("\t[$i] TABLE $t has $tables_infos{$t}{num_rows} rows\n", 0);
			$i++;
			last if ($i > $self->{top_max});
		}
		if ($self->{prefix} eq 'DBA') {
			$self->logit("Top $self->{top_max} of largest tables:\n", 0);
			$i = 1;
			my %largest_table = $self->_get_largest_tables();
			foreach my $t (sort { $largest_table{$b} <=> $largest_table{$a} } keys %largest_table) {
				$self->logit("\t[$i] TABLE $t: $largest_table{$t} MB ($self->{tables}{$t}{table_info}{num_rows} rows)\n", 0);
				$i++;
			}
		}
	}
}

=head2 _get_version

This function retrieves the Oracle version information

=cut

sub _get_version
{
	my $self = shift;

	my $oraver = '';
	my $sql = "SELECT BANNER FROM v\$version";

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

=head2 _get_database_size

This function retrieves the size of the Oracle database in MB

=cut

sub _get_database_size
{
	my $self = shift;

	my $mb_size = '';
	my $sql = "SELECT sum(bytes)/1024/1024 FROM USER_SEGMENTS";
        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$mb_size = sprintf("%.2f MB", $row[0]);
		last;
	}
	$sth->finish();

	return $mb_size;
}

=head2 _get_objects

This function retrieves all object the Oracle information

=cut

sub _get_objects
{
	my $self = shift;

	my $oraver = '';
	# OWNER|OBJECT_NAME|SUBOBJECT_NAME|OBJECT_ID|DATA_OBJECT_ID|OBJECT_TYPE|CREATED|LAST_DDL_TIME|TIMESTAMP|STATUS|TEMPORARY|GENERATED|SECONDARY
	my $sql = "SELECT OBJECT_NAME,OBJECT_TYPE,STATUS FROM $self->{prefix}_OBJECTS WHERE TEMPORARY='N' AND GENERATED='N' AND SECONDARY='N'";
        if ($self->{schema}) {
                $sql .= " AND OWNER='$self->{schema}'";
        } else {
                $sql .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
        }

	my @infos = ();
        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
	push(@infos, join('|', @{$sth->{NAME}}));
        $sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		push(@{$infos{$row[1]}}, { ( name => $row[0], invalid => ($row[2] eq 'VALID') ? 0 : 1) });
	}
	$sth->finish();

	return %infos;
}

=head2 _schema_list

This function retrieves all Oracle-native user schema.

Returns a handle to a DB query statement.

=cut

sub _schema_list
{
	my $self = shift;

	my $sql = "SELECT DISTINCT OWNER FROM ALL_TABLES WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "') ORDER BY OWNER";

        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
        $sth;
}

=head2 _get_largest_tables

This function retrieves the list of largest table of the Oracle database in MB

=cut

sub _get_largest_tables
{
	my $self = shift;

	my %table_size = ();

	my $sql = "SELECT * FROM ( SELECT SEGMENT_NAME, ROUND(BYTES/1024/1024) SIZE_MB FROM DBA_SEGMENTS WHERE SEGMENT_TYPE='TABLE'";
        if ($self->{schema}) {
                $sql .= " AND OWNER='$self->{schema}'";
        } else {
                $sql .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
        }
	$sql .= " ORDER BY BYTES DESC) WHERE ROWNUM <= $self->{top_max}";
        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		next if ($self->skip_this_object('TABLE', $row[0]));
		$table_size{$row[0]} = $row[1];
	}
	$sth->finish();

	return %table_size;
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
	while ( my @row = $sth->fetchrow()) {
		#$self->logit("DATABASE PARAMETERS: $row[0] $row[1]\n", 1);
		if ($row[0] eq 'NLS_LANGUAGE') {
			$language = $row[1];
		} elsif ($row[0] eq 'NLS_TERRITORY') {
			$territory = $row[1];
		} elsif ($row[0] eq 'NLS_CHARACTERSET') {
			$charset = $row[1];
		}
	}
	$sth->finish();
	$sql = "SELECT * FROM NLS_SESSION_PARAMETERS";
        $sth = $dbh->prepare($sql) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
        $sth->execute() or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
	my $encoding = '';
	while ( my @row = $sth->fetchrow()) {
		#$self->logit("SESSION PARAMETERS: $row[0] $row[1]\n", 1);
		if ($row[0] eq 'NLS_LANGUAGE') {
			$language = $row[1];
		} elsif ($row[0] eq 'NLS_TERRITORY') {
			$territory = $row[1];
		}
	}
	$sth->finish();

	$encoding = $language . '_' . $territory . '.' . $charset;

	return $encoding;
}

=head2 _compile_schema

This function force Oracle database to compile a schema and validate or
invalidate PL/SQL code

=cut


sub _compile_schema
{
	my ($self, $dbh, $schema) = @_;

	my $qcomp = '';

	if ($schema || ($schema =~ /[a-z]/i)) {
		$qcomp = qq{begin
DBMS_UTILITY.compile_schema(schema => '$schema');
end;
};
	} elsif ($schema) {
		$qcomp = "EXEC DBMS_UTILITY.compile_schema(schema => sys_context('USERENV', 'SESSION_USER'));";
	}
	if ($qcomp) {
		my $sth = $dbh->do($qcomp) or $self->logit("FATAL: " . $dbh->errstr . "\n", 0, 1);
		$sth = undef;
	}

}

=head2 _datetime_format

This function force Oracle database to format the time correctly

=cut

sub _datetime_format
{
	my ($self) = @_;

	if ($self->{enable_microsecond}) {
		my $sth = $self->{dbh}->do("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF'") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	} else {
		my $sth = $self->{dbh}->do("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS'") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
	my $sth = $self->{dbh}->do("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	if ($self->{enable_microsecond}) {
		$sth = $self->{dbh}->do("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS TZH:TZM'") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	} else {
		$sth = $self->{dbh}->do("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	}
}

sub _numeric_format
{
	my ($self) = @_;

	my $sth = $self->{dbh}->do("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
}


=head2 multiprocess_progressbar

This function is used to display a progress bar during object scanning.

=cut

sub multiprocess_progressbar
{
	my ($self, $total_rows) = @_;

	$self->logit("Starting progressbar writer process\n", 1);

	$0 = 'ora2pg logger';

	my $width = 25;
	my $char  = '=';
	my $kind  = 'rows';
	my $table_count = 0;
	my $table = '';
	my $global_count = 0;
	my $global_start_time = 0;

	# Terminate the process when we doesn't read the complete file but must exit
	local $SIG{USR1} = sub {
		print STDERR "\n";
		if ($global_count) {
			my $end_time = time();
			my $dt = $end_time - $global_start_time;
			$dt ||= 1;
			my $rps = sprintf("%.1f", $global_count / ($dt+.0001));
			print STDERR $self->progress_bar($global_count, $total_rows, 25, '=', 'rows', "on total data (avg: $rps recs/sec)");
			print STDERR "\n";
		}
		exit 0;
	};

	$pipe->reader();
	while ( my $r = <$pipe> ) {
		chomp($r);
		# When quit is received, then exit immediatly
		last if ($r eq 'quit');
		my @infos = split(/\s+/, $r);
		my $table_numrows = $infos[2];
		my $start_time = $infos[3];
		$global_start_time = $start_time if (!$global_start_time);
		# Display total and reset counter when it is a new table
		if ($table && ($infos[1] ne $table)) {
			print STDERR "\n";
			my $end_time = time();
			my $dt = $end_time - $global_start_time;
			$dt ||= 1;
			my $rps = sprintf("%.1f", $global_count / ($dt+.0001));
			print STDERR $self->progress_bar($global_count, $total_rows, 25, '=', 'rows', "on total data (avg: $rps recs/sec)");
			print STDERR "\n";
			$table_count = 0;
		}
		$table = $infos[1];
		$table_count += $infos[0];
		$global_count += $infos[0];
		my $end_time = time();
		my $dt = $end_time - $start_time;
		$dt ||= 1;
		my $rps = sprintf("%.1f", $table_count / ($dt+.0001));
		print STDERR $self->progress_bar($table_count, $table_numrows, 25, '=', 'rows', "Table $table ($rps recs/sec)");
	}
	print STDERR "\n";
	if ($global_count) {
		my $end_time = time();
		my $dt = $end_time - $global_start_time;
		$dt ||= 1;
		my $rps = sprintf("%.1f", $global_count / ($dt+.0001));
		print STDERR $self->progress_bar($global_count, $total_rows, 25, '=', 'rows', "on total data (avg: $rps recs/sec)");
		print STDERR "\n";
	}

	exit 0;
}


=head2 progress_bar

This function is used to display a progress bar during object scanning.

=cut

sub progress_bar
{
	my ($self, $got, $total, $width, $char, $kind, $msg) = @_;

	$width ||= 25;
	$char  ||= '=';
	$kind  ||= 'rows';
	my $num_width = length $total;
	my $ratio = 1;
	if ($total > 0) {
		$ratio = $got / +$total;
	}
	my $len = (($width - 1) * $ratio);
	$len = $width - 1 if ($len >= $width);
	my $str = sprintf(
		"[%-${width}s] %${num_width}s/%s $kind (%.1f%%) $msg",
		$char x $len . '>',
		$got, $total, 100 * $ratio
	);
	$len = length($str);
	$self->{prgb_len} ||= $len;
	if ($len < $self->{prgb_len}) {
		$str .= ' ' x ($self->{prgb_len} - $len);
	}
	$self->{prgb_len} = $len;

	return "$str\r";
}

=head2 auto_set_encoding

This function is used to find the PostgreSQL charset corresponding to the
Oracle NLS_LANG value

=cut

sub auto_set_encoding
{
	my $oracle_encoding = shift;

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
		return $ENCODING{$k} if ($oracle_encoding =~ /\.$k/i);
	}

	return '';
}

# Return 0 if the object should be exported, 1 if it not found in allow list
# and 2 if it is found in the exclude list
sub skip_this_object
{
	my ($self, $obj_type, $name) = @_;

	# Exclude object in Recycle Bin from the export
	return 3 if ($name =~ /^BIN\$/);

	# Check if this object is in the allowed list of object to export.
	if (($obj_type ne 'INDEX') && ($obj_type ne 'FKEY')) {
		return 1 if (($#{$self->{limited}} >= 0) && !grep($name =~ /^$_$/i, @{$self->{limited}}));
	}

	# Check if this object is in the exlusion list of object to export.
	return 2 if (($#{$self->{excluded}} >= 0) && grep($name =~ /^$_$/i, @{$self->{excluded}}));

	return 0;
}

sub limit_to_tables
{
	my ($self, $column) = @_;

	my $str = '';
	$column ||= 'TABLE_NAME';

	if ($#{$self->{limited}} >= 0) {
		$str = "AND $column IN ('" .  join("','", @{$self->{limited}}) . "') ";
	} elsif ($#{$self->{excluded}} >= 0) {
		$str = "AND $column NOT IN ('" .  join("','", @{$self->{excluded}}) . "') ";
	}

	return uc($str);
}


# Preload the bytea array at lib init
BEGIN
{
	build_escape_bytea();
}


=head2 _lookup_check_constraint

This function return an array of the SQL code of the check constraints of a table

=cut
sub _lookup_check_constraint
{
	my ($self, $table, $check_constraint, $field_name, $nonotnull) = @_;

	my  @chk_constr = ();

	my $tbsaved = $table;
	$table = $self->get_replaced_tbname($table);

	# Set the check constraint definition 
	foreach my $k (keys %{$check_constraint->{constraint}}) {
		my $chkconstraint = $check_constraint->{constraint}->{$k};
		next if (!$chkconstraint);
		my $skip_create = 0;
		if (exists $check_constraint->{notnull}) {
			foreach my $col (@{$check_constraint->{notnull}}) {
				$skip_create = 1, last if (lc($chkconstraint) eq lc("\"$col\" IS NOT NULL"));
			}
		}
		if (!$skip_create) {
			if (exists $self->{replaced_cols}{"\L$tbsaved\E"} && $self->{replaced_cols}{"\E$tbsaved\L"}) {
				foreach my $c (keys %{$self->{replaced_cols}{"\L$tbsaved\E"}}) {
					$chkconstraint =~ s/"$c"/"$self->{replaced_cols}{"\L$tbsaved\E"}{$c}"/gsi;
					$chkconstraint =~ s/\b$c\b/$self->{replaced_cols}{"\L$tbsaved\E"}{$c}/gsi;
				}
			}
			if ($self->{plsql_pgsql}) {
				$chkconstraint = Ora2Pg::PLSQL::plsql_to_plpgsql($chkconstraint, $self->{allow_code_break},$self->{null_equal_empty});
			}
			next if ($nonotnull && ($chkconstraint =~ /IS NOT NULL/));
			if (!$self->{preserve_case}) {
				foreach my $c (@$field_name) {
					# Force lower case
					my $ret = $self->quote_reserved_words($c);
					$chkconstraint =~ s/"$c"/$ret/igs;
					$chkconstraint =~ s/\b$c\b/$ret/gsi;
				}
				$k = lc($k);
			}
			push(@chk_constr,  "ALTER TABLE $table ADD CONSTRAINT $k CHECK ($chkconstraint);\n");
		}
	}

	return @chk_constr;
}


=head2 _lookup_package

This function is used to look at Oracle PACKAGE code to estimate the cost
of a migration. It return an hash: function name => function code

=cut

sub _lookup_package
{
	my ($self, $plsql) = @_;

	my $content = '';
	my %infos = ();
	if ($plsql =~ /PACKAGE[\s\t]+BODY[\s\t]*([^\s\t]+)[\s\t]*(AS|IS)[\s\t]*(.*)/is) {
		my $pname = $1;
		my $type = $2;
		$content = $3;
		$pname =~ s/"//g;
		$self->logit("Looking at package $pname...\n", 1);
		$self->{idxcomment} = 0;
		$content =~ s/END[^;]*;$//is;
		my %comments = $self->_remove_comments(\$content);
		my @functions = $self->_extract_functions($content);
		foreach my $f (@functions) {
			next if (!$f);
			my $func_name = $self->_lookup_function($f, $pname);
			$infos{"$pname.$func_name"} = $f if ($func_name);
		}
	}

	return %infos;
}

=head2 _lookup_function

This function is used to look at Oracle FUNCTION code to estimate
the cost of a migration.

Return the function name and the code

=cut

sub _lookup_function
{
	my ($self, $plsql, $pname) = @_;

	my %fct_infos = ();

	my $func_name = '';

	# Split data into declarative and code part
	my ($func_declare, $func_code) = split(/\bBEGIN\b/i,$plsql,2);
	if ( $func_declare =~ s/(.*?)\b(FUNCTION|PROCEDURE)[\s\t]+([^\s\t\(]+)[\s\t]*(\([^\)]*\)|[\s\t]*)//is ) {
		$func_name = $3;
		$func_name =~ s/"//g;

		# forget or not this object if it is in the exclude or allow lists.
		$func_name = '' if ($self->skip_this_object('FUNCTION', $func_name));
	}

	return $func_name;
}

####
# Return a string to set the current search path
####
sub set_search_path
{
	my $self = shift;

	my $search_path = '';

	if ($self->{export_schema}) {
		if ($self->{pg_schema}) {
			if (!$self->{preserve_case}) {
				$search_path = "SET search_path = \L$self->{pg_schema}\E;";
			} else {
				$search_path = "SET search_path = \"$self->{pg_schema}\";";
			}
		} elsif ($self->{schema}) {
			if (!$self->{preserve_case}) {
				$search_path = "SET search_path = \L$self->{schema}\E, pg_catalog;";
			} else {
				$search_path = "SET search_path = \"$self->{schema}\", pg_catalog;";
			}
		}
	}

	return $search_path;
}

sub _get_human_cost
{
	my ($self, $total_cost_value) = @_;

	return 0 if (!$total_cost_value);

	my $human_cost = $total_cost_value * $self->{cost_unit_value};
	if ($human_cost >= 420) {
		my $tmp = $human_cost/420;
		$tmp++ if ($tmp =~ s/\.\d+//);
		$human_cost = "$tmp man-day(s)";
	} else {
		my $tmp = $human_cost/60;
		$tmp++ if ($tmp =~ s/\.\d+//);
		$human_cost = "$tmp man-hour(s)";
	} 

	return $human_cost;
}

sub _show_report
{
	my ($self, %report_info) = @_;

	# Generate report text report
	if (!$self->{dump_as_html}) {
		my $cost_header = '';
		$cost_header = "\tEstimated cost" if ($self->{estimate_cost});
		$self->logit("-------------------------------------------------------------------------------\n", 0);
		$self->logit("Ora2Pg v$VERSION - Database Migration Report\n", 0);
		$self->logit("-------------------------------------------------------------------------------\n", 0);
		$self->logit("Version\t$report_info{'Version'}\n", 0);
		$self->logit("Schema\t$report_info{'Schema'}\n", 0);
		$self->logit("Size\t$report_info{'Size'}\n\n", 0);
		$self->logit("-------------------------------------------------------------------------------\n", 0);
		$self->logit("Object\tNumber\tInvalid$cost_header\tComments\tDetails\n", 0);
		$self->logit("-------------------------------------------------------------------------------\n", 0);
		foreach my $typ (sort keys %{ $report_info{'Objects'} } ) {
			$report_info{'Objects'}{$typ}{'detail'} =~ s/\n/\. /gs;
			if ($self->{estimate_cost}) {
				$self->logit("$typ\t$report_info{'Objects'}{$typ}{'number'}\t$report_info{'Objects'}{$typ}{'invalid'}\t$report_info{'Objects'}{$typ}{'cost_value'}\t$report_info{'Objects'}{$typ}{'comment'}\t$report_info{'Objects'}{$typ}{'detail'}\n", 0);
			} else {
				$self->logit("$typ\t$report_info{'Objects'}{$typ}{'number'}\t$report_info{'Objects'}{$typ}{'invalid'}\t$report_info{'Objects'}{$typ}{'comment'}\t$report_info{'Objects'}{$typ}{'detail'}\n", 0);
			}
		}
		$self->logit("-------------------------------------------------------------------------------\n", 0);
		if ($self->{estimate_cost}) {
			my $human_cost = $self->_get_human_cost($report_info{'total_cost_value'});
			my $comment = "$report_info{'total_cost_value'} cost migration units means approximatively $human_cost. The migration unit was set to $self->{cost_unit_value} minute(s)\n";
			$self->logit("Total\t$report_info{'total_object_number'}\t$report_info{'total_object_invalid'}\t$report_info{'total_cost_value'}\t$comment\n", 0);
		} else {
			$self->logit("Total\t$report_info{'total_object_number'}\t$report_info{'total_object_invalid'}\n", 0);
		}
		$self->logit("-------------------------------------------------------------------------------\n", 0);
		if ($self->{estimate_cost}) {
			$self->logit("\nDetails of cost assessment per function\n", 0);
			foreach my $fct (sort { $report_info{'full_function_details'}{$b}{count} <=> $report_info{'full_function_details'}{$a}{count} } keys %{ $report_info{'full_function_details'} } ) {
				$self->logit("Function $fct total estimated cost: $report_info{'full_function_details'}{$fct}{count}\n", 0);
				$self->logit($report_info{'full_function_details'}{$fct}{info}, 0);
			}
			$self->logit("-------------------------------------------------------------------------------\n", 0);
		}
	} else {
		my $cost_header = '';
		$cost_header = "<th>Estimated cost</th>" if ($self->{estimate_cost});
		my $date = localtime(time);
		my $html_header = qq{<!DOCTYPE html>
<html>
  <head>
  <title>Ora2Pg - Database Migration Report</title>
  <meta HTTP-EQUIV="Generator" CONTENT="Ora2Pg v$VERSION">
  <meta HTTP-EQUIV="Date" CONTENT="$date">
  <style>
body {
	margin: 30px 0;
	padding: 0;
	background: #EFEFEF;
	font-size: 12px;
	color: #1e1e1e;
}

h1 {
	margin-bottom: 20px;
	border-bottom: 1px solid #DFDFDF;
	font-size: 22px;
	padding: 0px;
	padding-bottom: 5px;
	font-weight: bold;
	color: #0094C7;
}

h2 {
	margin-bottom: 10px;
	font-size: 18px;
	padding: 0px;
	padding-bottom: 5px;
	font-weight: bold;
	color: #0094C7;
}

#header table {
	padding: 0 5px 0 5px;
	border: 1px solid #DBDBDB;
	margin-bottom: 20px;
	margin-left: 30px;
}

#header th {
	padding: 0 5px 0 5px;
	text-decoration: none;
	font-size: 16px;
	color: #EC5800;
}

#content table {
	padding: 0 5px 0 5px;
	border: 1px solid #DBDBDB;
	margin-bottom: 20px;
	margin-left: 10px;
	margin-right: 10px;
}
#content td {
	padding: 0 5px 0 5px;
	border-bottom: 1px solid #888888;
	margin-bottom: 20px;
	text-align: left;
	vertical-align: top;
}

#content th {
	border-bottom: 1px solid #BBBBBB;
	padding: 0 5px 0 5px;
	text-decoration: none;
	font-size: 16px;
	color: #EC5800;
}

.object_name {
        font-weight: bold;
        color: #0094C7;
	text-align: left;
	white-space: pre;
}

.detail {
	white-space: pre;
}

#footer {
	margin-right: 10px;
	text-align: right;
}

#footer a {
	color: #EC5800;
}

#footer a:hover {
	text-decoration: none;
}
  </style>
</head>
<body>
<div id="header">
<h1>Ora2Pg - Database Migration Report</h1>
<table>
<tr><th>Version</th><td>$report_info{'Version'}</td></tr>
<tr><th>Schema</th><td>$report_info{'Schema'}</td></tr>
<tr><th>Size</th><td>$report_info{'Size'}</td></tr>
</table>
</div>
<div id="content">
<table>
<tr><th>Object</th><th>Number</th><th>Invalid</th>$cost_header<th>Comments</th><th>Details</th></tr>
};

		$self->logit($html_header, 0);
		foreach my $typ (sort keys %{ $report_info{'Objects'} } ) {
			$report_info{'Objects'}{$typ}{'detail'} =~ s/\n/<br>/gs;
			if ($self->{estimate_cost}) {
				$self->logit("<tr><td class=\"object_name\">$typ</td><td style=\"text-align: center;\">$report_info{'Objects'}{$typ}{'number'}</td><td style=\"text-align: center;\">$report_info{'Objects'}{$typ}{'invalid'}</td><td style=\"text-align: center;\">$report_info{'Objects'}{$typ}{'cost_value'}</td><td>$report_info{'Objects'}{$typ}{'comment'}</td><td class=\"detail\">$report_info{'Objects'}{$typ}{'detail'}</td></tr>\n", 0);
			} else {
				$self->logit("<tr><td class=\"object_name\">$typ</td><td style=\"text-align: center;\">$report_info{'Objects'}{$typ}{'number'}</td><td style=\"text-align: center;\">$report_info{'Objects'}{$typ}{'invalid'}</td><td>$report_info{'Objects'}{$typ}{'comment'}</td><td class=\"detail\">$report_info{'Objects'}{$typ}{'detail'}</td></tr>\n", 0);
			}
		}
		if ($self->{estimate_cost}) {
			my $human_cost = $self->_get_human_cost($report_info{'total_cost_value'});
			my $comment = "$report_info{'total_cost_value'} cost migration units means approximatively $human_cost. The migration unit was set to $self->{cost_unit_value} minute(s)\n";
			$self->logit("<tr><th style=\"text-align: center; border-bottom: 0px; vertical-align: bottom;\">Total</th><td style=\"text-align: center; border-bottom: 0px; vertical-align: bottom;\">$report_info{'total_object_number'}</td><td style=\"text-align: center; border-bottom: 0px; vertical-align: bottom;\">$report_info{'total_object_invalid'}</td><td style=\"text-align: center; border-bottom: 0px; vertical-align: bottom;\">$report_info{'total_cost_value'}</td><td colspan=\"2\" style=\"border-bottom: 0px; vertical-align: bottom;\">$comment</td></tr>\n", 0);
		} else {
			$self->logit("<tr><th style=\"text-align: center; border-bottom: 0px; vertical-align: bottom;\">Total</th><td style=\"text-align: center; border-bottom: 0px; vertical-align: bottom; border-bottom: 0px; vertical-align: bottom;\">$report_info{'total_object_number'}</td><td style=\"text-align: center; border-bottom: 0px; vertical-align: bottom;\">$report_info{'total_object_invalid'}</td><td colspan=\"3\" style=\"border-bottom: 0px; vertical-align: bottom;\"></td></tr>\n", 0);
		}
		$self->logit("</table>\n</div>\n", 0);

		if ($self->{estimate_cost}) {
			$self->logit("<h2>Details of cost assessment per function</h2>\n", 0);
			$self->logit("<ul>\n", 0);
			foreach my $fct (sort { $report_info{'full_function_details'}{$b}{count} <=> $report_info{'full_function_details'}{$a}{count} } keys %{ $report_info{'full_function_details'} } ) {
				
				$self->logit("<li>Function $fct total estimated cost: $report_info{'full_function_details'}{$fct}{count}</li>\n", 0);
				$self->logit("<ul>\n", 0);
				$report_info{'full_function_details'}{$fct}{info} =~ s/\t/<li>/gs;
				$report_info{'full_function_details'}{$fct}{info} =~ s/\n/<\/li>\n/gs;
				$self->logit($report_info{'full_function_details'}{$fct}{info}, 0);
				$self->logit("</ul>\n", 0);
			}
			$self->logit("</ul>\n", 0);
		}
		my $html_footer = qq{
<div id="footer">
Generated by <a href="http://ora2pg.darold.net/">Ora2Pg v$VERSION</a>
</div>
</body>
</html>
};
		$self->logit($html_footer, 0);
	}

}


sub get_kettle_xml
{

	return <<EOF
<transformation>
  <info>
    <name>template</name>
    <description/>
    <extended_description/>
    <trans_version/>
    <trans_type>Normal</trans_type>
    <trans_status>0</trans_status>
    <directory>&#47;</directory>
    <parameters>
    </parameters>
    <log>
<trans-log-table><connection/>
<schema/>
<table/>
<size_limit_lines/>
<interval/>
<timeout_days/>
<field><id>ID_BATCH</id><enabled>Y</enabled><name>ID_BATCH</name></field><field><id>CHANNEL_ID</id><enabled>Y</enabled><name>CHANNEL_ID</name></field><field><id>TRANSNAME</id><enabled>Y</enabled><name>TRANSNAME</name></field><field><id>STATUS</id><enabled>Y</enabled><name>STATUS</name></field><field><id>LINES_READ</id><enabled>Y</enabled><name>LINES_READ</name><subject/></field><field><id>LINES_WRITTEN</id><enabled>Y</enabled><name>LINES_WRITTEN</name><subject/></field><field><id>LINES_UPDATED</id><enabled>Y</enabled><name>LINES_UPDATED</name><subject/></field><field><id>LINES_INPUT</id><enabled>Y</enabled><name>LINES_INPUT</name><subject/></field><field><id>LINES_OUTPUT</id><enabled>Y</enabled><name>LINES_OUTPUT</name><subject/></field><field><id>LINES_REJECTED</id><enabled>Y</enabled><name>LINES_REJECTED</name><subject/></field><field><id>ERRORS</id><enabled>Y</enabled><name>ERRORS</name></field><field><id>STARTDATE</id><enabled>Y</enabled><name>STARTDATE</name></field><field><id>ENDDATE</id><enabled>Y</enabled><name>ENDDATE</name></field><field><id>LOGDATE</id><enabled>Y</enabled><name>LOGDATE</name></field><field><id>DEPDATE</id><enabled>Y</enabled><name>DEPDATE</name></field><field><id>REPLAYDATE</id><enabled>Y</enabled><name>REPLAYDATE</name></field><field><id>LOG_FIELD</id><enabled>Y</enabled><name>LOG_FIELD</name></field></trans-log-table>
<perf-log-table><connection/>
<schema/>
<table/>
<interval/>
<timeout_days/>
<field><id>ID_BATCH</id><enabled>Y</enabled><name>ID_BATCH</name></field><field><id>SEQ_NR</id><enabled>Y</enabled><name>SEQ_NR</name></field><field><id>LOGDATE</id><enabled>Y</enabled><name>LOGDATE</name></field><field><id>TRANSNAME</id><enabled>Y</enabled><name>TRANSNAME</name></field><field><id>STEPNAME</id><enabled>Y</enabled><name>STEPNAME</name></field><field><id>STEP_COPY</id><enabled>Y</enabled><name>STEP_COPY</name></field><field><id>LINES_READ</id><enabled>Y</enabled><name>LINES_READ</name></field><field><id>LINES_WRITTEN</id><enabled>Y</enabled><name>LINES_WRITTEN</name></field><field><id>LINES_UPDATED</id><enabled>Y</enabled><name>LINES_UPDATED</name></field><field><id>LINES_INPUT</id><enabled>Y</enabled><name>LINES_INPUT</name></field><field><id>LINES_OUTPUT</id><enabled>Y</enabled><name>LINES_OUTPUT</name></field><field><id>LINES_REJECTED</id><enabled>Y</enabled><name>LINES_REJECTED</name></field><field><id>ERRORS</id><enabled>Y</enabled><name>ERRORS</name></field><field><id>INPUT_BUFFER_ROWS</id><enabled>Y</enabled><name>INPUT_BUFFER_ROWS</name></field><field><id>OUTPUT_BUFFER_ROWS</id><enabled>Y</enabled><name>OUTPUT_BUFFER_ROWS</name></field></perf-log-table>
<channel-log-table><connection/>
<schema/>
<table/>
<timeout_days/>
<field><id>ID_BATCH</id><enabled>Y</enabled><name>ID_BATCH</name></field><field><id>CHANNEL_ID</id><enabled>Y</enabled><name>CHANNEL_ID</name></field><field><id>LOG_DATE</id><enabled>Y</enabled><name>LOG_DATE</name></field><field><id>LOGGING_OBJECT_TYPE</id><enabled>Y</enabled><name>LOGGING_OBJECT_TYPE</name></field><field><id>OBJECT_NAME</id><enabled>Y</enabled><name>OBJECT_NAME</name></field><field><id>OBJECT_COPY</id><enabled>Y</enabled><name>OBJECT_COPY</name></field><field><id>REPOSITORY_DIRECTORY</id><enabled>Y</enabled><name>REPOSITORY_DIRECTORY</name></field><field><id>FILENAME</id><enabled>Y</enabled><name>FILENAME</name></field><field><id>OBJECT_ID</id><enabled>Y</enabled><name>OBJECT_ID</name></field><field><id>OBJECT_REVISION</id><enabled>Y</enabled><name>OBJECT_REVISION</name></field><field><id>PARENT_CHANNEL_ID</id><enabled>Y</enabled><name>PARENT_CHANNEL_ID</name></field><field><id>ROOT_CHANNEL_ID</id><enabled>Y</enabled><name>ROOT_CHANNEL_ID</name></field></channel-log-table>
<step-log-table><connection/>
<schema/>
<table/>
<timeout_days/>
<field><id>ID_BATCH</id><enabled>Y</enabled><name>ID_BATCH</name></field><field><id>CHANNEL_ID</id><enabled>Y</enabled><name>CHANNEL_ID</name></field><field><id>LOG_DATE</id><enabled>Y</enabled><name>LOG_DATE</name></field><field><id>TRANSNAME</id><enabled>Y</enabled><name>TRANSNAME</name></field><field><id>STEPNAME</id><enabled>Y</enabled><name>STEPNAME</name></field><field><id>STEP_COPY</id><enabled>Y</enabled><name>STEP_COPY</name></field><field><id>LINES_READ</id><enabled>Y</enabled><name>LINES_READ</name></field><field><id>LINES_WRITTEN</id><enabled>Y</enabled><name>LINES_WRITTEN</name></field><field><id>LINES_UPDATED</id><enabled>Y</enabled><name>LINES_UPDATED</name></field><field><id>LINES_INPUT</id><enabled>Y</enabled><name>LINES_INPUT</name></field><field><id>LINES_OUTPUT</id><enabled>Y</enabled><name>LINES_OUTPUT</name></field><field><id>LINES_REJECTED</id><enabled>Y</enabled><name>LINES_REJECTED</name></field><field><id>ERRORS</id><enabled>Y</enabled><name>ERRORS</name></field><field><id>LOG_FIELD</id><enabled>N</enabled><name>LOG_FIELD</name></field></step-log-table>
    </log>
    <maxdate>
      <connection/>
      <table/>
      <field/>
      <offset>0.0</offset>
      <maxdiff>0.0</maxdiff>
    </maxdate>
    <size_rowset>__rowset__</size_rowset>
    <sleep_time_empty>10</sleep_time_empty>
    <sleep_time_full>10</sleep_time_full>
    <unique_connections>N</unique_connections>
    <feedback_shown>Y</feedback_shown>
    <feedback_size>500000</feedback_size>
    <using_thread_priorities>Y</using_thread_priorities>
    <shared_objects_file/>
    <capture_step_performance>Y</capture_step_performance>
    <step_performance_capturing_delay>1000</step_performance_capturing_delay>
    <step_performance_capturing_size_limit>100</step_performance_capturing_size_limit>
    <dependencies>
    </dependencies>
    <partitionschemas>
    </partitionschemas>
    <slaveservers>
    </slaveservers>
    <clusterschemas>
    </clusterschemas>
  <created_user>-</created_user>
  <created_date>2013&#47;02&#47;28 14:04:49.560</created_date>
  <modified_user>-</modified_user>
  <modified_date>2013&#47;03&#47;01 12:35:39.999</modified_date>
  </info>
  <notepads>
  </notepads>
  <connection>
    <name>__oracle_db__</name>
    <server>__oracle_host__</server>
    <type>ORACLE</type>
    <access>Native</access>
    <database>__oracle_instance__</database>
    <port>__oracle_port__</port>
    <username>__oracle_username__</username>
    <password>__oracle_password__</password>
    <servername/>
    <data_tablespace/>
    <index_tablespace/>
    <attributes>
      <attribute><code>EXTRA_OPTION_ORACLE.defaultRowPrefetch</code><attribute>10000</attribute></attribute>
      <attribute><code>EXTRA_OPTION_ORACLE.fetchSize</code><attribute>1000</attribute></attribute>
      <attribute><code>FORCE_IDENTIFIERS_TO_LOWERCASE</code><attribute>N</attribute></attribute>
      <attribute><code>FORCE_IDENTIFIERS_TO_UPPERCASE</code><attribute>N</attribute></attribute>
      <attribute><code>IS_CLUSTERED</code><attribute>N</attribute></attribute>
      <attribute><code>PORT_NUMBER</code><attribute>__oracle_port__</attribute></attribute>
      <attribute><code>QUOTE_ALL_FIELDS</code><attribute>N</attribute></attribute>
      <attribute><code>SUPPORTS_BOOLEAN_DATA_TYPE</code><attribute>N</attribute></attribute>
      <attribute><code>USE_POOLING</code><attribute>N</attribute></attribute>
    </attributes>
  </connection>
  <connection>
    <name>__postgres_db__</name>
    <server>__postgres_host__</server>
    <type>POSTGRESQL</type>
    <access>Native</access>
    <database>__postgres_database_name__</database>
    <port>__postgres_port__</port>
    <username>__postgres_username__</username>
    <password>__postgres_password__</password>
    <servername/>
    <data_tablespace/>
    <index_tablespace/>
    <attributes>
      <attribute><code>FORCE_IDENTIFIERS_TO_LOWERCASE</code><attribute>N</attribute></attribute>
      <attribute><code>FORCE_IDENTIFIERS_TO_UPPERCASE</code><attribute>N</attribute></attribute>
      <attribute><code>IS_CLUSTERED</code><attribute>N</attribute></attribute>
      <attribute><code>PORT_NUMBER</code><attribute>__postgres_port__</attribute></attribute>
      <attribute><code>QUOTE_ALL_FIELDS</code><attribute>N</attribute></attribute>
      <attribute><code>SUPPORTS_BOOLEAN_DATA_TYPE</code><attribute>Y</attribute></attribute>
      <attribute><code>USE_POOLING</code><attribute>N</attribute></attribute>
      <attribute><code>EXTRA_OPTION_POSTGRESQL.synchronous_commit</code><attribute>__sync_commit_onoff__</attribute></attribute>
    </attributes>
  </connection>
  <order>
  <hop> <from>Table input</from><to>Modified Java Script Value</to><enabled>Y</enabled> </hop>  <hop> <from>Modified Java Script Value</from><to>Table output</to><enabled>Y</enabled> </hop>

  </order>
  <step>
    <name>Table input</name>
    <type>TableInput</type>
    <description/>
    <distribute>Y</distribute>
    <copies>__select_copies__</copies>
         <partitioning>
           <method>none</method>
           <schema_name/>
           </partitioning>
    <connection>__oracle_db__</connection>
    <sql>__select_query__</sql>
    <limit>0</limit>
    <lookup/>
    <execute_each_row>N</execute_each_row>
    <variables_active>N</variables_active>
    <lazy_conversion_active>N</lazy_conversion_active>
     <cluster_schema/>
 <remotesteps>   <input>   </input>   <output>   </output> </remotesteps>    <GUI>
      <xloc>122</xloc>
      <yloc>160</yloc>
      <draw>Y</draw>
      </GUI>
    </step>

  <step>
    <name>Table output</name>
    <type>TableOutput</type>
    <description/>
    <distribute>Y</distribute>
    <copies>__insert_copies__</copies>
         <partitioning>
           <method>none</method>
           <schema_name/>
           </partitioning>
    <connection>__postgres_db__</connection>
    <schema/>
    <table>__postgres_table_name__</table>
    <commit>__commit_size__</commit>
    <truncate>__truncate__</truncate>
    <ignore_errors>Y</ignore_errors>
    <use_batch>Y</use_batch>
    <specify_fields>N</specify_fields>
    <partitioning_enabled>N</partitioning_enabled>
    <partitioning_field/>
    <partitioning_daily>N</partitioning_daily>
    <partitioning_monthly>Y</partitioning_monthly>
    <tablename_in_field>N</tablename_in_field>
    <tablename_field/>
    <tablename_in_table>Y</tablename_in_table>
    <return_keys>N</return_keys>
    <return_field/>
    <fields>
    </fields>
     <cluster_schema/>
 <remotesteps>   <input>   </input>   <output>   </output> </remotesteps>    <GUI>
      <xloc>369</xloc>
      <yloc>155</yloc>
      <draw>Y</draw>
      </GUI>
    </step>

  <step>
    <name>Modified Java Script Value</name>
    <type>ScriptValueMod</type>
    <description/>
    <distribute>Y</distribute>
    <copies>__js_copies__</copies>
         <partitioning>
           <method>none</method>
           <schema_name/>
           </partitioning>
    <compatible>N</compatible>
    <optimizationLevel>9</optimizationLevel>
    <jsScripts>      <jsScript>        <jsScript_type>0</jsScript_type>
        <jsScript_name>Script 1</jsScript_name>
        <jsScript_script>for (var i=0;i&lt;getInputRowMeta().size();i++) { 
  var valueMeta = getInputRowMeta().getValueMeta(i);
  if (valueMeta.getTypeDesc().equals(&quot;String&quot;)) {
    row[i]=replace(row[i],&quot;\\00&quot;,&apos;&apos;);
  }
} </jsScript_script>
      </jsScript>    </jsScripts>    <fields>    </fields>     <cluster_schema/>
 <remotesteps>   <input>   </input>   <output>   </output> </remotesteps>    <GUI>
      <xloc>243</xloc>
      <yloc>166</yloc>
      <draw>Y</draw>
      </GUI>
    </step>

  <step_error_handling>
  </step_error_handling>
   <slave-step-copy-partition-distribution>
</slave-step-copy-partition-distribution>
   <slave_transformation>N</slave_transformation>
</transformation>
EOF

}

# Constants for creating kettle files from the template
sub create_kettle_output
{
	my ($self, $table, $output_dir) = @_;

	my $oracle_host = 'localhost';
	if ($self->{oracle_dsn} =~ /host=([^;]+)/) {
		$oracle_host = $1;
	}
	my $oracle_port = 1521;
	if ($self->{oracle_dsn} =~ /port=(\d+)/) {
		$oracle_port = $1;
	}
	my $oracle_instance='';
	if ($self->{oracle_dsn} =~ /sid=([^;]+)/) {
		$oracle_instance = $1;
	} elsif ($self->{oracle_dsn} =~ /dbi:Oracle:([^:]+)/) {
		$oracle_instance = $1;
	}
	if ($self->{oracle_dsn} =~ /\/\/([^:]+):(\d+)\/(.*)/) {
		$oracle_host = $1;
		$oracle_port = $2;
		$oracle_instance = $3;
	} elsif ($self->{oracle_dsn} =~ /\/\/([^\/]+)\/(.*)/) {
		$oracle_host = $1;
		$oracle_instance = $2;
	}

	my $pg_host = 'localhost';
	if ($self->{pg_dsn} =~ /host=([^;]+)/) {
		$pg_host = $1;
	}
	my $pg_port = 5432;
	if ($self->{pg_dsn} =~ /port=(\d+)/) {
		$pg_port = $1;
	}
	my $pg_dbname = '';
	if ($self->{pg_dsn} =~ /dbname=([^;]+)/) {
		$pg_dbname = $1;
	}

	my $select_query = "SELECT * FROM $table";
	if ($self->{schema}) {
		$select_query = "SELECT * FROM $self->{schema}.$table";
	}
	my $select_copies = $self->{oracle_copies} || 1;
	if (($self->{oracle_copies} > 1) && $self->{defined_pk}{"\L$table\E"}) {
		if ($self->{schema}) {
			$select_query = "SELECT * FROM $self->{schema}.$table WHERE ABS(MOD(" . $self->{defined_pk}{"\L$table\E"} . ",\${Internal.Step.Unique.Count}))=\${Internal.Step.Unique.Number}";
		} else {
			$select_query = "SELECT * FROM $table WHERE ABS(MOD(" . $self->{defined_pk}{"\L$table\E"} . ",\${Internal.Step.Unique.Count}))=\${Internal.Step.Unique.Number}";
		}
	} else {
		$select_copies = 1;
	}

	my $insert_copies = $self->{jobs} || 4;
	my $js_copies = $insert_copies;
	my $rowset = $self->{data_limit} || 10000;
	my $commit_size = 500;
	my $sync_commit_onoff = 'off';
	my $truncate = 'Y';
	$truncate = 'N' if (!$self->{truncate_table});

	my $pg_table = $table;
	if ($self->{export_schema}) {
		if ($self->{pg_schema}) {
			$pg_table = "$self->{pg_schema}.$table";
		} elsif ($self->{schema}) {
			$pg_table = "$self->{schema}.$table";
		}
	}
	$table = "$self->{schema}.$table" if ($self->{schema});

	my $xml = &get_kettle_xml();
	$xml =~ s/__oracle_host__/$oracle_host/gs;
	$xml =~ s/__oracle_instance__/$oracle_instance/gs;
	$xml =~ s/__oracle_port__/$oracle_port/gs;
	$xml =~ s/__oracle_username__/$self->{oracle_user}/gs;
	$xml =~ s/__oracle_password__/$self->{oracle_pwd}/gs;
	$xml =~ s/__postgres_host__/$pg_host/gs;
	$xml =~ s/__postgres_database_name__/$pg_dbname/gs;
	$xml =~ s/__postgres_port__/$pg_port/gs;
	$xml =~ s/__postgres_username__/$self->{pg_user}/gs;
	$xml =~ s/__postgres_password__/$self->{pg_pwd}/gs;
	$xml =~ s/__select_copies__/$select_copies/gs;
	$xml =~ s/__select_query__/$select_query/gs;
	$xml =~ s/__insert_copies__/$insert_copies/gs;
	$xml =~ s/__js_copies__/$js_copies/gs;
	$xml =~ s/__truncate__/$truncate/gs;
	$xml =~ s/__transformation_name__/$table/gs;
	$xml =~ s/__postgres_table_name__/$pg_table/gs;
	$xml =~ s/__rowset__/$rowset/gs;
	$xml =~ s/__commit_size__/$commit_size/gs;
	$xml =~ s/__sync_commit_onoff__/$sync_commit_onoff/gs;

	my $fh = new IO::File;
	$fh->open(">$output_dir$table.ktr") or $self->logit("FATAL: can't write to $output_dir$table.ktr, $!\n", 0, 1);
	$fh->print($xml);
	$fh->close();

	return "JAVAMAXMEM=4096 ./pan.sh -file \$KETTLE_TEMPLATE_PATH/$output_dir$table.ktr -level Detailed\n";
}

1;

__END__


=head1 AUTHOR

Gilles Darold <gilles _AT_ darold _DOT_ net>


=head1 COPYRIGHT

Copyright (c) 2000-2014 Gilles Darold - All rights reserved.

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program. If not, see < http://www.gnu.org/licenses/ >.


=head1 SEE ALSO

L<DBD::Oracle>, L<DBD::Pg>


=cut

