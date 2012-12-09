package Ora2Pg;
#------------------------------------------------------------------------------
# Project  : Oracle to PostgreSQL database schema converter
# Name     : Ora2Pg.pm
# Language : Perl
# Authors  : Gilles Darold, gilles _AT_ darold _DOT_ net
# Copyright: Copyright (c) 2000-2012 : Gilles Darold - All rights reserved -
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
use POSIX qw(locale_h);
use IO::File;
use Config;

#set locale to LC_NUMERIC C
setlocale(LC_NUMERIC,"C");

$VERSION = '9.3';
$PSQL = $ENV{PLSQL} || 'psql';

$| = 1;

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
	#'SDO_GEOMETRY' => 'geometry(Geometry,27572)',
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
	'disabled'=> 't',
);

our @GRANTS = (
	'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
	'REFERENCES', 'TRIGGER', 'USAGE', 'CREATE', 'CONNECT',
	'TEMPORARY', 'TEMP', 'USAGE', 'ALL', 'ALL PRIVILEGES',
	'EXECUTE'
);

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


=head2 export_data FILENAME

OBSOLETE: you must use export_schema instead. Still here
for backward compatibility. It simply callback export_schema().

=cut

sub export_data
{
	my ($self, $outfile) = @_;

	$self->export_schema($outfile);
}


=head2 export_schema FILENAME

Print SQL data output to a file name or
to STDOUT if no file name is specified.

=cut

sub export_schema
{
	my ($self, $outfile) = @_;

	# Init with configuration OUTPUT filename
	$outfile ||= $self->{output};
	if ($self->{output_dir} && $outfile) {
		$outfile = $self->{output_dir} . "/" . $outfile;
	}

	if ($outfile) {
		if ($outfile eq $self->{input_file}) {
			$self->logit("FATAL: input file is the same as output file: $outfile, can not overwrite it.\n",0,1);
		}
		# Send output to the specified file
		if ($outfile =~ /\.gz$/) {
			use Compress::Zlib;
			$self->{compress} = 'Zlib';
			$self->{zlib_hdl} = gzopen($outfile, "wb") or $self->logit("FATAL: Can't create deflation file $outfile\n", 0, 1);
		} elsif ($outfile =~ /\.bz2$/) {
			$self->logit("FATAL: can't run bzip2\n",0,1) if (!-x $self->{bzip2});
			$self->{fhout} = new IO::File;
			$self->{fhout}->open("|$self->{bzip2} --stdout >$outfile") or $self->logit("FATAL: Can't open pipe to $self->{bzip2} --stdout >$outfile: $!\n", 0, 1);
		} else {
			$self->{fhout} = new IO::File;
			$self->{fhout}->open(">$outfile") or $self->logit("FATAL: Can't open $outfile: $!\n", 0, 1);
			binmode($self->{fhout},$self->{'binmode'});
		}
		foreach my $t (@{$self->{export_type}}) {
			$self->{type} = $t;
			# Return data as string
			$self->_get_sql_data();
		}
		if ($outfile =~ /\.gz$/) {
			$self->{zlib_hdl}->gzclose();
		} else {
			$self->{fhout}->close();
		}

	} else {

		foreach my $t (@{$self->{export_type}}) {
			$self->{type} = $t;
			# Return data as string
			$self->_get_sql_data();
		}

	}

}


=head2 export_file FILENAME

Open a file handle to a given filename.

=cut

sub export_file
{
	my ($self, $outfile, $noprefix) = @_;

	my $filehdl = undef;

	if ($outfile) {
		if ($self->{output_dir} && !$noprefix) {
			$outfile = $self->{output_dir} . '/' . $outfile;
		}
		# If user request data compression
		if ($self->{output} =~ /\.gz$/) {
			use Compress::Zlib;
			$self->{compress} = 'Zlib';
			$filehdl = gzopen("$outfile.gz", "wb") or $self->logit("FATAL: Can't create deflation file $outfile.gz\n",0,1);
		} elsif ($self->{output} =~ /\.bz2$/) {
			$self->logit("Error: can't run bzip2\n",0,1) if (!-x $self->{bzip2});
			$filehdl = new IO::File;
			$filehdl->open("|$self->{bzip2} --stdout >$outfile.bz2") or $self->logit("FATAL: Can't open pipe to $self->{bzip2} --stdout >$outfile.bz2: $!\n", 0,1);
		} else {
			$filehdl = new IO::File;
			$filehdl->open(">$outfile") or $self->logit("FATAL: Can't open $outfile: $!\n", 0, 1);
			binmode($filehdl, $self->{'binmode'});
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
	@{$self->{sysusers}} = ('SYSTEM','SYS','DBSNMP','OUTLN','PERFSTAT','CTXSYS','XDB','WMSYS','SYSMAN','SQLTXPLAIN','MDSYS','EXFSYS','ORDSYS','DMSYS','OLAPSYS','FLOWS_020100','FLOWS_FILES','TSMSYS');
	$self->{ora_reserved_words} = (); 
	# Init PostgreSQL DB handle
	$self->{dbhdest} = undef;
	$self->{idxcomment} = 0;
	$self->{standard_conforming_strings} = 1;
	$self->{allow_code_break} = 1;
	$self->{create_schema} = 1;
	$self->{external_table} = ();
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

	# Default boolean values
	foreach my $k (keys %BOOLEAN_MAP) {
		$self->{ora_boolean_values}{lc($k)} = $BOOLEAN_MAP{$k};
	}
	# additional boolean values given from config file
	foreach my $k (keys %{$self->{boolean_values}}) {
		$self->{ora_boolean_values}{lc($k)} = $self->{boolean_values}{$k};
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

	# Free some memory
	%options = ();
	%AConfig = ();

	# If iThreads and THREAD_COUNT are enabled
	$self->{queue_todo_bytea} = undef;
	$self->{queue_done_bytea} = undef;
	$self->{bytea_worker_threads} = ();
	if ($Config{useithreads} && $self->{thread_count}) {
		require threads;
		require threads::shared;
		require Thread::Queue;
		# Create the worker threads. They all wait on us to feed them
		# records. We get the records back formatted by the CPU-hungry
		# format_data.
		$self->{queue_todo_bytea} = Thread::Queue->new();
		$self->{queue_done_bytea} = Thread::Queue->new();
		for (my $i=0;$i<$self->{thread_count};$i++) {
			my $thread=threads->create('worker_format_data',$self,$i);
			push(@{$self->{bytea_worker_threads}},($thread));
		}
	} else {
		$self->{thread_count} = 0;
	}
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
	$self->{global_where} = '';
	$self->{prefix} = 'DBA';
	if ($self->{user_grants}) {
		$self->{prefix} = 'ALL';
	}
	$self->{bzip2} ||= '/usr/bin/bzip2';
	$self->{default_numeric} ||= 'bigint';
	$self->{type_of_type} = ();
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
	# Backward compatibility with LongTrunkOk with typo
	if ($self->{longtrunkok} && not defined $self->{longtruncok}) {
		$self->{longtruncok} = $self->{longtrunkok};
	}
	$self->{longtruncok} = 0 if (not defined $self->{longtruncok});
	$self->{longreadlen} ||= (1024*1024);
	#$self->{ora_piece_size} ||= $self->{longreadlen};
	#if ($self->{ora_piece_size} > $self->{longreadlen}) {
	#	$self->{longreadlen} = $self->{ora_piece_size};
	#}
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
	if ($self->{plsql_pgsql}) {
		use Ora2Pg::PLSQL;
	}

	$self->{fhout} = undef;
	$self->{compress} = '';
	$self->{zlib_hdl} = undef;
	$self->{pkgcost} = 0;
	$self->{total_pkgcost} = 0;

	if (!$self->{input_file}) {
		# Connect the database
		$self->logit("Trying to connect to database: $self->{oracle_dsn}\n", 1);
		$self->{dbh} = DBI->connect($self->{oracle_dsn}, $self->{oracle_user}, $self->{oracle_pwd}, { LongReadLen=>$self->{longreadlen}, LongTruncOk=>$self->{longtruncok} });

		# Fix a problem when exporting type LONG and LOB
		$self->{dbh}->{'LongReadLen'} = $self->{longreadlen};
		$self->{dbh}->{'LongTruncOk'} = $self->{longtruncok};

		# Check for connection failure
		if (!$self->{dbh}) {
			$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
		}

		# Use consistent reads for concurrent dumping...
		$self->{dbh}->begin_work || $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		if ($self->{debug}) {
			$self->logit("Isolation level: $self->{transaction}\n", 1);
		}
		my $sth = $self->{dbh}->prepare($self->{transaction}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->finish;
		undef $sth;

		# Auto detect character set
		if ($self->{debug}) {
			$self->logit("Auto detecting Oracle character set and the corresponding PostgreSQL client encoding to use.\n", 1);
		}
		my $encoding = $self->_get_encoding();
		if (!$self->{nls_lang}) {
			$self->{nls_lang} = $encoding;
			if ($self->{debug}) {
				$self->logit("\tUsing Oracle character set: $self->{nls_lang}.\n", 1);
			}
		} else {
			$ENV{NLS_LANG} = $self->{nls_lang};
			if ($self->{debug}) {
				$self->logit("\tUsing the character set given in NLS_LANG configuration directive ($self->{nls_lang}).\n", 1);
			}
		}
		if (!$self->{client_encoding}) {
			if ($self->{'binmode'} =~ /utf8/i) {
				$self->{client_encoding} = 'UTF8';
				if ($self->{debug}) {
					$self->logit("\tUsing PostgreSQL client encoding forced to UTF8 as BINMODE configuration directive has been set to $self->{binmode}.\n", 1);
				}
			} else {
				$self->{client_encoding} = &auto_set_encoding($encoding);
				if ($self->{debug}) {
					$self->logit("\tUsing PostgreSQL client encoding: $self->{client_encoding}.\n", 1);
				}
			}
		} else {
			if ($self->{debug}) {
				$self->logit("\tUsing PostgreSQL client encoding given in CLIENT_ENCODING configuration directive ($self->{client_encoding}).\n", 1);
			}
		}
		if ($self->{debug}) {
			$self->logit("Force Oracle to compile schema before code extraction\n", 1);
		}
		$self->_compile_schema(uc($self->{compile_schema})) if ($self->{compile_schema});
	} else {
		$self->{plsql_pgsql} = 1;
		if (grep(/^$self->{type}$/, 'QUERY', 'FUNCTION','PROCEDURE','PACKAGE')) {
			$self->export_schema();
		} else {
			$self->logit("FATAL: bad export type using input file option\n", 0, 1);
		}
		return;
	}

	# Retreive all table informations
        foreach my $t (@{$self->{export_type}}) {
                $self->{type} = $t;
		if (($self->{type} eq 'TABLE') || ($self->{type} eq 'FDW') || ($self->{type} eq 'INSERT') || ($self->{type} eq 'COPY')) {
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
			warn "type option must be TABLE, VIEW, GRANT, SEQUENCE, TRIGGER, PACKAGE, FUNCTION, PROCEDURE, PARTITION, TYPE, INSERT, COPY, TABLESPACE, SHOW_REPORT, SHOW_VERSION, SHOW_SCHEMA, SHOW_TABLE, SHOW_COLUMN, SHOW_ENCODING, FDW, MVIEW, QUERY\n";
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
	# Disconnect from the database
	$self->{dbh}->disconnect() if ($self->{dbh});

	if ( ($self->{type} eq 'INSERT') || ($self->{type} eq 'COPY') ) {
		$self->_send_to_pgdb() if ($self->{pg_dsn} && !$self->{dbhdest});
	} elsif ($self->{dbhdest}) {
		$self->logit("WARNING: can't use direct import into PostgreSQL with this type of export.\n");
		$self->logit("Only INSERT or COPY export type can be use with direct import, file output will be used.\n");
		sleep(2);
	}
}


# We provide a DESTROY method so that the autoloader doesn't
# bother trying to find it. We also close the DB connexion
sub DESTROY
{
	my $self = shift;

	foreach my $thread (@{$self->{bytea_worker_threads}}) {
		$self->{queue_todo_bytea}->enqueue(undef);
	}
	foreach my $thread (@{$self->{bytea_worker_threads}}) {
		$thread->join();
	}

}


=head2 _send_to_pgdb DEST_DATASRC DEST_USER DEST_PASSWD

Open a DB handle to a PostgreSQL database

=cut

sub _send_to_pgdb
{
	my ($self, $destsrc, $destuser, $destpasswd) = @_;

	use DBD::Pg qw(:pg_types);

	# Init with configuration options if no parameters
	$destsrc ||= $self->{pg_dsn};
	$destuser ||= $self->{pg_user};
	$destpasswd ||= $self->{pg_pwd};

        # Then connect the destination database
        $self->{dbhdest} = DBI->connect($destsrc, $destuser, $destpasswd);

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
        if (!$self->{dbhdest}) {
		$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
	}

}

# Backward Compatibility
sub send_to_pgdb
{
	&_send_to_pgdb(@_);

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

	my $sth = $self->_table_info()  or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my @tables_infos = $sth->fetchall_arrayref();
	$sth->finish();

	my @done = ();
	my $id = 0;
	foreach my $table (@tables_infos) {
		# Set the table information for each class found
		my $i = 1;
		foreach my $t (@$table) {

			# forget or not this object if it is in the exclude or allow lists.
			if ($self->{tables}{$t->[2]}{type} ne 'view') {
				next if ($self->skip_this_object('TABLE', $t->[2]));
			}
			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($i, $#{$table} + 1, 25, '=', 'tables', "scanning table $t->[2]" );
			}

			if (grep(/^$t->[2]$/, @done)) {
				$self->logit("Duplicate entry found: $t->[0] - $t->[1] - $t->[2]\n", 1);
			} else {
				push(@done, $t->[2]);
			} 
			$self->logit("[$i] Scanning table $t->[2] (@$t rows)...\n", 1);
			
			# Check of uniqueness of the table
			if (exists $self->{tables}{$t->[2]}{field_name}) {
				$self->logit("Warning duplicate table $t->[2], maybe a SYNONYME ? Skipped.\n", 1);
				next;
			}
			# Try to respect order specified in the TABLES limited extraction array
			$self->{tables}{$t->[2]}{internal_id} = 0;
			if ($#{$self->{limited}} >= 0) {
				for (my $j = 0; $j <= $#{$self->{limited}}; $j++) {
					if (uc($self->{limited}->[$j]) eq uc($t->[2])) {
						$self->{tables}{$t->[2]}{internal_id} = $j;
						last;
					}
				}
			}
			# usually OWNER,TYPE,COMMENT,NUMROW
			$self->{tables}{$t->[2]}{table_info} = [($t->[1],$t->[3],$t->[4],$t->[5])];

			# Set the fields information
			my $query = "SELECT * FROM \"$t->[1]\".\"$t->[2]\" WHERE 1=0";
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
			$self->{tables}{$t->[2]}{type} = 'table';
			$self->{tables}{$t->[2]}{field_name} = $sth->{NAME};
			$self->{tables}{$t->[2]}{field_type} = $sth->{TYPE};
			# Retrieve column's details
			@{$self->{tables}{$t->[2]}{column_info}} = $self->_column_info($t->[2],$t->[1]) if (!$nodetail);
			# Retrieve comment of each columns
			@{$self->{tables}{$t->[2]}{column_comments}} = $self->_column_comments($t->[2],$t->[1]) if (!$nodetail);
			%{$self->{tables}{$t->[2]}{unique_key}} = $self->_unique_key($t->[2],$t->[1]);
			# We don't check for skip_ukeys/skip_pkeys here; this is taken care of inside _unique_key
			($self->{tables}{$t->[2]}{foreign_link}, $self->{tables}{$t->[2]}{foreign_key}) = $self->_foreign_key($t->[2],$t->[1]) if (!$self->{skip_fkeys});
			# Same for check cosntraints
			%{$self->{tables}{$t->[2]}{check_constraint}} = $self->_check_constraint($t->[2],$t->[1]) if (!$self->{skip_checks});
			# Retrieve indexes informations
			($self->{tables}{$t->[2]}{uniqueness}, $self->{tables}{$t->[2]}{indexes}, $self->{tables}{$t->[2]}{idx_type}) = $self->_get_indexes($t->[2],$t->[1]) if (!$self->{skip_indices} && !$self->{skip_indexes});
			$i++;
		}
		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($i - 1, $#{$table} + 1, 25, '=', 'tables', 'end of scan.'), "\n";
		}
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
			@{$self->{tables}{$view}{column_info}} = $self->_column_info($view);
		}
	}

	# Look at external tables
	%{$self->{external_table}} = $self->_get_external_tables();

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


=head2 _get_sql_data

Returns a string containing the entire PostgreSQL compatible SQL Schema
definition.

=cut

sub _get_sql_data
{
	my ($self, $outfile) = @_;

	my $sql_header = "-- Generated by Ora2Pg, the Oracle database Schema converter, version $VERSION\n";
	$sql_header .= "-- Copyright 2000-2012 Gilles DAROLD. All rights reserved.\n";
	$sql_header .= "-- DATASOURCE: $self->{oracle_dsn}\n\n";
	if ($self->{client_encoding}) {
		$sql_header .= "SET client_encoding TO '\U$self->{client_encoding}\E';\n\n";
	}
	if ($self->{export_schema} && ($self->{type} ne 'TABLE')) {
		if ($self->{pg_schema}) {
			if (!$self->{preserve_case}) {
				$sql_header .= "SET search_path = \L$self->{pg_schema}\E;\n\n";
			} else {
				$sql_header .= "SET search_path = \"$self->{pg_schema}\";\n\n";
			}
		} else {
			if (!$self->{preserve_case}) {
				$sql_header .= "SET search_path = \L$self->{schema}\E, pg_catalog;\n\n" if ($self->{schema});
			} else {
				$sql_header .= "SET search_path = \"$self->{schema}\", pg_catalog;\n\n" if ($self->{schema});
			}
		}
	}
	$sql_header .= "\\set ON_ERROR_STOP ON\n\n";

	my $sql_output = "";

	# Process view only
	if ($self->{type} eq 'VIEW') {
		$self->logit("Add views definition...\n", 1);
		my $nothing = 0;
		$self->dump($sql_header) if ($self->{file_per_table} && !$self->{dbhdest});
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		foreach my $view (sort { $a cmp $b } keys %{$self->{views}}) {
			$self->logit("\tAdding view $view...\n", 1);
			my $fhdl = undef;
			if ($self->{file_per_table} && !$self->{dbhdest}) {
				$self->dump("\\i $dirprefix${view}_$self->{output}\n");
				$self->logit("Dumping to one file per view : ${view}_$self->{output}\n", 1);
				$fhdl = $self->export_file("${view}_$self->{output}");
			}
			$self->{views}{$view}{text} =~ s/\s*\bWITH\b\s+.*$//s;
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
					if ($self->{views}{$view}{text} =~ /SELECT[^\s\t]*(.*?)[^\s\t]*FROM/is) {
						my $clause = $1;
						$clause =~ s/"([^"]+)"/"\L$1\E"/gs;
						$self->{views}{$view}{text} =~ s/SELECT[^\s\t]*(.*?)[^\s\t]*FROM/SELECT $clause FROM/is;
					}
				}
				$sql_output .= ") AS " . $self->{views}{$view}{text} . ";\n\n";
			}
			if ($self->{file_per_table} && !$self->{dbhdest}) {
				$self->dump($sql_header . $sql_output, $fhdl);
				$self->close_export_file($fhdl);
				$sql_output = '';
			}
			$nothing++;
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
		$self->dump($sql_header) if ($self->{file_per_table} && !$self->{dbhdest});
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		if ($self->{plsql_pgsql}) {
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
    EXECUTE 'SELECT * FROM materialized_views WHERE mview_name = ' || quote_literal(mview) || ' INTO entry;
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
			$self->dump($sqlout) if (!$self->{dbhdest});
		}
		foreach my $view (sort { $a cmp $b } keys %{$self->{materialized_views}}) {
			$self->logit("\tAdding materialized view $view...\n", 1);
			my $fhdl = undef;
			if ($self->{file_per_table} && !$self->{dbhdest}) {
				$self->dump("\\i $dirprefix${view}_$self->{output}\n");
				$self->logit("Dumping to one file per materialized view : ${view}_$self->{output}\n", 1);
				$fhdl = $self->export_file("${view}_$self->{output}");
			}
			if (!$self->{plsql_pgsql}) {
				$sql_output .= "CREATE MATERIALIZED VIEW $view\n";
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
				$sql_output .= "CREATE VIEW \L$view\E_mview AS\n";
				$sql_output .= $self->{materialized_views}{$view}{text};
				$sql_output .= ";\n\n";
				$sql_output .= "SELECT create_materialized_view('\L$view\E','\L$view\E_mview', change with the name of the colum to used for the index);\n\n\n";
			}

			if ($self->{file_per_table} && !$self->{dbhdest}) {
				$self->dump($sql_header . $sql_output, $fhdl);
				$self->close_export_file($fhdl);
				$sql_output = '';
			}
			$nothing++;
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
		foreach my $seq (sort { $a->[0] cmp $b->[0] } @{$self->{sequences}}) {
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
				$sql_output .= "ALTER SEQUENCE \"\L$seq->[0]\E\" OWNER TO \L$owner\E;\n";
			}

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
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		my $nothing = 0;
		foreach my $trig (sort {$a->[0] cmp $b->[0]} @{$self->{triggers}}) {
			my $fhdl = undef;
			if ($self->{file_per_function} && !$self->{dbhdest}) {
				$self->dump("\\i $dirprefix$trig->[0]_$self->{output}\n");
				$self->logit("Dumping to one file per trigger : $trig->[0]_$self->{output}\n", 1);
				$fhdl = $self->export_file("$trig->[0]_$self->{output}");
			}
			$trig->[1] =~ s/\s*EACH ROW//is;
			chop($trig->[4]);
			chomp($trig->[4]);
			$self->logit("\tDumping trigger $trig->[0] defined on table $trig->[3]...\n", 1);
			# Check if it's like a pg rule
			if (!$self->{pg_supports_insteadof} && $trig->[1] =~ /INSTEAD OF/) {
				if (!$self->{preserve_case}) {
					$sql_output .= "CREATE OR REPLACE RULE \L$trig->[0]\E AS\n\tON \L$trig->[3]\E\n\tDO INSTEAD\n(\n\t$trig->[4]\n);\n\n";
				} else {
					$sql_output .= "CREATE OR REPLACE RULE \L$trig->[0]\E AS\n\tON \"$trig->[3]\"\n\tDO INSTEAD\n(\n\t$trig->[4]\n);\n\n";
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
					if (!$self->{preserve_case}) {
						$sql_output .= "$trig->[1] $trig->[2] ON \L$trig->[3]\E ";
					} else {
						$sql_output .= "$trig->[1] $trig->[2] ON \"$trig->[3]\" ";
					}
					if ($trig->[1] =~ s/ STATEMENT//) {
						$sql_output .= "FOR EACH STATEMENT\n";
					} else {
						$sql_output .= "FOR EACH ROW\n";
					}
					$sql_output .= "\tEXECUTE PROCEDURE trigger_fct_\L$trig->[0]\E();\n\n";
				}
			}
			if ($self->{file_per_function} && !$self->{dbhdest}) {
				$self->dump($sql_header . $sql_output, $fhdl);
				$self->close_export_file($fhdl);
				$sql_output = '';
			}
			$nothing++;
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

			if ($self->{estimate_cost} && $self->{input_file}) {
				$total_size += length($self->{queries}{$q});
				my $cost = Ora2Pg::PLSQL::estimate_cost($self->{queries}{$q});
				$cost += $Ora2Pg::PLSQL::OBJECT_SCORE{'QUERY'};
				$cost_value += $cost;
				$self->logit("Query $q estimated cost: $cost\n", 0);
				next;
			}
			$self->logit("\tDumping query $q...\n", 1);
			my $fhdl = undef;
			my %comments = $self->_remove_comments($self->{queries}{$q});
			if ($self->{plsql_pgsql}) {
				$sql_output .= Ora2Pg::PLSQL::plsql_to_plpgsql($self->{queries}{$q}, $self->{allow_code_break},$self->{null_equal_empty}, $self->{type});
			} else {
				$sql_output .= $self->{queries}{$q};
			}
			$sql_output .= "\n\n";
			$self->_restore_comments(\$sql_output, \%comments);
			$nothing++;
		}
		if ($self->{estimate_cost} && $self->{input_file}) {
			$self->logit("Total number of queries: " . (scalar keys %{$self->{queries}}) . ".\n", 0);
			$self->logit("Total size of queries code: $total_size bytes.\n", 0);
			$self->logit("Total estimated cost: $cost_value units, " . $self->_get_human_cost($cost_value) . ".\n", 0);
			$self->{queries} = ();
			return;
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
		foreach my $fct (sort keys %{$self->{functions}}) {

			# forget or not this object if it is in the exclude or allow lists.
			next if ($self->skip_this_object('FUNCTION', $fct));
			if ($self->{estimate_cost} && $self->{input_file}) {
				$total_size += length($self->{functions}->{$fct});
				my $cost = Ora2Pg::PLSQL::estimate_cost($self->{functions}->{$fct});
				$cost += $Ora2Pg::PLSQL::OBJECT_SCORE{'FUNCTION'};
				$cost_value += $cost;
				$self->logit("Function $fct estimated cost: $cost\n", 0);
				next;
			}
			$self->logit("\tDumping function $fct...\n", 1);
			my $fhdl = undef;
			if ($self->{file_per_function} && !$self->{dbhdest}) {
				$self->dump("\\i $dirprefix${fct}_$self->{output}\n");
				$self->logit("Dumping to one file per function : ${fct}_$self->{output}\n", 1);
				$fhdl = $self->export_file("${fct}_$self->{output}");
			}
			$self->{idxcomment} = 0;
			my %comments = $self->_remove_comments(\$self->{functions}{$fct});
			if ($self->{plsql_pgsql}) {
				$sql_output .= $self->_convert_function($self->{functions}{$fct}) . "\n\n";
			} else {
				$sql_output .= $self->{functions}{$fct} . "\n\n";
			}
			$self->_restore_comments(\$sql_output, \%comments);
			if ($self->{file_per_function} && !$self->{dbhdest}) {
				$self->dump($sql_header . $sql_output, $fhdl);
				$self->close_export_file($fhdl);
				$sql_output = '';
			}
			$nothing++;
		}
		if ($self->{estimate_cost} && $self->{input_file}) {
			$self->logit("Total number of functions: " . (scalar keys %{$self->{functions}}) . ".\n", 0);
			$self->logit("Total size of function code: $total_size bytes.\n", 0);
			$self->logit("Total estimated cost: $cost_value units, " . $self->_get_human_cost($cost_value) . ".\n", 0);
			$self->{functions} = ();
			return;
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
		foreach my $fct (sort keys %{$self->{procedures}}) {

			# forget or not this object if it is in the exclude or allow lists.
			next if ($self->skip_this_object('PROCEDURE', $fct));
			if ($self->{estimate_cost} && $self->{input_file}) {
				$total_size += length($self->{procedures}->{$fct});
				my $cost = Ora2Pg::PLSQL::estimate_cost($self->{procedures}->{$fct});
				$cost += $Ora2Pg::PLSQL::OBJECT_SCORE{'PROCEDURE'};
				$cost_value += $cost;
				$self->logit("Function $fct estimated cost: $cost\n", 0);
				next;
			}

			$self->logit("\tDumping procedure $fct...\n", 1);
			my $fhdl = undef;
			if ($self->{file_per_function} && !$self->{dbhdest}) {
				$self->dump("\\i $dirprefix${fct}_$self->{output}\n");
				$self->logit("Dumping to one file per procedure : ${fct}_$self->{output}\n", 1);
				$fhdl = $self->export_file("${fct}_$self->{output}");
			}
			$self->{idxcomment} = 0;
			my %comments = $self->_remove_comments(\$self->{procedures}{$fct});
			if ($self->{plsql_pgsql}) {
				$sql_output .= $self->_convert_function($self->{procedures}{$fct}) . "\n\n";
			} else {
				$sql_output .= $self->{procedures}{$fct} . "\n\n";
			}
			$self->_restore_comments(\$sql_output, \%comments);
			if ($self->{file_per_function} && !$self->{dbhdest}) {
				$self->dump($sql_header . $sql_output, $fhdl);
				$self->close_export_file($fhdl);
				$sql_output = '';
			}
			$nothing++;
		}
		if ($self->{estimate_cost} && $self->{input_file}) {
			$self->logit("Total number of functions: " . (scalar keys %{$self->{procedures}}) . ".\n", 0);
			$self->logit("Total size of function code: $total_size bytes.\n", 0);
			$self->logit("Total estimated cost: $cost_value units, " . $self->_get_human_cost($cost_value) . ".\n", 0);
			$self->{procedures} = ();
			return;
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
		my $number_fct = 0;
		foreach my $pkg (sort keys %{$self->{packages}}) {
			next if (!$self->{packages}{$pkg});
			my $pkgbody = '';
			if (!$self->{plsql_pgsql}) {
				$self->logit("Dumping package $pkg...\n", 1);
				if ($self->{plsql_pgsql} && $self->{file_per_function} && !$self->{dbhdest}) {
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
				if ($self->{estimate_cost} && $self->{input_file}) {
					$total_size += length($self->{packages}->{$pkg});
					foreach my $txt (@codes) {
						my %infos = $self->_lookup_package("CREATE OR REPLACE PACKAGE BODY$txt");
						foreach my $f (sort keys %infos) {
							next if (!$f);
							my $cost = Ora2Pg::PLSQL::estimate_cost($infos{$f});
							$cost += $Ora2Pg::PLSQL::OBJECT_SCORE{'FUNCTION'};
							$self->logit("Function $f estimated cost: $cost\n", 0);
							$cost_value += $cost;
							$number_fct++;
						}
						$cost_value += $Ora2Pg::PLSQL::OBJECT_SCORE{'PACKAGE BODY'};
					}
				} else {
					foreach my $txt (@codes) {
						$pkgbody .= $self->_convert_package("CREATE OR REPLACE PACKAGE BODY$txt");
						$pkgbody =~ s/[\r\n]*END;[\t\s\r\n]*$//is;
						$pkgbody =~ s/([\r\n]*;)[\t\s\r\n]*$/$1/is;
					}
				}
			}
			if ($self->{estimate_cost} && $self->{input_file}) {
				$self->logit("Total size of package code: $total_size bytes.\n", 0);
				$self->logit("Total number of functions found inside those packages: $number_fct.\n", 0);
				$self->logit("Total estimated cost: $cost_value units, " . $self->_get_human_cost($cost_value) . ".\n", 0);
				$self->{packages} = ();
				return;
			}
			if ($pkgbody && ($pkgbody =~ /[a-z]/is)) {
				$sql_output .= "-- Oracle package '$pkg' declaration, please edit to match PostgreSQL syntax.\n";
				$sql_output .= $pkgbody . "\n";
				$sql_output .= "-- End of Oracle package '$pkg' declaration\n\n";
				$nothing++;
			}
		}
		if (!$nothing) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		} elsif ($self->{estimate_cost}) {
			$sql_output .= "\n-- Porting cost of all packages: $self->{total_pkgcost}\n";
		}
		$self->dump($sql_output);
		$self->{packages} = ();
		return;
	}

	# Process types only
	if ($self->{type} eq 'TYPE') {
		$self->logit("Add custom types definition...\n", 1);
		foreach my $tpe (sort {length($a->{name}) <=> length($b->{name}) } @{$self->{types}}) {
			$self->logit("Dumping type $tpe->{name}...\n", 1);
			my $typ = $tpe->{code};
			if ($self->{plsql_pgsql}) {
				$typ = $self->_convert_type($tpe->{code});
			}
			if ($typ) {
				$sql_output .= "-- Oracle type '$tpe->{name}' declaration, please edit to match PostgreSQL syntax.\n";
				$sql_output .= $typ . "\n";
				$sql_output .= "-- End of Oracle type '$tpe->{name}' declaration\n\n";
			}
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
						next if ($self->{file_per_index} && !$self->{dbhdest} && ($tb_type eq 'INDEX'));
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

		
		if ($self->{file_per_index} && !$self->{dbhdest}) {
			my $fhdl = undef;
			$self->logit("Dumping tablespace alter indexes to one separate file : TBSP_INDEXES_$self->{output}\n", 1);
			$fhdl = $self->export_file("TBSP_INDEXES_$self->{output}");
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

	# Extract data only
	if (($self->{type} eq 'INSERT') || ($self->{type} eq 'COPY')) {

		# Connect the database
		$self->{dbh} = DBI->connect($self->{oracle_dsn}, $self->{oracle_user}, $self->{oracle_pwd}, { LongReadLen=>$self->{longreadlen}, LongTruncOk=>$self->{longtruncok} });

		# Fix a problem when exporting type LONG and LOB
		$self->{dbh}->{'LongReadLen'} = $self->{longreadlen};
		$self->{dbh}->{'LongTruncOk'} = $self->{longtruncok};

		# Check for connection failure
		if (!$self->{dbh}) {
			$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
		}

		if (!$self->{dbhdest}) {
			$self->dump($sql_header);
		}

		if ($self->{dbhdest} && $self->{export_schema} &&  $self->{schema}) {
			my $search_path = "SET search_path = \L$self->{schema}\E, pg_catalog";
			if ($self->{preserve_case}) {
				$search_path = "SET search_path = \"$self->{schema}\", pg_catalog";
			}
			if ($self->{pg_schema}) {
				$search_path = "SET search_path = \L$self->{pg_schema}\E";
				if ($self->{preserve_case}) {
					$search_path = "SET search_path = \"$self->{schema}\"";
				}
			}
			my $s = $self->{dbhdest}->do($search_path) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
		}
		# Remove external table from data export
		foreach my $table (keys %{$self->{tables}}) {
			if ( grep(/^$table$/i, keys %{$self->{external_table}}) ) {
				delete $self->{tables}{$table};
			}
		}
		# Set total number of rows
		my $global_rows = 0;
		foreach my $table (keys %{$self->{tables}}) {
			$global_rows += $self->{tables}{$table}{table_info}[3];
		}

		if ($self->{drop_indexes} || $self->{drop_fkey}) {
			if (!$self->{dbhdest}) {
				$self->dump("\\set ON_ERROR_STOP OFF\n");
			}
		}
		my @ordered_tables = sort { $a cmp $b } keys %{$self->{tables}};
		# Ok ordering is impossible
		if ($self->{defer_fkey}) {
			if ($self->{dbhdest}) {
				my $s = $self->{dbhdest}->do("BEGIN;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				$self->{dbhdest}->do("SET CONSTRAINTS ALL DEFERRED;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
			} else {
				$self->dump("BEGIN;\n", $fhdl);
				$self->dump("SET CONSTRAINTS ALL DEFERRED;\n\n");
			}
		}
		if ($self->{drop_fkey}) {
			my $drop_all = '';
			# First of all we drop all foreign keys
			foreach my $table (sort { $self->{tables}{$a}{internal_id} <=> $self->{tables}{$b}{internal_id} } keys %{$self->{tables}}) {
				$self->logit("Dropping table $table foreign keys...\n", 1);
				$drop_all .= $self->_drop_foreign_keys($table, @{$self->{tables}{$table}{foreign_key}});
			}
			if ($drop_all) {
				$self->dump($drop_all);
			}
			$drop_all = '';
		}
		if (!$self->{drop_fkey} && !$self->{defer_fkey}) {
			$self->logit("WARNING: Please consider using DEFER_FKEY or DROP_FKEY configuration directives if foreign key have already been imported.\n", 0);
		}
		if ($self->{drop_indexes}) {
			my $drop_all = '';
			# First of all we drop all indexes
			foreach my $table (sort { $self->{tables}{$a}{internal_id} <=> $self->{tables}{$b}{internal_id} } keys %{$self->{tables}}) {
				$self->logit("Dropping table $table indexes...\n", 1);
				$drop_all .= $self->_drop_indexes($table, %{$self->{tables}{$table}{indexes}}) . "\n";
			}
			if ($drop_all) {
				$self->dump($drop_all);
			}
			$drop_all = '';
		}
		if ($self->{drop_indexes} || $self->{drop_fkey}) {
			if (!$self->{dbhdest}) {
				$self->dump("\\set ON_ERROR_STOP ON\n");
			}
		}
		# Force datetime format
		$self->_datetime_format();

		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		my $global_count = 0;
		my $start_time = time();
		foreach my $table (@ordered_tables) {
			next if ($self->skip_this_object('TABLE', $table));
			my $fhdl = undef;
			if ($self->{file_per_table} && !$self->{dbhdest}) {
				# Do not dump data again if the file already exists
				if (-e "$dirprefix${table}_$self->{output}") {
					$self->logit("WARNING: Skipping dumping data from $table, file already exists ${table}_$self->{output}\n", 0);
					next;
				}
				$self->dump("\\i $dirprefix${table}_$self->{output}\n");
				$self->logit("Dumping $table to file: ${table}_$self->{output}\n", 1);
				$fhdl = $self->export_file("${table}_$self->{output}");
			} else {
				$self->logit("Dumping data from table $table...\n", 1);
			}
			## Set client encoding if requested
			if ($self->{file_per_table} && $self->{client_encoding}) {
				$self->logit("Changing client encoding as \U$self->{client_encoding}\E...\n", 1);
				if ($self->{dbhdest}) {
					my $s = $self->{dbhdest}->do("SET client_encoding TO '\U$self->{client_encoding}\E';") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$self->dump("SET client_encoding TO '\U$self->{client_encoding}\E';\n", $fhdl);
				}
			}
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
			if ($search_path) {
				if ($self->{dbhdest}) {
					my $s = $self->{dbhdest}->do("$search_path") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$self->dump("$search_path\n", $fhdl);
				}
			}

			# Start transaction to speed up bulkload
			if (!$self->{defer_fkey}) {
				if ($self->{dbhdest}) {
					my $s = $self->{dbhdest}->do("BEGIN;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$self->dump("BEGIN;\n", $fhdl);
				}
			}
			# Rename table and double-quote it if required
			my $tmptb = $table;
			if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
				$self->logit("\tReplacing table $table as " . $self->{replaced_tables}{lc($table)} . "...\n", 1);
				$tmptb = $self->{replaced_tables}{lc($table)};
			}
			if (!$self->{preserve_case}) {
				$tmptb = lc($tmptb);
				$tmptb =~ s/"//g;
			} elsif ($tmptb !~ /"/) {
				$tmptb = '"' . $tmptb . '"';
			}
			$tmptb = $self->quote_reserved_words($tmptb);

			## disable triggers of current table if requested
			if ($self->{disable_triggers}) {
				my $trig_type = 'USER';
				$trig_type = 'ALL' if (uc($self->{disable_triggers}) eq 'ALL');
				if ($self->{dbhdest}) {
					my $s = $self->{dbhdest}->do("ALTER TABLE $tmptb DISABLE TRIGGER $trig_type;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$self->dump("ALTER TABLE $tmptb DISABLE TRIGGER $trig_type;\n", $fhdl);
				}
			}

			## Truncate current table if requested
			if ($self->{truncate_table}) {
				if ($self->{dbhdest}) {
					my $s = $self->{dbhdest}->do("TRUNCATE TABLE $tmptb;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$self->dump("TRUNCATE TABLE $tmptb;\n", $fhdl);
				}
			}

			my @tt = ();
			my @stt = ();
			my @nn = ();
			my $s_out = "INSERT INTO $tmptb (";
			if ($self->{type} eq 'COPY') {
				$s_out = "\nCOPY $tmptb (";
			}

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

				foreach my $f (@{$self->{tables}{$table}{column_info}}) {
					next if ($f->[0] ne "$fieldname");
					my $type = $self->_sql_type($f->[1], $f->[2], $f->[5], $f->[6]);
					$type = "$f->[1], $f->[2]" if (!$type);
					push(@stt, uc($f->[1]));
					push(@tt, $type);
					push(@nn, $f);
					# Change column names
					my $colname = $f->[0];
					if ($self->{replaced_cols}{lc($table)}{lc($f->[0])}) {
						$self->logit("\tReplacing column $f->[0] as " . $self->{replaced_cols}{lc($table)}{lc($f->[0])} . "...\n", 1);
						$colname = $self->{replaced_cols}{lc($table)}{lc($f->[0])};
					}
					if (!$self->{preserve_case}) {
						$colname = $self->quote_reserved_words($colname);
						$s_out .= "\L$colname\E,";
					} else {
						$s_out .= "\"$colname\",";
					}
					last;
				}
			}
			$s_out =~ s/,$//;
			if ($self->{type} eq 'COPY') {
				$s_out .= ") FROM STDIN;\n";
			} else {
				$s_out .= ") VALUES (";
			}

			my $sprep = undef;
			if ($self->{dbhdest}) {
				if ($self->{type} ne 'COPY') {
					$s_out .= '?,' foreach (@fname);
					$s_out =~ s/,$//;
					$s_out .= ")";
					$sprep = $s_out;
				}
			}
			# Extract all data from the current table
			my $total_record = $self->extract_data($table, $s_out, \@nn, \@tt, $fhdl, $sprep, \@stt);
			$global_count += $total_record;
			my $end_time = time();
			my $dt = $end_time - $start_time;
			$dt ||= 1;
			my $rps = sprintf("%.1f", $global_count / ($dt+.0001));
			$self->logit("Total extracted records from table $table: $total_record\n", 1);
			if (!$self->{quiet} && !$self->{debug}) {
				print STDERR $self->progress_bar($global_count, $global_rows, 25, '=', 'rows', "on total data ($rps recs/sec)" ), "\n";
			}

                        ## don't forget to enable all triggers if needed...
			if ($self->{disable_triggers}) {
				my $tmptb = $table;
				if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
					$self->logit("\tReplacing table $table as " . $self->{replaced_tables}{lc($table)} . "...\n", 1);
					$tmptb = $self->{replaced_tables}{lc($table)};
				}
				if (!$self->{preserve_case}) {
					$tmptb = lc($tmptb);
					$tmptb =~ s/"//g;
				} elsif ($tmptb !~ /"/) {
					$tmptb = '"' . $tmptb . '"';
				}
				$tmptb = $self->quote_reserved_words($tmptb);
				my $trig_type = 'USER';
				$trig_type = 'ALL' if (uc($self->{disable_triggers}) eq 'ALL');
				if ($self->{dbhdest}) {
					my $s = $self->{dbhdest}->do("ALTER TABLE $tmptb ENABLE TRIGGER $trig_type;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$self->dump("ALTER TABLE $tmptb ENABLE TRIGGER $trig_type;\n\n", $fhdl);
				}
			}
			# COMMIT transaction at end for speed improvement
			if (!$self->{defer_fkey}) {
				if ($self->{dbhdest}) {
					my $s = $self->{dbhdest}->do("COMMIT;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$self->dump("COMMIT;\n\n", $fhdl);
				}
			}
			if ($self->{file_per_table} && !$self->{dbhdest}) {
				$self->close_export_file($fhdl);
			}
		}
		if (!$self->{quiet} && !$self->{debug}) {
			my $ratio = 1;
			if ($global_rows) {
				$ratio = ($global_count / +$global_rows) * 100;
				if ($ratio != 100) {
					print STDERR "The total number of rows is an estimation so the final percentage may not be equal to 100.\n";
				}
			}
		}

		# extract sequence information
		if (($#ordered_tables >= 0) && !$self->{disable_sequence}) {
			$self->logit("Restarting sequences\n", 1);
			$self->dump($self->_extract_sequence_info());
		}
		if ($self->{drop_fkey}) {
			my @create_all = ();
			# Recreate all foreign keys of the concerned tables
			foreach my $table (sort { $self->{tables}{$a}{internal_id} <=> $self->{tables}{$b}{internal_id} } keys %{$self->{tables}}) {
				$self->logit("Restoring table $table foreign keys...\n", 1);
				push(@create_all, $self->_create_foreign_keys($table, @{$self->{tables}{$table}{foreign_key}}));
			}
			foreach my $str (@create_all) {
				if ($self->{dbhdest}) {
					my $s = $self->{dbhdest}->do($str) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					$self->dump("$str\n");
				}
			}
		}
		if ($self->{defer_fkey}) {
			if ($self->{dbhdest}) {
				my $s = $self->{dbhdest}->do("COMMIT;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
			} else {
				$self->dump("COMMIT;\n\n", $fhdl);
			}
		}
		if ($self->{drop_indexes}) {
			my @create_all = ();
			# Recreate all indexes
			foreach my $table (sort { $self->{tables}{$a}{internal_id} <=> $self->{tables}{$b}{internal_id} } keys %{$self->{tables}}) {
				$self->logit("Restoring table $table indexes...\n", 1);
				push(@create_all, $self->_create_indexes($table, %{$self->{tables}{$table}{indexes}}));
			}
			if ($#create_all >= 0) {
				if ($self->{plsql_pgsql}) {
					for (my $i = 0; $i <= $#create_all; $i++) {
						$create_all[$i] = Ora2Pg::PLSQL::plsql_to_plpgsql($create_all[$i], $self->{allow_code_break},$self->{null_equal_empty});
					}
				}
				if ($self->{dbhdest}) {
					foreach my $str (@create_all) {
						my $s = $self->{dbhdest}->do($str) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					}
				} else {
					$self->dump(join("\n", @create_all, "\n"));
				}
			}
		}
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
					$create_table{$table}{table} .= "CREATE TABLE $part ( CHECK (\n";
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
						$create_table{$table}{'index'} .= "CREATE INDEX ${part}_$self->{partitions}{$table}{$pos}{$part}[$i]->{column} ON $part ($self->{partitions}{$table}{$pos}{$part}[$i]->{column});\n";
						if ($self->{partitions}{$table}{$pos}{$part}[$i]->{type} eq 'LIST') {
							push(@condition, "NEW.$self->{partitions}{$table}{$pos}{$part}[$i]->{column} IN (" . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$pos}{$part}[$i]->{value}, $self->{allow_code_break}, $self->{null_equal_empty}) . ")");
						} else {
							push(@condition, "NEW.$self->{partitions}{$table}{$pos}{$part}[$i]->{column} <= " . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$pos}{$part}[$i]->{value}, $self->{allow_code_break},$self->{null_equal_empty}));
						}
					}
					$create_table{$table}{table} .= "\n) ) INHERITS ($table);\n";
					$funct_cond .= "\t$cond ( " . join(' AND ', @condition) . " ) THEN INSERT INTO $part VALUES (NEW.*);\n";
					$cond = 'ELSIF';
					$old_part = $part;
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

		if (!$sql_output) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}
	
		$self->dump($sql_header . $sql_output);

		return;
	}

	# Dump the database structure: tables, constraints, indexes, etc.
	if ($self->{export_schema} &&  $self->{schema}) {
		if ($self->{create_schema}) {
			if (!$self->{preserve_case}) {
				$sql_output .= "CREATE SCHEMA \L$self->{schema}\E;\n\n";
			} else {
				$sql_output .= "CREATE SCHEMA \"$self->{schema}\";\n\n";
			}
		}
		if ($self->{pg_schema}) {
			if (!$self->{preserve_case}) {
				$sql_output .= "SET search_path = \L$self->{pg_schema}\E;\n\n";
			} else {
				$sql_output .= "SET search_path = \"$self->{pg_schema}\";\n\n";
			}
		} else {
			if (!$self->{preserve_case}) {
				$sql_output .= "SET search_path = \L$self->{schema}, pg_catalog\E;\n\n";
			} else {
				$sql_output .= "SET search_path = \"$self->{schema}, pg_catalog\";\n\n";
			}
		}
	}

	my $constraints = '';
	if ($self->{export_schema} && $self->{file_per_constraint}) {
		if ($self->{pg_schema}) {
			if (!$self->{preserve_case}) {
				$constraints .= "SET search_path = \L$self->{pg_schema}\E;\n\n";
			} else {
				$constraints .= "SET search_path = \"$self->{pg_schema}\";\n\n";
			}
		} elsif ($self->{schema}) {
			if (!$self->{preserve_case}) {
				$constraints .= "SET search_path = \L$self->{schema}\E, pg_catalog;\n\n";
			} else {
				$constraints .= "SET search_path = \"$self->{schema}\", pg_catalog;\n\n";
			}
		}
	}
	my $indices = '';
	if ($self->{export_schema} && $self->{file_per_index}) {
		if ($self->{pg_schema}) {
			if (!$self->{preserve_case}) {
				$indices .= "SET search_path = \L$self->{pg_schema}\E;\n\n";
			} else {
				$indices .= "SET search_path = \"$self->{pg_schema}\";\n\n";
			}
		} elsif ($self->{schema}) {
			if (!$self->{preserve_case}) {
				$indices .= "SET search_path = \L$self->{schema}\E, pg_catalog;\n\n";
			} else {
				$indices .= "SET search_path = \"$self->{schema}\", pg_catalog;\n\n";
			}
		}
	}
	foreach my $table (sort { $self->{tables}{$a}{internal_id} <=> $self->{tables}{$b}{internal_id} } keys %{$self->{tables}}) {

		# forget or not this object if it is in the exclude or allow lists.
		if ($self->{tables}{$table}{type} ne 'view') {
			next if ($self->skip_this_object('TABLE', $table));
		}

		$self->logit("Dumping table $table...\n", 1);
		# Create FDW server if required
		if ($self->{external_to_fdw}) {
			if ( grep(/^$table$/i, keys %{$self->{external_table}}) ) {
					$sql_header .= "CREATE EXTENSION file_fdw;\n\n" if ($sql_header !~ /CREATE EXTENSION file_fdw;/is);
					$sql_header .= "CREATE SERVER \L$self->{external_table}{$table}{directory}\E FOREIGN DATA WRAPPER file_fdw;\n\n" if ($sql_header !~ /CREATE SERVER $self->{external_table}{$table}{directory} FOREIGN DATA WRAPPER file_fdw;/is);
			}
		}

		my $tbname = $table;
		if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
			$tbname = $self->{replaced_tables}{"\L$table\E"};
			$self->logit("\tReplacing tablename $table as $tbname...\n", 1);
		}
		my $foreign = '';
		if ( ($self->{type} eq 'FDW') || ($self->{external_to_fdw} && grep(/^$table$/i, keys %{$self->{external_table}})) ) {
			$foreign = ' FOREIGN';
		}
		my $obj_type = ${$self->{tables}{$table}{table_info}}[1] || 'TABLE';
		if (!$self->{preserve_case}) {
			$tbname = $self->quote_reserved_words($tbname);
			$sql_output .= "CREATE$foreign $obj_type \L$tbname\E (\n";
		} else {
			$sql_output .= "CREATE$foreign $obj_type \"$tbname\" (\n";
		}
		foreach my $i ( 0 .. $#{$self->{tables}{$table}{field_name}} ) {
			foreach my $f (@{$self->{tables}{$table}{column_info}}) {
				next if ($f->[0] ne "${$self->{tables}{$table}{field_name}}[$i]");
				my $type = $self->_sql_type($f->[1], $f->[2], $f->[5], $f->[6]);
				$type = "$f->[1], $f->[2]" if (!$type);
				# Change column names
				my $fname = $f->[0];
				if (exists $self->{replaced_cols}{"\L$table\E"}{"\L$fname\E"} && $self->{replaced_cols}{"\L$table\E"}{"\L$fname\E"}) {
					$self->logit("\tReplacing column \L$f->[0]\E as " . $self->{replaced_cols}{lc($table)}{lc($fname)} . "...\n", 1);
					$fname = $self->{replaced_cols}{"\L$table\E"}{"\L$fname\E"};
				}
				# Check if this column should be replaced by a boolean following table/column name
				if (grep(/^$f->[0]$/i, @{$self->{'replace_as_boolean'}{uc($table)}})) {
					$type = 'boolean';
				# Check if this column should be replaced by a boolean following type/precision
				} elsif (exists $self->{'replace_as_boolean'}{uc($f->[1])} && ($self->{'replace_as_boolean'}{uc($f->[1])}[0] == $f->[5])) {
					$type = 'boolean';
				}
				if (!$self->{preserve_case}) {
					$fname = $self->quote_reserved_words($fname);
					$sql_output .= "\t\L$fname\E $type";
				} else {
					$sql_output .= "\t\"$fname\" $type";
				}
				if ($f->[4] ne "") {
					$f->[4] =~ s/SYSDATE[\s\t]*\([\s\t]*\)/LOCALTIMESTAMP/igs;
					$f->[4] =~ s/SYSDATE/LOCALTIMESTAMP/ig;
					$f->[4] =~ s/^[\s\t]+//;
					$f->[4] =~ s/[\s\t]+$//;
					if (($f->[4] eq "''") && (!$f->[3] || ($f->[3] eq 'N'))) {
						$sql_output .= " NOT NULL";
						push(@{$self->{tables}{$table}{check_constraint}{notnull}}, $f->[0]);
					} elsif ($self->{type} ne 'FDW') {
						$sql_output .= " DEFAULT $f->[4]";
					}
				} elsif (!$f->[3] || ($f->[3] eq 'N')) {
					push(@{$self->{tables}{$table}{check_constraint}{notnull}}, $f->[0]);
					$sql_output .= " NOT NULL";
				}
				$sql_output .= ",\n";
				last;
			}
		}
		if ($self->{pkey_in_create}) {
			$sql_output .= $self->_get_primary_keys($table, $self->{tables}{$table}{unique_key});
		}
		$sql_output =~ s/,$//;
		if ( ($self->{type} ne 'FDW') && (!$self->{external_to_fdw} || !grep(/^$table$/i, keys %{$self->{external_table}})) ) {
			$sql_output .= ");\n";
		} elsif ( grep(/^$table$/i, keys %{$self->{external_table}}) ) {
			$sql_output .= ") SERVER \L$self->{external_table}{$table}{directory}\E OPTIONS(filename '$self->{external_table}{$table}{directory_path}$self->{external_table}{$table}{location}', format 'csv', delimiter '$self->{external_table}{$table}{delimiter}');\n";
		} else {
			my $schem = "schema '$self->{schema}'," if ($self->{schema});
			if ($self->{preserve_case}) {
				$sql_output .= ") SERVER $self->{fdw_server} OPTIONS($schem table '$table');\n";
			} else {
				$sql_output .= ") SERVER $self->{fdw_server} OPTIONS($schem table \L$table\E);\n";
			}
		}
		# Add comments on table
		if (!$self->{disable_comment} && ${$self->{tables}{$table}{table_info}}[2]) {
			if (!$self->{preserve_case}) {
				$sql_output .= "COMMENT ON TABLE \L$tbname\E IS E'${$self->{tables}{$table}{table_info}}[2]';\n";
			} else {
				$sql_output .= "COMMENT ON TABLE \"$tbname\".\"$f->[0]\" IS E'${$self->{tables}{$table}{table_info}}[2]';\n";
			}
		}

		# Add comments on columns
		if (!$self->{disable_comment}) {
			foreach $f (@{$self->{tables}{$table}{column_comments}}) {
				next unless $f->[1];
				$f->[1] =~ s/'/\\'/gs;
				if (!$self->{preserve_case}) {
					$sql_output .= "COMMENT ON COLUMN \L$tbname\.$f->[0]\E IS E'$f->[1]';\n";
				} else {
					$sql_output .= "COMMENT ON COLUMN \"$tbname\".\"$f->[0]\" IS E'$f->[1]';\n";
				}
			}
		}

		# Change ownership
		if ($self->{force_owner}) {
			my $owner = ${$self->{tables}{$table}{table_info}}[0];
			$owner = $self->{force_owner} if ($self->{force_owner} ne "1");
			if (!$self->{preserve_case}) {
				$sql_output .= "ALTER ${$self->{tables}{$table}{table_info}}[1] \L$tbname\E OWNER TO \L$owner\E;\n";
			} else {
				$sql_output .= "ALTER ${$self->{tables}{$table}{table_info}}[1] \"$tbname\" OWNER TO $owner;\n";
			}
		}
		if ($self->{type} ne 'FDW') {
			# Set the unique (and primary) key definition 
			$constraints .= $self->_create_unique_keys($table, $self->{tables}{$table}{unique_key});
			# Set the check constraint definition 
			$constraints .= $self->_create_check_constraint($table, $self->{tables}{$table}{check_constraint},$self->{tables}{$table}{field_name});
			if (!$self->{file_per_constraint} || $self->{dbhdest}) {
				$sql_output .= $constraints;
				$constraints = '';
			}

			# Set the index definition
			$indices .= $self->_create_indexes($table, %{$self->{tables}{$table}{indexes}}) . "\n";
			if ($self->{plsql_pgsql}) {
				$indices = Ora2Pg::PLSQL::plsql_to_plpgsql($indices, $self->{allow_code_break},$self->{null_equal_empty});
			}
			if (!$self->{file_per_index} || $self->{dbhdest}) {
				$sql_output .= $indices;
				$indices = '';
			}
		}
	}
	if ($self->{file_per_index} && !$self->{dbhdest}) {
		my $fhdl = undef;
		$self->logit("Dumping indexes to one separate file : INDEXES_$self->{output}\n", 1);
		$fhdl = $self->export_file("INDEXES_$self->{output}");
		$indices = "-- Nothing found of type indexes\n" if (!$indices);
		$self->dump($sql_header . $indices, $fhdl);
		$self->close_export_file($fhdl);
		$indices = '';
	}

	foreach my $table (keys %{$self->{tables}}) {
		next if ($#{$self->{tables}{$table}{foreign_key}} < 0);
		$self->logit("Dumping RI $table...\n", 1);
		# Add constraint definition
		my $create_all = $self->_create_foreign_keys($table, @{$self->{tables}{$table}{foreign_key}});
		if ($create_all) {
			if ($self->{file_per_constraint} && !$self->{dbhdest}) {
				$constraints .= $create_all;
			} else {
				$sql_output .= $create_all;
			}
		}
	}

	if ($self->{file_per_constraint} && !$self->{dbhdest}) {
		my $fhdl = undef;
		$self->logit("Dumping constraints to one separate file : CONSTRAINTS_$self->{output}\n", 1);
		$fhdl = $self->export_file("CONSTRAINTS_$self->{output}");
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

=head2 _column_comments

This function return comments associated to columns

=cut
sub _column_comments
{
	my ($self, $table, $owner) = @_;

	$owner = "AND upper(OWNER)='\U$owner\E' " if ($owner);
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("WARNING only: " . $self->{dbh}->errstr . "\n", 0, 0);
SELECT COLUMN_NAME,COMMENTS
FROM $self->{prefix}_COL_COMMENTS
WHERE upper(TABLE_NAME)='\U$table\E' $owner       
END

	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my $data = $sth->fetchall_arrayref();

	return @$data;
}


=head2 _create_indexes

This function return SQL code to create indexes of a table

=cut
sub _create_indexes
{
	my ($self, $table, %indexes) = @_;

	my $tbsaved = $table;
	if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
		$table = $self->{replaced_tables}{"\L$table\E"};
	}
	my @out = ();
	# Set the index definition
	foreach my $idx (keys %indexes) {
		# Cluster, domain, bitmap join, reversed and IOT indexes will not be exported at all
		next if ($self->{tables}{$table}{idx_type}{$idx} =~ /JOIN|IOT|CLUSTER|DOMAIN|REV/i);

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
		my $unique_key = $self->{tables}{$table}{unique_key};
		foreach my $consname (keys %$unique_key) {
			my $constype =   $unique_key->{$consname}{type};
			next if (($constype ne 'P') && ($constype ne 'U'));
			my @conscols = @{$unique_key->{$consname}{columns}};
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
			$unique = ' UNIQUE' if ($self->{tables}{$table}{uniqueness}{$idx} eq 'UNIQUE');
			my $str = '';
			if (!$self->{preserve_case}) {
				$table = $self->quote_reserved_words($table);
				$str .= "CREATE$unique INDEX \L$idx\E ON \L$table\E (\L$columns\E);";
			} else {
				$str .= "CREATE$unique INDEX \L$idx\E ON \"$table\" ($columns);";
			}
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
	if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
		$table = $self->{replaced_tables}{"\L$table\E"};
	}
	my @out = ();
	# Set the index definition
	foreach my $idx (keys %indexes) {
		# Cluster, domain, bitmap join, reversed and IOT indexes will not be exported at all
		next if ($self->{tables}{$table}{idx_type}{$idx} =~ /JOIN|IOT|CLUSTER|DOMAIN|REV/i);

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
		my $unique_key = $self->{tables}{$table}{unique_key};
		foreach my $consname (keys %$unique_key) {
			my $constype =   $unique_key->{$consname}{type};
			next if (($constype ne 'P') && ($constype ne 'U'));
			my @conscols = @{$unique_key->{$consname}{columns}};
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
			my $str = "DROP INDEX \L$idx\E;";
			if ($self->{dbhdest}) {
				my $s = $self->{dbhdest}->do($str);
			} else {
				push(@out, $str);
			}
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
		my $unique_key = $self->{tables}{$table}{unique_key};
		foreach my $consname (keys %$unique_key) {
			my $constype =   $unique_key->{$consname}{type};
			next if (($constype ne 'P') && ($constype ne 'U'));
			my @conscols = @{$unique_key->{$consname}{columns}};
			$columnlist = join(',', @conscols);
			$columnlist =~ s/"//gs;
			if (lc($columnlist) eq lc($colscompare)) {
				$skip_index_creation = 1;
				last;
			}
		}

		# The index iwill not be created if there is already a constraint on the same column list.
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

	my $tbsaved = $table;
	if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
		$table = $self->{replaced_tables}{"\L$table\E"};
	}

	# Set the unique (and primary) key definition 
	foreach my $consname (keys %$unique_key) {
		next if ($self->{pkey_in_create} && ($unique_key->{$consname}{type} ne 'P'));
		my $constype =   $unique_key->{$consname}{type};
		my $constgen =   $unique_key->{$consname}{generated};
		my @conscols = @{$unique_key->{$consname}{columns}};
		my %constypenames = ('U' => 'UNIQUE', 'P' => 'PRIMARY KEY');
		my $constypename = $constypenames{$constype};
		for (my $i = 0; $i <= $#conscols; $i++) {
			# Change column names
			if (exists $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"}) {
				$conscols[$i] = $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"};
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
				if ($self->{pkey_in_create}) {
					$out .= "\tPRIMARY KEY ($columnlist),\n";
				}
			} else {
				if ($self->{pkey_in_create}) {
					$out .= "\tCONSTRAINT \L$consname\E PRIMARY KEY ($columnlist),\n";
				}
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
	if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
		$table = $self->{replaced_tables}{"\L$table\E"};
	}

	# Set the unique (and primary) key definition 
	foreach my $consname (keys %$unique_key) {
		next if ($self->{pkey_in_create} && ($unique_key->{$consname}{type} eq 'P'));
		my $constype =   $unique_key->{$consname}{type};
		my $constgen =   $unique_key->{$consname}{generated};
		my @conscols = @{$unique_key->{$consname}{columns}};
		my %constypenames = ('U' => 'UNIQUE', 'P' => 'PRIMARY KEY');
		my $constypename = $constypenames{$constype};
		for (my $i = 0; $i <= $#conscols; $i++) {
			# Change column names
			if (exists $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"}) {
				$conscols[$i] = $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"};
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
				if (!$self->{preserve_case}) {
					$table = $self->quote_reserved_words($table);
					$out .= "ALTER TABLE \L$table\E ADD $constypename ($columnlist);\n";
				} else {
					$out .= "ALTER TABLE \"$table\" ADD $constypename ($columnlist);\n";
				}
			} else {
				if (!$self->{preserve_case}) {
					$table = $self->quote_reserved_words($table);
					$out .= "ALTER TABLE \L$table\E ADD CONSTRAINT \L$consname\E $constypename ($columnlist);\n";
				} else {
					$out .= "ALTER TABLE \"$table\" ADD CONSTRAINT \L$consname\E $constypename ($columnlist);\n";
				}
			}
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
	if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
		$table = $self->{replaced_tables}{"\L$table\E"};
	}

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
			if (exists $self->{replaced_cols}{"\L$tbsaved\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}) {
				foreach my $c (keys %{$self->{replaced_cols}{"\L$tbsaved\E"}}) {
					$chkconstraint =~ s/"$c"/"$self->{replaced_cols}{"\L$tbsaved\E"}{$c}"/gsi;
					$chkconstraint =~ s/\b$c\b/$self->{replaced_cols}{"\L$tbsaved\E"}{$c}/gsi;
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
				$table = $self->quote_reserved_words($table);
				$out .= "ALTER TABLE \L$table\E ADD CONSTRAINT \L$k\E CHECK ($chkconstraint);\n";
			} else {
				$out .= "ALTER TABLE \"$table\" ADD CONSTRAINT $k CHECK ($chkconstraint);\n";
			}
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

	# Add constraint definition
	my @done = ();
	foreach my $h (@foreign_key) {
		next if (grep(/^$h->[0]$/, @done));
		foreach my $desttable (keys %{$self->{tables}{$table}{foreign_link}{$h->[0]}{remote}}) {
			my $str = '';
			push(@done, $h->[0]);
			map { $_ = '"' . $_ . '"' } @{$self->{tables}{$table}{foreign_link}{$h->[0]}{local}};
			map { $_ = '"' . $_ . '"' } @{$self->{tables}{$table}{foreign_link}{$h->[0]}{remote}{$desttable}};
			my $substable = $table;
			my $subsdesttable = $desttable;
			if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
				$substable = $self->{replaced_tables}{"\L$table\E"};
			}
			if (exists $self->{replaced_tables}{"\L$desttable\E"} && $self->{replaced_tables}{"\L$desttable\E"}) {
				$subsdesttable = $self->{replaced_tables}{"\L$desttable\E"};
			}
			my @lfkeys = ();
			push(@lfkeys, @{$self->{tables}{$table}{foreign_link}{$h->[0]}{local}});
			if (exists $self->{replaced_cols}{"\L$table\E"} && $self->{replaced_cols}{"\L$table\E"}) {
				foreach my $c (keys %{$self->{replaced_cols}{"\L$table\E"}}) {
					map { s/"$c"/"$self->{replaced_cols}{"\L$table\E"}{$c}"/i } @lfkeys;
				}
			}
			my @rfkeys = ();
			push(@rfkeys, @{$self->{tables}{$table}{foreign_link}{$h->[0]}{remote}{$desttable}});
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
				$substable = $self->quote_reserved_words($substable);
				map { $_ = $self->quote_reserved_words($_) } @lfkeys;
				map { $_ = $self->quote_reserved_words($_) } @rfkeys;
				$str .= "ALTER TABLE \L$substable\E ADD CONSTRAINT \L$h->[0]\E FOREIGN KEY (" . lc(join(',', @lfkeys)) . ") REFERENCES \L$subsdesttable\E (" . lc(join(',', @rfkeys)) . ")";
			} else {
				$str .= "ALTER TABLE \"$substable\" ADD CONSTRAINT $h->[0] FOREIGN KEY (" . join(',', @lfkeys) . ") REFERENCES \"$subsdesttable\" (" . join(',', @rfkeys) . ")";
			}
			$str .= " MATCH $h->[2]" if ($h->[2]);
			$str .= " ON DELETE $h->[3]";
			$str .= " $h->[4]";
			$str .= " INITIALLY $h->[5];\n";
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

	my $out = '';

	# Add constraint definition
	my @done = ();
	foreach my $h (@foreign_key) {
		next if (grep(/^$h->[0]$/, @done));
		push(@done, $h->[0]);
		if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
			$table = $self->{replaced_tables}{"\L$table\E"};
		}
		my $str = '';
		if (!$self->{preserve_case}) {
			$table = $self->quote_reserved_words($table);
			$str = "ALTER TABLE \L$table\E DROP CONSTRAINT \L$h->[0]\E;";
		} else {
			$str .= "ALTER TABLE \"$table\" DROP CONSTRAINT $h->[0];";
		}
		if ($self->{dbhdest}) {
			my $s = $self->{dbhdest}->do($str);
		} else {
			$out .= $str . "\n";
		}
	}

	return $out;
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
		$sql .= " WHERE upper(SEQUENCE_OWNER)='\U$self->{schema}\E'";
	} else {
		$sql .= " WHERE SEQUENCE_OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	my $script = '';

	my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr ."\n", 0, 1);
	$sth->execute() or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);    

	while (my $seq_info = $sth->fetchrow_hashref) {

		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('SEQUENCE', $seq_info->{SEQUENCE_NAME}));

		my $nextvalue = $seq_info->{LAST_NUMBER} + $seq_info->{INCREMENT_BY};
		my $alter ="ALTER SEQUENCE $seq_info->{SEQUENCE_NAME} RESTART WITH $nextvalue;";
		$script .= "$alter\n";
		$self->logit("Extracted sequence information for sequence \"$seq_info->{SEQUENCE_NAME}\"\n", 1);

	}

	$sth->finish();
	return $script;

}


=head2 _get_data TABLE

This function implements an Oracle-native data extraction.

Returns a list of array references containing the data

=cut

sub _get_data
{
	my ($self, $table, $name, $type, $src_type) = @_;

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
		} elsif ( $src_type->[$k] =~ /xmltype/i) {
			if ($self->{xml_pretty}) {
				$str .= "$alias.$name->[$k]->[0].extract('/').getStringVal(),";
			} else {
				$str .= "$alias.$name->[$k]->[0].extract('/').getClobVal(),";
			}
		} else {
			$str .= "$name->[$k]->[0],";
		}
	}
	$str =~ s/,$//;

	# Fix a problem when using data_limit AND where clause
	if (exists $self->{where}{"\L$table\E"} && $self->{where}{"\L$table\E"}) {
		$extraStr .= ' AND (' . $self->{where}{"\L$table\E"} . ')';
	} elsif ($self->{global_where}) {
		$extraStr .= ' AND (' . $self->{global_where} . ')';
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

	#my $sth = $self->{dbh}->prepare($str,{ora_piece_lob=>1,ora_piece_size=>$self->{ora_piece_size}}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my $sth = $self->{dbh}->prepare($str,{ora_auto_lob => 1}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	return $sth;	

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
)]

=cut

sub _column_info
{
	my ($self, $table, $owner, $not_show_info) = @_;

	my $schema = '';
	$schema = "AND upper(OWNER)='\U$owner\E' " if ($owner);
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("WARNING only: " . $self->{dbh}->errstr . "\n", 0, 0);
SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE, DATA_DEFAULT, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH
FROM $self->{prefix}_TAB_COLUMNS
WHERE TABLE_NAME='$table' $schema
ORDER BY COLUMN_ID
END
	if (not defined $sth) {
		# Maybe a 8i database.
		$sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE, DATA_DEFAULT, DATA_PRECISION, DATA_SCALE
FROM $self->{prefix}_TAB_COLUMNS
WHERE TABLE_NAME='$table' $schema
ORDER BY COLUMN_ID
END
		$self->logit("INFO: please forget the above error message, this is a warning only for Oracle 8i database.\n", 0, 0);
	}
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my $data = $sth->fetchall_arrayref();
	foreach my $d (@$data) {
		if ($#{$d} == 7) {
			$self->logit("\t$d->[0] => type:$d->[1] , length:$d->[2] (char_length:$d->[7]), precision:$d->[5], scale:$d->[6], nullable:$d->[3] , default:$d->[4]\n", 1) if (!$not_show_info);
			$d->[2] = $d->[7] if $d->[1] =~ /char/i;
		} elsif (!$not_show_info) {
			$self->logit("\t$d->[0] => type:$d->[1] , length:$d->[2] (char_length:$d->[2]), precision:$d->[5], scale:$d->[6], nullable:$d->[3] , default:$d->[4]\n", 1);
		}
	}

	return @$data;	
}

=head2 _unique_key TABLE OWNER

This function implements an Oracle-native unique (including primary)
key column information.

Returns a hash of hashes in the following form:
    ( constraintname => (type => 'PRIMARY',
                         columns => ('a', 'b', 'c')),
      constraintname => (type => 'UNIQUE',
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
	$owner = "AND upper(OWNER)='\U$owner\E'" if ($owner);
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,DEFERRABLE,DEFERRED,R_OWNER,CONSTRAINT_TYPE,GENERATED
FROM $self->{prefix}_CONSTRAINTS
WHERE CONSTRAINT_TYPE IN $cons_types
AND STATUS='ENABLED'
AND TABLE_NAME='$table' $owner
END
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch) {
		my %constraint = (type => $row->[7], 'generated' => $row->[8], columns => ());
		my $sql = "SELECT DISTINCT COLUMN_NAME,POSITION FROM $self->{prefix}_CONS_COLUMNS WHERE CONSTRAINT_NAME='$row->[0]' $owner ORDER BY POSITION";
		my $sth2 = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
		while (my $r = $sth2->fetch) {
			push @{$constraint{'columns'}}, $r->[0];
		}
		$result{$row->[0]} = \%constraint;
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

	$owner = "AND upper(OWNER)='\U$owner\E'" if ($owner);
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,DEFERRABLE,DEFERRED,R_OWNER
FROM $self->{prefix}_CONSTRAINTS
WHERE CONSTRAINT_TYPE='C'
AND STATUS='ENABLED'
AND GENERATED != 'GENERATED NAME'
AND TABLE_NAME='$table' $owner
END
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		$data{constraint}{$row->[0]} = $row->[2];
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

	$owner = "AND upper(OWNER)='\U$owner\E'" if ($owner);
	my $deferrable = $self->{fkey_deferrable} ? "'DEFERRABLE' AS DEFERRABLE" : "DEFERRABLE";
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,$deferrable,DEFERRED,R_OWNER
FROM $self->{prefix}_CONSTRAINTS
WHERE CONSTRAINT_TYPE='R'
AND STATUS='ENABLED'
AND GENERATED != 'GENERATED NAME'
AND TABLE_NAME='$table' $owner
END
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @data = ();
	my %link = ();
	my @tab_done = ();
	while (my $row = $sth->fetch) {
		next if (grep(/^$row->[0]$/, @tab_done));
		push(@data, [ @$row ]);
		push(@tab_done, $row->[0]);
		my $sql = "SELECT DISTINCT COLUMN_NAME,POSITION FROM $self->{prefix}_CONS_COLUMNS WHERE CONSTRAINT_NAME='$row->[0]' $owner ORDER BY POSITION";
		my $sth2 = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
		my @done = ();
		while (my $r = $sth2->fetch) {
			if (!grep(/^$r->[0]$/, @done)) {
				push(@{$link{$row->[0]}{local}}, $r->[0]);
				push(@done, $r->[0]);
			}
		}
		$sql = "SELECT DISTINCT TABLE_NAME,COLUMN_NAME,POSITION FROM $self->{prefix}_CONS_COLUMNS WHERE CONSTRAINT_NAME='$row->[1]' " . ($owner ? "AND OWNER = '$row->[6]'" : '') . " ORDER BY POSITION";
		$sth2 = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth2->execute or $self->logit("FATAL: " . $sth2->errstr . "\n", 0, 1);
		@done = ();
		while (my $r = $sth2->fetch) {
			if (!grep(/^$r->[1]$/, @done)) {
				#            column             tablename  column  
				push(@{$link{$row->[0]}{remote}{$r->[0]}}, $r->[1]);
				push(@done, $r->[1]);
			}

		}
	}

	return \%link, \@data;
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
		$str .= " WHERE upper(b.GRANTOR) = '\U$self->{schema}\E'";
	} else {
		$str .= " WHERE b.GRANTOR NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$str .= " AND b.TABLE_NAME=a.OBJECT_NAME AND a.OWNER=b.GRANTOR";
	
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
		$str .= " WHERE upper(b.GRANTOR) = '\U$self->{schema}\E'";
	} else {
		$str .= " WHERE b.GRANTOR NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	if (!$self->{export_invalid}) {
		$str .= " AND a.STATUS='VALID' AND b.TABLE_NAME=a.OBJECT_NAME AND a.OWNER=b.GRANTOR";
	}
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
		$idxowner = "AND upper(IC.TABLE_OWNER) = '\U$owner\E'";
	}
	my $sub_owner = '';
	if ($owner) {
		$owner = "AND upper($self->{prefix}_INDEXES.OWNER)='\U$owner\E' AND $self->{prefix}_IND_COLUMNS.INDEX_OWNER=$self->{prefix}_INDEXES.OWNER";
		$sub_owner = "AND OWNER=$self->{prefix}_INDEXES.TABLE_OWNER";
	}
	# Retrieve all indexes 
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("WARNING ONLY: " . $self->{dbh}->errstr . "\n", 0, 0);
SELECT DISTINCT $self->{prefix}_IND_COLUMNS.INDEX_NAME,$self->{prefix}_IND_COLUMNS.COLUMN_NAME,$self->{prefix}_INDEXES.UNIQUENESS,$self->{prefix}_IND_COLUMNS.COLUMN_POSITION,$self->{prefix}_INDEXES.INDEX_TYPE,$self->{prefix}_INDEXES.TABLE_TYPE,$self->{prefix}_INDEXES.GENERATED,$self->{prefix}_INDEXES.JOIN_INDEX
FROM $self->{prefix}_IND_COLUMNS, $self->{prefix}_INDEXES
WHERE $self->{prefix}_IND_COLUMNS.TABLE_NAME='$table' $owner
AND $self->{prefix}_INDEXES.GENERATED <> 'Y'
AND $self->{prefix}_INDEXES.TEMPORARY <> 'Y'
AND $self->{prefix}_INDEXES.INDEX_NAME=$self->{prefix}_IND_COLUMNS.INDEX_NAME
ORDER BY $self->{prefix}_IND_COLUMNS.COLUMN_POSITION
END

	if (not defined $sth) {
		# Maybe a 8i database.
		$sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT DISTINCT $self->{prefix}_IND_COLUMNS.INDEX_NAME,$self->{prefix}_IND_COLUMNS.COLUMN_NAME,$self->{prefix}_INDEXES.UNIQUENESS,$self->{prefix}_IND_COLUMNS.COLUMN_POSITION,$self->{prefix}_INDEXES.INDEX_TYPE,$self->{prefix}_INDEXES.TABLE_TYPE,$self->{prefix}_INDEXES.GENERATED
FROM $self->{prefix}_IND_COLUMNS, $self->{prefix}_INDEXES
WHERE $self->{prefix}_IND_COLUMNS.TABLE_NAME='$table' $owner
AND $self->{prefix}_INDEXES.GENERATED <> 'Y'
AND $self->{prefix}_INDEXES.TEMPORARY <> 'Y'
AND $self->{prefix}_INDEXES.INDEX_NAME=$self->{prefix}_IND_COLUMNS.INDEX_NAME
ORDER BY $self->{prefix}_IND_COLUMNS.COLUMN_POSITION
END
		$self->logit("INFO: please forget the above error message, this is a warning only for Oracle 8i database.\n", 0, 0);
	}
#AND $self->{prefix}_IND_COLUMNS.INDEX_NAME NOT IN (SELECT CONSTRAINT_NAME FROM $self->{prefix}_CONSTRAINTS WHERE TABLE_NAME='$table' $sub_owner)
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	my $idxnc = qq{SELECT IE.COLUMN_EXPRESSION FROM $self->{prefix}_IND_EXPRESSIONS IE, $self->{prefix}_IND_COLUMNS IC
WHERE  IE.INDEX_OWNER = IC.INDEX_OWNER
AND    IE.INDEX_NAME = IC.INDEX_NAME
AND    IE.TABLE_OWNER = IC.TABLE_OWNER
AND    IE.TABLE_NAME = IC.TABLE_NAME
AND    IE.COLUMN_POSITION = IC.COLUMN_POSITION
AND    IC.COLUMN_NAME = ?
AND    IE.TABLE_NAME = '$table'
$idxowner
};
	my $sth2 = $self->{dbh}->prepare($idxnc);
	my %data = ();
	my %unique = ();
	my %idx_type = ();
	while (my $row = $sth->fetch) {
		# forget or not this object if it is in the exclude or allow lists.
		next if ($self->skip_this_object('INDEX', $row->[0]));
		$unique{$row->[0]} = $row->[2];
		if (($#{$row} > 6) && ($row->[7] eq 'Y')) {
			$idx_type{$row->[0]} = $row->[4] . ' JOIN';
		} else {
			$idx_type{$row->[0]} = $row->[4];
		}
		# Replace function based index type
		if ($row->[1] =~ /^SYS_NC/i) {
			$sth2->execute($row->[1]) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			my $nc = $sth2->fetch();
			$row->[1] = $nc->[0];
		}
		$row->[1] =~ s/SYS_EXTRACT_UTC[\s\t]*\(([^\)]+)\)/$1/isg;
		push(@{$data{$row->[0]}}, $row->[1]);
	}

	return \%unique, \%data, \%idx_type;
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
		$str .= " WHERE upper(SEQUENCE_OWNER) = '\U$self->{schema}\E'";
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
		$str .= " WHERE upper(a.OWNER) = '\U$self->{schema}\E'";
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
		$str .= " WHERE upper(OWNER) = '\U$self->{schema}\E'";
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
		$str .= " WHERE upper(SCHEMA_USER) = '\U$self->{schema}\E'";
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
		$str .= " WHERE upper(v.OWNER) = '\U$self->{schema}\E'";
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
	my $str = "SELECT MVIEW_NAME,QUERY,UPDATABLE,REFRESH_MODE,REFRESH_METHOD,USE_NO_INDEX,REWRITE_ENABLED FROM $self->{prefix}_MVIEWS";
	if (!$self->{schema}) {
		$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE upper(OWNER) = '\U$self->{schema}\E'";
	}
	$str .= " ORDER BY MVIEW_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

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
	}

	return %data;
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
		$str .= " AND upper(OWNER) = '\U$self->{schema}\E'";
	} else {
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$str .= " ORDER BY COLUMN_ID ASC";
        my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        my $data = $sth->fetchall_arrayref();
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
		$str .= " AND upper(OWNER) = '\U$self->{schema}\E'";
	}
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
		$str .= " AND upper(OWNER) = '\U$self->{schema}\E'";
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
		$str .= " AND upper(OWNER) = '\U$self->{schema}\E'";
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
		$str .= " AND upper(OWNER) = '\U$self->{schema}\E'";
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
		$str .= " AND (upper(OWNER) = '\U$self->{schema}\E')";
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

	my $sql = "SELECT
                NULL            TABLE_CAT,
                at.OWNER        TABLE_SCHEM,
                at.TABLE_NAME,
                tc.TABLE_TYPE,
                tc.COMMENTS     REMARKS,
		NVL(at.num_rows,1) NUMBER_ROWS
            from ALL_TABLES at, ALL_TAB_COMMENTS tc
            where at.OWNER = tc.OWNER
            and at.TABLE_NAME = tc.TABLE_NAME
	";

	if ($self->{schema}) {
		$sql .= " and upper(at.OWNER)='\U$self->{schema}\E'";
	} else {
            $sql .= "AND upper(at.OWNER) NOT IN ('" . uc(join("','", @{$self->{sysusers}})) . "')";
	}
        $sql .= " order by tc.TABLE_TYPE, at.OWNER, at.TABLE_NAME";
        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
        $sth;
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
		$str .= " AND upper(a.OWNER)='\U$self->{schema}\E'";
	} else {
		$str .= " AND upper(a.OWNER) NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
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
			$str .= "\tAND upper(a.table_owner) ='\U$self->{schema}\E'\n";
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
		next if ($self->skip_this_object('PARTITION', $row->[2]));

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
			$str .= "\tAND upper(a.table_owner) ='\U$self->{schema}\E'\n";
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

# This is a routine paralellizing access to format_data_for_parallel over several threads
sub format_data_parallel
{
	my ($self, $rows, $data_types, $action, $src_data_types, $custom_types, $table) = @_;

	# Ok, here is the required parallelism

	# We use the threads::queue mecanism.
	# The problem is that the queued data structure must be shared, hence the following code
	# Not sure it is necessary for Perl > 5.8.6
	# We try not to enqueue too many messages at once, to save on memory. We don't enqueue more than 
	# 2*number of threads
	my $max_queued=$self->{thread_count}*2;
	my $total_rows=scalar(@$rows);
	my $retrieved_rows=0;
	my $sent_rows=0;
	my @new_rows;
	my $queue_todo=$self->{queue_todo_bytea};
	my $queue_done=$self->{queue_done_bytea};

	while (1)
	{
		# Do we have something to queue ?
		if ($sent_rows<$total_rows)
		{
			# we still have rows to send. Do we queue one ?
			if ($sent_rows-$retrieved_rows<$max_queued)
			{
				# We can send a new one
				my $row=$rows->[$sent_rows];
				# create new row (and its metadata), thread-shared this time
				# And add to it the types array and the action
				my @temprow :shared;
				my @temprecord :shared;
				my @temptypes :shared;
				my $tempaction :shared;
				my @tempsrctypes :shared;
				my %tempcustomtypes :shared;
				my $temptable :shared;
				foreach my $elt (@$row)
				{
					my $tempelt :shared;
					$tempelt=$elt;
					push @temprow,($tempelt);
				}
				push @temprecord,(\@temprow);
				foreach my $elt (@$data_types)
				{
					my $tempelt :shared;
					$tempelt=$elt;
					push @temptypes,($tempelt);
				}
				push @temprecord,(\@temptypes);
				$tempaction=$action;
				push @temprecord,($tempaction);
				foreach my $elt (@$src_data_types)
				{
					my $tempelt :shared;
					$tempelt=$elt;
					push @tempsrctypes,($tempelt);
				}
				push @temprecord,(\@tempsrctypes);
				foreach my $elt (%$custom_types)
				{
					my $tempelt :shared;
					$tempelt=$custom_types->{$elt};
					push @{$tempcustomtypes{$elt}},($tempelt);
				}
				push @temprecord,(\%tempcustomtypes);
				$temptable=$table;
				push @temprecord,$temptable;
				$queue_todo->enqueue(\@temprecord);
				$sent_rows++;
			}
		}
		# Do we have something to dequeue or is the queue full enough?
		if ($queue_done->pending()>0 or $sent_rows-$retrieved_rows>=$max_queued)
		{
			# get our record
			 my $payload=$queue_done->dequeue();
			 push @new_rows,($payload);

			$retrieved_rows++;
		}
		last if ($retrieved_rows==$total_rows);
	}
	return (\@new_rows);
}

# This gets messages from one queue, pass it to format data, and queue the response
sub worker_format_data
{
	my ($self,$thread_number)=@_;
	my $counter=0;
	my $queue_todo=$self->{queue_todo_bytea};
	my $queue_done=$self->{queue_done_bytea};
	while (1) {
		my $payload=$queue_todo->dequeue();
		last unless (defined $payload);
		# The received payload is an array of a columns array, types array, type of action and source types array, table name
		$self->format_data_row($payload->[0],$payload->[1],$payload->[2], $payload->[3], $payload->[4], $payload->[5]);
		$queue_done->enqueue($payload->[0]); # We only need data
		$counter++;
	}
	return 0;
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
			$row->[$idx] = $self->format_data_type($row->[$idx], $data_type, $action);
		}
	}
}

sub format_data_type
{
	my ($self, $col, $data_type, $action, $table) = @_;

	# Preparing data for output
	if ($action ne 'COPY') {
		if ($col eq '') {
			$col = 'NULL';
		} elsif ($data_type eq 'bytea') {
			$col = escape_bytea($col);
			if (!$self->{standard_conforming_strings}) {
				$col = "'$col'";
			} else {
				$col = "E'$col'";
			}
		} elsif ($data_type =~ /(char|text|xml)/) {
			$col =~ s/'/''/gs; # double single quote
			if (!$self->{standard_conforming_strings}) {
				$col =~ s/\\/\\\\/g;
				$col =~ s/\0//gs;
				$col = "'$col'";
			} else {
				$col =~ s/\0//gs;
				$col = "E'$col'";
			}
		} elsif ($data_type =~ /(date|time)/) {
			if ($col =~ /^0000-00-00/) {
				$col = 'NULL';
			} else {
				$col = "'$col'";
			}
		} elsif ($data_type eq 'boolean') {
			$col = "." . ($self->{ora_boolean_values}{lc($col)} || $col) . "'";
		} else {
			$col =~ s/,/\./;
			$col =~ s/\~/inf/;
		}
	} else {
		if ($col eq '') {
			$col = '\N';
		} elsif ($data_type eq 'boolean') {
			$col = ($self->{ora_boolean_values}{lc($col)} || $col);
		} elsif ($data_type !~ /(char|date|time|text|bytea|xml)/) {
			$col =~ s/,/\./;
			$col =~ s/\~/inf/;
		} elsif ($data_type eq 'bytea') {
			$col = escape_bytea($col);
		} elsif ($data_type !~ /(date|time)/) {
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
			$self->{zlib_hdl}->gzwrite($data) or $self->logit("FATAL: error writing compressed data\n", 0, 1);
		} else {
			$fh->gzwrite($data) or $self->logit("FATAL: error writing compressed data\n", 0, 1);
		}
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
		$l =~ s///gs;
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
		} elsif (!grep(/^$var$/, 'TABLES', 'ALLOW', 'MODIFY_STRUCT', 'REPLACE_TABLES', 'REPLACE_COLS', 'WHERE', 'EXCLUDE','VIEW_AS_TABLE','ORA_RESERVED_WORDS','SYSUSERS','REPLACE_AS_BOOLEAN','BOOLEAN_VALUES')) {
			$AConfig{$var} = $val;
		} elsif ( ($var eq 'TABLES') || ($var eq 'ALLOW') || ($var eq 'EXCLUDE') || ($var eq 'VIEW_AS_TABLE') ) {
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
				$AConfig{$var}{lc($no)} = 't';
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
		if ($self->{file_per_function} && !$self->{dbhdest}) {
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
			$content .= "-- Porting cost of package \L$pname\E: $self->{pkgcost}\n" if ($self->{pkgcost});
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
		return if ($self->skip_this_object('FUNCTION', $func_name));

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
				$self->logit("\tParsing function $self->{schema}\.$func_name...\n", 1);
			} else {
				$function = "\nCREATE OR REPLACE FUNCTION \"$self->{schema}\"\.$func_name $func_args";
				$self->logit("\tParsing function \"$self->{schema}\"\.$func_name...\n", 1);
			}
		} else {
			$self->logit("\tParsing function $func_name...\n", 1);
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
		if ($self->{estimate_cost}) {
			$func_name =~ s/"//g;
			my $cost = Ora2Pg::PLSQL::estimate_cost($function);
			$function .= "-- Porting cost of function \L$func_name\E: $cost\n" if ($cost);
			$self->{pkgcost} += ($cost || 0);
		}
		$function = "\n$func_before$function";

		if ($pname && $self->{file_per_function} && !$self->{dbhdest}) {
			$func_name =~ s/^"*$pname"*\.//i;
			$func_name =~ s/"//g; # Remove case sensitivity quoting
			$self->logit("\tDumping to one file per function: $dirprefix\L$pname/$func_name\E_$self->{output}\n", 1);
			my $sql_header = "-- Generated by Ora2Pg, the Oracle database Schema converter, version $VERSION\n";
			$sql_header .= "-- Copyright 2000-2012 Gilles DAROLD. All rights reserved.\n";
			$sql_header .= "-- DATASOURCE: $self->{oracle_dsn}\n\n";
			if ($self->{client_encoding}) {
				$sql_header .= "SET client_encoding TO '\U$self->{client_encoding}\E';\n";
			}
			my $search_path = '';
			if ($self->{export_schema}) {
				if ($self->{pg_schema}) {
					if (!$self->{preserve_case}) {
						$search_path = "SET search_path = \L$self->{pg_schema}\E;\n";
					} else {
						$search_path = "SET search_path = \"$self->{pg_schema}\";\n";
					}
				} elsif ($self->{schema}) {
					if (!$self->{preserve_case}) {
						$search_path = "SET search_path = \L$self->{schema}\E, pg_catalog;\n";
					} else {
						$search_path = "SET search_path = \"$self->{schema}\", pg_catalog;\n";
					}
				}
			}
			$sql_header .= "$search_path\n";

			my $fhdl = $self->export_file("$dirprefix\L$pname/$func_name\E_$self->{output}", 1);
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


	# Add missing AS in column alias => optional in Oracle
	# and requiered in PostgreSQL
# THIS PART IS REMOVED AS PG since 8.4 at least support optional AS for alias
#	if ($sqlstr =~ /(.*?)\bFROM\b(.*)/is) {
#		my $item = $1;
#		my $tmp = $2;
#		# Disable coma between brackets
#		my $i = 0;
#		my @repstr = ();
#		while ($item =~ s/(\([^\(\)]+\))/\@REPLACEME${i}HERE\@/s) {
#			push(@repstr, $1);
#			$i++;
#		}
#		$item =~ s/([a-z0-9_\$]+)([\t\s]+[a-z0-9_\$]+,)/$1 AS$2/igs;
#		$item =~ s/([a-z0-9_\$]+)([\t\s]+[a-z0-9_\$]+)$/$1 AS$2/igs;
#		$item =~ s/[\t\s]AS[\t\s]+as\b/ AS/igs;
#		$sqlstr = $item . ' FROM ' . $tmp;
#		while($sqlstr =~ s/\@REPLACEME(\d+)HERE\@/$repstr[$1]/sg) {};
#	}
#
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
		$sqlstr = Ora2Pg::PLSQL::plsql_to_plpgsql($sqlstr, $self->{allow_code_break},$self->{null_equal_empty});
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
			return '';
		}
	} elsif ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+(AS|IS)[\t\s]*OBJECT[\t\s]+\((.*?)(TYPE BODY.*)/is) {
		$self->{type_of_type}{'Type Boby'}++;
		$self->logit("WARNING: TYPE BODY are not supported, skipping type $1\n", 1);
		return '';
	} elsif ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+(AS|IS)[\t\s]*OBJECT[\t\s]+\((.*)\)([^\)]*)/is) {
		my $type_name = $1;
		my $description = $3;
		my $notfinal = $4;
		$notfinal =~ s/[\s\t\r\n]+/ /gs;
		if ($description =~ /[\s\t]*(MAP MEMBER|MEMBER|CONSTRUCTOR)[\t\s]+(FUNCTION|PROCEDURE).*/is) {
			$self->{type_of_type}{'Type with member method'}++;
			$self->logit("WARNING: TYPE with CONSTRUCTOR and MEMBER FUNCTION are not supported, skipping type $type_name\n", 1);
			return '';
		}
		$description =~ s/^[\s\t\r\n]+//s;
		my $declar = Ora2Pg::PLSQL::replace_sql_type($description, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});
		$type_name =~ s/"//g;
		if ($notfinal =~ /FINAL/is) {
			$content = "-- Inherited types are not supported in PostgreSQL, replacing with inherited table\n";
			$content .= qq{CREATE TABLE \"\L$type_name\E\" (
$declar
);
};
			$self->{type_of_type}{'Type inherited'}++;
		} else {
			$content = qq{
CREATE TYPE \L$type_name\E AS (
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
			return '';
		}
		$description =~ s/^[\s\t\r\n]+//s;
		my $declar = Ora2Pg::PLSQL::replace_sql_type($description, $self->{pg_numeric_type}, $self->{default_numeric}, $self->{pg_integer_type});
		$type_name =~ s/"//g;
		$content = qq{
CREATE TABLE \"\L$type_name\E\" (
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
		$content = "CREATE $plsql;"
	}
	return $content;
}

sub extract_data
{
	my ($self, $table, $s_out, $nn, $tt, $fhdl, $sprep, $stt) = @_;

	my $total_record = 0;
	$self->{data_limit} ||= 10000;

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

	$self->logit("Looking for data from $table...\n", 1);
	my $sth = $self->_get_data($table, $nn, $tt, $stt);

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
	my $start_time = time();
	my $total_row = $self->{tables}{$table}{table_info}->[3];
	$self->logit("Fetching all data from $table...\n", 1);
	while ( my $rows = $sth->fetchall_arrayref(undef,$self->{data_limit})) {

		my $sql = '';
		if ($self->{type} eq 'COPY') {
			$sql = $s_out;
		}
		# Preparing data for output
		$self->logit("DEBUG: Preparing bulk of $self->{data_limit} data for output\n", 1);
		if (!defined $sprep) {
			# If there is a bytea column
			if ($self->{thread_count} && scalar(grep(/bytea/,@$tt))) {
				$self->logit("DEBUG: Parallelizing this formatting, as it is costly (bytea found)\n", 1);
				$rows = $self->format_data_parallel($rows, $tt, $self->{type}, $stt, \%user_type, $table);
				# we change $rows reference!
			} else {
				$self->format_data($rows, $tt, $self->{type}, $stt, \%user_type, $table);
			}
		}
		# Creating output
		$self->logit("DEBUG: Creating output for $self->{data_limit} tuples\n", 1);
		my $count = 0;
		if ($self->{type} eq 'COPY') {
			if ($self->{dbhdest}) {
				$self->logit("DEBUG: Sending COPY bulk output directly to PostgreSQL backend\n", 1);
				my $s = $self->{dbhdest}->do($sql) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				$sql = '';
				foreach my $row (@$rows) {
					$count++;
					$s = $self->{dbhdest}->pg_putcopydata(join("\t", @$row) . "\n") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				}
				$s = $self->{dbhdest}->pg_putcopyend() or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
			} else {
				map { $sql .= join("\t", @$_) . "\n"; $count++; } @$rows;
				$sql .= "\\.\n";
			}
		} elsif (!defined $sprep) {
			foreach my $row (@$rows) {
				$sql .= $s_out;
				$sql .= join(',', @$row) . ");\n";
				$count++;
			}
		}

		$self->logit("DEBUG: Dumping output of total size of " . length($sql) . "\n", 1);

		# Insert data if we are in online processing mode
		if ($self->{dbhdest}) {
			if ($self->{type} ne 'COPY') {
				if (!defined $sprep) {
					$self->logit("DEBUG: Sending INSERT output directly to PostgreSQL backend\n", 1);
					my $s = $self->{dbhdest}->do($sql) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					my $ps = $self->{dbhdest}->prepare($sprep) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					for (my $i = 0; $i <= $#{$tt}; $i++) {
						if ($tt->[$i] eq 'bytea') {
							$ps->bind_param($i+1, undef, { pg_type => DBD::Pg::PG_BYTEA });
						}
					}
					$self->logit("DEBUG: Sending INSERT bulk output directly to PostgreSQL backend\n", 1);
					foreach my $row (@$rows) {
						$ps->execute(@$row) or $self->logit("FATAL: " . $ps->errstr . "\n", 0, 1);
						$count++;
					}
					$ps->finish();
				}
			}
		} else {
			$self->dump($sql, $fhdl);
		}

		my $end_time = time();
		my $dt = $end_time - $start_time;
		$start_time = $end_time;
		$dt ||= 1;
		my $rps = sprintf("%2.1f", $count / ($dt+.0001));
		$total_record += $count;
		if (!$self->{quiet} && !$self->{debug}) {
			print STDERR $self->progress_bar($total_record, $total_row, 25, '=', 'rows', "table $table ($rps recs/sec)");
		} elsif ($self->{debug}) {
			$self->logit("Total extracted records from table $table: $total_record\n", 1);
			if ($dt > 0) {
				$self->logit("$count records in $dt secs = $rps recs/sec\n", 1);
			} else {
				$self->logit("$count records in $dt secs\n", 1);
			}
		}
	}
	$sth->finish();
	if (!$self->{quiet}) {
		print STDERR "\n";
	}

	return $total_record;
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
	 # In this function, we use the array built by build_escape_bytea
	 my @array= unpack("C*",$_[0]);
	 foreach my $elt(@array)
	 {
		 $elt=$bytea_array[$elt];
	 }
	 return join('',@array);
}

=head2 _show_infos

This function display a list of schema, table or column only to stdout.

=cut

sub _show_infos
{
	my ($self, $type) = @_;

	if ($type eq 'SHOW_ENCODING') {
		$self->logit("Showing Oracle encoding...\n", 1);
		my $encoding = $self->_get_encoding();
		$self->logit("NLS_LANG $encoding\n", 0);
		$self->logit("CLIENT ENCODING $self->{client_encoding}\n", 0);
	} elsif ($type eq 'SHOW_VERSION') {
		$self->logit("Showing Oracle Version...\n", 1);
		my $ver = $self->_get_version();
		$self->logit("$ver\n", 0);
	} elsif ($type eq 'SHOW_REPORT') {
		my $ver = $self->_get_version();
		my $size = $self->_get_database_size();
		$self->logit("Reporting Oracle Content...\n", 1);
		my %report = $self->_get_report();
		$self->logit("--------------------------------------\n", 0);
		$self->logit("Ora2Pg: Oracle Database Content Report\n", 0);
		$self->logit("--------------------------------------\n", 0);
		$self->logit("Version\t$ver\n", 0);
		$self->logit("Schema\t$self->{schema}\n", 0);
		$self->logit("Size\t$size\n\n", 0);

		# Determining how many non automatiques indexes will be exported
		my %all_indexes = ();
		$self->{skip_fkeys} = $self->{skip_indices} = $self->{skip_indexes} = $self->{skip_checks} = 0;
		$self->{view_as_table} = ();
		$self->_tables(1);
		my $total_index = 0;
		foreach my $table (sort keys %{$self->{tables}}) {
			push(@exported_indexes, $self->_exportable_indexes($table, %{$self->{tables}{$table}{indexes}}));
			foreach my $idx (sort keys %{$self->{tables}{$table}{idx_type}}) {
				next if (!grep(/^$idx$/i, @exported_indexes));
				my $typ = $self->{tables}{$table}{idx_type}{$idx};
				push(@{$all_indexes{$typ}}, $idx);
				$total_index++;
			}
		}
		$self->_types();
		foreach my $tpe (sort {length($a->{name}) <=> length($b->{name}) } @{$self->{types}}) {
			$self->_convert_type($tpe->{code});
		}

		my $cost_header = '';
		$cost_header = "\tEstimated cost" if ($self->{estimate_cost});
		$self->logit("--------------------------------------\n", 0);
		$self->logit("Object\tNumber\tInvalid$cost_header\tComments\n", 0);
		$self->logit("--------------------------------------\n", 0);
		my $total_cost_value = 0;
		my $total_object_invalid = 0;
		my $total_object_number = 0;
		foreach my $typ (sort keys %report) {
			next if ($typ eq 'PACKAGE');
			my $number = 0;
			my $invalid = 0;
			for (my $i = 0; $i <= $#{$report{$typ}}; $i++) {
				$number++;
				$invalid++ if ($report{$typ}[$i]->{invalid});
			}
			$total_object_invalid += $invalid;
			$total_object_number += $number;
			my $comment = '';
			my $cost_value = 0;
			my $real_number = 0;
			my $detail = '';
			if ($number > 0) {
				$real_number = ($number-$invalid);
				$real_number = $number if ($self->{export_invalid});
			}
			$cost_value = ($real_number*$Ora2Pg::PLSQL::OBJECT_SCORE{$typ}) if ($self->{estimate_cost});
			if ($typ eq 'INDEX') {
				my $bitmap = 0;
				foreach my $t (sort keys %INDEX_TYPE) {
					my $len = ($#{$all_indexes{$t}}+1);
					$detail .= ". $len $INDEX_TYPE{$t} index(es)" if ($len);
					if ($self->{estimate_cost} && $len && ( ($t =~ /FUNCTION.*NORMAL/) || ($t eq 'FUNCTION-BASED BITMAP') ) ) {
						$cost_value += ($len * $Ora2Pg::PLSQL::OBJECT_SCORE{'FUNCTION-BASED-INDEX'});
					}
					if ($self->{estimate_cost} && $len && ($t =~ /REV/)) {
						$cost_value += ($len * $Ora2Pg::PLSQL::OBJECT_SCORE{'REV-INDEX'});
					}
				}
				$cost_value = ($Ora2Pg::PLSQL::OBJECT_SCORE{$typ}*$total_index) if ($self->{estimate_cost});
				$comment = "$total_index index(es) are concerned by the export, others are automatically generated and will do so on PostgreSQL";
				$comment .= $detail;
				$comment .= ". Note that bitmap index(es) will be exported as b-tree index(es) if any. Cluster, domain, bitmap join and IOT indexes will not be exported at all. Reverse indexes are not exported too, you may use a trigram-based index (see pg_trgm) or a reverse() function based index and search.";
				$comment .= " You may also use 'varchar_pattern_ops', 'text_pattern_ops' or 'bpchar_pattern_ops' operators in your indexes to improve search with the LIKE operator respectively into varchar, text or char columns.";
			} elsif ($typ eq 'MATERIALIZED VIEW') {
				$comment = "All materialized view will be exported as snapshot materialized views, they are only updated when fully refreshed.";
			} elsif ($typ eq 'TABLE') {
				my $exttb = scalar keys %{$self->{external_table}};
				if ($exttb) {
					if (!$self->{external_to_fdw}) {
						$comment = "$exttb external table(s) will be exported as standard table. See EXTERNAL_TO_FDW configuration directive to export as file_fdw foreign tables or use COPY in your code if you just want to load data from external files.";
					} else {
						$comment = "$exttb external table(s) will be exported as file_fdw foreign table. See EXTERNAL_TO_FDW configuration directive to export as standard table or use COPY in your code if you just want to load data from external files.";
					}
				}

				my $sth = $self->_table_info()  or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
				my @tables_infos = $sth->fetchall_arrayref();
				$sth->finish();

				my %table_detail = ();
				my $virt_column = 0;
				my @done = ();
				my $id = 0;
				my $total_check = 0;
				foreach my $table (@tables_infos) {
					# Set the table information for each class found
					my $i = 1;
					foreach my $t (@$table) {

						# forget or not this object if it is in the exclude or allow lists.
						next if ($self->skip_this_object('TABLE', $t->[2]));

						# Jump to desired extraction
						if (grep(/^$t->[2]$/, @done)) {
							next;
						} else {
							push(@done, $t->[2]);
						}
						if (&is_reserved_words($t->[2])) {
							$table_detail{'reserved words in table name'}++;
						}
						# Set the fields information
						@{$self->{tables}{$t->[1]}{column_info}} = $self->_column_info($t->[2],$t->[1], 1);
						foreach my $d (@{$self->{tables}{$t->[1]}{column_info}}) {
							if (&is_reserved_words($d->[0])) {
								$table_detail{'reserved words in column name'}++;
							}
							$d->[1] =~ s/TIMESTAMP\(\d+\)/TIMESTAMP/i;
							if (!exists $TYPE{uc($d->[1])}) {
								$table_detail{'unknow types'}++;
							}
							if ( (uc($d->[1]) eq 'NUMBER') && ($d->[2] eq '') ) {
								$table_detail{'numbers with no precision'}++;
							}
							if ( $TYPE{uc($d->[1])} eq 'bytea' ) {
								$table_detail{'binary columns'}++;
							}
						}

						%{$self->{tables}{$t->[2]}{check_constraint}} = $self->_check_constraint($t->[2],$t->[1]);
						my @constraints = $self->_lookup_check_constraint($t->[2], $self->{tables}{$t->[2]}{check_constraint},$self->{tables}{$t->[2]}{field_name});
						$total_check += ($#constraints + 1);
						if ($self->{estimate_cost} && ($#constraints >= 0)) {
							$cost_value += (($#constraints + 1) * $Ora2Pg::PLSQL::OBJECT_SCORE{'CHECK'});
						}
					}
				}
				$comment .= " $total_check check constraint(s)." if ($total_check);
				foreach my $d (sort keys %table_detail) {
					$comment .= " $table_detail{$d} $d.";
				}
				$comment = "Nothing particular." if (!$comment);
			} elsif ($typ eq 'TYPE') {
				my $detail = '';
				my $total_type = 0;
				foreach my $t (sort keys %{$self->{type_of_type}}) {
					$total_type++ if (!grep(/^$t$/, 'Associative Arrays','Type Boby','Type with member method'));
					$detail .= ". $self->{type_of_type}{$t} $t" if ($self->{type_of_type}{$t});
				}
				$cost_value = ($Ora2Pg::PLSQL::OBJECT_SCORE{$typ}*$total_type) if ($self->{estimate_cost});
				$comment = "$total_type type(s) are concerned by the export, others are not supported";
				$comment .= $detail;
				$comment .= '. Note that Type inherited and Subtype are converted as table, type inheritance is not supported.';
			} elsif ($typ eq 'TYPE BODY') {
				$comment = "Export of type with member method are not supported, they will not be exported.";
			} elsif ($typ eq 'TRIGGER') {
				my $triggers = $self->_get_triggers();
				my $total_size = 0;
				foreach my $trig (@{$triggers}) {
					$total_size += length($trig->[4]);
					if ($self->{estimate_cost}) {
						my $cost = Ora2Pg::PLSQL::estimate_cost($trig->[4]);
						$cost_value += $cost;
						$detail .= "$trig->[0]: $cost, ";
					}
				}
				$comment = "Total size of trigger code: $total_size bytes.";
				if ($detail) {
					$detail =~ s/, $/./;
					$comment .= " " . $detail;
				}
			} elsif ($typ eq 'SEQUENCE') {
				$comment = "Sequences are fully supported, but all call to sequence_name.NEXTVAL or sequence_name.CURRVAL will be transformed into NEXTVAL('sequence_name') or CURRVAL('sequence_name').";
			} elsif ($typ eq 'FUNCTION') {
				my $functions = $self->_get_functions();
				my $total_size = 0;
				foreach my $fct (keys %{$functions}) {
					$total_size += length($functions->{$fct});
					if ($self->{estimate_cost}) {
						my $cost = Ora2Pg::PLSQL::estimate_cost($functions->{$fct});
						$cost_value += $cost;
						$detail .= "$fct: $cost, ";
					}
				}
				$comment = "Total size of function code: $total_size bytes.";
				if ($detail) {
					$detail =~ s/, $/./;
					$comment .= " " . $detail;
				}
			} elsif ($typ eq 'PROCEDURE') {
				my $procedures = $self->_get_procedures();
				my $total_size = 0;
				foreach my $proc (keys %{$procedures}) {
					$total_size += length($procedures->{$proc});
					if ($self->{estimate_cost}) {
						my $cost = Ora2Pg::PLSQL::estimate_cost($procedures->{$proc});
						$cost_value += $cost;
						$detail .= "$proc: $cost, ";
					}
				}
				$comment = "Total size of procedure code: $total_size bytes.";
				if ($detail) {
					$detail =~ s/, $/./;
					$comment .= " " . $detail;
				}
			} elsif ($typ eq 'PACKAGE BODY') {
				my $packages = $self->_get_packages();
				my $total_size = 0;
				my $number_fct = 0;
				foreach my $pkg (keys %{$packages}) {
					next if (!$packages->{$pkg});
					$total_size += length($packages->{$pkg});
					my @codes = split(/CREATE(?: OR REPLACE)? PACKAGE BODY/, $packages->{$pkg});
					foreach my $txt (@codes) {
						my %infos = $self->_lookup_package("CREATE OR REPLACE PACKAGE BODY$txt");
						foreach my $f (sort keys %infos) {
							next if (!$f);
							if ($self->{estimate_cost}) {
								my $cost = Ora2Pg::PLSQL::estimate_cost($infos{$f});
								$cost_value += $cost;
								$detail .= "$f: $cost, ";
							}
							$number_fct++;
						}
					}
					$cost_value += ($number_fct*$Ora2Pg::PLSQL::OBJECT_SCORE{'FUNCTION'}) if ($self->{estimate_cost});
				}
				$comment = "Total size of package code: $total_size bytes. Number of procedures and functions found inside those packages: $number_fct";
				if ($detail) {
					$detail =~ s/, $/./;
					$comment .= ". " . $detail;
				}
			} elsif ($typ eq 'SYNONYME') {
				$comment = "SYNONYME are not exported at all. An usual workaround is to use View instead or set the PostgreSQL search_path in your session to access object outside the current schema.";
			} elsif ($typ eq 'TABLE PARTITION') {
				my %partitions = $self->_get_partitions_list();
				my $detail = '';
				foreach my $t (sort keys %partitions) {
					$detail .= ". $partitions{$t} $t partitions";
				}
				$comment = "Partitions are exported using table inheritance and check constraint";
				$comment .= $detail;
				$comment .= ". Note that Hash partitions are not supported.";
			} elsif ($typ eq 'CLUSTER') {
				$comment = "Clusters are not supported and will not be exported.";
			} elsif ($typ eq 'VIEW') {
				$comment = "Views are fully supported, but if you have updatable views you will need to use INSTEAD OF triggers.";
			}
			$total_cost_value += $cost_value;
			if ($self->{estimate_cost}) {
				$self->logit("$typ\t" . ($number-$invalid) . "\t$invalid\t$cost_value\t$comment\n", 0);
			} else {
				$self->logit("$typ\t" . ($number-$invalid) . "\t$invalid\t$comment\n", 0);
			}
		}
		my %dblink = $self->_get_dblink();
		my $ndlink = scalar keys %dblink;
		$total_object_number += $ndlink;
		my $comment = "Database links will not be exported. You may try the dblink perl contrib module or use the SQL/MED PostgreSQL features with the different Foreign Data Wrapper (FDW) extentions.";
		if ($self->{estimate_cost}) {
			$cost_value = ($Ora2Pg::PLSQL::OBJECT_SCORE{'DATABASE LINK'}*$ndlink);
			$self->logit("DATABASE LINK\t$ndlink\t0\t$cost_value\t$comment\n", 0);
			$total_cost_value += $cost_value;
		} else {
			$self->logit("DATABASE LINK\t$ndlink\t0\t$comment\n", 0);
		}

		my %jobs = $self->_get_job();
		my $njob = scalar keys %jobs;
		$total_object_number += $njob;
		$comment = "Job are not exported. You may set external cron job with them.";
		if ($self->{estimate_cost}) {
			$cost_value = ($Ora2Pg::PLSQL::OBJECT_SCORE{'JOB'}*$ndlink);
			$self->logit("JOB\t$njob\t0\t$cost_value\t$comment\n", 0);
			$total_cost_value += $cost_value;
		} else {
			$self->logit("JOB\t$njob\t0\t$comment\n", 0);
		}
		$self->logit("--------------------------------------\n", 0);
		if ($self->{estimate_cost}) {
			my $human_cost = $self->_get_human_cost($total_cost_value);
			$comment = "$total_cost_value cost migration units means approximatively $human_cost.\n";
			$self->logit("Total\t$total_object_number\t$total_object_invalid\t$total_cost_value\t$comment\n", 0);
		} else {
			$self->logit("Total\t$total_object_number\t$total_object_invalid\n", 0);
		}
		$self->logit("--------------------------------------\n", 0);
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

		my $sth = $self->_table_info()  or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		my @tables_infos = $sth->fetchall_arrayref();
		$sth->finish();

		my @done = ();
		my $id = 0;
		foreach my $table (@tables_infos) {
			# Set the table information for each class found
			my $i = 1;
			foreach my $t (@$table) {

				# forget or not this object if it is in the exclude or allow lists.
				if ($self->{tables}{$t->[2]}{type} ne 'view') {
					next if ($self->skip_this_object('TABLE', $t->[2]));
				}

				# Jump to desired extraction
				if (grep(/^$t->[2]$/, @done)) {
					$self->logit("Duplicate entry found: $t->[0] - $t->[1] - $t->[2]\n", 1);
					next;
				} else {
					push(@done, $t->[2]);
				}
				my $warning = '';
				if (&is_reserved_words($t->[2])) {
					$warning = " (Warning: '$t->[2]' is a reserved word in PostgreSQL)";
				}

				$self->logit("[$i] TABLE $t->[2] ($t->[5] rows)$warning\n", 0);

				# Set the fields information
				if ($type eq 'SHOW_COLUMN') {
					@{$self->{tables}{$t->[1]}{column_info}} = $self->_column_info($t->[2],$t->[1], 1);
					foreach my $d (@{$self->{tables}{$t->[1]}{column_info}}) {
						my $type = $self->_sql_type($d->[1], $d->[2], $d->[5], $d->[6]);
						$type = "$d->[1], $d->[2]" if (!$type);
						my $len = $d->[2];
						if ($#{$d} == 7) {
							$d->[2] = $d->[7] if $d->[1] =~ /char/i;
						}
						$self->logit("\t$d->[0] : $d->[1]");
						if ($d->[2] && !$d->[5]) {
							$self->logit("($d->[2])");
						} elsif ($d->[5] && ($d->[1] =~ /NUMBER/i) ) {
							$self->logit("($d->[5]");
							$self->logit("$d->[6]") if ($d->[6]);
							$self->logit(")");
						}
						$warning = '';
						if (&is_reserved_words($d->[0])) {
							$warning = " (Warning: '$d->[0]' is a reserved word in PostgreSQL)";
						}
						# Check if this column should be replaced by a boolean following table/column name
						if (grep(/^$d->[0]$/i, @{$self->{'replace_as_boolean'}{$t->[2]}})) {
							$type = 'boolean';
						# Check if this column should be replaced by a boolean following type/precision
						} elsif (exists $self->{'replace_as_boolean'}{uc($d->[1])} && ($self->{'replace_as_boolean'}{uc($d->[1])}[0] == $d->[5])) {
							$type = 'boolean';
						}
						$self->logit(" => $type$warning\n");
					}
				}
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
	my $sql = "SELECT sum(bytes)/1024/1024 FROM DBA_DATA_FILES";
        my $sth = $self->{dbh}->prepare( $sql ) or return undef;
        $sth->execute or return undef;
	while ( my @row = $sth->fetchrow()) {
		$mb_size = sprintf("%.2f MB", $row[0]);
		last;
	}
	$sth->finish();

	return $mb_size;
}

=head2 _get_report

This function retrieves the Oracle content information

=cut

sub _get_report
{
	my $self = shift;

	my $oraver = '';
	# OWNER|OBJECT_NAME|SUBOBJECT_NAME|OBJECT_ID|DATA_OBJECT_ID|OBJECT_TYPE|CREATED|LAST_DDL_TIME|TIMESTAMP|STATUS|TEMPORARY|GENERATED|SECONDARY
	my $sql = "SELECT OBJECT_NAME,OBJECT_TYPE,STATUS FROM $self->{prefix}_OBJECTS WHERE TEMPORARY='N' AND GENERATED='N' AND SECONDARY='N'";
        if ($self->{schema}) {
                $sql .= " AND upper(OWNER)='\U$self->{schema}\E'";
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

=head2 _get_encoding

This function retrieves the Oracle database encoding

Returns a handle to a DB query statement.

=cut

sub _get_encoding
{
	my $self = shift;

	my $sql = "SELECT * FROM NLS_DATABASE_PARAMETERS";
        my $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute() or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
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
        $sth = $self->{dbh}->prepare($sql) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
        $sth->execute() or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
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
	my ($self, $schema) = @_;

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
		my $sth = $self->{dbh}->do($qcomp) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
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
	my $str = sprintf(
		"[%-${width}s] dumped %${num_width}s of %s $kind (%.1f%%) $msg",
		$char x (($width - 1) * $ratio) . '>',
		$got, $total, 100 * $ratio
	);
	my $len = length($str);
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

	# Check if this object is in the allowed list of object to export.
	return 1 if (($#{$self->{limited}} >= 0) && !grep($name =~ /^$_$/i, @{$self->{limited}}));

	# Check if this object is in the exlusion list of object to export.
	return 2 if (($#{$self->{excluded}} >= 0) && grep($name =~ /^$_$/i, @{$self->{excluded}}));

	return 0;
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
	my ($self, $table, $check_constraint, $field_name) = @_;

	my  @chk_constr = ();

	my $tbsaved = $table;
	if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
		$table = $self->{replaced_tables}{"\L$table\E"};
	}

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
			if (exists $self->{replaced_cols}{"\L$tbsaved\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}) {
				foreach my $c (keys %{$self->{replaced_cols}{"\L$tbsaved\E"}}) {
					$chkconstraint =~ s/"$c"/"$self->{replaced_cols}{"\L$tbsaved\E"}{$c}"/gsi;
					$chkconstraint =~ s/\b$c\b/$self->{replaced_cols}{"\L$tbsaved\E"}{$c}/gsi;
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
				$table = $self->quote_reserved_words($table);
				push(@chk_constr,  "ALTER TABLE \L$table\E ADD CONSTRAINT \L$k\E CHECK ($chkconstraint);\n");
			} else {
				push(@chk_constr,  "ALTER TABLE \"$table\" ADD CONSTRAINT $k CHECK ($chkconstraint);\n");
			}
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

sub _get_human_cost
{
	my ($self, $total_cost_value) = @_;

	return 0 if (!$total_cost_value);

	my $human_cost = $total_cost_value * $self->{cost_unit_value};
	if ($human_cost >= 420) {
		my $tmp = $human_cost/420;
		$tmp++ if ($tmp =~ s/\.\d+//);
		$human_cost = "$tmp man day(s)";
	} else {
		my $tmp = $human_cost/60;
		$tmp++ if ($tmp =~ s/\.\d+//);
		$human_cost = "$tmp man hour(s)";
	} 

	return $human_cost;
}

1;

__END__


=head1 AUTHOR

Gilles Darold <gilles _AT_ darold _DOT_ net>


=head1 COPYRIGHT

Copyright (c) 2000-2012 Gilles Darold - All rights reserved.

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


