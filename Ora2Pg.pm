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

$VERSION = '8.13';
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
);


=head1 NAME

Ora2Pg - Oracle to PostgreSQL database schema converter


=head1 SYNOPSIS

Ora2pg has a companion script called ora2pg. When use in
conjonction with a custom version of ora2pg.conf they perform what
I'm trying to explain bellow. See content of the ora2pg.conf
file for more explanation on configuration directives.

	use Ora2Pg;

	# Create an instance of the Ora2Pg Perl module
	my $schema = new Ora2Pg (config => './ora2pg.conf');

	# Create a PostgreSQL representation of Oracle export
	# you've defined in ora2pg.conf.
	$schema->export_schema();

	exit(0);

You can always overwrite any configuration option set in ora2pg.conf
by passing a hash where keys are the same that in configuration file
but in lower case. For example, if you only want to extract only a
selection of tables:

	my @tables = ('t1', 't2', 't3');
	my $schema = new Ora2Pg (
		datasource => $dbsrc,   # Oracle DBD datasource
		user => $dbuser,        # Database user
		password => $dbpwd,     # Database password
		tables => \@tables,
	# or				
	#	tables => [('tab1','tab2')],  # Tables to extract
		debug => 1		      # Verbose running.
	);

To choose a particular Oracle schema to export just set the following option
to your schema name:

	schema => 'APPS'

This schema definition can also be needed when you want to export data. If
export fails and complaining that the table doesn't exists use this directive
to prefix the table name by the schema name.

If you want to use PostgreSQL 7.4+ schema support set the init option
'export_schema' set to 1. Default is no schema export.

You can process multiple types of extractions at the same time by setting the
value to a space separated list of the following keywords.

To extract all views set the type option as follows:

	type => 'VIEW'

To extract all grants set the type option as follows:

	type => 'GRANT'

To extract all sequences set the type option as follows:

	type => 'SEQUENCE'

To extract all triggers set the type option as follows:

	type => 'TRIGGER'

To extract all functions set the type option as follows:

	type => 'FUNCTION'

To extract all procedures set the type option as follows:

	type => 'PROCEDURE'

To extract all packages and packages bodies set the type option as follows:

	type => 'PACKAGE'

Default is table extraction:

	type => 'TABLE'

To extract table and index tablespaces (PostgreSQL >= v8):

	type => 'TABLESPACE'

To extract table range or list partition (PostgreSQL >= v8.4):

	type => 'PARTITION'

To extract user defined Oracle type

	type => 'TYPE'

To extract table datas as INSERT statements use:

	type => 'DATA'

To extract table datas as COPY statements use:

	type => 'COPY'

and set data_limit => n to specify the bulk size of tuples to be return at once.
If you set this option to 0 or nothing, data_limit will be forced to 10000.

Oracle export is done by calling method:

	$schema->export_schema();

The extracted data is dumped to filename specified in the OUTPUT configuration
directive or to stdout if it's set to nothing. You can always overwrite this
configuration value by specifying a filename as argument of this function.
If you want to dump files to a specific directory set the OUTPUT_DIR directive
to the destination directory.

You can also send the data directly to a PostgreSQL backend by setting PG_DSN,
PG_USER and PG_PWD configuration directives. This feature is only available for
COPY or DATA export types. The data will not be sent via DBD::Pg but will be
loaded to the PG database using the psql command.
Edit the $PSQL environment variable to specify the path of your psql command
(nothing to edit if psql is in your path).

When copying tables, Ora2Pg normally exports constraints as they are;
if they are non-deferrable they will be exported as non-deferrable.
However, non-deferrable constraints will probably cause problems when
attempting to import data to PostgreSQL. The option:

       fkey_deferrable => 1

will cause all foreign key constraints to be exported as deferrable,
even if they are non-deferrable.

In addition, for data export, setting:

       defer_fkey => 1

will export all data in a transaction and set all constraints deferred.
This imply that all constraints have been created 'deferrable' otherwise
it will not works. Than if this fail the ultimate solution is:

	drop_fkey => 1

will add a command to actually drop all foreign constraints
before importing data and recreate them at the end of the import.

If you want to gain speed during fresh data import, use the following:

	drop_indexes => 1

Ora2Pg will drop any table indexes that is not an automatic primary key
index and recreate them at end of the import.

To non perl gurus, you can use the configuration file and run ora2pg as is.
You will find all information into the ora2pg.conf to be able to set it
correctly.


=head1 DESCRIPTION

Ora2Pg is a perl OO module used to export an Oracle database schema
to a PostgreSQL compatible schema.

It simply connects to your Oracle database, extracts its structures and
generates an SQL script that you can load into your PostgreSQL database.

Ora2Pg.pm dumps the database schema (tables, views, sequences, indexes,
grants, etc.), with primary, unique and foreign keys into PostgreSQL syntax
without need to edit the SQL code generated.

It can also dump Oracle data into a PostgreSQL database 'on the fly'. Also
you can choose a selection of columns to be exported for each table.

The SQL and/or PL/SQL code generated for functions, procedures and triggers
has to be reviewed to match the PostgreSQL syntax. You find some useful
recommandations on porting Oracle PL/SQL code to PostgreSQL PL/PGSQL at
"http://techdocs.postgresql.org/" under the topic "Converting from other
Databases to PostgreSQL", Oracle.

Notice that the trunc() function in Oracle is the same for number and date
types. Be carefull when porting to PostgreSQL to use trunc() for numbers
and date_trunc() for dates.


=head1 ABSTRACT

The goal of the Ora2Pg Perl module is to cover everything needed to export
an Oracle database to a PostgreSQL database without other thing than providing
the parameters needed for connecting to the Oracle database.

Features include:

	- Exporting the database schema (tables, views, sequences, indexes),
	  with unique, primary and foreign key and check constraints.
	- Exporting grants/privileges for users and groups.
	- Exporting range and list table partition.
	- Exporting a table selection (by specifying the table names or max
	  tables).
	- Exporting the Oracle schema to a PostgreSQL 7.3+ schema.
	- Exporting predefined functions/triggers/procedures/packages.
	- Exporting user defined data type.
	- Exporting table data.
	- Exporting Oracle views as PG tables.
	- Providing basic help for converting PLSQL code to PLPGSQL (needs
	  manual work).

See ora2pg.conf for more information on use.

My knowledge about database is really poor especially for Oracle RDBMS.
Any contributions, particularly in this matter, are welcome.


=head1 REQUIREMENTS

You just need the DBI, DBD::Pg and DBD::Oracle Perl module to be installed.
DBD::Pg is optional and needed only for 'on the fly' migration. The PostgreSQL
client (psql) must also be installed on the host running Ora2Pg.

If you want to compress output as a gzip file you need Compress::Zlib Perl
module. And if you want to use bzip2 compression, program bzip2 must be
available.


=head1 PUBLIC METHODS

=head2 new HASH_OPTIONS

Creates a new Ora2Pg object.

The only required option is:

    - config : Path to the configuration file (required).

All directives found in the configuration file can be overwritten in the
instance call by passing them in lowercase as arguments. These supported
options are (See ora2pg.conf for more details):

    - datasource : Oracle DBD datasource (required)
    - user : Oracle DBD user (optional with public access)
    - password : Oracle DBD password (optional with public access)
    - schema : Oracle internal schema to extract (optional)
    - type : Type of data to extract, can be TABLE,VIEW,GRANT,SEQUENCE,
      TRIGGER,FUNCTION,PROCEDURE,DATA,COPY,PACKAGE,TABLESPACE,PARTTION
      or a combinaison of these keywords separated by blanks.
    - debug : verbose mode.
    - export_schema : Export Oracle schema to PostgreSQL >7.3 schema
    - tables : Extract only the specified tables (arrayref) and set the
      extracting order
    - exclude : Exclude the specified tables from extraction (arrayref)
    - data_limit : bulk size of tuples to return at once during data extraction
      (defaults to 10000).
    - case_sensitive: Allow to preserve Oracle object names as they are
      written. Default is not.
    - skip_fkeys : Skip foreign key constraints extraction. Defaults to 0
      (extraction)
    - skip_pkeys : Skip primary keys extraction. Defaults to 0 (extraction)
    - skip_ukeys : Skip unique column constraints extraction. Defaults to 0
      (extraction)
    - skip_indexes : Skip all other index types extraction. Defaults to 0
      (extraction)
    - skip_checks : Skip check constraints extraction. Defaults to 0
      (extraction)
    - keep_pkey_names : By default, primary key names in the source database
      are ignored, and default key names are created in the target database.
      If this is set to true, primary key names are preserved.
    - bzip2: Path to the Bzip2 program to compress data export. Default
      /usr/bin/bzip2
    - gen_user_pwd : When set to 1 this will replace the default password
      'change_my_secret' with a random string.
    - fkey_deferrable: Force foreign key constraints to be exported as
      deferrable. Defaults to 0: export as is.
    - defer_fkey : Force all foreign key constraints to be deferred before
      data import, this require that all fkeys are deferrable and that all
      datas can be imported in a single transaction. Defaults to 0, as is.
    - drop_fkey  : Force all foreign key constraints to be dropped before
      data import and recreated at the end. Defaults to 0: export as is.
    - drop_indexes: Force deletion of non automatic index on tables before
      data import and recreate them at end of the import. Default 0, disabled.
    - pg_numeric_type: Convert the Oracle NUMBER data type to adequate PG data
      types instead of using the slow numeric(p,s) data type.
    - default_numeric: By default the NUMBER(x) type without precision is
      converted to bigint. You can overwrite this data type by any PG type.
    - keep_pkey_names: Preserve oracle primary key names. The default is to
      ignore and use PostgreSQl defaults.
      Must be used with PostgreSQL > 8.1. Defaults to none support (backward
      compatibility).
    - disable_triggers: Disable triggers on all tables in COPY and
      DATA mode.
    - disable_sequence: Disables alter sequence on all tables in COPY or
      DATA mode.
    - noescape: Disable character escaping during data export.
    - datatype: Redefine Oracle to PostgreSQl data type conversion.
    - binmode: Force Perl to use the specified binary mode for output. The
      default is ':raw';
    - sysusers: Add other system users to the default exclusion list
      (SYS,SYSTEM,DBSNMP,OUTLN,PERFSTAT,CTXSYS,XDB,WMSYS,SYSMAN,SQLTXPLAIN,
      MDSYS,EXFSYS,ORDSYS,DMSYS,OLAPSYS,FLOWS_020100,FLOWS_FILES,TSMSYS).
    - ora_sensitive: Force the use of Oracle case sensitive table/view names.
    - plsql_pgsql: Enable plsql to plpgsql conversion.
    - pg_schema: Allow to specify a coma delimited list of PostgreSQL schema.
    - file_per_constraint: Allow to create one output file containing all
      constraints during schema or table export.
    - file_per_index: Allow to create one output file containing all indexes
      during schema or table export.
    - file_per_table: Allow to create one output file per table loaded from
      output.sql. The table's files will be named ${table}_$output.
    - pg_supports_when: allow WHEN clause on trigger definition (Pg >= 9.0). 
    - pg_supports_insteadof: allow INSTEAD OF usage on trigger definition
      (Pg >= 9.1).
    - file_per_function: Allow to create one output file per function loaded
      from output.sql. The function's files will be named ${funcname}_$output.
    - truncate_table: adds trunctate table instruction before loading data.
    - xtable : used to specify a table name from which SHOW_COLUMN will work.
      Default is to show the column name of all table.
    - force_owner: force to set the table and sequences owner. If set to 1 it
      will use the oracle owner else it will use the name given as value.
    - input_file: force to use a given file as datasoure instead of an Oracle
      database connection. This file must contain either PL/SQL function or
      procedure or a package body source code. Ora2Pg will parse this code
      and outputed converted plpgsql.
    - standard_conforming_strings: same as the PostgreSQL configuration option,
      string constants should be written as E'...'. Default is on, take care for
      backward compatibility, before v8.5 Ora2Pg do not use this syntax. 
    - compile_schema: used to force Oracle to compile all PL/SQL code before
      code extraction.
    - export_invalid: force Ora2Pg to export all PL/SQL code event if it is
      maked as invalid by Oracle.
    - allow_code_break: allow plsql to pgplsql conversion that could break the
      original code if they include complex subqueries. See decode() and substr()
      replacement.


Beware that this list may grow longer because all initialization is
performed this way.


Special configuration options to handle character encoding:
-----------------------------------------------------------

NLS_LANG

If you experience any issues where mutibyte characters are being substituted
with replacement characters during the export try to set the NLS_LANG
configuration directive to the Oracle encoding. This may help a lot especially
with UTF8 encoding.

BINMODE

If you experience the Perl warning: "Wide character in print", it means
that you tried to write a Unicode string to a non-unicode file handle.
You can force Perl to use binary mode for output by setting the BINMODE
configuration option to the specified encoding. If you set it to 'utf8', it
will force printing like this: binmode OUTFH, ":utf8"; By default Ora2Pg opens
the output file in 'raw' binary mode.

CLIENT_ENCODING

If you experience ERROR: invalid byte sequence for encoding "UTF8": 0xe87472
when loading data you may want to set the encoding of the PostgreSQL client.
By default it is not set and it will depend of you system client encoding.

For example, let's say you have an Oracle database with all data encoded in
FRENCH_FRANCE.WE8ISO8859P15, your system use fr_FR.UTF-8 as console encoding
and your PostgreSQL database is encoded in UTF8. What you have to do is set the
NLS_LANG to FRENCH_FRANCE.WE8ISO8859P15 and the CLIENT_ENCODING to LATIN9.


Exporting Oracle views as PostgreSQL tables:
--------------------------------------------

Since version 4.10 you can export Oracle views as PostgreSQL tables simply
by setting TYPE configuration option to TABLE and COPY or DATA and specifying
your views in the TABLES configuration option.
Then if Ora2Pg does not find the name in Oracle table names it automatically
deduces that it must search for it in the view names, and if it finds
the view it will extract its schema (if TYPE=TABLE) into a PG create table form,
then it will extract the data (if TYPE=COPY or DATA) following the view schema.

Case sensitive table names in Oracle:
-------------------------------------

Since version 4.10 you can extract/export Oracle databases with case sensitive
table/view names. This requires the use of quoted table/view names during
Oracle querying. Set the configuration option ORA_SENSITIVE to 1 to enable this
feature. By default it is off.

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

	if (!$self->{case_sensitive}) {
		map { $_ = lc($_) } @fields;
		$table = lc($table);
	}

	push(@{$self->{modify}{$table}}, @fields);

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
	$self->{modify} = ();
	$self->{replaced_tables} = ();
	$self->{replaced_cols} = ();
	$self->{where} = ();
	@{$self->{sysusers}} = ('SYSTEM','SYS','DBSNMP','OUTLN','PERFSTAT','CTXSYS','XDB','WMSYS','SYSMAN','SQLTXPLAIN','MDSYS','EXFSYS','ORDSYS','DMSYS','OLAPSYS','FLOWS_020100','FLOWS_FILES','TSMSYS');
	$self->{ora_reserved_words} = (); 
	# Init PostgreSQL DB handle
	$self->{dbhdest} = undef;
	$self->{idxcomment} = 0;
	$self->{standard_conforming_strings} = 1;
	$self->{allow_code_break} = 1;
	# Initialyze following configuration file
	foreach my $k (keys %AConfig) {
		if (lc($k) eq 'tables') {
			$self->{limited} = $AConfig{TABLES};
		} elsif (lc($k) eq 'exclude') {
			$self->{excluded} = $AConfig{EXCLUDE};
		} else {
			$self->{lc($k)} = $AConfig{uc($k)};
		}
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

	# Overwrite configuration with all given parameters
	# and try to preserve backward compatibility
	foreach my $k (keys %options) {
		if ($options{tables} && (lc($k) eq 'tables')) {
			$self->{limited} = $options{tables};
		} elsif ($options{exclude} && (lc($k) eq 'exclude')) {
			$self->{excluded} = $options{exclude};
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
	if ($self->{nls_lang}) {
		$ENV{NLS_LANG} = $self->{nls_lang};
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
	# Set user defined datatype translation
	if ($self->{datatype}) {
		my @transl = split(/[,;]/, $self->{datatype});
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
	# backward compatibility
	if ($self->{disable_table_triggers}) {
		$self->{disable_triggers} = $self->{disable_table_triggers};
	}
	$self->{binmode} ||= ':raw';
	$self->{binmode} =~ s/^://;
	$self->{binmode} = ':' . lc($self->{binmode});
	$self->{enable_microsecond} ||= 0;

	if (($self->{standard_conforming_strings} =~ /^off$/i) || ($self->{standard_conforming_strings} == 0)) {
		$self->{standard_conforming_strings} = 0;
	} else {
		$self->{standard_conforming_strings} = 1;
	}
	$self->{compile_schema} ||= 0;
	$self->{export_invalid} ||= 0;

	# Allow multiple or chained extraction export type
	$self->{export_type} = ();
	if ($self->{type}) {
		@{$self->{export_type}} = split(/[\s\t,;]+/, $self->{type});
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
		$self->{dbh} = DBI->connect($self->{oracle_dsn}, $self->{oracle_user}, $self->{oracle_pwd});

		# Check for connection failure
		if (!$self->{dbh}) {
			$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
		}
		$self->{dbh}->{LongReadLen} = 0;

		# Use consistent reads for concurrent dumping...
		$self->{dbh}->begin_work || $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		if ($self->{debug}) {
			$self->logit("Isolation level: $self->{transaction}\n", 1);
		}
		my $sth = $self->{dbh}->prepare($self->{transaction}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->finish;
		undef $sth;

		if ($self->{debug}) {
			$self->logit("Force Oracle to compile schema before code extraction\n", 1);
		}
		$self->_compile_schema(uc($self->{compile_schema})) if ($self->{compile_schema});
	} else {
		$self->{plsql_pgsql} = 1;
		if (grep(/^$self->{type}$/, 'FUNCTION','PROCEDURE','PACKAGE')) {
			$self->export_schema();
			if ( ($self->{type} eq 'DATA') || ($self->{type} eq 'COPY') ) {
				$self->_send_to_pgdb() if ($self->{pg_dsn} && !$self->{dbhdest});
			} else {
				$self->logit("WARNING: can't use direct import into PostgreSQL with this type of export.\n");
				$self->logit("Only DATA or COPY export type can be use with direct import, file output will be used.\n");
				sleep(2);
			}
		} else {
			$self->logit("FATAL: bad export type using input file option\n", 0, 1);
		}
		return;
	}

	# Retreive all table informations
        foreach my $t (@{$self->{export_type}}) {
                $self->{type} = $t;
		if (($self->{type} eq 'TABLE') || ($self->{type} eq 'FDW') || ($self->{type} eq 'DATA') || ($self->{type} eq 'COPY')) {
			$self->{dbh}->{LongReadLen} = 100000;
			$self->_tables();
		} elsif ($self->{type} eq 'VIEW') {
			$self->{dbh}->{LongReadLen} = 100000;
			$self->_views();
		} elsif ($self->{type} eq 'GRANT') {
			$self->_grants();
		} elsif ($self->{type} eq 'SEQUENCE') {
			$self->_sequences();
		} elsif ($self->{type} eq 'TRIGGER') {
			$self->{dbh}->{LongReadLen} = 100000;
			$self->_triggers();
		} elsif ($self->{type} eq 'FUNCTION') {
			$self->{dbh}->{LongReadLen} = 100000;
			$self->_functions();
		} elsif ($self->{type} eq 'PROCEDURE') {
			$self->{dbh}->{LongReadLen} = 100000;
			$self->_procedures();
		} elsif ($self->{type} eq 'PACKAGE') {
			$self->{dbh}->{LongReadLen} = 100000;
			$self->_packages();
		} elsif ($self->{type} eq 'TYPE') {
			$self->{dbh}->{LongReadLen} = 100000;
			$self->_types();
		} elsif ($self->{type} eq 'TABLESPACE') {
			$self->_tablespaces();
		} elsif ($self->{type} eq 'PARTITION') {
			$self->{dbh}->{LongReadLen} = 100000;
			$self->_partitions();
		} elsif (($self->{type} eq 'SHOW_SCHEMA') || ($self->{type} eq 'SHOW_TABLE') || ($self->{type} eq 'SHOW_COLUMN') || ($self->{type} eq 'SHOW_ENCODING')) {
			$self->{dbh}->{LongReadLen} = 100000;
			$self->_show_infos($self->{type});
			$self->{dbh}->disconnect() if ($self->{dbh}); 
			exit 0;
		} else {
			warn "type option must be TABLE, VIEW, GRANT, SEQUENCE, TRIGGER, PACKAGE, FUNCTION, PROCEDURE, PARTITION, DATA, COPY, TABLESPACE, SHOW_SCHEMA, SHOW_TABLE, SHOW_COLUMN, SHOW_ENCODING, FDW\n";
		}
		# Mofify export structure if required
		if ($self->{type} =~ /^(DATA|COPY)$/) {
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

	if ( ($self->{type} eq 'DATA') || ($self->{type} eq 'COPY') ) {
		$self->_send_to_pgdb() if ($self->{pg_dsn} && !$self->{dbhdest});
	} else {
		$self->logit("WARNING: can't use direct import into PostgreSQL with this type of export.\n");
		$self->logit("Only DATA or COPY export type can be use with direct import, file output will be used.\n");
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

    $self->{tables}{$class_name}{table_info} = [(OWNER,TYPE)];

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
	my ($self) = @_;

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
			next if ($t->[2] =~ /\$/);
			# Jump to desired extraction
			if (grep(/^$t->[2]$/, @done)) {
				$self->logit("Duplicate entry found: $t->[0] - $t->[1] - $t->[2]\n", 1);
			} else {
				push(@done, $t->[2]);
			}
			next if (($#{$self->{limited}} >= 0) && !grep($t->[2] =~ /^$_$/i, @{$self->{limited}}));
			next if (($#{$self->{excluded}} >= 0) && grep($t->[2] =~ /^$_$/i, @{$self->{excluded}}));

			$self->logit("[$i] Scanning $t->[2] (@$t)...\n", 1);
			
			# Check of uniqueness of the table
			if (exists $self->{tables}{$t->[2]}{field_name}) {
				$self->logit("Warning duplicate table $t->[2], SYNONYME ? Skipped.\n", 1);
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
			# usually OWNER,TYPE. QUALIFIER is omitted until I know what to do with that
			$self->{tables}{$t->[2]}{table_info} = [($t->[1],$t->[3],$t->[4])];
			# Set the fields information
			my $query = "SELECT * FROM $t->[1].$t->[2] WHERE 1=0";
			if ($self->{ora_sensitive} && ($t->[2] !~ /"/)) {
				$query = "SELECT * FROM $t->[1].\"$t->[2]\" WHERE 1=0";
			}
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
			$self->{tables}{$t->[2]}{field_name} = $sth->{NAME};
			$self->{tables}{$t->[2]}{field_type} = $sth->{TYPE};

			@{$self->{tables}{$t->[2]}{column_info}} = $self->_column_info($t->[2],$t->[1]);
			@{$self->{tables}{$t->[2]}{column_comments}} = $self->_column_comments($t->[2],$t->[1]);
                        # We don't check for skip_ukeys/skip_pkeys here; this is taken care of inside _unique_key
			%{$self->{tables}{$t->[2]}{unique_key}} = $self->_unique_key($t->[2],$t->[1]);
			($self->{tables}{$t->[2]}{foreign_link}, $self->{tables}{$t->[2]}{foreign_key}) = $self->_foreign_key($t->[2],$t->[1]) if (!$self->{skip_fkeys});
			($self->{tables}{$t->[2]}{uniqueness}, $self->{tables}{$t->[2]}{indexes}) = $self->_get_indexes($t->[2],$t->[1]) if (!$self->{skip_indices} && !$self->{skip_indexes});
			%{$self->{tables}{$t->[2]}{check_constraint}} = $self->_check_constraint($t->[2],$t->[1]) if (!$self->{skip_checks});
			$i++;
		}
	}

	# Try to search requested TABLE names in the VIEW names if not found in
	# real TABLE names
	if ($#{$self->{limited}} >= 0) {
		my $search_in_view = 0;
		foreach (@{$self->{limited}}) {
			if (not exists $self->{tables}{$_}) {
				$self->logit("Found view extraction for $_\n", 1);
				$search_in_view = 1;
				last;
			}
		}
		if ($search_in_view) {
			my %view_infos = $self->_get_views();
			foreach my $table (sort keys %view_infos) {
				# Set the table information for each class found
				# Jump to desired extraction
				next if (!grep($table =~ /^$_$/i, @{$self->{limited}}));
				$self->logit("Scanning view $table...\n", 1);

				$self->{views}{$table}{text} = $view_infos{$table};
				$self->{views}{$table}{alias}= $view_infos{$table}{alias};
				my $realview = $table;
				if ($self->{ora_sensitive} && ($table !~ /"/)) {
					$realview = "\"$table\"";
				}
				if ($self->{schema}) {
					$realview = $self->{schema} . ".$realview";
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
				$self->{views}{$table}{field_name} = $sth->{NAME};
				$self->{views}{$table}{field_type} = $sth->{TYPE};
				@{$self->{views}{$table}{column_info}} = $self->_column_info($table);
			}
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
	foreach my $table (sort keys %view_infos) {
		# Set the table information for each class found
		# Jump to desired extraction
		next if ($table =~ /\$/);
		next if (($#{$self->{limited}} >= 0) && !grep($table =~ /^$_$/i, @{$self->{limited}}));
		next if (($#{$self->{excluded}} >= 0) && grep($table =~ /^$_$/i, @{$self->{excluded}}));

		$self->logit("[$i] Scanning $table...\n", 1);

		$self->{views}{$table}{text} = $view_infos{$table};
                ## Added JFR : 3/3/02 : Retrieve also aliases from views
                $self->{views}{$table}{alias}= $view_infos{$table}{alias};
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
		$sql_header .= "SET client_encoding TO \U$self->{client_encoding}\E;\n\n";
	}
	if ($self->{export_schema} && ($self->{type} ne 'TABLE')) {
		if ($self->{pg_schema}) {
			if (!$self->{case_sensitive}) {
				$sql_header .= "SET search_path = \L$self->{pg_schema}\E;\n\n";
			} else {
				$sql_header .= "SET search_path = \"$self->{pg_schema}\";\n\n";
			}
		} else {
			if (!$self->{case_sensitive}) {
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
				if (!$self->{case_sensitive}) {
					$sql_output .= "CREATE OR REPLACE VIEW \"\L$view\E\" AS ";
				} else {
					$sql_output .= "CREATE OR REPLACE VIEW \"$view\" AS ";
				}
				$sql_output .= $self->{views}{$view}{text} . ";\n";
			} else {
				if (!$self->{case_sensitive}) {
					$sql_output .= "CREATE OR REPLACE VIEW \"\L$view\E\" (";
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
					if (!$self->{case_sensitive}) {
						$sql_output .= "\"\L$d->[0]\E\"";
					} else {
						$sql_output .= "\"$d->[0]\"";
					}
				}
				if (!$self->{case_sensitive}) {
					if ($self->{views}{$view}{text} =~ /SELECT[^\s\t]*(.*?)[^\s\t]*FROM/is) {
						my $clause = $1;
						$clause =~ s/"([^"]+)"/"\L$1\E"/gs;
						$self->{views}{$view}{text} =~ s/SELECT[^\s\t]*(.*?)[^\s\t]*FROM/SELECT $clause FROM/is;
					}
				}
				$sql_output .= ") AS " . $self->{views}{$view}{text} . ";\n";
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

	# Process grant only
	if ($self->{type} eq 'GRANT') {
		$self->logit("Add users/roles/grants privileges...\n", 1);
		my $grants = '';
		my $users = '';

                #push(@{$privs{$row->[2]}{column}{$row->[4]}{$row->[0]}}, $row->[3]);

		# Add privilege definition
		foreach my $table (sort keys %{$self->{grants}}) {
			my $realtable = lc($table);
			my $isfunc = 0;
			my $obj = 'TABLE';
			if ($self->{export_schema} &&  $self->{schema}) {
				if (!$self->{case_sensitive}) {
					$realtable =  "\L$self->{schema}.$table\E";
				} else {
					$realtable =  "\"$self->{schema}\".\"$table\"";
				}
			} elsif ($self->{case_sensitive}) {
				$realtable =  "\"$table\"";
			}
			if ($self->{grants}{$table}{owner}) {
				if ($self->{grants}{$table}{type} eq 'function') {
					$obj = 'FUNCTION';
					$isfunc = 1;
				}
				if (grep(/^$self->{grants}{$table}{owner}$/, @{$self->{roles}{roles}})) {
					$grants .= "ALTER $obj $realtable OWNER TO ROLE $self->{grants}{$table}{owner};\n";
					$obj = '' if ($obj eq 'TABLE');
					$grants .= "GRANT ALL ON $obj $realtable TO ROLE $self->{grants}{$table}{owner};\n";
				} else {
					$grants .= "ALTER $obj $realtable OWNER TO $self->{grants}{$table}{owner};\n";
					$obj = '' if ($obj eq 'TABLE');
					$grants .= "GRANT ALL ON $obj $realtable TO $self->{grants}{$table}{owner};\n";
				}
			}
			if ($isfunc) {
				$grants .= "REVOKE ALL ON FUNCTION $realtable FROM PUBLIC;\n";
			} else {
				$grants .= "REVOKE ALL ON $realtable FROM PUBLIC;\n";
			}
			foreach my $usr (sort keys %{$self->{grants}{$table}{privilege}}) {
				$obj = 'TABLE';
				$obj = 'FUNCTION' if (grep(/^EXECUTE$/i, @{$self->{grants}{$table}{privilege}{$usr}}));
				$grants .= "GRANT " . join(',', @{$self->{grants}{$table}{privilege}{$usr}}) . " ON $obj $realtable TO $usr;\n";
			}
			$grants .= "\n";
		}

		foreach my $r (@{$self->{roles}{owner}}, @{$self->{roles}{grantee}}) {
			my $secret = 'change_my_secret';
			if ($self->{gen_user_pwd}) {
				$secret = &randpattern("CccnCccn");
			}
			$sql_header .= "CREATE " . ($self->{roles}{type}{$r} ||'USER') . " $r WITH PASSWORD '$secret'";
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
			if (!$self->{case_sensitive}) {
				$sql_output .= "CREATE SEQUENCE \"\L$seq->[0]\E\" INCREMENT $seq->[3]";
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
				if (!$self->{case_sensitive}) {
					$sql_output .= "ALTER SEQUENCE \"\L$seq->[0]\E\" OWNER TO \L$owner\E;\n";
				} else {
					$sql_output .= "ALTER SEQUENCE \"$seq->[0]\" OWNER TO $owner;\n";
				}
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
			next if ($trig->[0] =~ /\$/);
			if ($self->{file_per_function} && !$self->{dbhdest}) {
				$self->dump("\\i $dirprefix$trig->[0]_$self->{output}\n");
				$self->logit("Dumping to one file per trigger : $trig->[0]_$self->{output}\n", 1);
				$fhdl = $self->export_file("$trig->[0]_$self->{output}");
			}
			$trig->[1] =~ s/\s*EACH ROW//is;
			chop($trig->[4]);
			chomp($trig->[4]);
			$self->logit("\tDumping trigger $trig->[0]...\n", 1);
			# Check if it's like a pg rule
			if (!$self->{pg_supports_insteadof} && $trig->[1] =~ /INSTEAD OF/) {
				if (!$self->{case_sensitive}) {
					$sql_output .= "CREATE OR REPLACE RULE \"\L$trig->[0]\E\" AS\n\tON \L$trig->[3]\E\n\tDO INSTEAD\n(\n\t$trig->[4]\n);\n\n";
				} else {
					$sql_output .= "CREATE OR REPLACE RULE \"$trig->[0]\" AS\n\tON $trig->[3]\n\tDO INSTEAD\n(\n\t$trig->[4]\n);\n\n";
				}
			} else {
				if ($self->{plsql_pgsql}) {
					$trig->[4] = Ora2Pg::PLSQL::plsql_to_plpgsql($trig->[4], $self->{allow_code_break});
					$trig->[4] =~ s/\b(END[;]*)$/RETURN NEW;\n$1/igs;
				}

				if ($self->{pg_supports_when} && $trig->[5]) {
					$sql_output .= "DROP TRIGGER IF EXISTS \L$trig->[0]\E ON \L$trig->[3]\E CASCADE;\n";
					$sql_output .= "CREATE OR REPLACE FUNCTION trigger_fct_\L$trig->[0]\E () RETURNS trigger AS \$BODY\$\n$trig->[4]\n\$BODY\$\n LANGUAGE 'plpgsql';\n\n";
					$trig->[6] =~ s/\n+$//s;
					$trig->[6] =~ s/^[^\.\s\t]+\.//;
					$sql_output .= "CREATE TRIGGER $trig->[6]\n";
					if ($trig->[5]) {
						if ($self->{plsql_pgsql}) {
							$trig->[5] = Ora2Pg::PLSQL::plsql_to_plpgsql($trig->[5], $self->{allow_code_break});
						}
						$sql_output .= "\tWHEN ($trig->[5])\n";
					}
					$sql_output .= "\tEXECUTE PROCEDURE trigger_fct_\L$trig->[0]\E();\n\n";
				} elsif (!$self->{case_sensitive}) {
					$sql_output .= "DROP TRIGGER IF EXISTS \L$trig->[0]\E ON \"\L$trig->[3]\E\" CASCADE;\n";
					$sql_output .= "CREATE OR REPLACE FUNCTION trigger_fct_\L$trig->[0]\E () RETURNS trigger AS \$BODY\$\n$trig->[4]\n\$BODY\$\n LANGUAGE 'plpgsql';\n\n";
					$sql_output .= "CREATE TRIGGER \L$trig->[0]\E\n\t";
					if ($trig->[1] =~ s/ STATEMENT//) {
						$sql_output .= "$trig->[1] $trig->[2] ON \"\L$trig->[3]\E\" FOR EACH STATEMENT\n";
					} else {
						$sql_output .= "$trig->[1] $trig->[2] ON \"\L$trig->[3]\E\" FOR EACH ROW\n";
					}
					$sql_output .= "\tEXECUTE PROCEDURE trigger_fct_\L$trig->[0]\E();\n\n";
				} else {
					$sql_output .= "DROP TRIGGER IF EXISTS \L$trig->[0]\E ON \L$trig->[3]\E CASCADE;\n";
					$sql_output .= "CREATE OR REPLACE FUNCTION trigger_fct_$trig->[0] () RETURNS trigger AS \$BODY\$\n$trig->[4]\n\$BODY\$ LANGUAGE 'plpgsql';\n\n";
					$sql_output .= "CREATE TRIGGER $trig->[0]\n\t";
					if ($trig->[1] =~ s/ STATEMENT//) {
						$sql_output .= "$trig->[1] $trig->[2] ON \"$trig->[3]\" FOR EACH STATEMENT\n";
					} else {
						$sql_output .= "$trig->[1] $trig->[2] ON \"$trig->[3]\" FOR EACH ROW\n";
					}
					$sql_output .= "\tEXECUTE PROCEDURE trigger_fct_$trig->[0]();\n\n";
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

	# Process functions only
	if ($self->{type} eq 'FUNCTION') {
		use constant SQL_DATATYPE => 2;
		$self->logit("Add functions definition...\n", 1);
		$self->dump($sql_header) if ($self->{file_per_function} && !$self->{dbhdest});
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
			foreach my $l (@allfct) {
				chomp($l);
				if ($l =~ /^(function|procedure)[\s\t]+([^\s\(\t]+)/i) {
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

		foreach my $fct (sort keys %{$self->{functions}}) {
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
				$sql_output .= $self->_convert_function($self->{functions}{$fct}) . "\n";
			} else {
				$sql_output .= $self->{functions}{$fct} . "\n";
			}
			$self->_restore_comments(\$sql_output, \%comments);
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
		$self->dump($sql_header) if ($self->{file_per_function} && !$self->{dbhdest});
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
			foreach my $l (@allfct) {
				chomp($l);
				if ($l =~ /^(function|procedure)[\s\t]+([^\s\(\t]+)/i) {
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

		foreach my $fct (sort keys %{$self->{procedures}}) {
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
				$sql_output .= $self->_convert_function($self->{procedures}{$fct}) . "\n";
			} else {
				$sql_output .= $self->{procedures}{$fct} . "\n";
			}
			$self->_restore_comments(\$sql_output, \%comments);
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
		$self->{procedures} = ();

		return;
	}

	# Process packages only
	if ($self->{type} eq 'PACKAGE') {
		$self->logit("Add packages definition...\n", 1);
		my $nothing = 0;
		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		$self->dump($sql_header) if ($self->{file_per_function} && !$self->{dbhdest});

		#---------------------------------------------------------
		# Code to use to find package parser bugs, it load a file
		# containing the untouched PL/SQL code from Oracle Package
		#---------------------------------------------------------
		if ($self->{input_file}) {
			$self->{packages} = ();
			$self->logit("Reading input code from file $self->{input_file}...\n", 1);
			sleep(1);
			open(IN, "$self->{input_file}");
			my @allpkg = <IN>;
			close(IN);
			my $pknm = '';
			my $before = '';
			foreach my $l (@allpkg) {
				chomp($l);
				if ($l =~ /^PACKAGE[\s\t]+([^\t\s]+)[\s\t]*(AS|IS)/is) {
					$pknm = lc($1);
				}
				if ($pknm) {
					$self->{packages}{$pknm} .= "$l\n";
				}
			}
		}
		#--------------------------------------------------------

		foreach my $pkg (sort keys %{$self->{packages}}) {
			next if (!$self->{packages}{$pkg});
			my $pkgbody = '';
			$self->logit("Dumping package $pkg...\n", 1);
			if ($self->{plsql_pgsql} && $self->{file_per_function} && !$self->{dbhdest}) {
				my $dir = lc("$dirprefix${pkg}");
				if (not mkdir($dir)) {
					$self->logit("Fail creating directory package : $dir - $!\n", 1);
					next;
				} else {
					$self->logit("Creating directory package: $dir\n", 1);
				}
			}
			if ($self->{plsql_pgsql}) {
				$pkgbody = $self->_convert_package($self->{packages}{$pkg});
				$pkgbody =~ s/[\r\n]*END;$//is;
			} else {
				$pkgbody = $self->{packages}{$pkg};
			}
			if ($pkgbody && ($pkgbody =~ /[a-z]/i)) {
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
			next if ($tpe->{name} =~ /\$/);
			$self->logit("Dumping type $tpe->{name}...\n", 1);
			$sql_output .= "-- Oracle type '$tpe->{name}' declaration, please edit to match PostgreSQL syntax.\n";
			if ($self->{plsql_pgsql}) {
				$sql_output .= $self->_convert_type($tpe->{code}) . "\n";
			} else {
				$sql_output .= $tpe->{code} . "\n";
			}
			$sql_output .= "-- End of Oracle type '$tpe->{name}' declaration\n\n";
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
		foreach my $tb_type (sort keys %{$self->{tablespaces}}) {
			# TYPE - TABLESPACE_NAME - FILEPATH - OBJECT_NAME
			foreach my $tb_name (sort keys %{$self->{tablespaces}{$tb_type}}) {
				foreach my $tb_path (sort keys %{$self->{tablespaces}{$tb_type}{$tb_name}}) {
					# Replace Oracle tablespace filename
					my $loc = $tb_name;
					$tb_path =~ /^(.*)[^\\\/]+$/;
					$loc = $1 . $loc;
					$create_tb .= "CREATE TABLESPACE $tb_name LOCATION '$loc';\n" if ($create_tb !~ /CREATE TABLESPACE $tb_name LOCATION/s);
					foreach my $obj (@{$self->{tablespaces}{$tb_type}{$tb_name}{$tb_path}}) {
						next if ($obj =~ /\$/);
						next if (($#{$self->{limited}} >= 0) && !grep($obj =~ /^$_$/i, @{$self->{limited}}));
						next if (($#{$self->{excluded}} >= 0) && grep($obj =~ /^$_$/i, @{$self->{excluded}}));
						next if ($sql_output =~ /ALTER $tb_type $obj SET TABLESPACE/i);
						$sql_output .= "ALTER $tb_type $obj SET TABLESPACE $tb_name;\n";
					}
				}
			}
		}

		if (!$sql_output) {
			$sql_output = "-- Nothing found of type $self->{type}\n";
		}
		$self->dump($sql_header . "$create_tb\n" . $sql_output);

		return;
	}

	# Extract data only
	if (($self->{type} eq 'DATA') || ($self->{type} eq 'COPY')) {
		# Connect the database
		$self->{dbh} = DBI->connect($self->{oracle_dsn}, $self->{oracle_user}, $self->{oracle_pwd});
		# Check for connection failure
		if (!$self->{dbh}) {
			$self->logit("FATAL: $DBI::err ... $DBI::errstr\n", 0, 1);
		}

		if (!$self->{dbhdest}) {
			$self->dump($sql_header);
		} else {
			if ($self->{type} eq 'COPY') {
				if ($self->{dbuser}) {
					open(DBH, "| $PSQL -h $self->{dbhost} -p $self->{dbport} -d $self->{dbname} -U $self->{dbuser}") or $self->logit("FATAL: Can't open $PSQL command, $!\n", 0, 1);
				} else {
					# Executed as current user
					open(DBH, "| $PSQL -h $self->{dbhost} -p $self->{dbport} -d $self->{dbname}") or $self->logit("FATAL: Can't open $PSQL command, $!\n", 0, 1);
				}
				binmode(DBH, $self->{binmode});
			}
		}

		if ($self->{dbhdest} && $self->{export_schema} &&  $self->{schema}) {
			my $search_path = "SET search_path = \L$self->{schema}\E, pg_catalog";
			if ($self->{case_sensitive}) {
				$search_path = "SET search_path = \"$self->{schema}\", pg_catalog";
			}
			if ($self->{pg_schema}) {
				$search_path = "SET search_path = \L$self->{pg_schema}\E";
				if ($self->{case_sensitive}) {
					$search_path = "SET search_path = \"$self->{schema}\"";
				}
			}
			if ($self->{type} ne 'COPY') {
				my $s = $self->{dbhdest}->do($search_path) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
			} else {
				print DBH "$search_path;\n";
			}
		}
		# Try to ordered table for export as we don't have issues with foreign keys
		# we first extract all tables that dont have foreign keys
		# second we extract all tables that only references the previous tables
		# If there's still tables with reference, it is not possible to ordering so
		my @ordered_tables = ();
		foreach my $table (keys %{$self->{tables}}) {
			if (!exists $self->{tables}{$table}{foreign_link} || (scalar keys %{$self->{tables}{$table}{foreign_link}} == 0) ) {
				push(@ordered_tables, $table);
			}
		}
		my $cur_len = 0;
		while ($cur_len != $#ordered_tables) {
			$cur_len = $#ordered_tables;
			foreach my $table (keys %{$self->{tables}}) {
				next if (grep(/^$table$/i, @ordered_tables));
				my $notfound = 0;
				foreach my $key (keys %{$self->{tables}{$table}{foreign_link}}) {
					if (!exists $self->{tables}{$table}{foreign_link}{$key}{remote} || (scalar keys %{$self->{tables}{$table}{foreign_link}{$key}{remote}} == 0) ) {
						push(@ordered_tables, $table) if (!grep(/^$table$/i, @ordered_tables));
						next;
					}
					foreach my $desttable (keys %{$self->{tables}{$table}{foreign_link}{$key}{remote}}) {
						next if ($desttable eq $table);
						if (!grep(/^$desttable$/i, @ordered_tables)) {
							$notfound = 1;
							last;
						}
					}
				}
				if (!$notfound && !grep(/^$table$/i, @ordered_tables)) {
					$cur_len = 0;
					push(@ordered_tables, $table);
				}
			}
		}

		if ($self->{drop_indexes} || $self->{drop_fkey}) {
			if ($self->{dbhdest}) {
				if ($self->{type} eq 'COPY') {
					print DBH "\\set ON_ERROR_STOP OFF\n";
				}
			} else {
				$self->dump("\\set ON_ERROR_STOP OFF\n");
			}
		}
		my $deferred_fkey = 0;
		if ( ($#ordered_tables + 1) != scalar keys %{$self->{tables}} ) {
			# Ok ordering is impossible
			@ordered_tables = keys %{$self->{tables}};
			if ($self->{defer_fkey} && !$self->{drop_fkey}) {
				$deferred_fkey = 1;
				if ($self->{dbhdest}) {
					if ($self->{type} ne 'COPY') {
						my $s = $self->{dbhdest}->do("BEGIN;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
						$s = $self->{dbhdest}->do("SET CONSTRAINTS ALL DEFERRED;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					} else {
						print DBH "BEGIN;\n";
						print DBH "SET CONSTRAINTS ALL DEFERRED;\n";
					}
				} else {
					$self->dump("BEGIN;\n");
					$self->dump("SET CONSTRAINTS ALL DEFERRED;\n");
				}
			} elsif ($self->{drop_fkey}) {
				$deferred_fkey = 1;
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
			} else {
				$self->logit("WARNING: ordering table export to respect foreign keys is not possible.\n", 0);
				$self->logit("Please consider using DEFER_FKEY or DROP_FKEY configuration directives.\n", 0);
			}
		}
		if ($self->{drop_indexes}) {
			my $drop_all = '';
			# First of all we drop all indexes
			foreach my $table (sort { $self->{tables}{$a}{internal_id} <=> $self->{tables}{$b}{internal_id} } keys %{$self->{tables}}) {
				$self->logit("Dropping table $table indexes...\n", 1);
				$drop_all .= $self->_drop_indexes(%{$self->{tables}{$table}{indexes}});
			}
			if ($drop_all) {
				$self->dump($drop_all);
			}
			$drop_all = '';
		}
		if ($self->{drop_indexes} || $self->{drop_fkey}) {
			if ($self->{dbhdest}) {
				if ($self->{type} eq 'COPY') {
					print DBH "\\set ON_ERROR_STOP ON\n";
				}
			} else {
				$self->dump("\\set ON_ERROR_STOP ON\n");
			}
		}
		# Force datetime format
		if ($self->{enable_microsecond}) {
			$self->_datetime_format();
		}

		my $dirprefix = '';
		$dirprefix = "$self->{output_dir}/" if ($self->{output_dir});
		foreach my $table (@ordered_tables) {
			my $start_time = time();
			my $fhdl = undef;
			if ($self->{file_per_table} && !$self->{dbhdest}) {
				# Do not dump data again if the file already exists
				if (-e "$dirprefix${table}_$self->{output}") {
					$self->logit("Skipping $table, file already exists ${table}_$self->{output}\n", 1);
					next;
				}
				$self->dump("\\i $dirprefix${table}_$self->{output}\n");
				$self->logit("Dumping $table to file: ${table}_$self->{output}\n", 1);
				$fhdl = $self->export_file("${table}_$self->{output}");
			} else {
				$self->logit("Dumping table $table...\n", 1);
			}
			## Set client encoding if requested
			if ($self->{client_encoding}) {
				$self->logit("Changing client encoding as \U$self->{client_encoding}\E...\n", 1);
				if ($self->{dbhdest}) {
					if ($self->{type} ne 'COPY') {
						my $s = $self->{dbhdest}->do("SET client_encoding TO '\U$self->{client_encoding}\E';") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					} else {
						print DBH "SET client_encoding TO '\U$self->{client_encoding}\E';\n";
					}
				} else {
					$self->dump("SET client_encoding TO '\U$self->{client_encoding}\E';\n", $fhdl);
				}
			}
			my $search_path = '';
			if ($self->{pg_schema}) {
				if (!$self->{case_sensitive}) {
					$search_path = "SET search_path = \L$self->{pg_schema}\E;";
				} else {
					$search_path = "SET search_path = \"$self->{pg_schema}\";";
				}
			} elsif ($self->{schema}) {
				if (!$self->{case_sensitive}) {
					$search_path = "SET search_path = \L$self->{schema}\E, pg_catalog;";
				} else {
					$search_path = "SET search_path = \"$self->{schema}\", pg_catalog;";
				}
			}
			if ($search_path) {
				if ($self->{dbhdest}) {
					if ($self->{type} ne 'COPY') {
						my $s = $self->{dbhdest}->do("$search_path") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					} else {
						print DBH "$search_path\n";
					}
				} else {
					$self->dump("$search_path\n", $fhdl);
				}
			}


			# Start transaction to speed up bulkload
			if ($self->{dbhdest}) {
				if ($self->{type} ne 'COPY') {
					my $s = $self->{dbhdest}->do("BEGIN;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					print DBH "BEGIN;\n";
				}
			} else {
				$self->dump("BEGIN;\n", $fhdl);
			}
			## disable triggers of current table if requested
			if ($self->{disable_triggers}) {
				my $tmptb = $table;
				if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
					$self->logit("\tReplacing table $table as " . $self->{replaced_tables}{lc($table)}, "...\n", 1);
					$tmptb = $self->{replaced_tables}{lc($table)};
				}
				if ($self->{case_sensitive} && ($tmptb !~ /"/)) {
					$tmptb = '"' . $tmptb . '"';
				} else {
					$tmptb = lc($tmptb);
				}
				if ($self->{dbhdest}) {
					if ($self->{type} ne 'COPY') {
						my $s = $self->{dbhdest}->do("ALTER TABLE $tmptb DISABLE TRIGGER ALL;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					} else {
						print DBH "ALTER TABLE $tmptb DISABLE TRIGGER ALL;\n";
					}
				} else {
					$self->dump("ALTER TABLE $tmptb DISABLE TRIGGER ALL;\n", $fhdl);
				}
			}

			## Truncate current table if requested
			if ($self->{truncate_table}) {
				my $tmptb = $table;
				if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
					$tmptb = $self->{replaced_tables}{lc($table)};
				}
				if ($self->{case_sensitive} && ($tmptb !~ /"/)) {
					$tmptb = '"' . $tmptb . '"';
				} else {
					$tmptb = lc($tmptb);
				}
				if ($self->{dbhdest}) {
					if ($self->{type} ne 'COPY') {
						my $s = $self->{dbhdest}->do("TRUNCATE TABLE $tmptb;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					} else {
						print DBH "TRUNCATE TABLE $tmptb;\n";
					}
				} else {
					$self->dump("TRUNCATE TABLE $tmptb;\n", $fhdl);
				}
			}

			my @tt = ();
			my @stt = ();
			my @nn = ();
			my $s_out = "INSERT INTO \"\L$table\E\" (";
			$s_out = "INSERT INTO \"$table\" (" if ($self->{case_sensitive});
			if ($self->{type} eq 'COPY') {
				$s_out = "\nCOPY \"\L$table\E\" ";
				$s_out = "\nCOPY \"$table\" " if ($self->{case_sensitive});
			}
			my @fname = ();
			foreach my $i ( 0 .. $#{$self->{tables}{$table}{field_name}} ) {
				my $fieldname = ${$self->{tables}{$table}{field_name}}[$i];
				if (!$self->{case_sensitive}) {
					if (exists $self->{modify}{"\L$table\E"}) {
						next if (!grep(/$fieldname/i, @{$self->{modify}{"\L$table\E"}}));
					}
				} else {
					if (exists $self->{modify}{"$table"}) {
						next if (!grep(/$fieldname/i, @{$self->{modify}{"$table"}}));
					}
				}
				if (!$self->{case_sensitive}) {
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
					push(@nn, $f->[0]);
					if ($self->{type} ne 'COPY') {
						if (!$self->{case_sensitive}) {
							$s_out .= "\"\L$f->[0]\E\",";
						} else {
							$s_out .= "\"$f->[0]\",";
						}
					}
					last;
				}
			}
			if ($self->{type} eq 'COPY') {
				map { $_ = '"' . $_ . '"' } @fname;
				$s_out .= '(' . join(',', @fname) . ") FROM stdin;\n";
			}

			if ($self->{type} ne 'COPY') {
				$s_out =~ s/,$//;
				$s_out .= ") VALUES (";
			}

			# Change table name
			if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
				$self->logit("\tReplacing table $table as " . $self->{replaced_tables}{lc($table)} . "...\n", 1);
				$s_out =~ s/INSERT INTO "$table"/INSERT INTO "$self->{replaced_tables}{lc($table)}"/si;
				$s_out =~ s/COPY "$table"/COPY "$self->{replaced_tables}{lc($table)}"/si;
			}
			# Change column names
			if (exists $self->{replaced_cols}{"\L$table\E"} && $self->{replaced_cols}{"\L$table\E"}) {
				foreach my $c (keys %{$self->{replaced_cols}{"\L$table\E"}}) {
					$self->logit("\tReplacing column $c as " . $self->{replaced_cols}{lc($table)}{$c} . "...\n", 1);
					$s_out =~ s/"$c"/"$self->{replaced_cols}{lc($table)}{$c}"/si;
				}
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

                        ## don't forget to enable all triggers if needed...
			if ($self->{disable_triggers}) {
				my $tmptb = $table;
				if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
					$self->logit("\tReplacing table $table as " . $self->{replaced_tables}{lc($table)} . "...\n", 1);
					$tmptb = $self->{replaced_tables}{lc($table)};
				}
				if ($self->{case_sensitive} && ($tmptb !~ /"/)) {
					$tmptb = '"' . $tmptb . '"';
				} else {
					$tmptb = lc($tmptb);
				}
				if ($self->{dbhdest}) {
					if ($self->{type} ne 'COPY') {
						my $s = $self->{dbhdest}->do("ALTER TABLE $tmptb ENABLE TRIGGER ALL;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					} else {
						print DBH "ALTER TABLE $tmptb ENABLE TRIGGER ALL;\n";
					}
				} else {
					$self->dump("ALTER TABLE $tmptb ENABLE TRIGGER ALL;\n", $fhdl);
				}
			}
			# COMMIT transaction at end for speed improvement
			if ($self->{dbhdest}) {
				if ($self->{type} ne 'COPY') {
					my $s = $self->{dbhdest}->do("COMMIT;") or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					print DBH "COMMIT;\n";
				}
			} else {
				$self->dump("COMMIT;\n", $fhdl);
			}
			if ($self->{file_per_table} && !$self->{dbhdest}) {
				$self->close_export_file($fhdl);
			}
			$self->logit("Total extracted records from table $table: $total_record\n", 1);
			my $end_time = time();
			my $dt = $end_time - $start_time;
			my $rps = sprintf("%.1f", $total_record / ($dt+.0001));
			if ($dt > 0) {
				$self->logit("in $dt secs = $rps recs/sec\n", 1);
			} else {
				$self->logit("in $dt secs\n", 1);
			}
		}

		# extract sequence information
		if (($#ordered_tables >= 0) && !$self->{disable_sequence}) {
			$self->logit("Restarting sequences\n", 1);
			$self->dump($self->_extract_sequence_info());
		}

		# Extract data from view if requested
		my $search_in_view = 0;
		foreach (@{$self->{limited}}) {
			if (not exists $self->{tables}{$_} && not exists $self->{tables}{lc($_)}) {
				$self->logit("Found view data export for $_\n", 1);
				$search_in_view = 1;
				last;
			}
		}
		if ($search_in_view) {
			foreach my $table (sort keys %{$self->{views}}) {
				my $start_time = time();
				$self->logit("Dumping view $table...\n", 1);

				my $fhdl = undef;
				if ($self->{file_per_table} && !$self->{dbhdest}) {
					$self->dump("\\i $dirprefix${table}_$self->{output}\n");
					$self->logit("Dumping to one file per table/view : ${table}_$self->{output}\n", 1);
					$fhdl = $self->export_file("${table}_$self->{output}");
				}

				## disable triggers of current table if requested
				if ($self->{disable_triggers}) {
					my $tmptb = $table;
					if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
						$self->logit("\tReplacing table $table as " . $self->{replaced_tables}{lc($table)} . "...\n", 1);
						$tmptb = $self->{replaced_tables}{lc($table)};
					}
					if ($self->{case_sensitive} && ($tmptb !~ /"/)) {
						$tmptb = '"' . $tmptb . '"';
					} else {
						$tmptb = lc($tmptb);
					}
					if ($self->{dbhdest}) {
						print DBH "ALTER TABLE $tmptb DISABLE TRIGGER $self->{disable_triggers};\n";
					} else {
						$self->dump("ALTER TABLE $tmptb DISABLE TRIGGER $self->{disable_triggers};\n", $fhdl);
					}
				}

				my @tt = ();
				my @stt = ();
				my @nn = ();
				my $s_out = "INSERT INTO \"\L$table\E\" (";
				$s_out = "INSERT INTO \"$table\" (" if ($self->{case_sensitive});
				if ($self->{type} eq 'COPY') {
					$s_out = "\nCOPY \"\L$table\E\" ";
					$s_out = "\nCOPY \"$table\" " if ($self->{case_sensitive});
				}
				my @fname = ();
				foreach my $i ( 0 .. $#{$self->{views}{$table}{field_name}} ) {
					my $fieldname = ${$self->{views}{$table}{field_name}}[$i];
					if (!$self->{case_sensitive}) {
						if (exists $self->{modify}{"\L$table\E"}) {
							next if (!grep(/$fieldname/i, @{$self->{modify}{"\L$table\E"}}));
						}
					} else {
						if (exists $self->{modify}{"$table"}) {
							next if (!grep(/$fieldname/i, @{$self->{modify}{"$table"}}));
						}
					}
					if (!$self->{case_sensitive}) {
						push(@fname, lc($fieldname));
					} else {
						push(@fname, $fieldname);
					}

					foreach my $f (@{$self->{views}{$table}{column_info}}) {
						next if ($f->[0] ne "$fieldname");

						my $type = $self->_sql_type($f->[1], $f->[2], $f->[5], $f->[6]);
						$type = "$f->[1], $f->[2]" if (!$type);
						push(@tt, $type);
						push(@stt, $f->[1]);
						push(@nn, $f->[0]);
						if ($self->{type} ne 'COPY') {
							if (!$self->{case_sensitive}) {
								$s_out .= "\"\L$f->[0]\E\",";
							} else {
								$s_out .= "\"$f->[0]\",";
							}
						}
						last;
					}
				}
				if ($self->{type} eq 'COPY') {
					map { $_ = '"' . $_ . '"' } @fname;
					$s_out .= '(' . join(',', @fname) . ") FROM stdin;\n";
				}

				if ($self->{type} ne 'COPY') {
					$s_out =~ s/,$//;
					$s_out .= ") VALUES (";
				}

				# Change table name
				if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
					$self->logit("\tReplacing table $table as " . $self->{replaced_tables}{lc($table)} . "...\n", 1);
					$s_out =~ s/INSERT INTO "$table"/INSERT INTO "$self->{replaced_tables}{lc($table)}"/si;
					$s_out =~ s/COPY "$table"/COPY "$self->{replaced_tables}{lc($table)}"/si;
				}
				# Change column names
				if (exists $self->{replaced_cols}{"\L$table\E"} && $self->{replaced_cols}{"\L$table\E"}) {
					foreach my $c (keys %{$self->{replaced_cols}{"\L$table\E"}}) {
						$self->logit("\tReplacing column $c as " . $self->{replaced_cols}{lc($table)}{$c} . "...\n", 1);
						$s_out =~ s/"$c"/"$self->{replaced_cols}{lc($table)}{$c}"/si;
					}
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

				## don't forget to enable all triggers if needed...
				if ($self->{disable_triggers}) {
					my $tmptb = $table;
					if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
						$self->logit("\tReplacing table $table as " . $self->{replaced_tables}{lc($table)} . "...\n", 1);
						$tmptb = $self->{replaced_tables}{lc($table)};
					}
					if ($self->{case_sensitive} && ($tmptb !~ /"/)) {
						$tmptb = '"' . $tmptb . '"';
					} else {
						$tmptb = lc($tmptb);
					}
					if ($self->{dbhdest}) {
						print DBH "ALTER TABLE $tmptb ENABLE TRIGGER $self->{disable_triggers};\n";
					} else {
						$self->dump("ALTER TABLE $tmptb ENABLE TRIGGER $self->{disable_triggers};\n", $fhdl);
					}
				}

				if ($self->{file_per_table} && !$self->{dbhdest}) {
					$self->close_export_file($fhdl);
				}
				$self->logit("Total extracted records from table $table: $total_record\n", 1);
				my $end_time = time();
				my $dt = $end_time - $start_time;
				my $rps = sprintf("%.1f", $total_record / ($dt+.0001));
				if ($dt > 0) {
					$self->logit("in $dt secs = $rps recs/sec\n", 1);
				} else {
					$self->logit("in $dt secs\n", 1);
				}
			}
		}
		if ($deferred_fkey) {
			if ($self->{defer_fkey} && !$self->{drop_fkey}) {
				$deferred_fkey = 1;
				if ($self->{dbhdest}) {
					print DBH "COMMIT;\n";
				} else {
					$self->dump("COMMIT;\n");
				}
			} elsif ($self->{drop_fkey}) {
				my @create_all = ();
				# Recreate all foreign keys of the concerned tables
				foreach my $table (sort { $self->{tables}{$a}{internal_id} <=> $self->{tables}{$b}{internal_id} } keys %{$self->{tables}}) {
					$self->logit("Restoring table $table foreign keys...\n", 1);
					push(@create_all, $self->_create_foreign_keys($table, @{$self->{tables}{$table}{foreign_key}}));
				}
				foreach my $str (@create_all) {
					if ($self->{dbhdest}) {
						if ($self->{type} ne 'COPY') {
							my $s = $self->{dbhdest}->do($str) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
						} else {
							print DBH "$str\n";
						}
					} else {
						$self->dump("$str\n");
					}
				}
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
				if ($self->{dbhdest}) {
					foreach my $str (@create_all) {
						if ($self->{type} ne 'COPY') {
							my $s = $self->{dbhdest}->do($str) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
						} else {
							print DBH "$str\n";
						}
					}
				} else {
					$self->dump(join("\n", @create_all, "\n"));
				}
			}
		}
		# Disconnect from the database
		$self->{dbh}->disconnect() if ($self->{dbh});
		$self->{dbhdest}->disconnect() if ($self->{dbhdest});

		if ($self->{type} eq 'COPY') {
			close DBH;
		}
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
		#push(@{$parts{$rows->[0]}{$rows->[1]}{$rows->[2]}}, [ { 'type' => $rows->[5], 'value' => $rows->[3], 'column' => $rows->[7], 'colpos' => $rows->[8], 'tablespace' => $rows->[4] } ]);
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
								$create_table{$table}{table} .= "\t$self->{partitions}{$table}{$pos}{$part}[$i]->{column} <= " . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$pos}{$part}[$i]->{value}, $self->{allow_code_break});
							} else {
								$create_table{$table}{table} .= "\t$self->{partitions}{$table}{$pos}{$part}[$i]->{column} > " . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$old_pos}{$old_part}[$i]->{value}, $self->{allow_code_break}) . " AND $self->{partitions}{$table}{$pos}{$part}[$i]->{column} <= " . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$pos}{$part}[$i]->{value}, $self->{allow_code_break});
							}
						}
						$create_table{$table}{table} .= " AND" if ($i < $#{$self->{partitions}{$table}{$pos}{$part}});
						$create_table{$table}{'index'} .= "CREATE INDEX ${part}_$self->{partitions}{$table}{$pos}{$part}[$i]->{column} ON $part ($self->{partitions}{$table}{$pos}{$part}[$i]->{column});\n";
						if ($self->{partitions}{$table}{$pos}{$part}[$i]->{type} eq 'LIST') {
							push(@condition, "NEW.$self->{partitions}{$table}{$pos}{$part}[$i]->{column} IN (" . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$pos}{$part}[$i]->{value}, $self->{allow_code_break}) . ")");
						} else {
							push(@condition, "NEW.$self->{partitions}{$table}{$pos}{$part}[$i]->{column} <= " . Ora2Pg::PLSQL::plsql_to_plpgsql($self->{partitions}{$table}{$pos}{$part}[$i]->{value}, $self->{allow_code_break}));
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

	# Dump the database structure
	if ($self->{export_schema} &&  $self->{schema}) {
		if (!$self->{case_sensitive}) {
			$sql_output .= "CREATE SCHEMA \"\L$self->{schema}\E\";\n\n";
			if ($self->{pg_schema}) {
				$sql_output .= "SET search_path = \L$self->{pg_schema}\E;\n\n";
			} else {
				$sql_output .= "SET search_path = \L$self->{schema}\E, pg_catalog;\n\n";
			}
		} else {
			$sql_output .= "CREATE SCHEMA \"$self->{schema}\";\n\n";
			if ($self->{pg_schema}) {
				$sql_output .= "SET search_path = \"$self->{pg_schema}\";\n\n";
			} else {
				$sql_output .= "SET search_path = \"$self->{schema}\", pg_catalog;\n\n";
			}
		}
	}

	my $constraints = '';
	my $indices = '';
	foreach my $table (sort { $self->{tables}{$a}{internal_id} <=> $self->{tables}{$b}{internal_id} } keys %{$self->{tables}}) {
		$self->logit("Dumping table $table...\n", 1);
		my $tbname = $table;
		if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
			$tbname = $self->{replaced_tables}{"\L$table\E"};
			$self->logit("\tReplacing tablename $table as $tbname...\n", 1);
		}
		my $foreign = '';
		if ($self->{type} eq 'FDW') {
			$foreign = ' FOREIGN';
		}
		if (!$self->{case_sensitive}) {
			$sql_output .= "CREATE$foreign ${$self->{tables}{$table}{table_info}}[1] \"\L$tbname\E\" (\n";
		} else {
			$sql_output .= "CREATE$foreign ${$self->{tables}{$table}{table_info}}[1] \"$tbname\" (\n";
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
				if (!$self->{case_sensitive}) {
					$sql_output .= "\t\"\L$fname\E\" $type";
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
					} elsif ($self->{type} ne 'FDW') {
						$sql_output .= " DEFAULT $f->[4]";
					}
				} elsif (!$f->[3] || ($f->[3] eq 'N')) {
					$sql_output .= " NOT NULL";
				}
				$sql_output .= ",\n";
				last;
			}
		}
		$sql_output =~ s/,$//;
		if ($self->{type} ne 'FDW') {
			$sql_output .= ");\n";
		} else {
			my $schem = "schema '$self->{schema}'," if ($self->{schema});
			if ($self->{case_sensitive}) {
				$sql_output .= ") SERVER $self->{fdw_server} OPTIONS($schem table '$table');\n";
			} else {
				$sql_output .= ") SERVER $self->{fdw_server} OPTIONS($schem table \L$table\E);\n";
			}
		}
		# Add comments on table
		if (${$self->{tables}{$table}{table_info}}[2]) {
			if (!$self->{case_sensitive}) {
				$sql_output .= "COMMENT ON TABLE \"\L$tbname\E\" IS E'${$self->{tables}{$table}{table_info}}[2]';\n";
			} else {
				$sql_output .= "COMMENT ON TABLE \"$tbname\".\"$f->[0]\" IS E'${$self->{tables}{$table}{table_info}}[2]';\n";
			}
		}

		if ($self->{force_owner}) {
			my $owner = ${$self->{tables}{$table}{table_info}}[0];
			$owner = $self->{force_owner} if ($self->{force_owner} ne "1");
			if (!$self->{case_sensitive}) {
				$sql_output .= "ALTER ${$self->{tables}{$table}{table_info}}[1] \"\L$tbname\E\" OWNER TO \L$owner\E;\n";
			} else {
				$sql_output .= "ALTER ${$self->{tables}{$table}{table_info}}[1] \"$tbname\" OWNER TO $owner;\n";
			}
		}

		# Add comments on columns
		foreach $f (@{$self->{tables}{$table}{column_comments}}) {
			next unless $f->[1];
			$f->[1] =~ s/'/\\'/gs;
			if (!$self->{case_sensitive}) {
				$sql_output .= "COMMENT ON COLUMN \L$tbname\.$f->[0]\E IS E'$f->[1]';\n";
			} else {
				$sql_output .= "COMMENT ON COLUMN \"$tbname\".\"$f->[0]\" IS E'$f->[1]';\n";
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
			$indices .= $self->_create_indexes($table, %{$self->{tables}{$table}{indexes}});
			if ($self->{plsql_pgsql}) {
				$indices = Ora2Pg::PLSQL::plsql_to_plpgsql($indices, $self->{allow_code_break});
			}
			if (!$self->{file_per_index} || $self->{dbhdest}) {
				$sql_output .= $indices;
				$sql_output .= "\n" if ($indices);
				$indices = '';
			}
		}
	}
	if ($self->{file_per_index} && !$self->{dbhdest}) {
		my $fhdl = undef;
		$self->logit("Dumping indexes to one separate file : INDEXES_$self->{output}\n", 1);
		$fhdl = $self->export_file("INDEXES_$self->{output}");
		$indices = "-- Nothing found of type indexes\n" if (!$indices);
		my $search_path = '';
		if ($self->{pg_schema}) {
			if (!$self->{case_sensitive}) {
				$search_path = "SET search_path = \L$self->{pg_schema}\E;\n\n";
			} else {
				$search_path = "SET search_path = \"$self->{pg_schema}\";\n\n";
			}
		} else {
			if (!$self->{case_sensitive}) {
				$search_path = "SET search_path = \L$self->{schema}\E, pg_catalog;\n\n" if ($self->{schema});
			} else {
				$search_path = "SET search_path = \"$self->{schema}\", pg_catalog;\n\n" if ($self->{schema});
			}
		}
		$self->dump($sql_header . "\n$search_path" . $indices, $fhdl);
		$self->close_export_file($fhdl);
		$indices = '';
	}

	# Extract data from view if requested
	my $search_in_view = 0;
	foreach (@{$self->{limited}}) {
		if (not exists $self->{tables}{$_}) {
			$self->logit("Found view data export for $_\n", 1);
			$search_in_view = 1;
			last;
		}
	}
	if ($search_in_view) {
		foreach my $table (sort keys %{$self->{views}}) {
			$self->logit("Dumping views as table $table...\n", 1);
			my $tbname = $table;
			if (exists $self->{replaced_tables}{"\L$table\E"} && $self->{replaced_tables}{"\L$table\E"}) {
				$tbname = $self->{replaced_tables}{"\L$table\E"};
			}
			if (!$self->{case_sensitive}) {
				$sql_output .= "CREATE TABLE \"\L$tbname\E\" (\n";
			} else {
				$sql_output .= "CREATE TABLE \"$tbname\" (\n";
			}
			foreach my $i ( 0 .. $#{$self->{views}{$table}{field_name}} ) {
				foreach my $f (@{$self->{views}{$table}{column_info}}) {
					next if ($f->[0] ne "${$self->{views}{$table}{field_name}}[$i]");
					my $type = $self->_sql_type($f->[1], $f->[2], $f->[5], $f->[6]);
					$type = "$f->[1], $f->[2]" if (!$type);
					if (!$self->{case_sensitive}) {
						$sql_output .= "\t\"\L$f->[0]\E\" $type";
					} else {
						$sql_output .= "\t\"$f->[0]\" $type";
					}
					if ($f->[4] ne "") {
						$f->[4] =~  s/SYSDATE/CURRENT_TIMESTAMP/ig;
						$sql_output .= " DEFAULT $f->[4]";
					} elsif (!$f->[3] || ($f->[3] eq 'N')) {
						$sql_output .= " NOT NULL";
					}
					$sql_output .= ",\n";
					last;
				}
			}
			$sql_output =~ s/,$//;
			$sql_output .= ");\n";
		}
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
		map { if ($_ !~ /\(.*\)/) { s/^/"/; s/$/"/; } } @{$indexes{$idx}};
		if (exists $self->{replaced_cols}{"\L$tbsaved\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}) {
			foreach my $c (keys %{$self->{replaced_cols}{"\L$tbsaved\E"}}) {
				map { s/"$c"/"$self->{replaced_cols}{"\L$tbsaved\E"}{$c}"/i } @{$indexes{$idx}};
			}
		}
		my $columns = join(',', @{$indexes{$idx}});
		$columns =~ s/""/"/gs;
		my $unique = '';
		$unique = ' UNIQUE' if ($self->{tables}{$table}{uniqueness}{$idx} eq 'UNIQUE');
		my $str = '';
		if (!$self->{case_sensitive}) {
			$str .= "CREATE$unique INDEX \L$idx\E ON \"\L$table\E\" (\L$columns\E);";
		} else {
			$str .= "CREATE$unique INDEX $idx ON \"$table\" ($columns);";
		}
		push(@out, $str);
	}

	return wantarray ? @out : join("\n", @out);
}

=head2 _drop_indexes

This function return SQL code to drop indexes of a table

=cut
sub _drop_indexes
{
	my ($self, %indexes) = @_;

	my $out = '';
	# Set the index definition
	foreach my $idx (keys %indexes) {
		my $str = '';
		if (!$self->{case_sensitive}) {
			$str = "DROP INDEX \L$idx\E;";
		} else {
			$str = "DROP INDEX $idx;";
		}
		if ($self->{dbhdest}) {
			if ($self->{type} ne 'COPY') {
				my $s = $self->{dbhdest}->do($str);
			} else {
				print DBH "$str\n";
			}
		} else {
			$out .= $str . "\n";
		}
	}
	$out .= "\n" if ($out);

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
	my $newtabname = $self->{case_sensitive} ? $table : lc($table);
	foreach my $consname (keys %$unique_key) {
		my $newconsname = $self->{case_sensitive} ? $consname : lc($consname);
		my $constype =   $unique_key->{$consname}{type};
		my @conscols = @{$unique_key->{$consname}{columns}};
		my %constypenames = ('U' => 'UNIQUE', 'P' => 'PRIMARY KEY');
		my $constypename = $constypenames{$constype};
		for (my $i = 0; $i <= $#conscols; $i++) {
			# Change column names
			if (exists $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"}) {
				$conscols[$i] = $self->{replaced_cols}{"\L$tbsaved\E"}{"\L$conscols[$i]\E"};
			}
		}
		my $columnlist = join(',', map(qq{"$_"}, @conscols));
		$columnlist = lc($columnlist) unless ($self->{case_sensitive});
		if(($constype ne 'P') || $self->{keep_pkey_names}) {
		    $out .= qq{ALTER TABLE "$newtabname" ADD CONSTRAINT "$newconsname" $constypename ($columnlist);\n};
		} else {
		    $out .= qq{ALTER TABLE "$newtabname" ADD PRIMARY KEY ($columnlist);\n};
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
	foreach my $k (keys %$check_constraint) {
		my $chkconstraint = $check_constraint->{$k};
		if (exists $self->{replaced_cols}{"\L$tbsaved\E"} && $self->{replaced_cols}{"\L$tbsaved\E"}) {
			foreach my $c (keys %{$self->{replaced_cols}{"\L$tbsaved\E"}}) {
				$chkconstraint =~ s/"$c"/"$self->{replaced_cols}{"\L$tbsaved\E"}{$c}"/gsi;
				$chkconstraint =~ s/\b$c\b/$self->{replaced_cols}{"\L$tbsaved\E"}{$c}/gsi;
			}
		}
		if (!$self->{case_sensitive}) {
			foreach my $c (@$field_name) {
				# Force lower case
				$chkconstraint =~ s/"$c"/"\L$c\E"/igs;
			}
			$out .= "ALTER TABLE \"\L$table\E\" ADD CONSTRAINT \"\L$k\E\" CHECK ($chkconstraint);\n";
		} else {
			$out .= "ALTER TABLE \"$table\" ADD CONSTRAINT \"$k\" CHECK ($chkconstraint);\n";
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
			map { s/["]+/"/g; } @rfkeys;
			map { s/["]+/"/g; } @lfkeys;
			if (!$self->{case_sensitive}) {
				$str .= "ALTER TABLE \"\L$substable\E\" ADD CONSTRAINT \"\L$h->[0]\E\" FOREIGN KEY (" . lc(join(',', @lfkeys)) . ") REFERENCES \"\L$subsdesttable\E\" (" . lc(join(',', @rfkeys)) . ")";
			} else {
				$str .= "ALTER TABLE \"$substable\" ADD CONSTRAINT \"$h->[0]\" FOREIGN KEY (" . join(',', @lfkeys) . ") REFERENCES \"$subsdesttable\" (" . join(',', @rfkeys) . ")";
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
		if (!$self->{case_sensitive}) {
			$str = "ALTER TABLE \"\L$table\E\" DROP CONSTRAINT \"\L$h->[0]\E\";";
		} else {
			$str .= "ALTER TABLE \"$table\" DROP CONSTRAINT \"$h->[0]\";";
		}
		if ($self->{dbhdest}) {
			if ($self->{type} ne 'COPY') {
				my $s = $self->{dbhdest}->do($str);
			} else {
				print DBH "$str\n";
			}
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
	if ($self->{ora_sensitive} && ($table !~ /"/)) {
		$realtable = "\"$table\"";
	}
	if ($self->{schema}) {
		$realtable = $self->{schema} . ".$realtable";
	}
	my $alias = 'a';

	my $str = "SELECT ";
	my $extraStr = "";
	my $dateformat = 'YYYY-MM-DD HH24:MI:SS';
	if ($self->{enable_microsecond}) {
		$dateformat = 'YYYY-MM-DD HH24:MI:SS.FF3';
	}
	for my $k (0 .. $#{$name}) {

		if ($self->{ora_sensitive} && ($name->[$k] !~ /"/)) {
			$name->[$k] = '"' . $name->[$k] . '"';
		}
		if (!$self->{ora_sensitive}) {
			$name->[$k] = lc($name->[$k]);
		}
		if ( $type->[$k] =~ /(date|time)/) {
			$str .= "to_char($name->[$k], '$dateformat'),";
		} elsif ( $src_type->[$k] =~ /xmltype/i) {
			if ($self->{xml_pretty}) {
				$str .= "$alias.$name->[$k].extract('/').getStringVal(),";
			} else {
				$str .= "$alias.$name->[$k].extract('/').getClobVal(),";
			}
		} else {
			$str .= "$name->[$k],";
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

	# Backward compatibility with LongTrunkOk with typo
	if ($self->{longtrunkok} && not defined $self->{longtruncok}) {
		$self->{longtruncok} = $self->{longtrunkok};
	}
	# Fix a problem when exporting type LONG and LOB
	$self->{dbh}->{'LongReadLen'} = $self->{longreadlen} || (1023*1024);
	$self->{dbh}->{'LongTruncOk'} = $self->{longtruncok} || 0;

	my $sth = $self->{dbh}->prepare($str,{ora_pers_lob=>1}) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	return $sth;	

}


=head2 _sql_type INTERNAL_TYPE LENGTH PRECISION SCALE

This function returns the PostgreSQL datatype corresponding to the
Oracle data type.

=cut

sub _sql_type
{
        my ($self, $type, $len, $precision, $scale) = @_;

	my $data_type = '';

	# Simplify timestamp type
	$type =~ s/TIMESTAMP.*/TIMESTAMP/i;

        # Overiride the length
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
						if ($self->{pg_numeric_type}) {
							if ($precision < 5) {
								return 'smallint';
							} elsif ($precision <= 10) {
								return 'integer'; # The speediest in PG
							} else {
								return 'bigint';
							}
						}
						return "numeric($precision)";
					} elsif ($self->{pg_numeric_type}) {
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
			if (($type eq 'NUMBER') && $self->{pg_numeric_type}) {
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
	my ($self, $table, $owner) = @_;

	$owner = "AND upper(OWNER)='\U$owner\E' " if ($owner);
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("WARNING only: " . $self->{dbh}->errstr . "\n", 0, 0);
SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE, DATA_DEFAULT, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH
FROM $self->{prefix}_TAB_COLUMNS
WHERE TABLE_NAME='$table' $owner
ORDER BY COLUMN_ID
END
	if (not defined $sth) {
		# Maybe a 8i database.
		$sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE, DATA_DEFAULT, DATA_PRECISION, DATA_SCALE
FROM $self->{prefix}_TAB_COLUMNS
WHERE TABLE_NAME='$table' $owner
ORDER BY COLUMN_ID
END
		$self->logit("INFO: please forget the above error message, this is a warning only for Oracle 8i database.\n", 0, 0);
	}
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my $data = $sth->fetchall_arrayref();
	foreach my $d (@$data) {
		if ($#{$d} == 7) {
			$self->logit("\t$d->[0] => type:$d->[1] , length:$d->[2] (char_length:$d->[7]), precision:$d->[5], scale:$d->[6], nullable:$d->[3] , default:$d->[4]\n", 1);
			$d->[2] = $d->[7] if $d->[1] =~ /char/i;
		} else {
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
SELECT CONSTRAINT_NAME,R_CONSTRAINT_NAME,SEARCH_CONDITION,DELETE_RULE,DEFERRABLE,DEFERRED,R_OWNER,CONSTRAINT_TYPE
FROM $self->{prefix}_CONSTRAINTS
WHERE CONSTRAINT_TYPE IN $cons_types
AND STATUS='ENABLED'
AND TABLE_NAME='$table' $owner
END
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	while (my $row = $sth->fetch) {
		my %constraint = (type => $row->[7], columns => ());
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
		$data{$row->[0]} = $row->[2];
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
	my $str = "SELECT GRANTEE,OWNER,TABLE_NAME,PRIVILEGE FROM DBA_TAB_PRIVS";
	if ($self->{schema}) {
		$str .= " WHERE upper(GRANTOR) = '\U$self->{schema}\E'";
	} else {
		$str .= " WHERE GRANTOR NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	}
	$str .= " ORDER BY TABLE_NAME, GRANTEE";
	my $error = "\n\nFATAL: YOU MUST BE CONNECTED AS AN ORACLE DBA USER TO RETRIEVED GRANTS\n\n";
	my $sth = $self->{dbh}->prepare($str) or $self->logit($error . "FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	while (my $row = $sth->fetch) {
		$privs{$row->[2]}{type} = 'table';
		$privs{$row->[2]}{type} = 'function' if (uc($row->[3]) eq 'EXECUTE');
		$privs{$row->[2]}{owner} = $row->[1] if (!$privs{$row->[2]}{owner});
		push(@{$privs{$row->[2]}{privilege}{$row->[0]}}, $row->[3]);
		push(@{$roles{owner}}, $row->[1]) if (!grep(/^$row->[1]$/, @{$roles{owner}}));
		push(@{$roles{grantee}}, $row->[0]) if (!grep(/^$row->[0]$/, @{$roles{grantee}}));
	}
	$sth->finish();

	# Retrieve all privilege per column table defined in this database
	$str = "SELECT GRANTEE,OWNER,TABLE_NAME,PRIVILEGE,COLUMN_NAME FROM DBA_COL_PRIVS";
	if ($self->{schema}) {
		$str .= " WHERE upper(GRANTOR) = '\U$self->{schema}\E'";
	} else {
		$str .= " WHERE GRANTOR NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
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
		$str = "SELECT ROLE FROM DBA_ROLES WHERE ROLE='$u'";
		$sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while (my $row = $sth->fetch) {
			$roles{type}{$u} = 'ROLE';
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
	my $sth = $self->{dbh}->prepare(<<END) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
SELECT DISTINCT $self->{prefix}_IND_COLUMNS.INDEX_NAME,$self->{prefix}_IND_COLUMNS.COLUMN_NAME,$self->{prefix}_INDEXES.UNIQUENESS,$self->{prefix}_IND_COLUMNS.COLUMN_POSITION
FROM $self->{prefix}_IND_COLUMNS, $self->{prefix}_INDEXES
WHERE $self->{prefix}_IND_COLUMNS.TABLE_NAME='$table' $owner
AND $self->{prefix}_INDEXES.INDEX_NAME=$self->{prefix}_IND_COLUMNS.INDEX_NAME
AND $self->{prefix}_IND_COLUMNS.INDEX_NAME NOT IN (SELECT CONSTRAINT_NAME FROM $self->{prefix}_CONSTRAINTS WHERE TABLE_NAME='$table' $sub_owner)
ORDER BY $self->{prefix}_IND_COLUMNS.COLUMN_POSITION
END
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
	while (my $row = $sth->fetch) {
		$unique{$row->[0]} = $row->[2];
		# Replace function based index type
		if ($row->[1] =~ /^SYS_NC/i) {
			$sth2->execute($row->[1]) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
			my $nc = $sth2->fetch();
			$row->[1] = $nc->[0];
		}
		$row->[1] =~ s/SYS_EXTRACT_UTC[\s\t]*\(([^\)]+)\)/$1/isg;
		push(@{$data{$row->[0]}}, $row->[1]);
	}

	return \%unique, \%data;
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
		push(@seqs, [ @$row ]);
	}

	return \@seqs;
}


=head2 _get_views

This function implements an Oracle-native views information.

Returns a hash of view names with the SQL queries they are based on.

=cut

sub _get_views
{
	my($self) = @_;

	# Retrieve all views
	my $str = "SELECT VIEW_NAME,TEXT FROM $self->{prefix}_VIEWS";
	if (!$self->{schema}) {
		$str .= " WHERE OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
	} else {
		$str .= " WHERE upper(OWNER) = '\U$self->{schema}\E'";
	}
	$str .= " ORDER BY VIEW_NAME";
	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %data = ();
	while (my $row = $sth->fetch) {
		$data{$row->[0]} = $row->[1];
		@{$data{$row->[0]}{alias}} = $self->_alias_info ($row->[0]);
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
	my $str = "SELECT TRIGGER_NAME, TRIGGER_TYPE, TRIGGERING_EVENT, TABLE_NAME, TRIGGER_BODY, WHEN_CLAUSE, DESCRIPTION FROM $self->{prefix}_TRIGGERS WHERE STATUS='ENABLED'";
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
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE='PACKAGE'";
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
		next if ($row->[0] =~ /\$/);
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
	my ($self) = @_;

	# Retrieve all user defined types
	my $str = "SELECT DISTINCT OBJECT_NAME,OWNER FROM $self->{prefix}_OBJECTS WHERE OBJECT_TYPE='TYPE'";
	$str .= " AND STATUS='VALID'" if (!$self->{export_invalid});
	if (!$self->{schema}) {
		# We need to remove SYSTEM from the exclusion list
		shift(@{$self->{sysusers}});
		$str .= " AND OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
		unshift(@{$self->{sysusers}},'SYSTEM');
	} else {
		$str .= " AND (upper(OWNER) = '\U$self->{schema}\E' OR OWNER = 'SYSTEM')";
	}
	$str .= " ORDER BY OBJECT_NAME";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my @types = ();
	my @fct_done = ();
	while (my $row = $sth->fetch) {
		next if ($row->[0] =~ /\$/);
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
	my $xtable = shift;

	my $sql = "SELECT
                NULL            TABLE_CAT,
                at.OWNER        TABLE_SCHEM,
                at.TABLE_NAME,
                tc.TABLE_TYPE,
                tc.COMMENTS     REMARKS
            from ALL_TABLES at, ALL_TAB_COMMENTS tc
            where at.OWNER = tc.OWNER
            and at.TABLE_NAME = tc.TABLE_NAME
	";
	if ($xtable) {
		$sql .= " and upper(at.TABLE_NAME)='\U$xtable\E'";
	}

	if ($self->{schema}) {
		$sql .= " and at.OWNER='\U$self->{schema}\E'";
	} else {
            $sql .= "AND at.OWNER NOT IN ('" . join("','", @{$self->{sysusers}}) . "')";
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
SELECT a.SEGMENT_NAME,a.TABLESPACE_NAME,a.SEGMENT_TYPE,c.FILE_NAME
FROM $self->{prefix}_SEGMENTS a,$self->{prefix}_OBJECTS b, $self->{prefix}_DATA_FILES c
WHERE a.SEGMENT_TYPE IN ('INDEX', 'TABLE')
AND a.SEGMENT_NAME = b.OBJECT_NAME
AND a.SEGMENT_TYPE = b.OBJECT_TYPE
AND a.OWNER = b.OWNER
AND a.TABLESPACE_NAME = c.TABLESPACE_NAME
};
	if ($self->{schema}) {
		$str .= " AND upper(a.OWNER)='\U$self->{schema}\E'";
	} else {
		$str .= " AND a.TABLESPACE_NAME NOT IN ('SYSTEM','TOOLS')";
	}
	$str .= " ORDER BY TABLESPACE_NAME";
	my $error = "\n\nFATAL: YOU MUST BE CONNECTED AS AN ORACLE DBA USER TO RETRIEVED TABLESPACES\n\n";
	my $sth = $self->{dbh}->prepare($str) or $self->logit($error . "FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %tbs = ();
	while (my $row = $sth->fetch) {
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
FROM
	dba_tab_partitions a,
	dba_part_tables b,
	dba_part_key_columns c
WHERE
	a.table_name = b.table_name AND
	(b.partitioning_type = 'RANGE' OR b.partitioning_type = 'LIST')
	AND a.table_name = c.name
};
	if ($self->{prefix} ne 'USER') {
		if ($self->{schema}) {
			$str .= "\tAND a.table_owner ='\U$self->{schema}\E'\n";
		} else {
			$str .= "\tAND a.table_owner NOT IN ('" . join("','", @{$self->{sysusers}}) . "')\n";
		}
	}
	$str .= "ORDER BY a.table_name,a.partition_position,c.column_position\n";

	my $sth = $self->{dbh}->prepare($str) or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth->execute or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);

	my %parts = ();
	my %default = ();
	while (my $rows = $sth->fetch) {
		if ( ($rows->[3] eq 'MAXVALUE') || ($rows->[3] eq 'DEFAULT')) {
			$default{$rows->[0]} = $rows->[2];
			next;
		}
		push(@{$parts{$rows->[0]}{$rows->[1]}{$rows->[2]}}, { 'type' => $rows->[5], 'value' => $rows->[3], 'column' => $rows->[7], 'colpos' => $rows->[8], 'tablespace' => $rows->[4] });
		$self->logit(".",1);
	}
	$sth->finish;
	$self->logit("\n", 1);

	return \%parts, \%default;
}

# This is a routine paralellizing access to format_data_for_parallel over several threads
sub format_data_parallel
{
	my ($self, $rows, $data_types, $action, $src_data_types) = @_;

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
		# The received payload is an array of a columns array, types array, type of action and source types array
		$self->format_data_row($payload->[0],$payload->[1],$payload->[2], $payload->[3]);
		$queue_done->enqueue($payload->[0]); # We only need data
		$counter++;
	}
	return 0;
}

sub format_data_row
{
	my ($self, $row, $data_types, $action, $src_data_types) = @_;

	for (my $idx = 0; $idx < scalar(@$data_types); $idx++) {
		my $data_type = $data_types->[$idx] || '';

		# Preparing data for output
		if ($action ne 'COPY') {
			if ($row->[$idx] eq '') {
				$row->[$idx] = 'NULL';
			} elsif ($data_type eq 'bytea') {
				$row->[$idx] = escape_bytea($row->[$idx]);
				if (!$self->{standard_conforming_strings}) {
					$row->[$idx] = "'$row->[$idx]'";
				} else {
					$row->[$idx] = "E'$row->[$idx]'";
				}
			} elsif ($data_type =~ /(char|text|xml)/) {
				$row->[$idx] =~ s/'/''/gs; # escape single quote
				if (!$self->{standard_conforming_strings}) {
					$row->[$idx] =~ s/\\/\\\\/g;
					$row->[$idx] =~ s/\0//gs;
					$row->[$idx] = "'$row->[$idx]'";
				} else {
					$row->[$idx] =~ s/\0//gs;
					$row->[$idx] = "E'$row->[$idx]'";
				}
			} elsif ($data_type =~ /(date|time)/) {
				if ($row->[$idx] =~ /^0000-00-00/) {
					$row->[$idx] = 'NULL';
				} else {
					$row->[$idx] = "'$row->[$idx]'";
				}
			} else {
				$row->[$idx] =~ s/,/\./;
				$row->[$idx] =~ s/\~/inf/;
			}
		} else {
			if ($row->[$idx] eq '') {
				$row->[$idx] = '\N';
			} elsif ($data_type !~ /(char|date|time|text|bytea|xml)/) {
				$row->[$idx] =~ s/,/\./;
				$row->[$idx] =~ s/\~/inf/;
			} elsif ($data_type eq 'bytea') {
				$row->[$idx] = escape_bytea($row->[$idx]);
			} elsif ($data_type !~ /(date|time)/) {
				$row->[$idx] =~ s/\0//gs;
				$row->[$idx] =~ s/\\/\\\\/g;
				$row->[$idx] =~ s/\r/\\r/g;
				$row->[$idx] =~ s/\n/\\n/g;
				$row->[$idx] =~ s/\t/\\t/g;
				if (!$self->{noescape}) {
					$row->[$idx] =~ s/\f/\\f/gs;
					$row->[$idx] =~ s/([\1-\10])/sprintf("\\%03o", ord($1))/egs;
					$row->[$idx] =~ s/([\13-\14])/sprintf("\\%03o", ord($1))/egs;
					$row->[$idx] =~ s/([\16-\37])/sprintf("\\%03o", ord($1))/egs;
				}
			} elsif ($data_type =~ /(date|time)/) {
				if ($row->[$idx] =~ /^0000-00-00/) {
					$row->[$idx] = '\N';
				}
			}
		}
	}
}

sub format_data
{
	my ($self, $rows, $data_types, $action, $src_data_types) = @_;

	foreach my $row (@$rows) {
		format_data_row($self,$row,$data_types,$action, $src_data_types);
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
		} elsif (!grep(/^$var$/, 'TABLES', 'MODIFY_STRUCT', 'REPLACE_TABLES', 'REPLACE_COLS', 'WHERE', 'EXCLUDE', 'ORA_RESERVED_WORDS','SYSUSERS')) {
			$AConfig{$var} = $val;
		} elsif ( ($var eq 'TABLES') || ($var eq 'EXCLUDE') ) {
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
			my @replace_tables = split(/[\s\t]+/, $val);
			foreach my $r (@replace_tables) { 
				my ($old, $new) = split(/:/, $r);
				$AConfig{$var}{$old} = $new;
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
		if ($lines[$i] =~ /^(FUNCTION|PROCEDURE)[\t\s]+([a-z0-9_\-"]+)(.*)/i) {
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

	my $content = '';
	if ($plsql =~ /PACKAGE[\s\t]+BODY[\s\t]*([^\s\t]+)[\s\t]*(AS|IS)[\s\t]*(.*)/is) {
		my $pname = $1;
		my $type = $2;
		$content = $3;
		$pname =~ s/"//g;
		$self->{idxcomment} = 0;
		$content =~ s/END[^;]*;$//is;
		my %comments = $self->_remove_comments(\$content);
		my @functions = $self->_extract_functions($content);
		$content = "-- PostgreSQL does not recognize PACKAGES, using SCHEMA instead.\n";
		$content .= "DROP SCHEMA IF EXISTS $pname CASCADE;\n";
		$content .= "CREATE SCHEMA $pname;\n";
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
		$$content =~ s/$k/$comments->{$k}/;
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
		$func_args = Ora2Pg::PLSQL::replace_sql_type($func_args, $self->{pg_numeric_type}, $self->{default_numeric});

		#$func_declare = $self->_convert_declare($func_declare);
		$func_declare = Ora2Pg::PLSQL::replace_sql_type($func_declare, $self->{pg_numeric_type}, $self->{default_numeric});
		# Replace PL/SQL code into PL/PGSQL similar code
		$func_declare = Ora2Pg::PLSQL::plsql_to_plpgsql($func_declare, $self->{allow_code_break});
		if ($func_code) {
			$func_code = Ora2Pg::PLSQL::plsql_to_plpgsql("BEGIN".$func_code, $self->{allow_code_break});
		}
	} else {
		return $plsql;
	}
	if ($func_code) {
		$func_name= "\"$func_name\"" if ($self->{case_sensitive});
		$func_args = '()' if (!$func_args);
		$func_name = $pname . '.' . $func_name if ($pname);
		my $function = "\nCREATE OR REPLACE FUNCTION $func_name $func_args";
		$self->logit("\tParsing function $func_name...\n", 1);
		if ($hasreturn) {
			$function .= " RETURNS $func_ret_type AS \$body\$\n";
		} else {
			my @nout = $func_args =~ /\bOUT /ig;
			if ($#nout > 0) {
				$function .= " RETURNS RECORD AS \$body\$\n";
			} elsif ($#nout == 0) {
				$func_args =~ /([A-Z0-9_\$]+)[\s\t]+OUT[\s\t]+/;
				$function .= " RETURNS $1 AS \$body\$\n";
			} else {
				$function .= " RETURNS VOID AS \$body\$\n";
			}
		}
		$func_declare = '' if ($func_declare !~ /[a-z]/is);
		$function .= "DECLARE\n$func_declare\n" if ($func_declare);
		$function .= $func_code;
		$function .= "\n\$body\$\nLANGUAGE PLPGSQL;\n";
		if ($self->{estimate_cost}) {
			$func_name =~ s/"//g;
			my $cost = Ora2Pg::PLSQL::estimate_cost($function);
			$function .= "-- Porting cost of function \L$func_name\E: $cost\n" if ($cost);
			$self->{pkgcost} += ($cost || 0);
		}
		$function = "\n$func_before$function";

		$pname =~ s/"//g; # Remove case sensitivity quoting
		if ($pname && $self->{file_per_function} && !$self->{dbhdest}) {
			$func_name =~ s/^$pname\.//i;
			$func_name =~ s/"//g; # Remove case sensitivity quoting
			$self->logit("\tDumping to one file per function: $dirprefix\L$pname/$func_name\E_$self->{output}\n", 1);
			my $sql_header = "-- Generated by Ora2Pg, the Oracle database Schema converter, version $VERSION\n";
			$sql_header .= "-- Copyright 2000-2012 Gilles DAROLD. All rights reserved.\n";
			$sql_header .= "-- DATASOURCE: $self->{oracle_dsn}\n\n";
			if ($self->{client_encoding}) {
				$sql_header .= "SET client_encoding TO \U$self->{client_encoding}\E;\n\n";
			}
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
		if (!$self->{case_sensitive}) {
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
		$sqlstr = Ora2Pg::PLSQL::plsql_to_plpgsql($sqlstr, $self->{allow_code_break});
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
		my $type = Ora2Pg::PLSQL::replace_sql_type($3, $self->{pg_numeric_type}, $self->{default_numeric});
		$type_name =~ s/"//g;
		$content = qq{
--
CREATE TYPE \"\L$type_name\E\";
CREATE OR REPLACE FUNCTION \L$type_name\E_in_function(cstring) RETURNS \L$type_name\E AS
 ... CODE HERE WHAT TO DO WHEN INSERTING A VALUE ... ;
CREATE OR REPLACE FUNCTION \L$type_name\E_out_function(\L$type_name\E) RETURNS cstring AS
 ... CODE HERE WHAT TO DO WHEN QUERYING THE VALUE ... ;
CREATE TYPE \"\L$type_name\E\" (
        INTERNALLENGTH = VARIABLE,
        INPUT = \L$type_name\E_in_function,
        OUTPUT = \L$type_name\E_out_function,
        ELEMENT = $type
);
};
	} elsif ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+(AS|IS)[\t\s]*OBJECT[\t\s]+\((.*?)(TYPE BODY.*)/is) {
		my $type_name = $1;
		my $description = $3;
		my $body = $4;
		my %fctname = ();
		# extract input function
		while ($description =~ s/CONSTRUCTOR (FUNCTION|PROCEDURE)[\t\s]+([^\t\s\(]+)(.*?)RETURN[^,;]+[,;]//is) {
			$fctname{constructor} = lc($2) if (!$fctname{constructor});
		}
		while ($description =~ s/(MAP MEMBER |MEMBER )(FUNCTION|PROCEDURE)[\t\s]+([^\t\s\(]+)(.*?)RETURN[^,;]+[,;]//is) {
			push(@{$fctname{member}}, lc($3));
		}
		my $declar = Ora2Pg::PLSQL::replace_sql_type($description, $self->{pg_numeric_type}, $self->{default_numeric});
		$type_name =~ s/"//g;
		return if ($type_name =~ /\$/);

		if ($body =~ /TYPE BODY[\s\t]+$type_name[\s\t]*(IS|AS)[\s\t]*(.*)END;/is) {
			my $content2 = $2;
			my %comments = $self->_remove_comments(\$content2);
			$content2 =~ s/(CONSTRUCTOR |MAP MEMBER |MEMBER )(FUNCTION|PROCEDURE)/FUNCTION/igs;
			my @functions = $self->_extract_functions($content2);
			$content2 = '';
			foreach my $f (@functions) {
				$content .= $self->_convert_function($f);
			}
			$self->_restore_comments(\$content, \%comments);

		}
		if (!exists $fctname{constructor} && !exists $fctname{member}) {
			$content .= qq{
CREATE TYPE \"\L$type_name\E\" AS (
$declar
);
};
		} else {
			my $funcdecl = join(',', @{$fctname{member}});
			$content .= qq{
-- Oracle custom type body are equivalent to PostgreSQL custom type, feel free
-- to adapt the above function to the following custom type.
CREATE TYPE \"\L$type_name\E\" AS (
$declar
	INTERNALLENGTH = VARIABLE,
	INPUT = $fctname{constructor},
	OUTPUT = output_function
	[ , RECEIVE = receive_function ]
	[ , SEND = send_function ]
	[ , TYPMOD_IN = type_modifier_input_function ]
	[ , TYPMOD_OUT = type_modifier_output_function ]
	[ , ANALYZE = analyze_function ]
	-- List of available function declared aboved:
	$funcdecl
);
};
		}

	} elsif ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+(AS|IS)[\t\s]*OBJECT[\t\s]+\((.*)/is) {
		my $type_name = $1;
		my $description = $3;
		$description =~ s/\)[\t\s]*(FINAL|NOT FINAL|INSTANTIABLE|NOT INSTANTIABLE).*//is;
		my $notfinal = $1;
		$notfinal =~ s/[\s\t\r\n]+//gs;
		return $plsql if ($description =~ /[\s\t]*(MAP MEMBER |MEMBER )(FUNCTION|PROCEDURE).*/);
		# $description =~ s/[\s\t]*(MAP MEMBER |MEMBER )(FUNCTION|PROCEDURE).*//is;
		my $declar = Ora2Pg::PLSQL::replace_sql_type($description, $self->{pg_numeric_type}, $self->{default_numeric});
		$type_name =~ s/"//g;
		if ($notfinal =~ /FINAL/is) {
			$content = "-- Inherited types are not supported in PostgreSQL, replacing with inherited table\n";
			$content .= qq{CREATE TABLE \"\L$type_name\E\" (
$declar
);
};
		} else {
			$content = qq{
CREATE TYPE \"\L$type_name\E\" AS (
$declar
);
};
		}
	} elsif ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+UNDER[\t\s]*([^\t\s]+)[\t\s]+\((.*)/is) {
		my $type_name = $1;
		my $type_inherit = $2;
		my $description = $3;
		$description =~ s/\)[^\);]*;$//;
		return $plsql if ($description =~ /[\s\t]*(MAP MEMBER |MEMBER )(FUNCTION|PROCEDURE).*/);
		my $declar = Ora2Pg::PLSQL::replace_sql_type($description, $self->{pg_numeric_type}, $self->{default_numeric});
		$type_name =~ s/"//g;
		$content = qq{
CREATE TABLE \"\L$type_name\E\" (
$declar
) INHERITS (\L$type_inherit\E);
};
	} elsif ($plsql =~ /TYPE[\t\s]+([^\t\s]+)[\t\s]+(AS|IS)[\t\s]*VARRAY[\t\s]*\((\d+)\)[\t\s]*OF[\t\s]*(.*)/is) {
		my $type_name = $1;
		my $size = $3;
		my $tbname = $4;
		$type_name =~ s/"//g;
		$tbname =~ s/;//g;
		return if ($type_name =~ /\$/);
		$content = qq{
CREATE TYPE \"\L$type_name\E\" AS ($type_name $tbname\[$size\]);
};
	} else {
		$plsql =~ s/;$//;
		$content = "CREATE $plsql;"
	}
	return $content;
}

sub extract_data
{
	my ($self, $table, $s_out, $nn, $tt, $fhdl, $sprep, $stt) = @_;

	my $total_record = 0;
	$self->{data_limit} ||= 10000;


	my $sth = $self->_get_data($table, $nn, $tt, $stt);

	while( my $rows = $sth->fetchall_arrayref(undef,$self->{data_limit})) {
		my $inter_time = time();

		my $sql = '';
		if ($self->{type} eq 'COPY') {
			$sql = $s_out;
		}
		# Preparing data for output
		$self->logit("DEBUG: Preparing bulk of $self->{data_limit} data for output\n", 1);
		if (!defined $sprep) {
			# If there is a bytea column
			if ($self->{thread_count} && scalar(grep(/bytea/,@$tt))) {
				# We need to parallelize this formatting, as it is costly (bytea)
				$rows = $self->format_data_parallel($rows, $tt, $self->{type}, $stt);
				# we change $rows reference!
			} else {
				$self->format_data($rows, $tt, $self->{type}, $stt);
			}
		}
		# Creating output
		$self->logit("DEBUG: Creating output for $self->{data_limit} tuples\n", 1);
		my $count = 0;
		if ($self->{type} eq 'COPY') {
			map { $sql .= join("\t", @$_) . "\n"; $count++; } @$rows;
			$sql .= "\\.\n";
		} elsif (!defined $sprep) {
			foreach my $row (@$rows) {
				$sql .= $s_out;
				$sql .= join(',', @$row) . ");\n";
				$count++;
			}
		}
		# Insert data if we are in online processing mode
		if ($self->{dbhdest}) {
			if ($self->{type} ne 'COPY') {
				if (!defined $sprep) {
					$self->logit("DEBUG: Sending output directly to PostgreSQL backend\n", 1);
					my $s = $self->{dbhdest}->do($sql) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
				} else {
					my $ps = $self->{dbhdest}->prepare($sprep) or $self->logit("FATAL: " . $self->{dbhdest}->errstr . "\n", 0, 1);
					for (my $i = 0; $i <= $#{$tt}; $i++) {
						if ($tt->[$i] eq 'bytea') {
							$ps->bind_param($i+1, undef, { pg_type => DBD::Pg::PG_BYTEA });
						}
					}
					$self->logit("DEBUG: Sending bulk output directly to PostgreSQL backend\n", 1);
					foreach my $row (@$rows) {
						$ps->execute(@$row) or $self->logit("FATAL: " . $ps->errstr . "\n", 0, 1);
						$count++;
					}
					$ps->finish();
				}
			} else {
				$self->logit("DEBUG: Dumping output to psql\n", 1);
				print DBH $sql;
			}
		} else {
			$self->dump($sql, $fhdl);
		}

		my $end_time = time();
		my $dt = $end_time - $inter_time;
		my $rps = sprintf("%2.1f", $count / ($dt+.0001));
		if ($dt > 0) {
			$self->logit("\n$count records in $dt secs = $rps recs/sec\n", 1);
		} else {
			$self->logit("\n$count records in $dt secs\n", 1);
		}
		$total_record += $count;
	}
	$sth->finish();

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
		$self->logit("NLS_LANG2 $ENV{NLS_LANG}\n", 0);
		$self->logit("CLIENT ENCODING $self->{client_encoding}\n", 0);
	} elsif ($type eq 'SHOW_SCHEMA') {
		# Get all tables information specified by the DBI method table_info
		$self->logit("Showing all schema...\n", 1);
		my $sth = $self->_schema_list()  or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		while ( my @row = $sth->fetchrow()) {
			$self->logit("SCHEMA $row[0]\n", 0);
		}
		$sth->finish();
	} elsif ( ($type eq 'SHOW_TABLE') || ($type eq 'SHOW_COLUMN') ) {
		# Get all tables information specified by the DBI method table_info
		$self->logit("Showing table information...\n", 1);

		my $sth = $self->_table_info($self->{xtable})  or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
		my @tables_infos = $sth->fetchall_arrayref();
		$sth->finish();

		my @done = ();
		my $id = 0;
		foreach my $table (@tables_infos) {
			# Set the table information for each class found
			my $i = 1;
			foreach my $t (@$table) {
				# Jump to desired extraction
				if (grep(/^$t->[2]$/, @done)) {
					$self->logit("Duplicate entry found: $t->[0] - $t->[1] - $t->[2]\n", 1);
					next;
				} else {
					push(@done, $t->[2]);
				}
				next if (($#{$self->{limited}} >= 0) && !grep($t->[2] =~ /^$_$/i, @{$self->{limited}}));
				next if (($#{$self->{excluded}} >= 0) && grep($t->[2] =~ /^$_$/i, @{$self->{excluded}}));

				$self->logit("[$i] TABLE $t->[2]\n", 0);

				# Set the fields information
				if ($type eq 'SHOW_COLUMN') {
					my $query = "SELECT * FROM $t->[1].$t->[2] WHERE 1=0";
					if ($self->{ora_sensitive} && ($t->[2] !~ /"/)) {
						$query = "SELECT * FROM $t->[1].\"$t->[2]\" WHERE 1=0";
					}
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
					foreach my $j ( 0 .. $#{$sth->{NAME}} ) {
						$self->logit("\t$sth->{NAME}->[$j]\n", 0);
					}
					$sth->finish();
				}
				$i++;
			}
		}

	}
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

	my $sth = $self->{dbh}->do("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF3'") or $self->logit("FATAL: " . $self->{dbh}->errstr . "\n", 0, 1);
	$sth = undef;

}

# Preload the bytea array at lib init
BEGIN
{
	build_escape_bytea();
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


