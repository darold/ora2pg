use ExtUtils::MakeMaker qw(prompt WriteMakefile);

my @ALLOWED_ARGS = ('CONFDIR','DOCDIR','DESTDIR','QUIET','INSTALLDIRS','INSTALL_BASE','PREFIX');

# Parse command line arguments and store them as environment variables
while ($_ = shift) {
	my ($k,$v) = split(/=/, $_, 2);
	if (grep(/^$k$/, @ALLOWED_ARGS)) {
		$ENV{$k} = $v;
	}
}

# Default install path
my $CONFDIR = $ENV{CONFDIR} || '/etc/ora2pg';
my $DOCDIR = $ENV{DOCDIR} || '/usr/local/share/doc/ora2pg';
my $DEST_CONF_FILE = 'ora2pg.conf.dist';
if ($^O =~ /MSWin32|dos/i) {
	$DEST_CONF_FILE = 'ora2pg_dist.conf';
}

my $PREFIX = $ENV{DESTDIR} || $ENV{PREFIX} || $ENV{INSTALL_BASE} ||  '';
$PREFIX =~ s/\/$//;
$ENV{INSTALLDIRS} ||= 'site';

# Try to set the default configuration directory following $PREFIX
if ($^O !~ /MSWin32|dos/i) {
	if ($PREFIX =~ m#^/usr/local(/|$)#) {
		$CONFDIR = '/usr/local/etc';
		$DOCDIR = '/usr/local/share/doc/ora2pg';
	} elsif ($PREFIX =~ m#^/opt(/|$)#) {
		$CONFDIR =  '/etc/opt';
		$DOCDIR = '/opt/ora2pg';
	} else {
		$CONFDIR = '/etc';
		$DOCDIR = '/usr/share/doc/ora2pg';
	}
} else {
	$CONFDIR = 'C:\ora2pg';
	$DOCDIR = 'C:\ora2pg';
}

if ($PREFIX) {
	$CONFDIR = $PREFIX . $CONFDIR;
	$DOCDIR = $PREFIX . $DOCDIR;
}

# Try to find all binary used by Ora2Pg
my $bzip2 = '';
if ($^O !~ /MSWin32|dos/i) {
	my $bzip2 = `which bzip2`;
	chomp($bzip2);
	$bzip2 ||= '/usr/bin/bzip2';
}

my $oracle_home = $ENV{ORACLE_HOME} || '/usr/local/oracle/10g';

# Setup ok. generating default ora2pg.conf config file
unless(open(OUTCFG, ">$DEST_CONF_FILE")) {
	print "\nError: can't write config file $DEST_CONF_FILE, $!\n";
	exit 0;
}

	print OUTCFG qq{
####################  Ora2Pg Configuration file   #####################

# Support for including common config file that may containt any
# of the following configuration directives.
#IMPORT	common.conf

# Set this directive to a file containing PL/SQL Oracle Code like function,
# procedure or a full package body to prevent Ora2Pg from connecting to an
# Oracle database end just apply his convertion tool to the content of the
# file. This can only be used with the following export type: PROCEDURE,
# FUNCTION or PACKAGE. If you don't know what you do don't use this directive.
#INPUT_FILE	ora_plsql_src.sql

# Set the Oracle home directory
ORACLE_HOME	$oracle_home

# Set Oracle database connection (datasource, user, password)
ORACLE_DSN	dbi:Oracle:host=mydb.mydom.fr;sid=SIDNAME
ORACLE_USER	system
ORACLE_PWD	manager

# Set this to 1 if you connect as simple user and can not extract things
# from the DBA_... tables. It will use tables ALL_... This will not works
# with GRANT export, you should use an Oracle DBA username at ORACLE_USER
USER_GRANTS     0

# Trace all to stderr
DEBUG		0

# Export Oracle schema to PostgreSQL schema
EXPORT_SCHEMA	0

# Oracle schema/owner to use
#SCHEMA		SCHEMA_NAME

# Enable/disable the CREATE SCHEMA SQL order at starting of the output file.
# It is enable by default and concern on TABLE export type.
CREATE_SCHEMA	1

# Enable this directive to force Oracle to compile schema before exporting code.
# This will ask to Oracle to validate the PL/SQL that could have been invalidate
# after a export/import for example. If you set the value to 1 will exec:
# DBMS_UTILITY.compile_schema(schema => sys_context('USERENV', 'SESSION_USER'));
# but if you probvide the name of a particular schema it will use the following
# command: DBMS_UTILITY.compile_schema(schema => 'schemaname');
COMPILE_SCHEMA	0

# If the above configuration directive is not enough to validate your PL/SQL code
# enable this configuration directive to allow export of all PL/SQL code even if
# it is marked as invalid. The 'VALID' or 'INVALID' status applies to functions,
# procedures, packages and user defined types.
EXPORT_INVALID	0

# PostreSQL search path schem to use. Can be a coma delimited list,
# for example: users_schem,public will result in the following PostgreSQL 
# schema path: SET search_path = users_schema,public;
# By default search_path is set to Oracle schema and pg_catalog.
#PG_SCHEMA	pg_catalog

# Type of export. Values can be the following keyword:
#	TABLE		Export tables, constraints, indexex, ...
#	PACKAGE		Export packages
#	INSERT		Export data from table as INSERT statement
#	COPY		Export data from table as COPY statement
#	VIEW		Export views
#	GRANT		Export grants
#	SEQUENCE	Export sequences
#	TRIGGER		Export triggers
#	FUNCTION	Export functions
#	PROCEDURE	Export procedures
#	TABLESPACE	Export tablespace (PostgreSQL >= 8 only)
#	TYPE		Export user defined Oracle types
#	PARTITION	Export range or list partition (PostgreSQL >= v8.4)
#	FDW		Export table as foreign data wrapper tables
#	MVIEW		Export materialized view as snapshot refresh view
TYPE		TABLE

# Set which object to export from. By default Ora2Pg export all objects.
# Value must be a list of object name or regex separated by space. Note
# that regex will not works with 8i database.
#ALLOW		TABLE_TEST

# Set which object to exclude from export process. By default none.
# Value must be a list of object name or regexp separated by space.
# Note that regex will not works with 8i database.
#EXCLUDE	OTHER_TABLES

# Set which view to export as table. By default none. Value must be a list of
# view name or regexp separated by space. If the object name is a view and the
# export type is TABLE, the view will be exported as a create table statement.
# If export type is COPY or INSERT, the corresponding data will be exported.
#VIEW_AS_TABLE	VIEW_NAME

# Support for turning off certain schema features in the postgres side
# during schema export. Values can be : fkeys, pkeys, ukeys, indexes, checks
# separated by a space character.
# fkeys		: turn off foreign key constraints
# pkeys		: turn off primary keys
# ukeys		: turn off unique column constraints
# indexes	: turn off all other index types
# checks	: turn off check constraints
#SKIP	fkeys pkeys ukeys indexes checks

# Extract data by bulk of DATA_LIMIT tuples at once. Default 10000. If you set
# a high value be sure to have enougth memory if you have million of rows.
DATA_LIMIT	10000

# You may wish to just extract data from some fields, the following directives
# will help you to do that. Works only with export type INSERT or COPY
# Modify output from the following tables(fields separate by space or comma)
#MODIFY_STRUCT	TABLE_TEST(dico,dossier)

# Some time you need to force the destination type, for example a column
# exported as timestamp by Ora2Pg can be forced into type date. Value is
# a comma-separated list of TABLE:COLUMN:TYPE structure. If you need to use
# comma or space inside type definintion you will have to backslach them.
#MODIFY_TYPE     TABLE1:COL3:varchar,TABLE1:COL4:decimal(9\,6)

# You may wish to change table names during data extraction, especally for
# replication use. Give a list of tables separate by space as follow.
#REPLACE_TABLES	ORIG_TB_NAME1:NEW_TB_NAME1 ORIG_TB_NAME2:NEW_TB_NAME2 

# You may wish to change column names during data extraction, especally for
# replication use. Give a list of tables and columns separate by space as
# follow.
#REPLACE_COLS	TB_NAME(ORIG_COLNAME1:NEW_COLNAME1,ORIG_COLNAME2:NEW_COLNAME2)

# Define the following directive to send export directly to a PostgreSQL
# database. This will disable file output.
#PG_DSN		dbi:Pg:dbname=test_db;host=localhost;port=5432
#PG_USER	test
#PG_PWD		test

# By default all object names are converted to lower case, if you
# want to preserve Oracle object name asis set this to 1. Not recommanded
# unless you always quote all tables and columns on all your scripts.
PRESERVE_CASE	0

# Support for include a WHERE clause filter when dumping the contents
# of tables. Value is construct as follow: TABLE_NAME[WHERE_CLAUSE], or
# if you have only one where clause for each table just put the where
# clause as value. Both are possible too. Here are some examples:
#WHERE  1=1	# Apply to all tables
#WHERE	TABLE_TEST[ID1='001']	# Apply only on table TABLE_TEST
#WHERE	TABLE_TEST[ID1='001' OR ID1='002] DATE_CREATE > '2001-01-01' TABLE_INFO[NAME='test']
# The last applies two different where clause on tables TABLE_TEST and
# TABLE_INFO and a generic where clause on DATE_CREATE to all other tables

# By default all output is dump to STDOUT if not send directly to postgresql
# database (see above). Give a filename to save export to it. If you want
# a Gzipped compressed file just add the extension .gz to the filename, you
# need perl module Compress::Zlib from CPAN. Add extension .bz2 to use Bzip2
# compression.
OUTPUT		output.sql

# Base directory where all dumped files must be written
#OUTPUT_DIR	/var/tmp

# Path to the bzip2 program. See OUTPUT directive above.
BZIP2	$bzip2

# Set this to 1 to replace default password for all extracted user
# during GRANT export
GEN_USER_PWD	0

# When exporting tables, Ora2Pg normally exports constraints as they are;
# if they are non-deferrable they are exported as non-deferrable.
# However, non-deferrable constraints will probably cause problems when
# attempting to import data to PostgreSQL. The following option set to 1
# will cause all foreign key constraints to be exported as deferrable
FKEY_DEFERRABLE	0

# In addition when exporting data the DEFER_FKEY option set to 1 will add
# a command to defer all foreign key constraints during data export and
# the import will be done in a single transaction. This will work only if
# foreign keys have been exported as deferrables. Constraints will then be
# checked at the end of the transaction. This directive can also be enabled 
# if you want to force all foreign keys to be created as deferrable and
# initially deferred during schema export (TABLE export type).
DEFER_FKEY	0

# If deferring foreign keys is not possible du to the amount of data in a
# single transaction or you've not exported foreign keys as deferrables
# you can use the DROP_FKEY directive. It will drop all foreign keys before
# data import and recreate them at the end.
DROP_FKEY	0

# Enabling this directive force Ora2Pg to drop all indexes on data import
# tables, except automatic index on primary key, and recreate them at end
# of data import. This may improve speed a lot during a fresh import.
DROP_INDEXES	0

# If set to 1 replace portable numeric type into PostgreSQL internal type.
# Oracle data type NUMBER(p,s) is approximatively converted to real and
# float PostgreSQL data type. If you have monetary fields or don't want
# rounding issues with the extra decimals you should preserve the same
# numeric(p,s) PostgreSQL data type. Do that only if you need very good
# precision because using numeric(p,s) is slower than using real or double.
PG_NUMERIC_TYPE	1

# If set to 1 replace portable numeric type into PostgreSQL internal type.
# Oracle data type NUMBER(p) or NUMBER are converted to smallint, integer
# or bigint PostgreSQL data type following the length of the precision. If
# NUMBER without precision are set to DEFAULT_NUMERIC (see bellow).
PG_INTEGER_TYPE	1

# NUMBER() without precision are converted by default to bigint only if
# PG_INTEGER_TYPE is true. You can overwrite this value to any PG type,
# like integer or float.
DEFAULT_NUMERIC bigint

# By default, primary key names in the source database are ignored, and
# default key names are created in the target database. If this is set to true,
# primary key names are kept.
KEEP_PKEY_NAMES 0

# Disables triggers on all tables in COPY or INSERT mode. Available modes 
# are USER (userdefined triggers) and ALL (includes RI system 
# triggers). Default is 0 do not add SQL statement to disable trigger.
# If you want to disable triggers during data migration, set the value to
# USER if your are connected as non superuser and ALL if you are connected
# as PostgreSQL superuser. A value of 1 is equal to USER.
DISABLE_TRIGGERS 0

# By default all data that are not of type date or time are escaped. If you
# experience any problem with that you can set it to 1 to disable it. This
# directive is only used during a COPY export type.
# See STANDARD_CONFORMING_STRINGS for enabling/disabling escape with INSERT
# statements.
NOESCAPE	0

# If you're experiencing problems in data type export, the following directive
# will help you to redefine data type translation used in Ora2pg. The syntax is
# a coma separated list of "Oracle datatype:Postgresql datatype". Here are the
# data type that can be redefined and their default value.
# DATA_TYPE	DATE:timestamp,LONG:text,LONG RAW:bytea,CLOB:text,NCLOB:text,BLOB:bytea,BFILE:bytea,RAW:bytea,ROWID:oid,FLOAT:double precision,DEC:decimal,DECIMAL:decimal,DOUBLE PRECISION:double precision,INT:integer,INTEGER:integer,REAL:real,SMALLINT:smallint,BINARY_FLOAT:double precision,BINARY_DOUBLE:double precision,TIMESTAMP:timestamp,XMLTYPE:xml,BINARY_INTEGER:integer,PLS_INTEGER:integer,TIMESTAMP WITH TIME ZONE:timestamp with time zone,TIMESTAMP WITH LOCAL TIME ZONE:timestamp with time zone

# Enforce default language setting following the Oracle database encoding. This
# may be used with mutibyte characters like UTF8.
# This will set $ENV{NLS_LANG} to the given value.
#NLS_LANG	AMERICAN_AMERICA.UTF8

# Enforce perl to use binary mode for output using the given encoding. This
# must be used if you experience the perl message: "Wide character in print"
# The warning happens when you output a Unicode string to a non-unicode
# filehandle. If you set it to 'utf8' as follow, it will force printing
# like this: binmode OUTFH, ":utf8";
#BINMODE		utf8

# Allow to add a coma separated list of system user to exclude from 
# from Oracle extraction. Oracle have many of them following the modules
# installed. By default it will suppress all object owned by the following
# system users:
#        CTXSYS,DBSNMP,EXFSYS,LBACSYS,MDSYS,MGMT_VIEW,OLAPSYS,ORDDATA,OWBSYS,
#        ORDPLUGINS,ORDSYS,OUTLN,SI_INFORMTN_SCHEMA,SYS,SYSMAN,SYSTEM,WK_TEST,
#        WKSYS,WKPROXY,WMSYS,XDB,APEX_PUBLIC_USER,DIP,FLOWS_020100,FLOWS_030000,
#        FLOWS_040100,FLOWS_FILES,MDDATA,ORACLE_OCM,SPATIAL_CSW_ADMIN_USR,
#        SPATIAL_WFS_ADMIN_USR,XS$NULL,PERFSTAT,SQLTXPLAIN,DMSYS,TSMSYS,WKSYS,
#        APEX_040200,DVSYS,OJVMSYS,GSMADMIN_INTERNAL,APPQOSSYS
# Other list of users set to this directive will be added to this list.
#SYSUSERS	OE,HR

# Disables alter of sequences on all tables in COPY or INSERT mode.
# Set to 1 if you want to disable update of sequence during data migration.
DISABLE_SEQUENCE	0

# Enable PLSQL to PLPSQL convertion. This is a work in progress, feel
# free modify/add you own code and send me patches. The code is under
# function plsql_toplpgsql in Ora2PG/PLSQL.pm. Default enabled.
PLSQL_PGSQL	1

# Allow escaping of column name using Oracle reserved words.
ORA_RESERVED_WORDS	audit,comment

# Allow object constraints to be saved in a separate file during schema export.
# The file will be named CONSTRAINTS_OUTPUT. Where OUTPUT is the value of the
# corresponding configuration directive. You can use .gz xor .bz2 extension to
# enable compression. Default is to save all data in the OUTPUT file. This 
# directive is usable only with TABLE export type.
FILE_PER_CONSTRAINT	0

# Allow indexes to be saved in a separate file during schema export. The file
# will be named INDEXES_OUTPUT. Where OUTPUT is the value of the corresponding
# configuration directive. You can use .gz xor .bz2 file extension to enable
# compression. Default is to save all data in the OUTPUT file. This directive
# is usable only with TABLE or TABLESPACE export type.  With the TABLESPACE
# export, it is used to write "ALTER INDEX ... TABLESPACE ..." into a separate
# file named TBSP_INDEXES_OUTPUT that can be loaded at end of the migration
# after the indexes creation to move the indexes.
FILE_PER_INDEX		0

# Allow data export to be saved in one file per table/view. The files
# will be named as tablename_OUTPUT. Where OUTPUT is the value of the
# corresponding configuration directive. You can use .gz xor .bz2
# extension to enable compression. Default is to save all data in one
# file. This is usable only during INSERT or COPY export type.
FILE_PER_TABLE	0

# This directive may be used if you want to change the default isolation
# level of the data export transaction. Default is now to set the level
# to a serializable transaction to ensure data consistency. Here are the
# allowed value of this directive: readonly, readwrite, serializable and
# committed (read commited).
TRANSACTION	serializable

# Allow support of WHEN clause in trigger definition PG>=9.0
PG_SUPPORTS_WHEN		1

# Allow support of INSTEAD OF in triggers definition PG>=9.1
PG_SUPPORTS_INSTEADOF	1

# Allow function export to be saved in one file per function/procedure.
# The files will be named as funcname_OUTPUT. Where OUTPUT is the value
# of the corresponding configuration directive. You can use .gz xor .bz2
# extension to enable compression. Default is to save all data in one
# file. It is usable during FUNCTION, PROCEDURE, TRIGGER and PACKAGE
# export type.
FILE_PER_FUNCTION	0

# Add a TRUNCATE TABLE instruction before loading data on COPY and INSERT
# export.
TRUNCATE_TABLE	0

# If you experience ERROR: invalid byte sequence for encoding "UTF8": 0xe87472
# when loading data you may want to set the encoding of the PostgreSQL client.
# By default it is not set and it will depend of you system client encoding.
#CLIENT_ENCODING	LATIN9

# By default the owner of database objects is the one you're using to connect
# to PostgreSQL. If you use an other user (postgres for exemple) you can force
# Ora2Pg to set the object owner to be the one used in the Oracle database by
# setting the directive to 1, or to a completely different username by setting
# the directive value # to that username. 
FORCE_OWNER	0

# This controls whether ordinary string literals ('...') treat backslashes
# literally, as specified in SQL standard. This was the default before Ora2Pg
# v8.5 so that all strings was escaped first, now this is currently on, causing
# Ora2Pg to use the escape string syntax (E'...') if this parameter is not
# set to 0. This is the exact behavior of the same option in PostgreSQL.
# This directive is only used during INSERT export to build INSERT statements.
# See NOESCAPE for enabling/disabling escape in COPY statements.
STANDARD_CONFORMING_STRINGS	1

# Multiprocess support. This directive replace the obsolete THREAD_COUNT
# variable. Ora2Pg now use fork() to do parallel process instead of Perl
# threadis. This directive should defined the number of parallel connection
# to PostgreSQL for direct data migration. The limit is the number of cores
# on your machine. This is useful if PostgreSQL is the bottleneck. COPIES
JOBS		1

# Multiprocess support. This directive should defined the number of parallel
# connection to Oracle when extracting data. The limit is the number of cores
# on your machine. This is useful if Oracle is the bottleneck. Take care that
# this directive can only be used if there is a column defined in DEFINED_PK.
ORACLE_COPIES	1

# Multiprocess support. This directive should defined the number of tables
# in parallel data extraction. The limit is the number of cores on your machine.
# Ora2Pg will open one database connection for each parallel table extraction.
# This directive, when upper than 1, will invalidate ORACLE_COPIES but not JOBS.
# Note that this directive when set upper that 1 will also automatically enable
# the FILE_PER_TABLE directive if your are exporting to files.
PARALLEL_TABLES	1

# Multiprocess support. This directive is used to split the select queries
# between the differents connections to Oracle if ORA_COPIES is used. Ora2Pg
# will extract data with the following prepare statement:
# 	SELECT * FROM TABLE WHERE MOD(COLUMN, $ORA_COPIES) = ?
# Where $ORA_COPIES is the total number of cores used to extract data and set
# with ORA_COPIES directive, and ? is the current core used at execution time.
# This mean that Ora2Pg need to know the numeric column to used in this query,
# if this column is a real, float, numeric or decimal, you must add the ROUND()
# function with the column to round the value to the nearest integer.
#DEFINED_PK	TABLE:COLUMN TABLE:ROUND(COLUMN)

# This directive is use to enable/disable the plsql to pgplsql conversion part
# that could break the original code if they include complex subqueries.
# Default is enabled, you must disabled if to preserve backward compatibility.
# This concern the following replacement: decode(), substr() 
ALLOW_CODE_BREAK	1

# Use this directive to set the database handle's 'LongReadLen' attribute to
# a value that will be the larger than the expected size of the LOB. The default
# is 1Mb witch may not be enough to extract BLOB objects. If the size of the LOB
# exceeds the 'LongReadLen' DBD::Oracle will return a 'ORA-24345: A Truncation'
# error.  Default: 1023*1024 bytes. Take a look at this page to learn more:
# http://search.cpan.org/~pythian/DBD-Oracle-1.22/Oracle.pm#Data_Interface_for_Persistent_LOBs
#
# Important note: If you increase the value of this directive take care that 
# DATA_LIMIT will probably needs to be reduced. Even if you only have a 1Mb blob
# trying to read 10000 of them (the default DATA_LIMIT) all at once will require
# 10Gb of memory. You may extract data from those table separatly and set a
# DATA_LIMIT to 500 or lower, otherwise you may experience some Out of memory.
#LONGREADLEN	1047552

# If you want to bypass the 'ORA-24345: A Truncation' error, set this directive
# to 1, it will truncate the data extracted to the LongReadLen value.
#LONGTRUNCOK	0

# Use getStringVal() instead of getClobVal() for XML data export. Default is
# enabled for backward compatibility.
XML_PRETTY	0

# This directive is used to set the name of the foreign data server that is used
# in the "CREATE SERVER name FOREIGN DATA WRAPPER oracle_fdw ..." command. This
# name will then be used in the "CREATE FOREIGN TABLE ..." SQL command. Default
# is arbitrary set to orcl. This only concern export type FDW.
FDW_SERVER	orcl

# Set it to 0 if you don't want to export milliseconds from Oracle timestamp
# columns. Timestamp will be formated with to_char(..., 'YYYY-MM-DD HH24:MI:SS')
# Enabling this directive, the default, format to 'YYYY-MM-DD HH24:MI:SS.FF'.
#ENABLE_MICROSECOND      1

# Set this to 1 if you don't want to export comment associated to tables and
# columns definition. Default is enabled.
DISABLE_COMMENT         0

# Enable this directive if you have table or column names that are a reserved
# word for PostgreSQL. Ora2Pg will double quote the name of the object. 
USE_RESERVED_WORDS	0

# Enable this directive if you want to add primary key definition inside the
# create table statement. If disabled (the default) primary key definition
# will be add with an alter table statement. Enable it if you are exporting
# to GreenPlum PostgreSQL database.
PKEY_IN_CREATE		0

# If you want to replace some column as PostgreSQL boolean define here a list
# of tables and column separated by space as follow. You can also give a type
# and a precision to automatically convert all fields of that type as a boolean.
# For example: NUMBER:1 or CHAR:1 will replace any field of type number(1) or
# char(1) as a boolean in all exported tables.
#REPLACE_AS_BOOLEAN	TB_NAME1:COL_NAME1 TB_NAME1:COL_NAME2 TB_NAME2:COL_NAME2

# Use this to add additional definition of the possible boolean values in Oracle
# field. You must set a space separated list of TRUE:FALSE values. BY default:
#BOOLEAN_VALUES	yes:no y:n 1:0 true:false enabled:disabled

# By default Ora2Pg will replace all conditions with a test on NULL by a call
# to the coalesce() function to mimic the Oracle behavior where empty field are
# considered equal to NULL. Ex: (field1 IS NULL) and (field2 IS NOT NULL) will
# be replaced by (coalesce(field1::text, '') = '') and (field2 IS NOT NULL AND
# field2::text <> '')
NULL_EQUAL_EMPTY	1

# By default Ora2Pg will export your external table as file_fdw tables. If
# you don't want to export those tables at all, set the directive to 0.
EXTERNAL_TO_FDW		1

# Activate the migration cost evaluation. Must only be used with SHOW_REPORT,
# FUNCTION, PROCEDURE, PACKAGE and QUERY export type. Default is disabled.
# Not that enabling this directive will force PLSQL_PGSQL activation.
ESTIMATE_COST		0

# Set the value in minutes of the migration cost evaluation unit. Default
# is five minutes per unit.
COST_UNIT_VALUE		5

# By default when using SHOW_REPORT the migration report is generated as
# simple text, enabling this directive will force ora2pg to create a report
# in HTML format.
DUMP_AS_HTML		0

# Set it to 0 to not include the call to \set ON_ERROR_STOP ON in all SQL
# scripts. By default this order is always present.
STOP_ON_ERROR		1

# Set the total number of tables to display in the Top N per row and size
# list in the SHOW_TABLE and SHOW_REPORT output. Default 10.
TOP_MAX			10

# Use this directive to limit partition data export to some partition name.
# Tha value is a list of partition name separated by a space.
ALLOW_PARTITION		PARTNAME

# When enabled this directive force ora2pg to export all tables, indexes
# constraint and indexes using the tablespace name defined in Oracle database.
# This works only with tablespace that are not TEMP, USERS and SYSTEM.
USE_TABLESPACE		0

# Allow support of native MATERIALIZED VIEW PG>=9.3. If disable Ora2Pg
# will use old behavior, a normal table with a set of function to refresh
# the view.
PG_SUPPORTS_MVIEW	1

# Enable this directive to reordering columns and minimized the footprint
# on disc, so that more rows fit on a data page, which is the most important
# factor for speed. Default is same order than in Oracle table definition,
# that's should be enough for most usage.
REORDERING_COLUMNS	0

# Add the given value as suffix to indexes names. Useful if you have indexes
# with same name as tables. Not so common but can help.
#INDEXES_SUFFIX		_idx

# Specifies whether transaction commit will wait for WAL records to be written
# to disk before the command returns a "success" indication to the client. This
# is the equivalent to set synchronous_commit directive of postgresql.conf file.
# This is only used when you load data directly to PostgreSQL, the default is
# off to disable synchronous commit to gain speed at writing data. Some modified
# version of PostgreSQL, like greenplum, do not have this setting, so in this
# set this directive to 1, ora2pg will not  try to change the setting.
SYNCHRONOUS_COMMIT	0

# If enabled, export view with CHECK OPTION. Enable it if you have PostgreSQL
# version 9.4 or higher. Default, disabled
PG_SUPPORTS_CHECKOPTION	0

# Enable this directive if you want Ora2Pg to detect the real spatial type and
# dimension used in a spatial column. Otherwise column will be created with
# non-constrained "geometry" type. Enabling this feature will force Ora2Pg to
# scan a sample of 50000 column to look at the GTYPE used. You can increase
# the sample by setting the value of AUTODETECT_SPATIAL_TYPE to the desired
# number of line.
AUTODETECT_SPATIAL_TYPE	1

# Disable this directive if you don't want to automatically convert SRID to
# EPSG using the sdo_cs.map_oracle_srid_to_epsg() function. Default: enabled
# If the SDO_SRID returned by Oracle is NULL, it will be replaced by the
# default value 8307 converted to its EPSG value: 4326 (see DEFAULT_SRID)
# If the value is upper than 1, all SRID will be forced to this value, in
# this case DEFAULT_SRID will not be used when Oracle returns a null value
# and the value will be forced to CONVERT_SRID.
CONVERT_SRID		1

# Use this directive to override the default EPSG SRID to used: 4326.
# Can be overwritten by CONVERT_SRID, see above.
DEFAULT_SRID		4326

# If you want to replace call to SDO_UTIL.TO_WKTGEOMETRY() by a call to the
# more efficient methode SC4O.ST_AsText(), especially for 3D geomrtry, activate
# this directive. You must install the SC40 package before, this is the
# SpatialDB Advisor extension developped by Simon Greener and available at
# http://www.spatialdbadvisor.com/source_code_form
# If SC40 is not installed in the same schema, you can set the value to the
# schema name.
USE_SC40_PACKAGE	0

# Use this directive to add a specific schema to the search path to look
# for PostGis functions.
#POSTGIS_SCHEMA		

# Enable this directive if you want that your partition table name will be
# exported using the parent table name. Disabled by default. If you have
# multiple partitioned table, when exported to PostgreSQL some partitions
# could have the same name but dfferent parent tables. This is not allowed,
# table name must be unique. 
PREFIX_PARTITION	0

# Enable this directive if you want to continue direct data import on error.
# When Ora2Pg received an error in the COPY or INSERT statement from PostgreSQL
# it will log the statement to a file called TABLENAME_error.log in the output
# directory and continue to next bulk of data. Like this you can try to fix the
# statement and manually reload the error log file. Default is disabled: abort
# import on error.
LOG_ON_ERROR		0

# Sometime you may want to extract data from an Oracle table but you need a
# a custom query for that. Not just a "SELECT * FROM table" like Ora2Pg do
# but a more complexe query. This directive allow you to overwrite the query
# used by Ora2Pg to extract data. The format is TABLENAME[SQL_QUERY].
# If you have multiple table to extract by replacing the Ora2Pg query, you can
# define multiple REPLACE_QUERY lines.
#REPLACE_QUERY	EMPLOYEES[SELECT e.id,e.fisrtname,lastname FROM EMPLOYEES e JOIN EMP_UPDT u ON (e.id=u.id AND u.cdate>'2014-08-01 00:00:00')]

# PostgreSQL version below 9.x do not support IF EXISTS in DDL statements.
# Disabling the directive with value 0 will prevent Ora2Pg to add those
# keywords in all generated statments.
PG_SUPPORTS_IFEXISTS	1

};
close(OUTCFG);

if ($^O !~ /MSWin32|dos/i) {
	`perl -p -i -e "s#my \\\$CONFIG_FILE .*#my \\\$CONFIG_FILE = '$CONFDIR/ora2pg.conf';#" scripts/ora2pg`;
} else {
	my $tmp_conf = quotemeta($CONFDIR);
	`perl -p -e "s#my \\\$CONFIG_FILE .*#my \\\$CONFIG_FILE = '$tmp_conf\\\\ora2pg.conf';#" scripts\\ora2pg > scripts\\ora2pg.tmp`;
	`copy scripts\\ora2pg.tmp scripts\\ora2pg /Y`;
}

WriteMakefile(
    'NAME'         => 'Ora2Pg',
    'VERSION_FROM' => 'lib/Ora2Pg.pm',
    'LICENSE'      => 'GPLv3',
    'dist'         => {
			'COMPRESS'=>'gzip -9f', 'SUFFIX' => 'gz',
			'ZIP'=>'/usr/bin/zip','ZIPFLAGS'=>'-rl'
		      },
    'AUTHOR'       => 'Gilles Darold (gilles _AT_ darold _DOT_ net)',
    'ABSTRACT'     => 'Oracle to PostgreSQL migration toolkit',
    'EXE_FILES'    => [ qw(scripts/ora2pg) ],
    'MAN3PODS'     => { 'doc/Ora2Pg.pod' => 'blib/man3/ora2pg.3' },
    'DESTDIR'      => $PREFIX,
    'INSTALLDIRS'  => $ENV{INSTALLDIRS},
    'clean'        => {FILES => "$DEST_CONF_FILE lib/blib/"},
    'PREREQ_PM'    => { DBI => 0, DBD::Oracle },
    'META_MERGE'   => {
	resources  => {
		homepage => 'http://ora2pg.darold.net/',
		repository  => {
			type => 'git',
			git => 'git@github.com:darold/ora2pg.git',
			web => 'https://github.com/darold/ora2pg',
		},
	},
    }
);

sub MY::install {
	my $self = shift;

	my $string = $self->MM::install;
	$string =~ s/(pure_install\s+)(.*)/$1 install_all $2/;

	return $string;
}

sub MY::postamble {
	my $postamble = qq{
install_all :
	\@echo "Installing default configuration file ($DEST_CONF_FILE) to $CONFDIR"
	\@\$(MKPATH) $CONFDIR/ora2pg
	\@\$(CP) -f $DEST_CONF_FILE $CONFDIR/ora2pg/$DEST_CONF_FILE
	\@\$(MKPATH) $DOCDIR
	\@\$(CP) -f README $DOCDIR/README
	\@\$(CP) -f INSTALL $DOCDIR/INSTALL
	\@\$(CP) -f changelog $DOCDIR/changelog
};
	if ($^O =~ /MSWin32|dos/i) {
		my $tmp_conf = quotemeta($CONFDIR);
		$postamble = qq{
install_all :
	\@echo "Installing default configuration file ($DEST_CONF_FILE) to $CONFDIR"
	\@\$(MKPATH) $CONFDIR
	\@\$(CP) $DEST_CONF_FILE $CONFDIR\\$DEST_CONF_FILE
	\@\$(CP) README $CONFDIR\\README
	\@\$(CP) INSTALL $CONFDIR\\INSTALL
	\@\$(CP) changelog $CONFDIR\\changelog
};
	}
        return $postamble;
}


if (!$ENV{QUIET}) {
	print qq{
Done...
------------------------------------------------------------------------------
Please read documentation at http://ora2pg.darold.net/ before asking for help
------------------------------------------------------------------------------
};
	if ($^O !~ /MSWin32|dos/i) {
		print "Now type: make && make install\n";
	} else {
		print "Now type: dmake && dmake install\n";
	}

}
