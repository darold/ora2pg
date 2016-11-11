package Ora2Pg::PLSQL;
#------------------------------------------------------------------------------
# Project  : Oracle to PostgreSQL database schema converter
# Name     : Ora2Pg/PLSQL.pm
# Language : Perl
# Authors  : Gilles Darold, gilles _AT_ darold _DOT_ net
# Copyright: Copyright (c) 2000-2016 : Gilles Darold - All rights reserved -
# Function : Perl module used to convert Oracle PLSQL code into PL/PGSQL
# Usage    : See documentation
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

use vars qw($VERSION %OBJECT_SCORE $SIZE_SCORE $FCT_TEST_SCORE $QUERY_TEST_SCORE %UNCOVERED_SCORE %UNCOVERED_MYSQL_SCORE @ORA_FUNCTIONS @MYSQL_SPATIAL_FCT @MYSQL_FUNCTIONS);
use POSIX qw(locale_h);

#set locale to LC_NUMERIC C
setlocale(LC_NUMERIC,"C");


$VERSION = '17.5';

#----------------------------------------------------
# Cost scores used when converting PLSQL to PLPGSQL
#----------------------------------------------------

# Scores associated to each database objects:
%OBJECT_SCORE = (
	'CLUSTER' => 0, # Not supported and no equivalent
	'FUNCTION' => 1, # read/adapt the header
	'INDEX' => 0.1, # Read/adapt - use varcharops like operator ?
	'FUNCTION-BASED-INDEX' => 0.2, # Check code of function call
	'REV-INDEX' => 1, # Check/rewrite the index to use trigram
	'CHECK' => 0.1, # Check/adapt the check constraint
	'MATERIALIZED VIEW' => 3, # Read/adapt, will just concern automatic snapshot export
	'PACKAGE BODY' => 3, # Look at globals variables and global 
	'PROCEDURE' => 1, # read/adapt the header
	'SEQUENCE' => 0.1, # read/adapt to convert name.nextval() into nextval('name')
	'TABLE' => 0.1, # read/adapt the column type/name
	'TABLE PARTITION' => 0.1, # Read/check that table partitionning is ok
	'TABLE SUBPARTITION' => 0.2, # Read/check that table sub partitionning is ok
	'TRIGGER' => 1, # read/adapt the header
	'TYPE' => 1, # read
	'TYPE BODY' => 10, # Not directly supported need adaptation
	'VIEW' => 1, # read/adapt
	'DATABASE LINK' => 3, # Supported as FDW using oracle_fdw
	'DIMENSION' => 0, # Not supported and no equivalent
	'JOB' => 2, # read/adapt
	'SYNONYM' => 0.1, # read/adapt
	'QUERY' => 0.02, # read/adapt
);

# Scores following the number of characters: 1000 chars for one unit.
# Note: his correspond to the global read time not to the difficulty.
$SIZE_SCORE = 1000;

# Cost to apply on each function or query for testing
$FCT_TEST_SCORE = 2;
$QUERY_TEST_SCORE = 0.1;

# Scores associated to each code difficulties.
%UNCOVERED_SCORE = (
	'FROM' => 1,
	'TRUNC' => 0.1,
	'DECODE' => 1,
	'IS TABLE OF' => 4,
	'OUTER JOIN' => 1,
	'CONNECT BY' => 4,
	'BULK COLLECT' => 3,
	'GOTO' => 2,
	'FORALL' => 1,
	'ROWNUM' => 2,
	'NOTFOUND' => 1,
	'ISOPEN' => 1,
	'ROWCOUNT' => 1,
	'ROWID' => 2,
	'IS RECORD' => 1,
	'SQLCODE' => 1,
	'TABLE' => 2,
	'DBMS_' => 3,
	'DBMS_OUTPUT.put' => 1,
	'UTL_' => 3,
	'CTX_' => 3,
	'EXTRACT' => 0.1,
	'EXCEPTION' => 2,
	'TO_NUMBER' => 0.1,
	'REGEXP_LIKE' => 0.1,
	'TG_OP' => 1,
	'CURSOR' => 1,
	'PIPE ROW' => 1,
	'ORA_ROWSCN' => 3,
	'SAVEPOINT' => 1,
	'DBLINK' => 1,
	'PLVDATE' => 2,
	'PLVSTR' => 2,
	'PLVCHR' => 2,
	'PLVSUBST' => 2,
	'PLVLEX' => 2,
	'PLUNIT' => 2,
	'ADD_MONTHS' => 0.1,
	'LAST_DATE' => 1,
	'NEXT_DAY' => 1,
	'MONTHS_BETWEEN' => 1,
	'NVL2' => 1,
	'SDO_' => 3,
	'PRAGMA' => 3,
	'MDSYS' => 1,
	'MERGE INTO' => 3,
	'COMMIT' => 3,
);

@ORA_FUNCTIONS = qw(
	AsciiStr
	Compose
	Decompose
	Dump
	VSize
	Bin_To_Num
	CharToRowid
	From_Tz
	HexToRaw
	NumToDSInterval
	NumToYMInterval
	RawToHex
	To_Clob
	To_DSInterval
	To_Lob
	To_Multi_Byte
	To_NClob
	To_Single_Byte
	To_YMInterval
	BFilename
	Cardinality
	Group_ID
	LNNVL
	NANVL
	Sys_Context
	Uid
	UserEnv
	Bin_To_Num
	BitAnd
	Cosh
	Median
	Remainder
	Sinh
	Tanh
	DbTimeZone
	From_Tz
	New_Time
	SessionTimeZone
	Tz_Offset
	SysTimestamp
	Get_Env
);

@MYSQL_SPATIAL_FCT = (
	'AsBinary',
	'AsText',
	'Buffer',
	'Centroid',
	'Contains',
	'Crosses',
	'Dimension',
	'Disjoint',
	'EndPoint',
	'Envelope',
	'Equals',
	'ExteriorRing',
	'GeomCollFromText',
	'GeomCollFromWKB',
	'GeometryN',
	'GeometryType',
	'GeomFromText',
	'GeomFromWKB',
	'GLength',
	'InteriorRingN',
	'Intersects',
	'IsClosed',
	'IsSimple',
	'LineFromText',
	'LineFromWKB',
	'MLineFromText',
	'MPointFromText',
	'MPolyFromText',
	'NumGeometries',
	'NumInteriorRings',
	'NumPoints',
	'Overlaps',
	'Point',
	'PointFromText',
	'PointFromWKB',
	'PointN',
	'PolygonFromText',
	'Polygon',
	'SRID',
	'StartPoint',
	'Touches',
	'Within',
	'X',
	'Y'
);

@MYSQL_FUNCTIONS = (
	'AES_DECRYPT',
	'AES_ENCRYPT',
	'ASYMMETRIC_DECRYPT',
	'ASYMMETRIC_DERIVE',
	'ASYMMETRIC_ENCRYPT',
	'ASYMMETRIC_SIGN',
	'ASYMMETRIC_VERIFY',
	'CREATE_ASYMMETRIC_PRIV_KEY',
	'CREATE_ASYMMETRIC_PUB_KEY',
	'CREATE_DH_PARAMETERS',
	'CREATE_DIGEST',
	'DECODE',
	'DES_DECRYPT',
	'DES_ENCRYPT',
	'ENCODE',
	'ENCRYPT',
	'SHA1',
	'SHA2',
	'COLLATION',
	'COMPRESS',
	'CONVERT',
	'DEFAULT',
	'FOUND_ROWS',
	'GTID_SUBSET',
	'GTID_SUBTRACT',
	'INET6_ATON',
	'INET6_NTOA',
	'INTERVAL',
	'IS_FREE_LOCK',
	'IS_IPV4_COMPAT',
	'IS_IPV4_MAPPED',
	'IsEmpty',
	'LAST_INSERT_ID',
	'LOAD_FILE',
	'MASTER_POS_WAIT',
	'MATCH',
	'OLD_PASSWORD',
	'PERIOD_ADD',
	'PERIOD_DIFF',
	'RANDOM_BYTES',
	'ROW_COUNT',
	'SQL_THREAD_WAIT_AFTER_GTIDS',
	'WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS',
	'UNCOMPRESS',
	'UNCOMPRESSED_LENGTH',
	'UpdateXML',
	'UUID_SHORT',
	'VALIDATE_PASSWORD_STRENGTH',
	'WEIGHT_STRING',
);

# Scores associated to each code difficulties after replacement.
%UNCOVERED_MYSQL_SCORE = (
	'ARRAY_AGG_DISTINCT' => 1, # array_agg(distinct
	'SOUNDS LIKE' => 1,
	'CHARACTER SET' => 1,
	'COUNT(DISTINCT)' => 2,
	'MATCH' => 2,
	'JSON' => 2,
	'LOCK' => 2,
	'@VAR' => 0.1,
);



=head1 NAME

PSQL - Oracle to PostgreSQL procedural language converter


=head1 SYNOPSIS

	This external perl module is used to convert PLSQL code to PLPGSQL.
	It is in an external code to allow easy editing and modification.
	This converter is a work in progress and need your help.

	It is called internally by Ora2Pg.pm when you set PLSQL_PGSQL 
	configuration option to 1.
=cut

=head2 plsql_to_plpgsql

This function return a PLSQL code translated to PLPGSQL code

=cut

sub plsql_to_plpgsql
{
        my ($class, $str) = @_;

	return mysql_to_plpgsql($class, $str) if ($class->{is_mysql});

	my @xmlelt = ();
	my $field = '\s*([^\(\),]+)\s*';
	my $num_field = '\s*([\d\.]+)\s*';
	my $date_field = '\s*([^,\)\(]*(?:date|time)[^,\)\(]*)\s*';

	#--------------------------------------------
	# PL/SQL to PL/PGSQL code conversion
	# Feel free to add your contribution here.
	#--------------------------------------------
	# Change NVL to COALESCE
	$str =~ s/NVL\s*\(/coalesce(/igs;
	my $conv_current_time = 'clock_timestamp()';
	if (!grep(/$class->{type}/i, 'FUNCTION', 'PROCEDURE', 'PACKAGE')) {
		$conv_current_time = 'LOCALTIMESTAMP';
	}
	# Replace sysdate +/- N by localtimestamp - 1 day intervel
	$str =~ s/SYSDATE\s*(\+|\-)\s*(\d+)/$conv_current_time $1 interval '$2 days'/igs;
	# Change SYSDATE to 'now' or current timestamp.
	$str =~ s/SYSDATE\s*\(\s*\)/$conv_current_time/igs;
	$str =~ s/SYSDATE/$conv_current_time/igs;

	# Replace SYSTIMESTAMP 
	$str =~ s/SYSTIMESTAMP/CURRENT_TIMESTAMP/igs;
	# remove FROM DUAL
	$str =~ s/FROM DUAL//igs;

	# There's no such things in PostgreSQL
	$str =~ s/PRAGMA RESTRICT_REFERENCES[^;]+;//igs;
        $str =~ s/PRAGMA SERIALLY_REUSABLE[^;]*;//igs;
        $str =~ s/PRAGMA INLINE[^;]+;//igs;

	# Converting triggers
	#       :new. -> NEW.
	$str =~ s/([^\w]+):new\./$1NEW\./igs;
	#       :old. -> OLD.
	$str =~ s/([^\w]+):old\./$1OLD\./igs;

	# Replace EXEC function into variable, ex: EXEC :a := test(:r,1,2,3);
	$str =~ s/EXEC\s+:([^\s:]+)\s*:=/SELECT INTO $2/igs;

	# Replace simple EXEC function call by SELECT function
	$str =~ s/EXEC(\s+)/SELECT$1/igs;

	# Remove leading : on Oracle variable
	$str =~ s/([^\w]+):(\d+)/$1\$$2/igs;
	$str =~ s/([^\w]+):(\w+)/$1$2/igs;

	# INSERTING|DELETING|UPDATING -> TG_OP = 'INSERT'|'DELETE'|'UPDATE'
	$str =~ s/\bINSERTING\b/TG_OP = 'INSERT'/igs;
	$str =~ s/\bDELETING\b/TG_OP = 'DELETE'/igs;
	$str =~ s/\bUPDATING\b/TG_OP = 'UPDATE'/igs;

	# EXECUTE IMMEDIATE => EXECUTE
	$str =~ s/EXECUTE IMMEDIATE/EXECUTE/igs;

	# SELECT without INTO should be PERFORM. Exclude select of view when prefixed with AS ot IS
	if ( ($class->{type} ne 'QUERY') && ($class->{type} ne 'VIEW') ) {
		$str =~ s/(\s+)(?<!AS|IS)(\s+)SELECT((?![^;]+\bINTO\b)[^;]+;)/$1$2PERFORM$3/isg;
		$str =~ s/\bSELECT\b((?![^;]+\bINTO\b)[^;]+;)/PERFORM$1/isg;
		$str =~ s/(AS|IS|FOR|UNION ALL|UNION|MINUS|\()(\s*)(ORA2PG_COMMENT\d+\%)?(\s*)PERFORM/$1$2$3$4SELECT/isg;
		$str =~ s/(INSERT\s+INTO\s+[^;]+\s+)PERFORM/$1SELECT/isg;
	}

	# Change nextval on sequence
	# Oracle's sequence grammar is sequence_name.nextval.
	# Postgres's sequence grammar is nextval('sequence_name'). 
	$str =~ s/(\w+)\.nextval/nextval('\L$1\E')/isg;
	$str =~ s/(\w+)\.currval/currval('\L$1\E')/isg;
	# Oracle MINUS can be replaced by EXCEPT as is
	$str =~ s/\bMINUS\b/EXCEPT/igs;
	# Comment DBMS_OUTPUT.ENABLE calls
	$str =~ s/(DBMS_OUTPUT.ENABLE[^;]+;)/-- $1/isg;
	# Raise information to the client
	$str =~ s/DBMS_OUTPUT\.(put_line|put|new_line)\s*\((.*?)\);/&raise_output($2)/igse;

	# Substitution to replace type of sql variable in PLSQL code
#	foreach my $t (keys %Ora2Pg::TYPE) {
#		$str =~ s/\b$t\b/$Ora2Pg::TYPE{$t}/igs;
#	}

	# Procedure are the same as function in PG
	$str =~ s/\bPROCEDURE\b/FUNCTION/igs;
	# Simply remove this as not supported
	$str =~ s/\bDEFAULT\s+NULL\b//igs;
	# Replace DEFAULT empty_blob() and empty_clob()
	$str =~ s/(empty_blob|empty_clob)\(\s*\)//igs;
	$str =~ s/(empty_blob|empty_clob)\b//igs;

	# dup_val_on_index => unique_violation : already exist exception
	$str =~ s/\bdup_val_on_index\b/unique_violation/igs;

	# Replace raise_application_error by PG standard RAISE EXCEPTION
	$str =~ s/\braise_application_error\s*\(\s*[^,]+\s*,\s*(.*?)\);/RAISE EXCEPTION '%', $1;/igs;
	$str =~ s/DBMS_STANDARD\.RAISE EXCEPTION/RAISE EXCEPTION/igs;

	# Remove IN information from cursor declaration
	while ($str =~ s/(\bCURSOR\b[^\(]+)\(([^\)]+\bIN\b[^\)]+)\)/$1\(\%\%CURSORREPLACE\%\%\)/is) {
		my $args = $2;
		$args =~ s/\bIN\b//igs;
		$str =~ s/\%\%CURSORREPLACE\%\%/$args/is;
	}

	# Retrieve cursor names
	my @cursor_names = $str =~ /\bCURSOR\b\s*([A-Z0-9_\$]+)/isg;
	# Reorder cursor declaration
	$str =~ s/\bCURSOR\b\s*([A-Z0-9_\$]+)/$1 CURSOR/isg;

	# Replace call to cursor type if any
	foreach my $c (@cursor_names) {
		$str =~ s/\b$c\%ROWTYPE/RECORD/isg;
	}

	# Replace CURSOR IS SELECT by CURSOR FOR SELECT
	$str =~ s/\bCURSOR(\s*)IS(\s*)SELECT/CURSOR$1FOR$2SELECT/isg;
	# Replace CURSOR (param) IS SELECT by CURSOR FOR SELECT
	$str =~ s/\bCURSOR(\s*\([^\)]+\)\s*)IS(\s*)SELECT/CURSOR$1FOR$2SELECT/isg;

	# Rewrite TO_DATE formating call
	$str =~ s/TO_DATE\s*\(\s*('[^\']+'),\s*('[^\']+')[^\)]*\)/to_date($1,$2)/igs;

	# Normalize HAVING ... GROUP BY into GROUP BY ... HAVING clause	
	$str =~ s/\bHAVING\b(.*?)\bGROUP BY\b(.*?)((?=UNION|ORDER BY|LIMIT|INTO |FOR UPDATE|PROCEDURE)|$)/GROUP BY$2 HAVING$1/gis;

	# Cast round() call as numeric => Remove because most of the time this may not be necessary
	#$str =~ s/round\s*\((.*?),([\s\d]+)\)/round\($1::numeric,$2\)/igs;

	# Change trunc() to date_trunc('day', field)
	# Trunc is replaced with date_trunc if we find date in the name of
	# the value because Oracle have the same trunc function on number
	# and date type
	my %date_trunc = ();
	my $di = 0;
	while ($str =~ s/\bTRUNC\($date_field\)/\%\%DATETRUNC$di\%\%/is) {
		push(@date_trunc, "date_trunc('day', $1)");
		$di++;
	}
	while ($str =~ s/\bTRUNC\($date_field,$field\)/\%\%DATETRUNC$di\%\%/is) {
		push(@date_trunc, "date_trunc($2, $1)");
		$di++;
	}

	# Convert the call to the Oracle function add_months() into Pg syntax
	$str =~ s/ADD_MONTHS\s*\(\s*TO_CHAR\(\s*([^,]+)(.*?),\s*(\d+)\s*\)/$1 + '$3 month'::interval/gsi;
	$str =~ s/ADD_MONTHS\s*\(\s*TO_CHAR\(\s*([^,]+)(.*?),\s*([^,\(\)]+)\s*\)/$1 + $3*'1 month'::interval/gsi;
	$str =~ s/ADD_MONTHS\s*\((.*?),\s*(\d+)\s*\)/$1 + '$2 month'::interval/gsi;
	$str =~ s/ADD_MONTHS\s*\((.*?),\s*([^,\(\)]+)\s*\)/$1 + $2*'1 month'::interval/gsi;

	# Convert the call to the Oracle function add_years() into Pg syntax
	$str =~ s/ADD_YEARS\s*\(\s*TO_CHAR\(\s*([^,]+)(.*?),\s*(\d+)\s*\)/$1 + '$3 year'::interval/gsi;
	$str =~ s/ADD_YEARS\s*\(\s*TO_CHAR\(\s*([^,]+)(.*?),\s*([^,\(\)]+)\s*\)/$1 + $3*'1 year'::interval/gsi;
	$str =~ s/ADD_YEARS\s*\((.*?),\s*(\d+)\s*\)/$1 + '$2 year'::interval/gsi;
	$str =~ s/ADD_YEARS\s*\((.*?),\s*([^,\(\)]+)\s*\)/$1 + $2*' year'::interval/gsi;

	# Restore DATETRUNC call
	$str =~ s/\%\%DATETRUNC(\d+)\%\%/$date_trunc[$1]/igs;
	@date_trunc = ();
	$str =~ s/date_trunc\('MM'/date_trunc('month'/igs;

	# Add STRICT keyword when select...into and an exception with NO_DATA_FOUND/TOO_MANY_ROW is present
	if ($str !~ s/\b(SELECT\b[^;]*?INTO)(.*?)(EXCEPTION.*?NO_DATA_FOUND)/$1 STRICT $2 $3/igs) {
		$str =~ s/\b(SELECT\b[^;]*?INTO)(.*?)(EXCEPTION.*?TOO_MANY_ROW)/$1 STRICT $2 $3/igs;
	}

	# Remove the function name repetion at end
	$str =~ s/END\s+(?!IF|LOOP|CASE|INTO|FROM|,)[a-z0-9_"]+\s*([;]*)\s*$/END$1/igs;

	# Replace ending ROWNUM with LIMIT
        $str =~ s/(WHERE|AND)\s*ROWNUM\s*=\s*(\d+)/'LIMIT 1 OFFSET ' . ($2-1)/iges;
        $str =~ s/(WHERE|AND)\s*ROWNUM\s*<=\s*(\d+)/LIMIT $2/igs;
        $str =~ s/(WHERE|AND)\s*ROWNUM\s*>=\s*(\d+)/'LIMIT ALL OFFSET ' . ($2-1)/iges;
        $str =~ s/(WHERE|AND)\s*ROWNUM\s*<\s*(\d+)/'LIMIT ' . ($2-1)/iges;
        $str =~ s/(WHERE|AND)\s*ROWNUM\s*>\s*(\d+)/LIMIT ALL OFFSET $2/igs;

	# Rewrite comment in CASE between WHEN and THEN
	$str =~ s/(\s*)(WHEN\s+[^\s]+\s*)(ORA2PG_COMMENT\d+\%)(\s*THEN)/$1$3$1$2$4/igs;

	# Replace INSTR by POSITION
	$str =~ s/INSTR\s*\(\s*([^,]+),\s*('[^']+')\s*\)/POSITION($2 in $1)/igs;

	# Replace SQLCODE by SQLSTATE
	$str =~ s/\bSQLCODE\b/SQLSTATE/igs;

	# Replace some way of extracting date part of a date
	$str =~ s/TO_NUMBER\s*\(\s*TO_CHAR\s*\(([^,]+),\s*('[^']+')\s*\)\s*\)/to_char($1, $2)::integer/igs;

	# Replace the UTC convertion with the PG syntaxe
	$str =~ s/SYS_EXTRACT_UTC\s*\(([^\)]+)\)/($1 AT TIME ZONE 'UTC')/isg;

	# Revert order in FOR IN REVERSE
	$str =~ s/FOR(.*?)IN\s+REVERSE\s+([^\.\s]+)\s*\.\.\s*([^\s]+)/FOR$1IN REVERSE $3..$2/isg;

	# Replace exit at end of cursor
	$str =~ s/EXIT WHEN ([^\%]+)\%NOTFOUND\s*;/IF NOT FOUND THEN EXIT; END IF; -- apply on $1/isg;
	$str =~ s/EXIT WHEN \(\s*([^\%]+)\%NOTFOUND\s*\)\s*;/IF NOT FOUND THEN EXIT; END IF; -- apply on $1/isg;
	# Same but with additional conditions
	$str =~ s/EXIT WHEN ([^\%]+)\%NOTFOUND\s+([;]+);/IF NOT FOUND $2 THEN EXIT; END IF; -- apply on $1/isg;
	$str =~ s/EXIT WHEN \(\s*([^\%]+)\%NOTFOUND\s+([\)]+)\)\s*;/IF NOT FOUND $2 THEN EXIT; END IF; -- apply on $1/isg;
	# Replacle call to SQL%NOTFOUND
	$str =~ s/SQL\%NOTFOUND/NOT FOUND/isg;

	# Replace REF CURSOR as Pg REFCURSOR
	$str =~ s/\bIS(\s*)REF\s+CURSOR/REFCURSOR/isg;
	$str =~ s/\bREF\s+CURSOR/REFCURSOR/isg;

	# Replace SYS_REFCURSOR as Pg REFCURSOR
	$str =~ s/SYS_REFCURSOR/REFCURSOR/isg;

	# Replace known EXCEPTION equivalent ERROR code
	$str =~ s/\bINVALID_CURSOR\b/INVALID_CURSOR_STATE/igs;
	$str =~ s/\bZERO_DIVIDE\b/DIVISION_BY_ZERO/igs;
	$str =~ s/\bSTORAGE_ERROR\b/OUT_OF_MEMORY/igs;
	# PROGRAM_ERROR => INTERNAL ERROR ?
	# ROWTYPE_MISMATCH => DATATYPE MISMATCH ?

	# Replace special IEEE 754 values for not a number and infinity
	$str =~ s/BINARY_(FLOAT|DOUBLE)_NAN/'NaN'/igs;
	$str =~ s/([\-]*)BINARY_(FLOAT|DOUBLE)_INFINITY/'$1Infinity'/igs;
	$str =~ s/'([\-]*)Inf'/'$1Infinity'/igs;

	# REGEX_LIKE( string, pattern ) => string ~ pattern
	$str =~ s/REGEXP_LIKE\s*\(\s*([^,]+)\s*,\s*('[^\']+')\s*\)/$1 \~ $2/igs;

	# Remove call to XMLCDATA, there's no such function with PostgreSQL
	$str =~ s/XMLCDATA\s*\(([^\)]+)\)/'<![CDATA[' || $1 || ']]>'/igs;
	# Remove call to getClobVal() or getStringVal, no need of that
	$str =~ s/\.(getClobVal|getStringVal)\(\s*\)//igs;
	# Add the name keyword to XMLELEMENT
	$str =~ s/XMLELEMENT\s*\(\s*/XMLELEMENT(name /igs;

	# Store XML element into memory and replace it by a placeholder to be
	# able to use it into function call and not break decode replacement
	my $i = 0;
	while ($str =~ s/(XMLELEMENT\s*\([^\)]+\))/%%XMLELEMENT$i%%/is) {
		my $tmpstr = replace_decode($1);
		push(@xmlelt, $tmpstr);
		$i++;
	}

	# Replace PIPE ROW by RETURN NEXT
	$str =~ s/PIPE\s+ROW\s*/RETURN NEXT /igs;
	$str =~ s/(RETURN NEXT )\(([^\)]+)\)/$1$2/igs;

	# The to_number() function reclaim a second argument under postgres which is the format.
	# By default we use '99999999999999999999D99999999999999999999' that may allow bigint
	# and double precision number. Feel free to modify it
	$str =~ s/TO_NUMBER\s*\(([^,\(\)]+)\s*\)/to_number\($1,'99999999999999999999D99999999999999999999'\)/igs;

	# Replace sys_context call to the postgresql equivalent
	$str = &replace_sys_context($str);

	# Replace SDO_GEOM to the postgis equivalent
	$str = &replace_sdo_function($str);

	# Replace Spatial Operator to the postgis equivalent
	$str = &replace_sdo_operator($str);

	# Replace decode("user_status",'active',"username",null)
	# PostgreSQL (CASE WHEN "user_status"='ACTIVE' THEN "username" ELSE NULL END)
	$str = replace_decode($str);

	#  Convert all x <> NULL or x != NULL clauses to x IS NOT NULL.
	$str =~ s/\s*(<>|\!=)\s*NULL/ IS NOT NULL/igs;
	#  Convert all x = NULL clauses to x IS NULL.
	$str =~ s/(?!:)(.)=\s*NULL/$1 IS NULL/igs;
	# Revert changes on update queries in the column setting part of the query
	while ($str =~ s/(\bUPDATE\b[^;]+)\s+IS NULL(\s*(?!WHERE)([^;]+))/$1 = NULL$2/is) {};

	# Rewrite all IF ... IS NULL with coalesce because for Oracle empty and NULL is the same
	if ($class->{null_equal_empty}) {
		# Form: column IS NULL
		$str =~ s/([a-z0-9_\."]+)\s*IS NULL/coalesce($1::text, '') = ''/igs;
		$str =~ s/([a-z0-9_\."]+)\s*IS NOT NULL/($1 IS NOT NULL AND $1::text <> '')/igs;
		# Form: fct(expression) IS NULL
		$str =~ s/([a-z0-9_\."]+\s*\([^\)]*\))\s*IS NULL/coalesce($1::text, '') = ''/igs;
		$str =~ s/([a-z0-9_\."]+\s*\([^\)]*\))\s*IS NOT NULL/($1 IS NOT NULL AND ($1)::text <> '')/igs;
	}

	# Rewrite replace(a,b) with three argument
	$str =~ s/REPLACE\s*\($field,$field\)/replace\($1, $2, ''\)/igs;

	# Replace Oracle substr(string, start_position, length) with
	# PostgreSQL substring(string from start_position for length)
	$str =~ s/substr\s*\($field,$field,$field\)/substring($1 from $2 for $3)/igs;
	$str =~ s/substr\s*\($field,$field\)/substring($1 from $2)/igs;

	# Remove any call to MDSYS schema in the code
	$str =~ s/MDSYS\.//igs;

	# Restore XMLELEMENT call
	$str =~ s/\%\%XMLELEMENT(\d+)\%\%/$xmlelt[$1]/igs;
	@xmlelt = ();

	##############
	# Replace package.function call by package_function
	##############
	if (scalar keys %{$class->{package_functions}}) {
		my @text_values = ();
		my $j = 0;
		while ($str =~ s/'([^']+)'/\%TEXTVALUE-$j\%/s) {
			push(@text_values, $1);
			$j++;
		}
		foreach my $k (keys %{$class->{package_functions}}) {
			$str =~ s/($class->{package_functions}->{$k}{package}\.)?\b$k\s*\(/$class->{package_functions}->{$k}{name}\(/igs;
		}
		$str =~ s/\%TEXTVALUE-(\d+)\%/'$text_values[$1]'/gs;
	}

	# Replace call to trim into btrim
	$str =~ s/\bTRIM\(([^\(\)]+)\)/btrim($1)/igs;

	return $str;
}

sub replace_decode
{
	my $str = shift;

	my $decode_idx = 0;
	my @str_decode = ();
	while ($str =~ s/DECODE\s*\(([^\(\)]+)\)/DECODE%$decode_idx%/is) {
		push(@str_decode, $1);
		# When there is no potential subquery in the decode statement
		if ($str_decode[-1] !~ /(\(|\))/) {
			# Create an array with all parameter of the decode function
			my @fields = split(/\s*,\s*/s, $str_decode[-1]);
			my $case_str = 'CASE ';
			for (my $i = 1; $i <= $#fields; $i+=2) {
				if ($i < $#fields) {
					$case_str .= "WHEN $fields[0]=$fields[$i] THEN $fields[$i+1] ";
				} else {
					$case_str .= " ELSE $fields[$i] ";
				}
			}
			$case_str .= 'END';
			$str_decode[-1] = $case_str;
		}
		$decode_idx++;
	}
	while ($str =~ /DECODE%(\d+)%/) {
		$decode_idx = $1;
		$str =~ s/DECODE%$decode_idx%/$str_decode[$decode_idx]/s;
	}

	return $str;
}


# Function to replace call to SYS_CONTECT('USERENV', ...)
# List of Oracle environment variables: http://docs.oracle.com/cd/B28359_01/server.111/b28286/functions172.htm
# Possibly corresponding PostgreSQL variables: http://www.postgresql.org/docs/current/static/functions-info.html
sub replace_sys_context
{
	my $str = shift;

	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'(OS_USER|SESSION_USER|AUTHENTICATED_IDENTITY)'\s*\)/session_user/igs;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'BG_JOB_ID'\s*\)/pg_backend_pid()/igs;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'(CLIENT_IDENTIFIER|PROXY_USER)'\s*\)/session_user/igs;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'CURRENT_SCHEMA'\s*\)/current_schema/igs;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'CURRENT_USER'\s*\)/current_user/igs;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'(DB_NAME|DB_UNIQUE_NAME)'\s*\)/current_database/igs;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'(HOST|IP_ADDRESS)'\s*\)/inet_client_addr()/igs;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'SERVER_HOST'\s*\)/inet_server_addr()/igs;

	return $str;
}

sub replace_sdo_function
{
	my $str = shift;

	$str =~ s/SDO_GEOM\.RELATE/ST_Relate/igs;
	$str =~ s/SDO_GEOM\.VALIDATE_GEOMETRY_WITH_CONTEXT/ST_IsValidReason/igs;
	$str =~ s/SDO_GEOM\.WITHIN_DISTANCE/ST_DWithin/igs;
	$str =~ s/SDO_GEOM\.//igs;
	$str =~ s/SDO_DISTANCE/ST_Distance/igs;
	$str =~ s/SDO_BUFFER/ST_Buffer/igs;
	$str =~ s/SDO_CENTROID/ST_Centroid/igs;
	$str =~ s/SDO_UTIL\.GETVERTICES/ST_DumpPoints/igs;
	$str =~ s/SDO_TRANSLATE/ST_Translate/igs;
	$str =~ s/SDO_SIMPLIFY/ST_Simplify/igs;
	$str =~ s/SDO_AREA/ST_Area/igs;
	$str =~ s/SDO_CONVEXHULL/ST_ConvexHull/igs;
	$str =~ s/SDO_DIFFERENCE/ST_Difference/igs;
	$str =~ s/SDO_INTERSECTION/ST_Intersection/igs;
	$str =~ s/SDO_LENGTH/ST_Length/igs;
	$str =~ s/SDO_POINTONSURFACE/ST_PointOnSurface/igs;
	$str =~ s/SDO_UNION/ST_Union/igs;
	$str =~ s/SDO_XOR/ST_SymDifference/igs;

	# Note that with ST_DumpPoints and :
	# TABLE(SDO_UTIL.GETVERTICES(C.GEOLOC)) T
	# T.X, T.Y, T.ID must be replaced manually as ST_X(T.geom) X, ST_Y(T.geom) Y, (T).path[1] ID
	my $field = '\s*[^\(\),]+\s*';
	my $num_field = '\s*[\d\.]+\s*';

	# SDO_GEOM.RELATE(geom1 IN SDO_GEOMETRY,mask IN VARCHAR2,geom2 IN SDO_GEOMETRY,tol IN NUMBER)
	$str =~ s/(ST_Relate\s*\($field),$field,($field),($field)\)/$1,$2\)/igs;
	# SDO_GEOM.RELATE(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,mask IN VARCHAR2,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_Relate\s*\($field),$field,$field,($field),$field\)/$1,$2\)/igs;
	# SDO_GEOM.SDO_AREA(geom IN SDO_GEOMETRY, tol IN NUMBER [, unit IN VARCHAR2])
	# SDO_GEOM.SDO_AREA(geom IN SDO_GEOMETRY,dim IN SDO_DIM_ARRAY [, unit IN VARCHAR2])
	$str =~ s/(ST_Area\s*\($field),[^\)]+\)/$1\)/igs;
	# SDO_GEOM.SDO_BUFFER(geom IN SDO_GEOMETRY,dist IN NUMBER, tol IN NUMBER [, params IN VARCHAR2])
	$str =~ s/(ST_Buffer\s*\($field,$num_field),[^\)]+\)/$1\)/igs;
	# SDO_GEOM.SDO_BUFFER(geom IN SDO_GEOMETRY,dim IN SDO_DIM_ARRAY,dist IN NUMBER [, params IN VARCHAR2])
	$str =~ s/(ST_Buffer\s*\($field),$field,($num_field)[^\)]*\)/$1,$2\)/igs;
	# SDO_GEOM.SDO_CENTROID(geom1 IN SDO_GEOMETRY,tol IN NUMBER)
	# SDO_GEOM.SDO_CENTROID(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_Centroid\s*\($field),$field\)/$1\)/igs;
	# SDO_GEOM.SDO_CONVEXHULL(geom1 IN SDO_GEOMETRY,tol IN NUMBER)
	# SDO_GEOM.SDO_CONVEXHULL(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_ConvexHull\s*\($field),$field\)/$1\)/igs;
	# SDO_GEOM.SDO_DIFFERENCE(geom1 IN SDO_GEOMETRY,geom2 IN SDO_GEOMETRY,tol IN NUMBER)
	$str =~ s/(ST_Difference\s*\($field,$field),$field\)/$1\)/igs;
	# SDO_GEOM.SDO_DIFFERENCE(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_Difference\s*\($field),$field,($field),$field\)/$1,$2\)/igs;
	# SDO_GEOM.SDO_DISTANCE(geom1 IN SDO_GEOMETRY,geom2 IN SDO_GEOMETRY,tol IN NUMBER [, unit IN VARCHAR2])
	$str =~ s/(ST_Distance\s*\($field,$field),($num_field)[^\)]*\)/$1\)/igs;
	# SDO_GEOM.SDO_DISTANCE(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY [, unit IN VARCHAR2])
	$str =~ s/(ST_Distance\s*\($field),$field,($field),($field)[^\)]*\)/$1,$2\)/igs;
	# SDO_GEOM.SDO_INTERSECTION(geom1 IN SDO_GEOMETRY,geom2 IN SDO_GEOMETRY,tol IN NUMBER)
	$str =~ s/(ST_Intersection\s*\($field,$field),$field\)/$1\)/igs;
	# SDO_GEOM.SDO_INTERSECTION(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_Intersection\s*\($field),$field,($field),$field\)/$1,$2\)/igs;
	# SDO_GEOM.SDO_LENGTH(geom IN SDO_GEOMETRY, dim IN SDO_DIM_ARRAY [, unit IN VARCHAR2])
	# SDO_GEOM.SDO_LENGTH(geom IN SDO_GEOMETRY, tol IN NUMBER [, unit IN VARCHAR2])
	$str =~ s/(ST_Length\s*\($field),($field)[^\)]*\)/$1\)/igs;
	# SDO_GEOM.SDO_POINTONSURFACE(geom1 IN SDO_GEOMETRY, tol IN NUMBER)
	# SDO_GEOM.SDO_POINTONSURFACE(geom1 IN SDO_GEOMETRY, dim1 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_PointOnSurface\s*\($field),$field\)/$1\)/igs;
	# SDO_GEOM.SDO_UNION(geom1 IN SDO_GEOMETRY, geom2 IN SDO_GEOMETRY, tol IN NUMBER)
	$str =~ s/(ST_Union\s*\($field,$field),$field\)/$1\)/igs;
	# SDO_GEOM.SDO_UNION(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_Union\s*\($field),$field,($field),$field\)/$1,$2\)/igs;
	# SDO_GEOM.SDO_XOR(geom1 IN SDO_GEOMETRY,geom2 IN SDO_GEOMETRY, tol IN NUMBER)
	$str =~ s/(ST_SymDifference\s*\($field,$field),$field\)/$1\)/igs;
	# SDO_GEOM.SDO_XOR(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_SymDifference\s*\($field),$field,($field),$field\)/$1,$2\)/igs;
	# SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT(geom1 IN SDO_GEOMETRY, tol IN NUMBER)
	# SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT(geom1 IN SDO_GEOMETRY, dim1 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_IsValidReason\s*\($field),$field\)/$1\)/igs;
	# SDO_GEOM.WITHIN_DISTANCE(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,dist IN NUMBER,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY [, units IN VARCHAR2])
	$str =~ s/(ST_DWithin\s*\($field),$field,($field),($field),($field)[^\)]*\)/$1,$3,$2\)/igsgs;
	# SDO_GEOM.WITHIN_DISTANCE(geom1 IN SDO_GEOMETRY,dist IN NUMBER,geom2 IN SDO_GEOMETRY, tol IN NUMBER [, units IN VARCHAR2])
	$str =~ s/(ST_DWithin\s*\($field)(,$field)(,$field),($field)[^\)]*\)/$1$3$2\)/igs;

	return $str;
}

sub replace_sdo_operator
{
	my $str = shift;

	# SDO_CONTAINS(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_CONTAINS\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Contains($1)/igs;
	$str =~ s/SDO_CONTAINS\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Contains($1)/igs;
	$str =~ s/SDO_CONTAINS\s*\(([^\)]+)\)/ST_Contains($1)/igs;
	# SDO_RELATE(geometry1, geometry2, param) = 'TRUE'
	$str =~ s/SDO_RELATE\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Relate($1)/igs;
	$str =~ s/SDO_RELATE\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Relate($1)/igs;
	$str =~ s/SDO_RELATE\s*\(([^\)]+)\)/ST_Relate($1)/igs;
	# SDO_WITHIN_DISTANCE(geometry1, aGeom, params) = 'TRUE'
	$str =~ s/SDO_WITHIN_DISTANCE\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_DWithin($1)/igs;
	$str =~ s/SDO_WITHIN_DISTANCE\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_DWithin($1)/igs;
	$str =~ s/SDO_WITHIN_DISTANCE\s*\(([^\)]+)\)/ST_DWithin($1)/igs;
	# SDO_TOUCH(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_TOUCH\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Touches($1)/igs;
	$str =~ s/SDO_TOUCH\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Touches($1)/igs;
	$str =~ s/SDO_TOUCH\s*\(([^\)]+)\)/ST_Touches($1)/igs;
	# SDO_OVERLAPS(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_OVERLAPS\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Overlaps($1)/igs;
	$str =~ s/SDO_OVERLAPS\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Overlaps($1)/igs;
	$str =~ s/SDO_OVERLAPS\s*\(([^\)]+)\)/ST_Overlaps($1)/igs;
	# SDO_INSIDE(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_INSIDE\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Within($1)/igs;
	$str =~ s/SDO_INSIDE\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Within($1)/igs;
	$str =~ s/SDO_INSIDE\s*\(([^\)]+)\)/ST_Within($1)/igs;
	# SDO_EQUAL(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_EQUAL\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Equals($1)/igs;
	$str =~ s/SDO_EQUAL\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Equals($1)/igs;
	$str =~ s/SDO_EQUAL\s*\(([^\)]+)\)/ST_Equals($1)/igs;
	# SDO_COVERS(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_COVERS\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Covers($1)/igs;
	$str =~ s/SDO_COVERS\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Covers($1)/igs;
	$str =~ s/SDO_COVERS\s*\(([^\)]+)\)/ST_Covers($1)/igs;
	# SDO_COVEREDBY(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_COVEREDBY\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_CoveredBy($1)/igs;
	$str =~ s/SDO_COVEREDBY\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_CoveredBy($1)/igs;
	$str =~ s/SDO_COVEREDBY\s*\(([^\)]+)\)/ST_CoveredBy($1)/igs;
	# SDO_ANYINTERACT(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_ANYINTERACT\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Intersects($1)/igs;
	$str =~ s/SDO_ANYINTERACT\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Intersects($1)/igs;
	$str =~ s/SDO_ANYINTERACT\s*\(([^\)]+)\)/ST_Intersects($1)/igs;

	return $str;
}

# Function used to rewrite dbms_output.put, dbms_output.put_line and
# dbms_output.new_line by a plpgsql code
sub raise_output
{
	my $str = shift;

	my @strings = split(/\|\|/s, $str);

	my @params = ();
	my $pattern = '';
	foreach my $el (@strings) {
		if ($el =~ /^'(.*)'$/s) {
			$pattern .= $1;
		} else {
			$pattern .= '%';
			push(@params, $el);
		}
	}
	my $ret = "RAISE NOTICE '$pattern'";
	if ($#params >= 0) {
		$ret .= ', ' . join(', ', @params);
	}

	return $ret . ';';
}

sub replace_sql_type
{
        my ($str, $pg_numeric_type, $default_numeric, $pg_integer_type) = @_;


	$str =~ s/with local time zone/with time zone/igs;
	$str =~ s/([A-Z])ORA2PG_COMMENT/$1 ORA2PG_COMMENT/igs;

	# Replace type with precision
	my $oratype_regex = join('|', keys %Ora2Pg::TYPE);
	while ($str =~ /(.*)\b($oratype_regex)\s*\(([^\)]+)\)/i) {
		my $backstr = $1;
		my $type = uc($2);
		my $args = $3;
		# Remove extra CHAR or BYTE information from column type
		$args =~ s/\s*(CHAR|BYTE)\s*$//i;
		if ($backstr =~ /_$/) {
		    $str =~ s/\b($oratype_regex)\s*\(([^\)]+)\)/$1\%\|$2\%\|\%/is;
		    next;
		}

		my ($precision, $scale) = split(/,/, $args);
		$scale ||= 0;
		my $len = $precision || 0;
		$len =~ s/\D//;
		if ( $type =~ /CHAR|STRING/i ) {
			# Type CHAR have default length set to 1
			# Type VARCHAR(2) must have a specified length
			$len = 1 if (!$len && (($type eq "CHAR") || ($type eq "NCHAR")));
			$str =~ s/\b$type\b\s*\([^\)]+\)/$Ora2Pg::TYPE{$type}\%\|$len\%\|\%/is;
		} elsif ($type =~ /TIMESTAMP/i) {
			$len = 6 if ($len > 6);
			$str =~ s/\b$type\b\s*\([^\)]+\)/timestamp\%\|$len%\|\%/is;
 		} elsif ($type =~ /INTERVAL/i) {
 			# Interval precision for year/month/day is not supported by PostgreSQL
 			$str =~ s/(INTERVAL\s+YEAR)\s*\(\d+\)/$1/is;
 			$str =~ s/(INTERVAL\s+YEAR\s+TO\s+MONTH)\s*\(\d+\)/$1/is;
 			$str =~ s/(INTERVAL\s+DAY)\s*\(\d+\)/$1/is;
			# maximum precision allowed for seconds is 6
			if ($str =~ /INTERVAL\s+DAY\s+TO\s+SECOND\s*\((\d+)\)/) {
				if ($1 > 6) {
					$str =~ s/(INTERVAL\s+DAY\s+TO\s+SECOND)\s*\(\d+\)/$1(6)/i;
				}
			}
		} elsif ($type eq "NUMBER") {
			# This is an integer
			if (!$scale) {
				if ($precision) {
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
				} elsif ($pg_integer_type) {
					my $tmp = $default_numeric || 'bigint';
					$str =~ s/\b$type\b\s*\([^\)]+\)/$tmp/is;
				}
			} else {
				if ($precision) {
					if ($pg_numeric_type) {
						if ($precision <= 6) {
							$str =~ s/\b$type\b\s*\([^\)]+\)/real/is;
						} else {
							$str =~ s/\b$type\b\s*\([^\)]+\)/double precision/is;
						}
					} else {
						$str =~ s/\b$type\b\s*\([^\)]+\)/decimal\%\|$precision,$scale\%\|\%/is;
					}
				}
			}
		} elsif ($type eq "NUMERIC") {
			$str =~ s/\b$type\b\s*\([^\)]+\)/numeric\%\|$args\%\|\%/is;
		} elsif ( ($type eq "DEC") || ($type eq "DECIMAL") ) {
			$str =~ s/\b$type\b\s*\([^\)]+\)/decimal\%\|$args\%\|\%/is;
		} else {
			# Prevent from infinit loop
			$str =~ s/\(/\%\|/s;
			$str =~ s/\)/\%\|\%/s;
		}
	}
	$str =~ s/\%\|\%/\)/gs;
	$str =~ s/\%\|/\(/gs;

        # Replace datatype without precision
	my $number = $Ora2Pg::TYPE{'NUMBER'};
	$number = $default_numeric if ($pg_integer_type);
	$str =~ s/\bNUMBER\b/$number/igs;

	# Set varchar without length to text
	$str =~ s/\bVARCHAR2\b/VARCHAR/igs;
	$str =~ s/\bSTRING\b/VARCHAR/igs;
	$str =~ s/\bVARCHAR(\s*(?!\())/text$1/igs;

	foreach my $t ('DATE','LONG RAW','LONG','NCLOB','CLOB','BLOB','BFILE','RAW','ROWID','FLOAT','DOUBLE PRECISION','INTEGER','INT','REAL','SMALLINT','BINARY_FLOAT','BINARY_DOUBLE','BINARY_INTEGER','BOOLEAN','XMLTYPE') {
		$str =~ s/\b$t\b/$Ora2Pg::TYPE{$t}/igs;
	}

        return $str;
}

sub estimate_cost
{
	my ($class, $str, $type) = @_;

	return mysql_estimate_cost($str, $type) if ($class->{is_mysql});

	my %cost_details = ();

	# Remove some unused pragma from the cost assessment
	$str =~ s/PRAGMA RESTRICT_REFERENCES[^;]+;//igs;
        $str =~ s/PRAGMA SERIALLY_REUSABLE[^;]*;//igs;
        $str =~ s/PRAGMA INLINE[^;]+;//igs;

	# Default cost is testing that mean it at least must be tested
	my $cost = $FCT_TEST_SCORE;
	# When evaluating queries size must not be included here
	if ($type eq 'QUERY') {
		$cost = 0;
	}
	$cost_details{'TEST'} = $cost;

	# Set cost following code length
	my $cost_size = int(length($str)/$SIZE_SCORE) || 1;
	# When evaluating queries size must not be included here
	if ($type eq 'QUERY') {
		$cost_size = 0;
	}
	$cost += $cost_size;
	$cost_details{'SIZE'} = $cost_size;

	# Try to figure out the manual work
	my $n = () = $str =~ m/\bFROM\s*\(/igs;
	$cost_details{'FROM'} += $n;
	$n = () = $str =~ m/\bTRUNC\s*\(/igs;
	$cost_details{'TRUNC'} += $n;
	$n = () = $str =~ m/\bDECODE\s*\(/igs;
	$cost_details{'DECODE'} += $n;
	$n = () = $str =~ m/\bIS\s+TABLE\s+OF\b/igs;
	$cost_details{'IS TABLE OF'} += $n;
	$n = () = $str =~ m/\(\+\)/igs;
	$cost_details{'OUTER JOIN'} += $n;
	$n = () = $str =~ m/\bCONNECT\s+BY\b/igs;
	$cost_details{'CONNECT BY'} += $n;
	$n = () = $str =~ m/\bBULK\s+COLLECT\b/igs;
	$cost_details{'BULK COLLECT'} += $n;
	$n = () = $str =~ m/\bFORALL\b/igs;
	$cost_details{'FORALL'} += $n;
	$n = () = $str =~ m/\bGOTO\b/igs;
	$cost_details{'GOTO'} += $n;
	$n = () = $str =~ m/\bROWNUM\b/igs;
	$cost_details{'ROWNUM'} += $n;
	$n = () = $str =~ m/\bNOTFOUND\b/igs;
	$cost_details{'NOTFOUND'} += $n;
	$n = () = $str =~ m/\bROWID\b/igs;
	$cost_details{'ROWID'} += $n;
	$n = () = $str =~ m/\bSQLSTATE\b/igs;
	$cost_details{'SQLCODE'} += $n;
	$n = () = $str =~ m/\bIS RECORD\b/igs;
	$cost_details{'IS RECORD'} += $n;
	$n = () = $str =~ m/FROM[^;]*\bTABLE\s*\(/igs;
	$cost_details{'TABLE'} += $n;
	$n = () = $str =~ m/PIPE\s+ROW/igs;
	$cost_details{'PIPE ROW'} += $n;
	$n = () = $str =~ m/DBMS_\w/igs;
	$cost_details{'DBMS_'} += $n;
	$n = () = $str =~ m/DBMS_OUTPUT\.(put_line|new_line|put)/igs;
	$cost_details{'DBMS_'} -= $n;
	$n = () = $str =~ m/DBMS_OUTPUT\.put\(/igs;
	$cost_details{'DBMS_OUTPUT.put'} += $n;
	$n = () = $str =~ m/DBMS_STANDARD\.RAISE EXCEPTION/igs;
	$cost_details{'DBMS_'} -= $n;
	$n = () = $str =~ m/UTL_\w/igs;
	$cost_details{'UTL_'} += $n;
	$n = () = $str =~ m/CTX_\w/igs;
	$cost_details{'CTX_'} += $n;
	$n = () = $str =~ m/\bEXTRACT\s*\(/igs;
	$cost_details{'EXTRACT'} += $n;
	$n = () = $str =~ m/\bTO_NUMBER\s*\(/igs;
	$cost_details{'TO_NUMBER'} += $n;
	# See:  http://www.postgresql.org/docs/9.0/static/errcodes-appendix.html#ERRCODES-TABLE
	$n = () = $str =~ m/\b(DUP_VAL_ON_INDEX|TIMEOUT_ON_RESOURCE|TRANSACTION_BACKED_OUT|NOT_LOGGED_ON|LOGIN_DENIED|INVALID_NUMBER|PROGRAM_ERROR|VALUE_ERROR|ROWTYPE_MISMATCH|CURSOR_ALREADY_OPEN|ACCESS_INTO_NULL|COLLECTION_IS_NULL)\b/igs;
	$cost_details{'EXCEPTION'} += $n;
	$n = () = $str =~ m/REGEXP_LIKE/igs;
	$cost_details{'REGEXP_LIKE'} += $n;
	$n = () = $str =~ m/\b(INSERTING|DELETING|UPDATING)\b/igs;
	$cost_details{'TG_OP'} += $n;
	$n = () = $str =~ m/CURSOR/igs;
	$cost_details{'CURSOR'} += $n;
	$n = () = $str =~ m/ORA_ROWSCN/igs;
	$cost_details{'ORA_ROWSCN'} += $n;
	$n = () = $str =~ m/SAVEPOINT/igs;
	$cost_details{'SAVEPOINT'} += $n;
	$n = () = $str =~ m/(FROM|EXEC)((?!WHERE).)*\b[\w\_]+\@[\w\_]+\b/igs;
	$cost_details{'DBLINK'} += $n;
	$n = () = $str =~ m/%ISOPEN\b/igs;
	$cost_details{'ISOPEN'} += $n;
	$n = () = $str =~ m/%ROWCOUNT\b/igs;
	$cost_details{'ROWCOUNT'} += $n;

	$n = () = $str =~ m/PLVDATE/igs;
	$cost_details{'PLVDATE'} += $n;
	$n = () = $str =~ m/PLVSTR/igs;
	$cost_details{'PLVSTR'} += $n;
	$n = () = $str =~ m/PLVCHR/igs;
	$cost_details{'PLVCHR'} += $n;
	$n = () = $str =~ m/PLVSUBST/igs;
	$cost_details{'PLVSUBST'} += $n;
	$n = () = $str =~ m/PLVLEX/igs;
	$cost_details{'PLVLEX'} += $n;
	$n = () = $str =~ m/PLUNIT/igs;
	$cost_details{'PLUNIT'} += $n;
	$n = () = $str =~ m/ADD_MONTHS/igs;
	$cost_details{'ADD_MONTHS'} += $n;
	$n = () = $str =~ m/LAST_DATE/igs;
	$cost_details{'LAST_DATE'} += $n;
	$n = () = $str =~ m/NEXT_DAY/igs;
	$cost_details{'NEXT_DAY'} += $n;
	$n = () = $str =~ m/MONTHS_BETWEEN/igs;
	$cost_details{'MONTHS_BETWEEN'} += $n;
	$n = () = $str =~ m/NVL2/igs;
	$cost_details{'NVL2'} += $n;
	$str =~ s/MDSYS\.(["]*SDO_)/$1/igs;
	$n = () = $str =~ m/SDO_\w/igs;
	$cost_details{'SDO_'} += $n;
	$n = () = $str =~ m/PRAGMA/igs;
	$cost_details{'PRAGMA'} += $n;
	$n = () = $str =~ m/MDSYS\./igs;
	$cost_details{'MDSYS'} += $n;
	$n = () = $str =~ m/MERGE\sINTO/igs;
	$cost_details{'MERGE'} += $n;

	foreach my $f (@ORA_FUNCTIONS) {
		if ($str =~ /\b$f\b/igs) {
			$cost += 1;
			$cost_details{$f} += 1;
		}
	}
	foreach my $t (keys %UNCOVERED_SCORE) {
		$cost += $UNCOVERED_SCORE{$t}*$cost_details{$t};
	}

	return $cost, %cost_details;
}

=head2 mysql_to_plpgsql

This function turn a MySQL function code into a PLPGSQL code

=cut

sub mysql_to_plpgsql
{
        my ($class, $str) = @_;

	#--------------------------------------------
	# Procedure are functions returning void
	$str =~ s/\bPROCEDURE\b/FUNCTION/igs;

	# Simply remove this as not supported
	$str =~ s/\bDEFAULT\s+NULL\b//igs;

	# Change mysql varaible affectation 
	$str =~ s/\bSET\s+([^\s]+\s*)=([^;\n]+;)/$1:=$2/igs;

	# remove declared handler
	$str =~ s/[^\s]+\s+HANDLER\s+FOR\s+[^;]+;//igs;

	# Try to replace LEAVE label by EXIT label
	my %repl_leave = ();
	my $i = 0;
	while ($str =~ s/\bLEAVE\s+([^\s;]+)\s*;/%REPEXITLBL$i%/igs) {
		my $label = $1;
		if ( $str =~ /\b$label:/is) {
			$repl_leave{$i} = "EXIT $label;";
		} else {
			# This is a main block label
			$repl_leave{$i} = "RETURN;";
		}
	}
	foreach $i (keys %repl_leave) {
		$str =~ s/\%REPEXITLBL$i\%/$repl_leave{$i}/gs;
	}
	%repl_leave = ();
	$str =~ s/\bLEAVE\s*;/EXIT;/igs;

	# Try to replace ITERATE label by CONTINUE label
	my %repl_iterate = ();
	$i = 0;
	while ($str =~ s/\bITERATE\s+([^\s;]+)\s*;/%REPITERLBL$i%/igs) {
		my $label = $1;
		$repl_iterate{$i} = "CONTINUE $label;";
	}
	foreach $i (keys %repl_iterate) {
		$str =~ s/\%REPITERLBL$i\%/$repl_iterate{$i}/gs;
	}
	%repl_iterate = ();
	$str =~ s/\bITERATE\s*;/CONTINUE;/igs;

	# Replace now() with CURRENT_TIMESTAMP even if this is the same
	# because parenthesis can break the following regular expressions
	$str =~ s/\bNOW\(\s*\)/CURRENT_TIMESTAMP/igs;

	# Replace EXTRACT() with unit not supported by PostgreSQL
	if ($class->{mysql_internal_extract_format}) {
		$str =~ s/\bEXTRACT\(\s*YEAR_MONTH\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'YYYYMM')::integer/igs;
		$str =~ s/\bEXTRACT\(\s*DAY_HOUR\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'DDHH24')::integer/igs;
		$str =~ s/\bEXTRACT\(\s*DAY_MINUTE\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'DDHH24MI')::integer/igs;
		$str =~ s/\bEXTRACT\(\s*DAY_SECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'DDHH24MISS')::integer/igs;
		$str =~ s/\bEXTRACT\(\s*DAY_MICROSECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'DDHH24MISSUS')::bigint/igs;
		$str =~ s/\bEXTRACT\(\s*HOUR_MINUTE\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'HH24MI')::integer/igs;
		$str =~ s/\bEXTRACT\(\s*HOUR_SECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'HH24MISS')::integer/igs;
		$str =~ s/\bEXTRACT\(\s*HOUR_MICROSECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'HH24MISSUS')::bigint/igs;
		$str =~ s/\bEXTRACT\(\s*MINUTE_SECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'MISS')::integer/igs;
		$str =~ s/\bEXTRACT\(\s*MINUTE_MICROSECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'MISSUS')::bigint/igs;
		$str =~ s/\bEXTRACT\(\s*SECOND_MICROSECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'SSUS')::integer/igs;
	} else {
		$str =~ s/\bEXTRACT\(\s*YEAR_MONTH\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'YYYY-MM')/igs;
		$str =~ s/\bEXTRACT\(\s*DAY_HOUR\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'DD HH24')/igs;
		$str =~ s/\bEXTRACT\(\s*DAY_MINUTE\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'DD HH24:MI')/igs;
		$str =~ s/\bEXTRACT\(\s*DAY_SECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'DD HH24:MI:SS')/igs;
		$str =~ s/\bEXTRACT\(\s*DAY_MICROSECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'DD HH24:MI:SS.US')/igs;
		$str =~ s/\bEXTRACT\(\s*HOUR_MINUTE\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'HH24:MI')/igs;
		$str =~ s/\bEXTRACT\(\s*HOUR_SECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'HH24:MI:SS')/igs;
		$str =~ s/\bEXTRACT\(\s*HOUR_MICROSECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'HH24:MI:SS.US')/igs;
		$str =~ s/\bEXTRACT\(\s*MINUTE_SECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'MI:SS')/igs;
		$str =~ s/\bEXTRACT\(\s*MINUTE_MICROSECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'MI:SS.US')/igs;
		$str =~ s/\bEXTRACT\(\s*SECOND_MICROSECOND\s+FROM\s+([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'SS.US')/igs;
	}

	# Replace operators
	if (!$class->{mysql_pipes_as_concat}) {
		$str =~ s/\|\|/ OR /igs;
		$str =~ s/\&\&/ AND /igs;
	}
	$str =~ s/BIT_XOR\(\s*([^,]+)\s*,\s*(\d+)\s*\)/$1 # coalesce($2, 0)/igs;
	$str =~ s/\bXOR\b/#/igs;
	$str =~ s/\b\^\b/#/igs;

	####
	# Replace some function with their PostgreSQL syntax
	####

	# Math related fucntion
	$str =~ s/\bATAN\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/atan2($1, $2)/igs;
	$str =~ s/\bLOG\(/ln\(/igs;
	$str =~ s/\bLOG10\(\s*([^\(\)]+)\s*\)/log\(10, $1\)/igs;
	$str =~ s/\bLOG2\(\s*([^\(\)]+)\s*\)/log\(2, $1\)/igs;
	$str =~ s/([^\s]+)\s+MOD\s+([^\s]+)/mod\($1, $2\)/igs;
	$str =~ s/\bPOW\(/power\(/igs;
	$str =~ s/\bRAND\(\s*\)/random\(\)/igs;

	# Misc function
	$str =~ s/\bCHARSET\(\s*([^\(\)]+)\s*\)/current_setting('server_encoding')/igs;
	$str =~ s/\bCOLLATION\(\s*([^\(\)]+)\s*\)/current_setting('lc_collate')/igs;
	$str =~ s/\bCONNECTION_ID\(\s*\)/pg_backend_pid()/igs;
	$str =~ s/\b(DATABASE|SCHEMA)\(\s*\)/current_database()/igs;
	$str =~ s/\bSLEEP\(/pg_sleep\(/igs;
	$str =~ s/\bSYSTEM_USER\(\s*\)/CURRENT_USER/igs;
	$str =~ s/\bSESSION_USER\(\s*\)/SESSION_USER/igs;
	$str =~ s/\bTRUNCATE\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/trunc\($1, $2\)/igs;
	$str =~ s/\bUSER\(\s*\)/CURRENT_USER/igs;

	# Date/time related function
	$str =~ s/\b(CURDATE|CURRENT_DATE)\(\s*\)/CURRENT_DATE/igs;
	$str =~ s/\b(CURTIME|CURRENT_TIME)\(\s*\)/LOCALTIME(0)/igs;
	$str =~ s/\bCURRENT_TIMESTAMP\(\s*\)/CURRENT_TIMESTAMP::timestamp(0) without time zone/igs;
	$str =~ s/\b(LOCALTIMESTAMP|LOCALTIME)\(\s*\)/CURRENT_TIMESTAMP::timestamp(0) without time zone/igs;
	$str =~ s/\b(LOCALTIMESTAMP|LOCALTIME)\b/CURRENT_TIMESTAMP::timestamp(0) without time zone/igs;
	$str =~ s/\bSYSDATE\(\s*\)/timeofday()::timestamp(0) without time zone/igs;
	$str =~ s/\bUNIX_TIMESTAMP\(\s*\)/floor(extract(epoch from CURRENT_TIMESTAMP::timestamp with time zone))/igs;
	$str =~ s/\bUNIX_TIMESTAMP\(\s*([^\)]+)\s*\)/floor(extract(epoch from ($1)::timestamp with time zone))/igs;
	$str =~ s/\bUTC_DATE\(\s*\)/(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date/igs;
	$str =~ s/\bUTC_TIME\(\s*\)/(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::time(0)/igs;
	$str =~ s/\bUTC_TIMESTAMP\(\s*\)/(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::timestamp(0)/igs;

	# Replace some function with different name and format
	$str =~ s/\b(ADDDATE|DATE_ADD)\(\s*([^,]+)\s*,\s*INTERVAL ([^\(\),]+)\s*\)/($2)::timestamp + interval '$3'/igs;
	$str =~ s/\bADDDATE\(\s*([^,]+)\s*,\s*(\d+)\s*\)/($1)::timestamp + ($2 * interval '1 day')/igs;
	$str =~ s/\bADDTIME\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/($1)::timestamp + ($2)::interval/igs;
	$str =~ s/\bCONVERT_TZ\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/(($1)::timestamp without time zone AT TIME ZONE ($2)::text) AT TIME ZONE ($3)::text/igs;
	$str =~ s/\bDATEDIFF\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/extract(day from (date_trunc('day', ($1)::timestamp) - date_trunc('day', ($2)::timestamp)))/igs;
	$str =~ s/\bDATE_FORMAT\(\s*(.*?)\s*,\s*('[^\(\)]+')\s*\)/_mysql_dateformat_to_pgsql($1, $2)/eigs;
	$str =~ s/\b(DAY|DAYOFMONTH)\(\s*([^\(\)]+)\s*\)/extract(day from date($1))::integer/igs;
	$str =~ s/\bDAYNAME\(\s*([^\(\)]+)\s*\)/to_char(($1)::date, 'FMDay')/igs;
	$str =~ s/\bDAYOFWEEK\(\s*([^\(\)]+)\s*\)/extract(dow from date($1))::integer + 1/igs; # start on sunday = 1
	$str =~ s/\bDAYOFYEAR\(\s*([^\(\)]+)\s*\)/extract(doy from date($1))::integer/igs;
	$str =~ s/\bFORMAT\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/to_char(round($1, $2), 'FM999,999,999,999,999,999,990'||case when $2 > 0 then '.'||repeat('0', $2) else '' end)/igs;
	$str =~ s/\bFROM_DAYS\(\s*([^\(\)]+)\s*\)/'0001-01-01bc'::date + ($1)::integer/igs;
	$str =~ s/\bFROM_UNIXTIME\(\s*([^\(\),]+)\s*\)/to_timestamp($1)::timestamp without time zone/igs;
	$str =~ s/\bFROM_UNIXTIME\(\s*(.*?)\s*,\s*('[^\(\)]+')\s*\)/FROM_UNIXTIME2(to_timestamp($1), $2)/igs;
	$str =~ s/\bFROM_UNIXTIME2\(\s*(.*?)\s*,\s*('[^\)]+')\s*\)/_mysql_dateformat_to_pgsql($1, $2)/eigs;
	$str =~ s/\bGET_FORMAT\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/_mysql_getformat_to_pgsql($1, $2)/eigs;
	$str =~ s/\bHOUR\(\s*([^\(\)]+)\s*\)/extract(hour from ($1)::interval)::integer/igs;
	$str =~ s/\bLAST_DAY\(\s*([^\(\)]+)\s*\)/(date_trunc('month',($1)::timestamp + interval '1 month'))::date - 1/igs;
	$str =~ s/\bMAKEDATE\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/(date($1||'-01-01') + ($2 - 1) * interval '1 day')::date/igs;
	$str =~ s/\bMAKETIME\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/($1 * interval '1 hour' + $2 * interval '1 min' + $3 * interval '1 sec')/igs;
	$str =~ s/\bMICROSECOND\(\s*([^\(\)]+)\s*\)/extract(microsecond from ($1)::time)::integer/igs;
	$str =~ s/\bMINUTE\(\s*([^\(\)]+)\s*\)/extract(minute from ($1)::time)::integer/igs;
	$str =~ s/\bMONTH\(\s*([^\(\)]+)\s*\)/extract(month from date($1))::integer/igs;
	$str =~ s/\bMONTHNAME\(\s*([^\(\)]+)\s*\)/to_char(($1)::date, 'FMMonth')/igs;
	$str =~ s/\bQUARTER\(\s*([^\(\)]+)\s*\)/extract(quarter from date($1))::integer/igs;
	$str =~ s/\bSECOND\(\s*([^\(\)]+)\s*\)/extract(second from ($1)::interval)::integer/igs;
	$str =~ s/\bSEC_TO_TIME\(\s*([^\(\)]+)\s*\)/($1 * interval '1 second')/igs;
	$str =~ s/\bSTR_TO_DATE\(\s*(.*?)\s*,\s*('[^\(\),]+')\s*\)/_mysql_strtodate_to_pgsql($1, $2)/eigs;
	$str =~ s/\b(SUBDATE|DATE_SUB)\(\s*([^,]+)\s*,\s*INTERVAL ([^\(\)]+)\s*\)/($2)::timestamp - interval '$3'/igs;
	$str =~ s/\bSUBDATE\(\s*([^,]+)\s*,\s*(\d+)\s*\)/($1)::timestamp - ($2 * interval '1 day')/igs;
	$str =~ s/\bSUBTIME\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/($1)::timestamp - ($2)::interval/igs;
	$str =~ s/\bTIME(\([^\(\)]+\))/($1)::time/igs;
	$str =~ s/\bTIMEDIFF\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/($1)::timestamp - ($2)::timestamp/igs;
	$str =~ s/\bTIMESTAMP\(\s*([^\(\)]+)\s*\)/($1)::timestamp/igs;
	$str =~ s/\bTIMESTAMP\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/($1)::timestamp + ($2)::time/igs;
	$str =~ s/\bTIMESTAMPADD\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/($3)::timestamp + ($1 * interval '1 $2')/igs;
	$str =~ s/\bTIMESTAMPDIFF\(\s*YEAR\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/extract(year from ($2)::timestamp) - extract(year from ($1)::timestamp)/igs;
	$str =~ s/\bTIMESTAMPDIFF\(\s*MONTH\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/(extract(year from ($2)::timestamp) - extract(year from ($1)::timestamp))*12 + (extract(month from ($2)::timestamp) - extract(month from ($1)::timestamp))/igs;
	$str =~ s/\bTIMESTAMPDIFF\(\s*WEEK\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/floor(extract(day from ( ($2)::timestamp - ($1)::timestamp))\/7)/igs;
	$str =~ s/\bTIMESTAMPDIFF\(\s*DAY\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/extract(day from ( ($2)::timestamp - ($1)::timestamp))/igs;
	$str =~ s/\bTIMESTAMPDIFF\(\s*HOUR\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/floor(extract(epoch from ( ($2)::timestamp - ($1)::timestamp))\/3600)/igs;
	$str =~ s/\bTIMESTAMPDIFF\(\s*MINUTE\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/floor(extract(epoch from ( ($2)::timestamp - ($1)::timestamp))\/60)/igs;
	$str =~ s/\bTIMESTAMPDIFF\(\s*SECOND\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/extract(epoch from ($2)::timestamp) - extract(epoch from ($1)::timestamp))/igs;
	$str =~ s/\bTIME_FORMAT\(\s*(.*?)\s*,\s*('[^\(\),]+')\s*\)/_mysql_timeformat_to_pgsql($1, $2)/eigs;
	$str =~ s/\bTIME_TO_SEC\(\s*([^\(\)]+)\s*\)/(extract(hours from ($1)::time)*3600 + extract(minutes from ($1)::time)*60 + extract(seconds from ($1)::time))::bigint/igs;
	$str =~ s/\bTO_DAYS\(\s*([^\(\)]+)\s*\)/(($1)::date - '0001-01-01bc')::integer/igs;
	$str =~ s/\bWEEK(\([^\(\)]+\))/extract(week from date($1)) - 1/igs;
	$str =~ s/\bWEEKOFYEAR(\([^\(\)]+\))/extract(week from date($2))/igs;
	$str =~ s/\bWEEKDAY\(\s*([^\(\)]+)\s*\)/to_char(($1)::timestamp, 'ID')::integer - 1/igs; # MySQL: Monday = 0, PG => 1
	$str =~ s/\bYEAR\(\s*([^\(\)]+)\s*\)/extract(year from date($1))/igs;

	# String functions
	$str =~ s/\bBIN\(\s*([^\(\)]+)\s*\)/ltrim(textin(bit_out($1::bit(64))), '0')/igs;
	$str =~ s/\bBINARY\(\s*([^\(\)]+)\s*\)/($1)::bytea/igs;
	$str =~ s/\bBIT_COUNT\(\s*([^\(\)]+)\s*\)/length(replace(ltrim(textin(bit_out($1::bit(64))),'0'),'0',''))/igs;
	$str =~ s/\bCHAR\(\s*([^\(\),]+)\s*\)/array_to_string(ARRAY(SELECT chr(unnest($1))),'')/igs;
	$str =~ s/\bELT\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/(ARRAY[$2])[$1]/igs;
	$str =~ s/\bFIELD\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/(SELECT i FROM generate_subscripts(array[$2], 1) g(i) WHERE $1 = (array[$2])[i] UNION ALL SELECT 0 LIMIT 1)/igs;
	$str =~ s/\bFIND_IN_SET\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/(SELECT i FROM generate_subscripts(string_to_array($2,','), 1) g(i) WHERE $1 = (string_to_array($2,','))[i] UNION ALL SELECT 0 LIMIT 1)/igs;
	$str =~ s/\bFROM_BASE64\(\s*([^\(\),]+)\s*\)/decode(($1)::bytea, 'base64')/igs;
	$str =~ s/\bHEX\(\s*([^\(\),]+)\s*\)/upper(encode($1::bytea, 'hex'))/igs;
	$str =~ s/\bINSTR\s*\(\s*([^,]+),\s*('[^']+')\s*\)/position($2 in $1)/igs;
	$str =~ s/\bLOCATE\(\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/position($1 in substring ($2 from $3)) + $3 - 1/igs;
	$str =~ s/\bLOCATE\(\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/position($1 in $2)/igs;
	$str =~ s/\bLCASE\(/lower\(/igs;
	$str =~ s/\bMID\(/substring\(/igs;
	$str =~ s/\bORD\(/ascii\(/igs;
	$str =~ s/\bQUOTE\(/quote_literal\(/igs;
	$str =~ s/\bSPACE\(\s*([^\(\),]+)\s*\)/repeat(' ', $1)/igs;
	$str =~ s/\bSTRCMP\(\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/CASE WHEN $1 < $2 THEN -1 WHEN $1 > $2 THEN 1 ELSE 0 END/igs;
	$str =~ s/\bTO_BASE64\(\s*([^\(\),]+)\s*\)/encode($1, 'base64')/igs;
	$str =~ s/\bUCASE\(/upper\(/igs;
	$str =~ s/\bUNHEX\(\s*([^\(\),]+)\s*\)/decode($1, 'hex')::text/igs;
	$str =~ s/\bIS_IPV6\(\s*([^\(\)]+)\s*\)/CASE WHEN family($1) = 6 THEN 1 ELSE 0 END/igs;
	$str =~ s/\bIS_IPV4\(\s*([^\(\)]+)\s*\)/CASE WHEN family($1) = 4 THEN 1 ELSE 0 END/igs;
	$str =~ s/\bISNULL\(\s*([^\(\)]+)\s*\)/$1 IS NULL/igs;
	$str =~ s/\bRLIKE/REGEXP/igs;
	$str =~ s/\bSTD\(/STDDEV_POP\(/igs;
	$str =~ s/\bSTDDEV\(/STDDEV_POP\(/igs;
	$str =~ s/\bUUID\(/uuid_generate_v1\(/igs;
	$str =~ s/\bNOT REGEXP BINARY/\!\~/igs;
	$str =~ s/\bREGEXP BINARY/\~/igs;
	$str =~ s/\bNOT REGEXP/\!\~\*/igs;
	$str =~ s/\bREGEXP/\~\*/igs;

	$str =~ s/\bGET_LOCK/pg_advisory_lock/igs;
	$str =~ s/\bIS_USED_LOCK/pg_try_advisory_lock/igs;
	$str =~ s/\bRELEASE_LOCK/pg_advisory_unlock/igs;

	# GROUP_CONCAT doesn't exist, it must be replaced by calls to array_to_string() and array_agg() functions
	$str =~ s/GROUP_CONCAT\((.*?)\s+ORDER\s+BY\s+([^\s]+)\s+(ASC|DESC)\s+SEPARATOR\s+'([^']+)'\s*\)/array_to_string(array_agg($1 ORDER BY $2 $3), '$4')/igs;
	$str =~ s/GROUP_CONCAT\((.*?)\s+ORDER\s+BY\s+([^\s]+)\s+SEPARATOR\s+'([^']+)'\s*\)/array_to_string(array_agg($1 ORDER BY $2 ASC), '$3')/igs;
	$str =~ s/GROUP_CONCAT\((.*?)\s+SEPARATOR\s+'([^']+)'\s*\)/array_to_string(array_agg($1), '$2')/igs;

	# Replace IF() function in a query
	$str =~ s/\bIF\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\)]+\s*)\)/(CASE WHEN $1 THEN $2 ELSE $3 END)/igs;
	$str =~ s/\bIFNULL\(\s*([^,]+)\s*,\s*([^\)]+\s*)\)/COALESCE($1, $2)/igs;

	# Rewrite while loop
	$str =~ s/\bWHILE\s+(.*?)END WHILE\s*;/WHILE $1END LOOP;/igs;
	$str =~ s/\bWHILE\s+(.*?)DO\b/WHILE $1LOOP/igs;

	# Rewrite REPEAT loop
	my %repl_repeat = ();
	$i = 0;
	while ($str =~ s/\bREPEAT\s+(.*?)END REPEAT\s*;/%REPREPEATLBL$i%/igs) {
		my $code = $1;
		$code =~ s/\bUNTIL(.*)//;
		$repl_repeat{$i} = "LOOP ${code}EXIT WHEN $1;\nEND LOOP;";
	}
	foreach $i (keys %repl_repeat) {
		$str =~ s/\%REPREPEATLBL$i\%/$repl_repeat{$i}/gs;
	}
	%repl_repeat = ();

	# Replace spatial related lines
	$str = replace_mysql_spatial($str);

	return $str;
}

sub replace_mysql_spatial
{
	my $str = shift;

	$str =~ s/AsWKB\(/AsBinary\(/igs;
	$str =~ s/AsWKT\(/AsText\(/igs;
	$str =~ s/GeometryCollectionFromText\(/GeomCollFromText\(/igs;
	$str =~ s/GeometryCollectionFromWKB\(/GeomCollFromWKB\(/igs;
	$str =~ s/GeometryFromText\(/GeomFromText\(/igs;
	$str =~ s/GLength\(/ST_Length\(/igs;
	$str =~ s/LineStringFromWKB\(/LineFromWKB\(/igs;
	$str =~ s/MultiLineStringFromText\(/MLineFromText\(/igs;
	$str =~ s/MultiPointFromText\(/MPointFromText\(/igs;
	$str =~ s/MultiPolygonFromText\(/MPolyFromText\(/igs;
	$str =~ s/PolyFromText\(/PolygonFromText\(/igs;
	$str =~ s/MBRContains\(/ST_Contains\(/igs;
	$str =~ s/MBRDisjoint\(/ST_Disjoint\(/igs;
	$str =~ s/MBREqual\(/ST_Equals\(/igs;
	$str =~ s/MBRIntersects\(/ST_Intersects\(/igs;
	$str =~ s/MBROverlaps\(/ST_Overlaps\(/igs;
	$str =~ s/MBRTouches\(/ST_Touches\(/igs;
	$str =~ s/MBRWithin\(/ST_Within\(/igs;
	$str =~ s/MLineFromWKB\(/MultiLineStringFromWKB\(/igs;
	$str =~ s/MPointFromWKB\(/MultiPointFromWKB\(/igs;
	$str =~ s/MPolyFromWKB\(/MultiPolygonFromWKB\(/igs;
	$str =~ s/PolyFromWKB\(/PolygonFromWKB\(/igs;

	# Replace FromWKB functions
	foreach my $fct ('MultiLineStringFromWKB', 'MultiPointFromWKB', 'MultiPolygonFromWKB', 'PolygonFromWKB') {
		$str =~ s/\b$fct\(/ST_GeomFromWKB\(/igs;
	}

	# Add ST_ prefix to function alias
	foreach my $fct (@MYSQL_SPATIAL_FCT) {
		$str =~ s/\b$fct\(/ST_$fct\(/igs;
	}

	return $str;
}

sub _mysql_getformat_to_pgsql
{
	my ($type, $format) = @_;

	if (uc($type) eq 'DATE') {
		if (uc($format) eq "'USA'") {
			$format = "'%m.%d.%Y'";
		} elsif (uc($format) eq "'EUR'") {
			$format = "'%d.%m.%Y'";
		} elsif (uc($format) eq "'INTERNAL'") {
			$format = "'%Y%m%d'";
		} else {
			# ISO and JIS
			$format = "'%Y-%m-%d'";
		} 
	} elsif (uc($type) eq 'TIME') {
		if (uc($format) eq "'USA'") {
			$format = "'%h:%i:%s %p'";
		} elsif (uc($format) eq "'EUR'") {
			$format = "'%H.%i.%s'";
		} elsif (uc($format) eq "'INTERNAL'") {
			$format = "'%H%i%s'";
		} else {
			# ISO and JIS
			$format = "'%H:%i:%s'";
		}
	} else {
		if ( (uc($format) eq "'USA'") || (uc($format) eq "'EUR'") ) {
			$format = "'%Y-%m-%d %H.%i.%s'";
		} elsif (uc($format) eq "'INTERNAL'") {
			$format = "'%Y%m%d%H%i%s'";
		} else {
			# ISO and JIS
			$format = "'%Y-%m-%d %H:%i:%s'";
		}
	}

	return $format;
}

sub _mysql_strtodate_to_pgsql
{
	my ($datetime, $format) = @_;

	my $str = _mysql_dateformat_to_pgsql($datetime, $format, 1);

	return $str;
}

sub _mysql_timeformat_to_pgsql
{
	my ($datetime, $format) = @_;

	my $str = _mysql_dateformat_to_pgsql($datetime, $format, 0, 1);

	return $str;
}


sub _mysql_dateformat_to_pgsql
{
	my ($datetime, $format, $todate, $totime) = @_;

# Not supported:
# %X	Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V

	$format =~ s/\%a/Dy/g;
	$format =~ s/\%b/Mon/g;
	$format =~ s/\%c/FMMM/g;
	$format =~ s/\%d/DD/g;
	$format =~ s/\%D/FMDDth/g;
	$format =~ s/\%e/FMDD/g;
	$format =~ s/\%f/US/g;
	$format =~ s/\%H/HH24/g;
	$format =~ s/\%h/HH12/g;
	$format =~ s/\%I/HH/g;
	$format =~ s/\%i/MI/g;
	$format =~ s/\%j/DDD/g;
	$format =~ s/\%k/FMHH24/g;
	$format =~ s/\%l/FMHH12/g;
	$format =~ s/\%m/MM/g;
	$format =~ s/\%p/AM/g;
	$format =~ s/\%r/HH12:MI:SS AM/g;
	$format =~ s/\%s/SS/g;
	$format =~ s/\%S/SS/g;
	$format =~ s/\%T/HH24:MI:SS/g;
	$format =~ s/\%U/WW/g;
	$format =~ s/\%u/IW/g;
	$format =~ s/\%V/WW/g;
	$format =~ s/\%v/IW/g;
	$format =~ s/\%x/YYYY/g;
	$format =~ s/\%X/YYYY/g;
	$format =~ s/\%Y/YYYY/g;
	$format =~ s/\%y/YY/g;
	$format =~ s/\%W/Day/g;
	$format =~ s/\%M/Month/g;

	if ($todate) {
		return "to_date($datetime, $format)";
	} elsif ($totime) {
		return "to_char(($datetime)::time, $format)";
	}

	return "to_char(($datetime)::timestamp, $format)";
}

sub mysql_estimate_cost
{
	my $str = shift;
	my $type = shift;

	my %cost_details = ();

	# Default cost is testing that mean it at least must be tested
	my $cost = $FCT_TEST_SCORE;
	# When evaluating queries tests must not be included here
	if ($type eq 'QUERY') {
		$cost = 0;
	}
	$cost_details{'TEST'} = $cost;

	# Set cost following code length
	my $cost_size = int(length($str)/$SIZE_SCORE) || 1;
	# When evaluating queries size must not be included here
	if ($type eq 'QUERY') {
		$cost_size = 0;
	}

	$cost += $cost_size;
	$cost_details{'SIZE'} = $cost_size;

	# Try to figure out the manual work
	my $n = () = $str =~ m/(ARRAY_AGG|GROUP_CONCAT)\(\s*DISTINCT/igs;
	$cost_details{'ARRAY_AGG_DISTINCT'} += $n;
	$n = () = $str =~ m/\bSOUNDS\s+LIKE\b/igs;
	$cost_details{'SOUNDS LIKE'} += $n;
	$n = () = $str =~ m/CHARACTER\s+SET/igs;
	$cost_details{'CHARACTER SET'} += $n;
	$n = () = $str =~ m/\bCOUNT\(\s*DISTINCT\b/igs;
	$cost_details{'COUNT(DISTINCT)'} += $n;
	$n = () = $str =~ m/\bMATCH.*AGAINST\b/igs;
	$cost_details{'MATCH'} += $n;
	$n = () = $str =~ m/\bJSON_[A-Z\_]+\(/igs;
	$cost_details{'JSON FUNCTION'} += $n;
	$n = () = $str =~ m/_(un)?lock\(/igs;
	$cost_details{'LOCK'} += $n;
	$n = () = $str =~ m/\b\@+[A-Z0-9\_]+/igs;
	$cost_details{'@VAR'} += $n;

	foreach my $t (keys %UNCOVERED_MYSQL_SCORE) {
		$cost += $UNCOVERED_MYSQL_SCORE{$t}*$cost_details{$t};
	}
	foreach my $f (@MYSQL_FUNCTIONS) {
		if ($str =~ /\b$f\b/igs) {
			$cost += 2;
			$cost_details{$f} += 2;
		}
	}

	return $cost, %cost_details;
}

1;

__END__


=head1 AUTHOR

Gilles Darold <gilles@darold.net>


=head1 COPYRIGHT

Copyright (c) 2000-2016 Gilles Darold - All rights reserved.

This program is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.


=head1 BUGS

This perl module is in the same state as my knowledge regarding database,
it can move and not be compatible with older version so I will do my best
to give you official support for Ora2Pg. Your volontee to help construct
it and your contribution are welcome.


=head1 SEE ALSO

L<Ora2Pg>

=cut

