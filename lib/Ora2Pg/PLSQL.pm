package Ora2Pg::PLSQL;
#------------------------------------------------------------------------------
# Project  : Oracle to PostgreSQL database schema converter
# Name     : Ora2Pg/PLSQL.pm
# Language : Perl
# Authors  : Gilles Darold, gilles _AT_ darold _DOT_ net
# Copyright: Copyright (c) 2000-2022 : Gilles Darold - All rights reserved -
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

use vars qw($VERSION %OBJECT_SCORE $SIZE_SCORE $FCT_TEST_SCORE $QUERY_TEST_SCORE %UNCOVERED_SCORE %UNCOVERED_MYSQL_SCORE @ORA_FUNCTIONS @MYSQL_SPATIAL_FCT @MYSQL_FUNCTIONS %EXCEPTION_MAP %MAX_SCORE);
use POSIX qw(locale_h);

#set locale to LC_NUMERIC C
setlocale(LC_NUMERIC,"C");


$VERSION = '23.2';

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
	'GLOBAL TEMPORARY TABLE' => 10, # supported, but not permanent in PostgreSQL
	'DIMENSION' => 0, # Not supported and no equivalent
	'JOB' => 2, # read/adapt
	'SYNONYM' => 0.1, # read/adapt
	'QUERY' => 0.2, # read/adapt
	'ENCRYPTED COLUMN' => 20, ## adapt using pg_crypto
);

# Max score to applicate per type of object
%MAX_SCORE = (
	'INDEX' => 288, # 3 man days
	'SEQUENCE' => 288, # 3 man days
	'TABLE' => 672, # 7 man days
	'TABLE PARTITION' => 480, # 5 man days
	'TABLE SUBPARTITION' => 480, # 5 man days
	'GLOBAL TEMPORARY TABLE' => 288, # 3 man days
	'SYNONYM' => 192, # 2 man days
);

# Scores following the number of characters: 1000 chars for one unit.
# Note: his correspond to the global read time not to the difficulty.
$SIZE_SCORE = 1000;

# Cost to apply on each function or query for testing
$FCT_TEST_SCORE = 2;
$QUERY_TEST_SCORE = 0.1;

# Scores associated to each code difficulties.
%UNCOVERED_SCORE = (
	'TRUNC' => 0.1,
	'IS TABLE OF' => 4,
	'OUTER JOIN' => 2,
	'CONNECT BY' => 3,
	'BULK COLLECT' => 5,
	'GOTO' => 2,
	'FORALL' => 1,
	'ROWNUM' => 1,
	'NOTFOUND' => 0,
	'ISOPEN' => 1,
	'ROWCOUNT' => 1,
	'ROWID' => 2,
	'UROWID' => 2,
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
	'REGEXP_COUNT' => 0.2,
	'REGEXP_INSTR' => 1,
	'REGEXP_SUBSTR' => 1,
	'TG_OP' => 0,
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
	'LAST_DAY' => 1,
	'NEXT_DAY' => 1,
	'MONTHS_BETWEEN' => 1,
	'SDO_' => 3,
	'PRAGMA' => 3,
	'MDSYS' => 1,
	'MERGE INTO' => 3,
	'COMMIT' => 1,
	'CONTAINS' => 1,
	'SCORE' => 1,
	'FUZZY' => 1,
	'NEAR' => 1,
	'TO_CHAR' => 0.1,
	'TO_NCHAR' => 0.1,
	'ANYDATA' => 2,
	'CONCAT' => 0.1,
	'TIMEZONE' => 1,
	'JSON' => 3,
	'TO_CLOB' => 0.1,
	'XMLTYPE' => 3,
	'CREATENONSCHEMABASEDXML' => 3,
	'CREATESCHEMABASEDXML' => 3,
	'CREATEXML' => 3,
	'EXISTSNODE' => 3,
	'EXTRACT' => 3,
	'GETNAMESPACE' => 3,
	'GETROOTELEMENT' => 3,
	'GETSCHEMAURL' => 3,
	'ISFRAGMENT' => 3,
	'ISSCHEMABASED' => 3,
	'ISSCHEMAVALID' => 3,
	'ISSCHEMAVALIDATED' => 3,
	'SCHEMAVALIDATE' => 3,
	'SETSCHEMAVALIDATED' => 3,
	'TOOBJECT' => 3,
	'TRANSFORM' => 3,
	'FND_CONC_GLOBAL' => 3,
	'FND_CONCURRENT' => 3,
	'FND_FILE' => 1,
	'FND_PROGRAM' => 3,
	'FND_SET' => 3,
	'FND_REQUEST' => 3,
	'FND_REQUEST_INFO' => 3,
	'FND_SUBMIT' => 3,
	'FND_GLOBAL' => 1,
	'FND_PROFILE' => 1,
	'FND_CURRENCY' => 3,
	'FND_ORG' => 3,
	'FND_STANDARD' => 3,
	'FND_UTILITIES' => 3,
);

@ORA_FUNCTIONS = qw(
	AsciiStr
	Compose
	Decompose
	Dump
	VSize
	Bin_To_Num
	CharToRowid
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
	New_Time
	SessionTimeZone
	Tz_Offset
	Get_Env
	From_Tz
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

%EXCEPTION_MAP = (
	'INVALID_CURSOR' => 'invalid_cursor_state',
	'ZERO_DIVIDE' => 'division_by_zero',
	'STORAGE_ERROR' => 'out_of_memory',
	'INTEGRITY_ERROR' => 'integrity_constraint_violation',
	'VALUE_ERROR' => 'data_exception',
	'INVALID_NUMBER' => 'data_exception',
	'INVALID_CURSOR' => 'invalid_cursor_state',
	'NO_DATA_FOUND' => 'no_data_found',
	'LOGIN_DENIED' => 'connection_exception',
	'TOO_MANY_ROWS'=> 'too_many_rows',
	# 'PROGRAM_ERROR' => 'INTERNAL ERROR',
	# 'ROWTYPE_MISMATCH' => 'DATATYPE MISMATCH'
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

=head2 convert_plsql_code

Main function used to convert Oracle SQL and PL/SQL code into PostgreSQL
compatible code

=cut

sub convert_plsql_code
{
        my ($class, $str, @strings) = @_;

	return if ($str eq '');

	# Remove the SYS schema from calls
	$str =~ s/\bSYS\.//igs;

	# Replace outer join sign (+) with a placeholder
	$class->{outerjoin_idx} //= 0;
	while ( $str =~ s/\(\+\)/\%OUTERJOIN$class->{outerjoin_idx}\%/s ) {
		$class->{outerjoin_idx}++;
	}

	# Do some initialization of variables
	%{$class->{single_fct_call}} = ();
	$class->{replace_out_params} = '';

	if (uc($class->{type}) ne 'SHOW_REPORT')
	{
		# Rewrite all decode() call before
		$str = replace_decode($str);

		# Rewrite numeric operation with ADD_MONTHS(date, 1) to use interval
		$str =~ s/\b(ADD_MONTHS\s*\([^,]+,\s*\d+\s*\))\s*([+\-\*\/])\s*(\d+)(\s*[^\-\*\/]|$)/$1 $2 '$3 days'::interval$4/sgi;
		# Rewrite numeric operation with TRUNC() to use interval
		$str =~ s/\b(TRUNC\s*\(\s*(?:[^\(\)]+)\s*\))\s*([+\-\*\/])\s*(\d+)(\s*[^+\-\*\/]|$)/$1 $2 '$3 days'::interval$4/sgi;
		# Rewrite numeric operation with LAST_DAY() to use interval
		$str =~ s/\b(LAST_DAY\s*\(\s*(?:[^\(\)]+)\s*\))\s*([+\-\*\/])\s*(\d+)(\s*[^+\-\*\/]|$)/$1 $2 '$3 days'::interval$4/sgi;
	}

	# Replace array syntax arr(i).x into arr[i].x
	$str =~ s/\b([a-z0-9_]+)\(([^\(\)]+)\)(\.[a-z0-9_]+)/$1\[$2\]$3/igs;

	# Extract all block from the code by splitting it on the semi-comma
	# character and replace all necessary function call
	my @code_parts = split(/;/, $str);
	for (my $i = 0; $i <= $#code_parts; $i++)
	{
		next if (!$code_parts[$i]);

		# For mysql also replace if() statements in queries or views.
		if ($class->{is_mysql} && grep(/^$class->{type}$/i, 'VIEW', 'QUERY', 'FUNCTION', 'PROCEDURE')) {
			$code_parts[$i] = Ora2Pg::MySQL::replace_if($code_parts[$i]);
		}

		# Remove parenthesis from function parameters when they not belong to a function call
		my %subparams = ();
		my $p = 0;
		while ($code_parts[$i] =~ s/(\(\s*)(\([^\(\)]*\))(\s*,)/$1\%SUBPARAMS$p\%$3/is)
		{
			$subparams{$p} = $2;
			$p++;
		}
		while ($code_parts[$i] =~ s/(,\s*)(\([^\(\)]*\))(\s*[\),])/$1\%SUBPARAMS$p\%$3/is)
		{
			$subparams{$p} = $2;
			$p++;
		}

		# Remove some noisy parenthesis for outer join replacement
		if ($code_parts[$i] =~ /\%OUTERJOIN\d+\%/)
		{
			my %tmp_ph = ();
			my $idx = 0;
			while ($code_parts[$i] =~ s/\(([^\(\)]*\%OUTERJOIN\d+\%[^\(\)]*)\)/\%SUBPART$idx\%/s)
			{
				$tmp_ph{$idx} = $1;
				$idx++;
			}
			foreach my $k (keys %tmp_ph)
			{
				if ($tmp_ph{$k} =~ /^\s*[^\s]+\s*(=|NOT LIKE|LIKE)\s*[^\s]+\s*$/i) {
					$code_parts[$i] =~ s/\%SUBPART$k\%/$tmp_ph{$k}/s;
				} else {
					$code_parts[$i] =~ s/\%SUBPART$k\%/\($tmp_ph{$k}\)/s;
				}
			}
		}

		%{$class->{single_fct_call}} = ();
		$code_parts[$i] = extract_function_code($class, $code_parts[$i], 0);

		# Things that must ne done when functions are replaced with placeholder
		$code_parts[$i] = replace_without_function($class, $code_parts[$i]);

		foreach my $k (keys %{$class->{single_fct_call}})
		{
			$class->{single_fct_call}{$k} = replace_oracle_function($class, $class->{single_fct_call}{$k}, @strings);
			if ($class->{single_fct_call}{$k} =~ /^CAST\s*\(/i)
			{
				if (!$class->{is_mysql})
				{
					$class->{single_fct_call}{$k} = Ora2Pg::PLSQL::replace_sql_type($class->{single_fct_call}{$k}, $class->{pg_numeric_type}, $class->{default_numeric}, $class->{pg_integer_type}, $class->{varchar_to_text}, %{$class->{data_type}});
				} else {
					$class->{single_fct_call}{$k} = Ora2Pg::MySQL::replace_sql_type($class->{single_fct_call}{$k}, $class->{pg_numeric_type}, $class->{default_numeric}, $class->{pg_integer_type}, $class->{varchar_to_text}, %{$class->{data_type}});
				}
			}
			if ($class->{single_fct_call}{$k} =~ /^CAST\s*\(.*\%\%REPLACEFCT(\d+)\%\%/i)
			{
				if (!$class->{is_mysql}) {
					$class->{single_fct_call}{$1} = Ora2Pg::PLSQL::replace_sql_type($class->{single_fct_call}{$1}, $class->{pg_numeric_type}, $class->{default_numeric}, $class->{pg_integer_type}, $class->{varchar_to_text}, %{$class->{data_type}});
				} else {
					$class->{single_fct_call}{$1} = Ora2Pg::MySQL::replace_sql_type($class->{single_fct_call}{$1}, $class->{pg_numeric_type}, $class->{default_numeric}, $class->{pg_integer_type}, $class->{varchar_to_text}, %{$class->{data_type}});
				}
			}
		}
		while ($code_parts[$i] =~ s/\%\%REPLACEFCT(\d+)\%\%/$class->{single_fct_call}{$1}/) {};
		$code_parts[$i] =~ s/\%SUBPARAMS(\d+)\%/$subparams{$1}/igs;

		# Remove potential double affectation for function with out parameter
		$code_parts[$i] =~ s/(\s*)[^\s=;]+\s*:=\s*(?:\%ORA2PG_COMMENT\d+\%)?(\s*[^\s;=]+\s*:=)/$1$2/gs;
		$code_parts[$i] =~ s/(\s*)[^\s=;]+\s*:=\s*(SELECT\s+[^;]+INTO\s*)/$1$2/igs;
	}
	$str = join(';', @code_parts);

	if ($class->{replace_out_params})
	{
		if ($str !~ s/\b(DECLARE(?:\s+|\%ORA2PG_COMMENT\d+\%))/$1$class->{replace_out_params}\n/is) {
			$str =~ s/\b(BEGIN(?:\s+|\%ORA2PG_COMMENT\d+\%))/DECLARE\n$class->{replace_out_params}\n$1/is;
		}
		$class->{replace_out_params} = '';
	}

	# Apply code rewrite on other part of the code
	$str = plsql_to_plpgsql($class, $str, @strings);

	if ($class->{get_diagnostics})
	{
		if ($str !~ s/\b(DECLARE\s+)/$1$class->{get_diagnostics}\n/is) {
			$str =~ s/\b(BEGIN\s+)/DECLARE\n$class->{get_diagnostics}\n$1/is;
		}
		$class->{get_diagnostics} = '';
	}

	return $str;
}

sub clear_parenthesis
{
	my $str = shift;

	# Keep parenthesys with sub queries
	if ($str =~ /\bSELECT\b/i) {
		$str = '((' . $str . '))';
	} else {
		$str =~ s/^\s+//s;
		$str =~ s/\s+$//s;
		$str = '(' . $str . ')';
	}

	return $str;
}

=head2 extract_function_code

Recursive function used to extract call to function in Oracle SQL
and PL/SQL code

=cut

sub extract_function_code
{
        my ($class, $code, $idx) = @_;

	# Remove some extra parenthesis for better parsing
        $code =~ s/\(\s*\(([^\(\)]*)\)\s*\)/clear_parenthesis($1)/iges;

        # Look for a function call that do not have an other function
        # call inside, replace content with a marker and store the
        # replaced string into a hask to rewritten later to convert pl/sql
        if ($code =~ s/\b([a-zA-Z0-9\.\_]+)\s*\(([^\(\)]*)\)/\%\%REPLACEFCT$idx\%\%/s) {
		my $fct_name = $1;
		my $fct_code = $2;
		my $space = '';
		$space = ' ' if (grep (/^$fct_name$/i, 'FROM', 'AS', 'VALUES', 'DEFAULT', 'OR', 'AND', 'IN', 'SELECT', 'OVER', 'WHERE', 'THEN', 'IF', 'ELSIF', 'ELSE', 'EXISTS', 'ON'));

		# Move up any outer join inside a function otherwise it will not be detected
		my $outerjoin = '';
		if ($fct_code =~ /\%OUTERJOIN(\d+)\%/s) {
			my $idx_join = $1;
			# only if the placeholder content is a function not a predicate
			if ($fct_code !~ /(=|>|<|LIKE|NULL|BETWEEN)/i) {
				$fct_code =~ s/\%OUTERJOIN$idx_join\%//s;
				$outerjoin = "\%OUTERJOIN$idx_join\%";
			}
		}
                # recursively replace function
                $class->{single_fct_call}{$idx} = $fct_name . $space . '(' . $fct_code . ')' . $outerjoin;
                $code = extract_function_code($class, $code, ++$idx);
        }

        return $code;
}

sub append_alias_clause
{
	my $str = shift;

	# Divise code through UNION keyword marking a new query level
	my @q = split(/\b(UNION\s+ALL|UNION|EXCEPT)\b/i, $str);
	for (my $j = 0; $j <= $#q; $j+=2)
	{
		if ($q[$j] =~ s/\b(FROM\s+)(.*\%SUBQUERY.*?)(\s*)(WHERE|ORDER\s+BY|GROUP\s+BY|LIMIT|$)/$1\%FROM_CLAUSE\%$3$4/is)
		{
			my $from_clause = $2;
			if ($from_clause !~ /TABLE\%SUBQUERY\d+\%/ && $q[$j] !~ /\b(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|TIMEZONE_HOUR|TIMEZONE_MINUTE|TIMEZONE_ABBR|TIMEZONE_REGION|TIMEZONE_OFFSET)\s+FROM/is)
			{
				my @parts = split(/\b(WHERE|ORDER\s+BY|GROUP\s+BY|LIMIT)\b/i, $from_clause);
				$parts[0] =~ s/(?<!USING|[\s,]ONLY|[\s,]JOIN|..\sON|.\sAND|..\sOR)\s*\%SUBQUERY(\d+)\%(\s*,)/\%SUBQUERY$1\% alias$1$2/igs;
				$parts[0] =~ s/(?<!USING|[\s,]ONLY|[\s,]JOIN|.\sON\s|\sAND\s|.\sOR\s)\s*\%SUBQUERY(\d+)\%(\s*)$/\%SUBQUERY$1\% alias$1$2/is;
				# Remove unwanted alias appended with the REGEXP_SUBSTR translation
				$parts[0] =~ s/(\%SUBQUERY\d+\%\s+AS\s+[^\s]+)\s+alias\d+/$1/ig;
				# Remove unwanted alias appended with JOIN
				$parts[0] =~ s/\bON\s*(\%SUBQUERY\d+\%)\s+alias\d+/ON $1/ig;
				# Remove unwanted alias appended with function with subquery translation
				$parts[0] =~ s/\b([^\s,]+\%SUBQUERY\d+\%) alias\d+/$1/ig;
				$from_clause = join('', @parts);
			}
			$q[$j] =~ s/\%FROM_CLAUSE\%/$from_clause/s;
		}
	}
	$str = join('', @q);

	return $str;
}

sub remove_fct_name
{
	my $str = shift;

	if ($str !~ /(END\b\s*)(IF\b|LOOP\b|CASE\b|INTO\b|FROM\b|END\b|ELSE\b|AND\b|OR\b|WHEN\b|AS\b|,|\)|\(|\||[<>=]|NOT LIKE|LIKE|WHERE|GROUP|ORDER)/is) {
		$str =~ s/(END\b\s*)[\w"\.]+\s*(?:;|$)/$1;/is;
	}

	return $str;
}

=head2 set_error_code

Transform custom exception code by replacing the leading -20 by 45

=cut

sub set_error_code
{
	my $code = shift;

	my $orig_code = $code;

	$code =~ s/-20(\d{3})/'45$1'/;
	if ($code =~ s/-20(\d{2})/'450$1'/ || $code =~ s/-20(\d{1})/'4500$1'/) {
		print STDERR "WARNING: exception code has less than 5 digit, proceeding to automatic adjustement.\n";
		$code .= " /* code was: $orig_code */";
	}

	return $code;
}

# Fix case where the raise_application_error() parameters are named by removing them
sub remove_named_parameters
{
	my $str = shift;

	$str =~ s/\w+\s*=>\s*//g;

	return $str;
}

sub set_interval_value
{
	my $num = shift();

	if ($num !~ /\./) {
		return "'$num days'";
	} else {
		return "'" . int($num*86400) . " seconds'";
	}
}

=head2 plsql_to_plpgsql

This function return a PLSQL code translated to PLPGSQL code

=cut

sub plsql_to_plpgsql
{
        my ($class, $str, @strings) = @_;

	return if ($str eq '');

	return mysql_to_plpgsql($class, $str, @strings) if ($class->{is_mysql});

	my $field = '\s*([^\(\),]+)\s*';
	my $num_field = '\s*([\d\.]+)\s*';

	my $conv_current_time = 'clock_timestamp()';
	if (!grep(/$class->{type}/i, 'FUNCTION', 'PROCEDURE', 'PACKAGE')) {
		$conv_current_time = 'statement_timestamp()';
	}
	# Remove the SYS schema from calls
	$str =~ s/\bSYS\.//igs;

	# Replace sysdate +/- N by localtimestamp - N day interval
	$str =~ s/\bSYSDATE\s*(\+|\-)\s*([\d\.]+)/"$conv_current_time $1 interval " . set_interval_value($2)/iges;
	# Replace special case : (sysdate - to_date('01-Jan-1970', 'dd-Mon-yyyy'))*24*60*60
	# with: (extract(epoch from now())
	# When translating from code
	while ($str =~ /\bSYSDATE\s*\-\s*to_date\(\s*\?TEXTVALUE(\d+)\?\s*,\s*\?TEXTVALUE(\d+)\?\s*\)\s*\)(?:\s*\*\s*(?:24|60)){3}/is)
	{
		my $t1 = $1;
		my $t2 = $2;
		if ($class->{text_values}{$t1} =~ /^'(Jan|01|1970|\.|\-)+'$/ && $class->{text_values}{$t2} =~ /'(Mon|mm|dd|yyyy|\.|\-)+'/i)
		{
			$str =~ s/\bSYSDATE\s*\-\s*to_date\(\s*\?TEXTVALUE(\d+)\?\s*,\s*\?TEXTVALUE(\d+)\?\s*\)\s*\)(?:\s*\*\s*(?:24|60)){3}/extract(epoch from now()))/is;
		}
	}

	# When translating from default value (sysdate - to_date('01-01-1970','dd-MM-yyyy'))*24*60*60
	$str =~ s/\bSYSDATE\s*\-\s*to_date\(\s*'(Jan|01|1970|\.|\-)+'\s*,\s*'(Mon|mm|dd|yyyy|\.|\-)+'\s*\)\s*\)(\s*\*\s*(24|60)){3}/extract(epoch from now()))/igs;

	# Change SYSDATE to 'now' or current timestamp.
	$str =~ s/\bSYSDATE\s*\(\s*\)/$conv_current_time/igs;
	$str =~ s/\bSYSDATE\b/$conv_current_time/igs;
	# Cast call to to_date with localtimestamp 
	$str =~ s/(TO_DATE\($conv_current_time)\s*,/$1::text,/igs;

	# JSON validation mostly in CHECK contraints
	$str =~ s/((?:\w+\.)?\w+)\s+IS\s+JSON\b/\(CASE WHEN $1::json IS NULL THEN true ELSE true END\)/igs;

	# Drop temporary doesn't exist in PostgreSQL
	$str =~ s/DROP\s+TEMPORARY/DROP/igs;

	# Private temporary table doesn't exist in PostgreSQL
	$str =~ s/PRIVATE\s+TEMPORARY/TEMPORARY/igs;
	$str =~ s/ON\s+COMMIT\s+PRESERVE\s+DEFINITION/ON COMMIT PRESERVE ROWS/igs;
	$str =~ s/ON\s+COMMIT\s+DROP\s+DEFINITION/ON COMMIT DROP/igs;

	# Replace SYSTIMESTAMP 
	$str =~ s/\bSYSTIMESTAMP\b/CURRENT_TIMESTAMP/igs;
	# remove FROM DUAL
	$str =~ s/FROM\s+DUAL//igs;
	$str =~ s/FROM\s+SYS\.DUAL//igs;

	# DISTINCT and UNIQUE are synonym on Oracle
	$str =~ s/SELECT\s+UNIQUE\s+([^,])/SELECT DISTINCT $1/igs;

	# Remove space between operators
	$str =~ s/=\s+>/=>/gs;
	$str =~ s/<\s+=/<=/gs;
	$str =~ s/>\s+=/>=/gs;
	$str =~ s/!\s+=/!=/gs;
	$str =~ s/<\s+>/<>/gs;
	$str =~ s/:\s+=/:=/gs;
	$str =~ s/\|\s+\|/\|\|/gs;
	$str =~ s/!=([+\-])/!= $1/gs;

	# replace operator for named parameters in function calls
	if (!$class->{pg_supports_named_operator}) {
		$str =~ s/([^<])=>/$1:=/gs;
	}

	# Replace listagg() call
	$str =~ s/\bLISTAGG\s*\((.*?)(?:\s*ON OVERFLOW [^\)]+)?\)\s+WITHIN\s+GROUP\s*\((.*?)\)/string_agg($1 $2)/igs;
	# Try to fix call to string_agg with a single argument (allowed in oracle)
	$str =~ s/\bstring_agg\(([^,\(\)]+)\s+(ORDER\s+BY)/string_agg($1, '' $2/igs;

	# There's no such things in PostgreSQL
	$str =~ s/PRAGMA RESTRICT_REFERENCES[^;]+;//igs;
        $str =~ s/PRAGMA SERIALLY_REUSABLE[^;]*;//igs;
        $str =~ s/PRAGMA INLINE[^;]+;//igs;
	
	# Remove the extra TRUNCATE clauses not available in PostgreSQL
	$str =~ s/TRUNCATE\s+TABLE\s+(.*?)\s+(REUSE|DROP)\s+STORAGE/TRUNCATE TABLE $1/igs;
	$str =~ s/TRUNCATE\s+TABLE\s+(.*?)\s+(PRESERVE|PURGE)\s+MATERIALIZED\s+VIEW\s+LOG/TRUNCATE TABLE $1/igs;

	# Converting triggers
	#       :new. -> NEW.
	$str =~ s/:new\./NEW\./igs;
	#       :old. -> OLD.
	$str =~ s/:old\./OLD\./igs;

	# Change NVL to COALESCE
	$str =~ s/NVL\s*\(/coalesce(/isg;
	$str =~ s/NVL2\s*\($field,$field,$field\)/(CASE WHEN $1 IS NOT NULL THEN $2 ELSE $3 END)/isg;

	# NLSSORT to COLLATE
	while ($str =~ /NLSSORT\($field,$field[\)]?/is)
	{
		my $col = $1;
		my $nls_sort = $2;
		if ($nls_sort =~ s/\%\%string(\d+)\%\%/$strings[$1]/s) {
			$nls_sort =~ s/NLS_SORT=([^']+).*/COLLATE "$1"/is;
			$nls_sort =~ s/\%\%ESCAPED_STRING\%\%//ig;
			$str =~ s/NLSSORT\($field,$field[\)]?/$1 $nls_sort/is;
		} elsif ($nls_sort =~ s/\?TEXTVALUE(\d+)\?/$class->{text_values}{$1}/s) {
			$nls_sort =~ s/\s*'NLS_SORT=([^']+).*/COLLATE "$1"/is;
			$nls_sort =~ s/\%\%ESCAPED_STRING\%\%//ig;
			$str =~ s/NLSSORT\($field,$field[\)]?/$1 $nls_sort/is;
		} else {
			$str =~ s/NLSSORT\($field,\s*'NLS_SORT=([^']+)'\s*[\)]?/$1 COLLATE "$2"/is;
		}
	}

	# Replace EXEC function into variable, ex: EXEC :a := test(:r,1,2,3);
	$str =~ s/\bEXEC\s+:([^\s:]+)\s*:=/SELECT INTO $2/igs;

	# Replace simple EXEC function call by SELECT function
	$str =~ s/\bEXEC(\s+)(?:.*?)(?!FROM\b|WHERE\b)/SELECT$1/igs;

	# Remove leading : on Oracle variable taking care of regex character class
	$str =~ s/([^\w:]+):(\d+)/$1\$$2/igs;
	$str =~ s/([^\w:]+):((?!alpha:|alnum:|blank:|cntrl:|digit:|graph:|lower:|print:|punct:|space:|upper:|xdigit:)\w+)/$1$2/igs;

	# INSERTING|DELETING|UPDATING -> TG_OP = 'INSERT'|'DELETE'|'UPDATE'
	$str =~ s/\bINSERTING\b/TG_OP = 'INSERT'/igs;
	$str =~ s/\bDELETING\b/TG_OP = 'DELETE'/igs;
	$str =~ s/\bUPDATING\b/TG_OP = 'UPDATE'/igs;
	# Replace Oracle call to column in trigger event
	$str =~ s/TG_OP = '([^']+)'\s*\(\s*([^\)]+)\s*\)/TG_OP = '$1' AND NEW.$2 IS DISTINCT FROM OLD.$2/igs;

	# EXECUTE IMMEDIATE => EXECUTE
	$str =~ s/EXECUTE IMMEDIATE/EXECUTE/igs;

	# SELECT without INTO should be PERFORM. Exclude select of view when prefixed with AS ot IS
	if ( ($class->{type} ne 'QUERY') && ($class->{type} ne 'VIEW') )
	{
		$str =~ s/(\s+)(?<!AS|IS)(\s+)SELECT((?![^;]+\bINTO\b)[^;]+;)/$1$2PERFORM$3/isg;
		$str =~ s/\bSELECT\b((?![^;]+\bINTO\b)[^;]+;)/PERFORM$1/isg;
		$str =~ s/(AS|IS|FOR|UNION ALL|UNION|MINUS|INTERSECT|\()(\s*)(\%ORA2PG_COMMENT\d+\%)?(\s*)PERFORM/$1$2$3$4SELECT/isg;
		$str =~ s/(INSERT\s+INTO\s+[^;]+\s+)PERFORM/$1SELECT/isg;
		# Restore the SELECT from a CTE
		$str =~ s/(\)\s*)PERFORM\b/$1SELECT/isg;
	}

	# Change nextval on sequence
	# Oracle's sequence grammar is sequence_name.nextval.
	# Postgres's sequence grammar is nextval('sequence_name'). 
	if (!$class->{export_schema})
	{
		if (!$class->{preserve_case})
		{
			$str =~ s/\b(\w+)\.(\w+)\.nextval/nextval('\L$2\E')/isg;
			$str =~ s/\b(\w+)\.(\w+)\.currval/currval('\L$2\E')/isg;
		}
		else
		{
			$str =~ s/\b(\w+)\.(\w+)\.nextval/nextval('"$2"')/isg;
			$str =~ s/\b(\w+)\.(\w+)\.currval/currval('"$2"')/isg;
		}
	}
	else
	{
		my $sch = $class->{pg_schema} || $class->{schema};
		if (!$class->{preserve_case})
		{
			$str =~ s/\b(\w+)\.(\w+)\.nextval/nextval('\L$sch.$2\E')/isg;
			$str =~ s/\b(\w+)\.(\w+)\.currval/currval('\L$sch.$2\E')/isg;
		}
		else
		{
			$str =~ s/\b(\w+)\.(\w+)\.nextval/nextval('"$sch"."$2"')/isg;
			$str =~ s/\b(\w+)\.(\w+)\.currval/currval('"$sch"."$2"')/isg;
		}
	}
	if (!$class->{preserve_case})
	{
		$str =~ s/\b(\w+)\.nextval/nextval('\L$1\E')/isg;
		$str =~ s/\b(\w+)\.currval/currval('\L$1\E')/isg;
	}
	else
	{
		$str =~ s/\b(\w+)\.nextval/nextval('"$1"')/isg;
		$str =~ s/\b(\w+)\.currval/currval('"$1"')/isg;
	}

	# Oracle MINUS can be replaced by EXCEPT as is
	$str =~ s/\bMINUS\b/EXCEPT/igs;
	# Comment DBMS_OUTPUT.ENABLE calls
	if (!$class->{use_orafce}) {
		$str =~ s/(DBMS_OUTPUT.ENABLE[^;]+;)/-- $1/isg;
	}
	# DBMS_LOB.GETLENGTH can be replaced by binary length.
	$str =~ s/DBMS_LOB.GETLENGTH/octet_length/igs;
	# DBMS_LOB.SUBSTR can be replaced by SUBSTR()
	$str =~ s/DBMS_LOB.SUBSTR\s*\($field,$field,$field\)/substr($1, $3, $2)/igs;
	# TO_CLOB(), we just remove it
	$str =~ s/TO_CLOB\s*\(/\(/igs;

	# Raise information to the client
	if (!$class->{use_orafce}) {
		$str =~ s/DBMS_OUTPUT\.(put_line|put|new_line)\s*\((.*?)\)\s*;/&raise_output($class, $2) . ';'/isge;
	}

	# Simply remove this as not supported
	$str =~ s/\bDEFAULT\s+NULL\b//igs;

	# Fix some reserved keyword that could be used in a query
	$str =~ s/(\s+)(month|year)([\s,])/$1"$2"$3/igs;

	# Replace DEFAULT empty_blob() and empty_clob()
	my $empty = "''";
	$empty = 'NULL' if ($class->{empty_lob_null});
	$str =~ s/(empty_blob|empty_clob)\s*\(\s*\)/$empty/is;
	$str =~ s/(empty_blob|empty_clob)\b/$empty/is;

	# dup_val_on_index => unique_violation : already exist exception
	$str =~ s/\bdup_val_on_index\b/unique_violation/igs;

	# Replace raise_application_error by PG standard RAISE EXCEPTION
	$str =~ s/\braise_application_error\s*\(\s*([^,]+)\s*,\s*([^;]+),\s*(true|false)\s*\)\s*;/"RAISE EXCEPTION '%', " . remove_named_parameters($2) . " USING ERRCODE = " . set_error_code(remove_named_parameters($1)) . ";"/iges;
	$str =~ s/\braise_application_error\s*\(\s*([^,]+)\s*,\s*([^;]+)\)\s*;/"RAISE EXCEPTION '%', " . remove_named_parameters($2) . " USING ERRCODE = " . set_error_code(remove_named_parameters($1)) . ";"/iges;
	$str =~ s/DBMS_STANDARD\.RAISE EXCEPTION/RAISE EXCEPTION/igs;

	# Translate cursor declaration
	$str = replace_cursor_def($str);

	# Remove remaining %ROWTYPE in other prototype declaration
	#$str =~ s/\%ROWTYPE//isg;

	# Normalize HAVING ... GROUP BY into GROUP BY ... HAVING clause	
	$str =~ s/\bHAVING\b((?:(?!SELECT|INSERT|UPDATE|DELETE|WHERE|FROM).)*?)\bGROUP BY\b((?:(?!SELECT|INSERT|UPDATE|DELETE|WHERE|FROM).)*?)((?=UNION|ORDER BY|LIMIT|INTO |FOR UPDATE|PROCEDURE|\)\s+(?:AS)*[a-z0-9_]+\s+)|$)/GROUP BY$2 HAVING$1/gis;

	# Add STRICT keyword when select...into and an exception with NO_DATA_FOUND/TOO_MANY_ROW is present
	#$str =~ s/\b(SELECT\b[^;]*?INTO)(.*?)(EXCEPTION.*?(?:NO_DATA_FOUND|TOO_MANY_ROW))/$1 STRICT $2 $3/igs;
	# Add STRICT keyword when SELECT...INTO or EXECUTE ... INTO even if there's not EXCEPTION block
	$str =~ s/\b((?:SELECT|EXECUTE)\s+[^;]*?\s+INTO)(\s+(?!STRICT))/$1 STRICT$2/igs;
	$str =~ s/(INSERT\s+INTO\s+)STRICT\s+/$1/igs;

	# Remove the function name repetion at end
	$str =~ s/\b(END\s*[^;\s]+\s*(?:;|$))/remove_fct_name($1)/iges;

	# Rewrite comment in CASE between WHEN and THEN
	$str =~ s/(\s*)(WHEN\s+[^\s]+\s*)(\%ORA2PG_COMMENT\d+\%)(\s*THEN)/$1$3$1$2$4/igs;

	# Replace SQLCODE by SQLSTATE
	$str =~ s/\bSQLCODE\b/SQLSTATE/igs;

	# Revert order in FOR IN REVERSE
	$str =~ s/\bFOR(.*?)IN\s+REVERSE\s+([^\.\s]+)\s*\.\.\s*([^\s]+)/FOR$1IN REVERSE $3..$2/isg;

	# Comment call to COMMIT or ROLLBACK in the code if allowed
	if ($class->{comment_commit_rollback})
	{
		$str =~ s/\b(COMMIT|ROLLBACK)\s*;/-- $1;/igs;
		$str =~ s/(ROLLBACK\s+TO\s+[^;]+);/-- $1;/igs;
	}

	# Comment call to SAVEPOINT in the code if allowed
	if ($class->{comment_savepoint}) {
		$str =~ s/(SAVEPOINT\s+[^;]+);/-- $1;/igs;
	}

	# Replace exit at end of cursor
	$str =~ s/EXIT\s+WHEN\s+([^\%;]+)\%\s*NOTFOUND\s*;/EXIT WHEN NOT FOUND; \/\* apply on $1 \*\//isg;
	$str =~ s/EXIT\s+WHEN\s+\(\s*([^\%;]+)\%\s*NOTFOUND\s*\)\s*;/EXIT WHEN NOT FOUND;  \/\* apply on $1 \*\//isg;
	# Same but with additional conditions
	$str =~ s/EXIT\s+WHEN\s+([^\%;]+)\%\s*NOTFOUND\s+([^;]+);/EXIT WHEN NOT FOUND $2;  \/\* apply on $1 \*\//isg;
	$str =~ s/EXIT\s+WHEN\s+\(\s*([^\%;]+)\%\s*NOTFOUND\s+([^\)]+)\)\s*;/EXIT WHEN NOT FOUND $2;  \/\* apply on $1 \*\//isg;
	# Replacle call to SQL%NOTFOUND and SQL%FOUND
	$str =~ s/SQL\s*\%\s*NOTFOUND/NOT FOUND/isg;
	$str =~ s/SQL\s*\%\s*FOUND/FOUND/isg;

	# Replace all remaining CURSORNAME%NOTFOUND with NOT FOUND
	$str =~ s/\s+([^\(\%\s]+)\%\s*NOTFOUND\s*/ NOT FOUND /isg;

	# Replace UTL_MATH function by fuzzymatch function
	$str =~ s/UTL_MATCH.EDIT_DISTANCE/levenshtein/igs;

	# Replace UTL_ROW.CAST_TO_RAW function by encode function
	$str =~ s/UTL_RAW.CAST_TO_RAW\s*\(\s*([^\)]+)\s*\)/encode($1::bytea, 'hex')::bytea/igs;

	# Replace known EXCEPTION equivalent ERROR code
	foreach my $e (keys %EXCEPTION_MAP) {
		$str =~ s/\b$e\b/$EXCEPTION_MAP{"\U$e\L"}/igs;
	}

	# Replace special IEEE 754 values for not a number and infinity
	$str =~ s/BINARY_(FLOAT|DOUBLE)_NAN/'NaN'/igs;
	$str =~ s/([\-]*)BINARY_(FLOAT|DOUBLE)_INFINITY/'$1Infinity'/igs;
	$str =~ s/'([\-]*)Inf'/'$1Infinity'/igs;

	# Replace PIPE ROW by RETURN NEXT
	$str =~ s/PIPE\s+ROW\s*/RETURN NEXT /igs;
	$str =~ s/(RETURN NEXT )\(([^\)]+)\)/$1$2/igs;

	#  Convert all x <> NULL or x != NULL clauses to x IS NOT NULL.
	$str =~ s/\s*(<>|\!=)\s*NULL/ IS NOT NULL/igs;
	#  Convert all x = NULL clauses to x IS NULL.
	$str =~ s/(?!:)(.)=\s*NULL/$1 IS NULL/igs;

	# Add missing FROM clause in DELETE statements minus MERGE and FK ON DELETE
	$str =~ s/(\bDELETE\s+)(?!FROM|WHERE|RESTRICT|CASCADE|NO ACTION)\b/$1FROM /igs;

	# Revert changes on update queries for IS NULL transaltion in the target list only
	while ($str =~ s/\b(UPDATE\s+((?!WHERE|;).)*)\s+IS NULL/$1 = NULL/is) {};

	# Rewrite all IF ... IS NULL with coalesce because for Oracle empty and NULL is the same
	if ($class->{null_equal_empty})
	{
		# Form: column IS NULL
		$str =~ s/([a-z0-9_\."]+)\s*IS\s+NULL/coalesce($1::text, '') = ''/igs;
		my $i = 0;
		my %isnull = ();
		while ($str =~ s/([a-z0-9_\."]+)\s*IS\s+NULL/%ORA2PGISNULL$i%/is) {
			$isnull{$i} = "coalesce($1::text, '') = ''";
			$i++;
		}
		my %notnull = ();
		while ($str =~ s/([a-z0-9_\."]+)\s*IS NOT NULL/%ORA2PGNOTNULL$i%/is) {
			$notnull{$i} = "($1 AND $1::text <> '')";
			$i++;
		}
		# Form: fct(expression) IS NULL
		$str =~ s/([a-z0-9_\."]+\s*\([^\)\(]*\))\s*IS\s+NULL/coalesce($1::text, '') = ''/igs;
		$str =~ s/([a-z0-9_\."]+\s*\([^\)\(]*\))\s*IS\s+NOT\s+NULL/($1 IS NOT NULL AND ($1)::text <> '')/igs;
		$str =~ s/%ORA2PGISNULL(\d+)%/$isnull{$1}/gs;
		$str =~ s/%ORA2PGNOTNULL(\d+)%/$notnull{$1}/gs;
	}

	# Replace type in sub block
	if (!$class->{is_mysql}) {
		$str =~ s/(BEGIN.*?DECLARE\s+)(.*?)(\s+BEGIN)/$1 . Ora2Pg::PLSQL::replace_sql_type($2, $class->{pg_numeric_type}, $class->{default_numeric}, $class->{pg_integer_type}, $class->{varchar_to_text}, %{$class->{data_type}}) . $3/iges;
	} else {
		$str =~ s/(BEGIN.*?DECLARE\s+)(.*?)(\s+BEGIN)/$1 . Ora2Pg::MySQL::replace_sql_type($2, $class->{pg_numeric_type}, $class->{default_numeric}, $class->{pg_integer_type}, $class->{varchar_to_text}, %{$class->{data_type}}) . $3/iges;
	}

	# Remove any call to MDSYS schema in the code
	$str =~ s/\bMDSYS\.//igs;

	# Oracle doesn't require parenthesis after VALUES, PostgreSQL has
	# similar proprietary syntax but parenthesis are mandatory
	$str =~ s/(INSERT\s+INTO\s+(?:.*?)\s+VALUES\s+)([^\(\)\s]+)\s*;/$1\($2.*\);/igs;

	# Replace some windows function issues with KEEP (DENSE_RANK FIRST ORDER BY ...)
	$str =~ s/\b(MIN|MAX|SUM|AVG|COUNT|VARIANCE|STDDEV)\s*\(([^\)]+)\)\s+KEEP\s*\(DENSE_RANK\s+(FIRST|LAST)\s+(ORDER\s+BY\s+[^\)]+)\)\s*(OVER\s*\(PARTITION\s+BY\s+[^\)]+)\)/$3_VALUE($2) $5 $4)/igs;

	$class->{sub_queries} = ();
	$class->{sub_queries_idx} = 0;

	####
	# Replace ending ROWNUM with LIMIT or row_number() and replace (+) outer join
	####
	# Catch potential subquery first and replace rownum in subqueries
	my @statements = split(/;/, $str);
	for ( my $i = 0; $i <= $#statements; $i++ )
	{
		# Remove any unecessary parenthesis in code
		$statements[$i] = remove_extra_parenthesis($statements[$i]);

		$class->{sub_parts} = ();
		$class->{sub_parts_idx} = 0;
		extract_subpart($class, \$statements[$i]);

		# Translate all sub parts of the query before applying translation on the main query
		foreach my $z (sort {$a <=> $b } keys %{$class->{sub_parts}})
		{
			if ($class->{sub_parts}{$z} =~ /\S/is)
			{ 
				$class->{sub_parts}{$z} = translate_statement($class, $class->{sub_parts}{$z}, 1);
				if ($class->{sub_parts}{$z} =~ /SELECT/is)
				{
					$class->{sub_parts}{$z} .= $class->{limit_clause};
					$class->{limit_clause} = '';
				}
				# Try to append aliases of subqueries in the from clause
				$class->{sub_parts}{$z} = append_alias_clause($class->{sub_parts}{$z});
			}
			next if ($class->{sub_parts}{$z} =~ /^\(/ || $class->{sub_parts}{$z} =~ /^TABLE[\(\%]/i);
			# If subpart is not empty after transformation
			if ($class->{sub_parts}{$z} =~ /\S/is)
			{
				# add open and closed parenthesis 
				$class->{sub_parts}{$z} = '(' . $class->{sub_parts}{$z} . ')';
			}
			elsif ($statements[$i] !~ /\s+(WHERE|AND|OR)\s*\%SUBQUERY$z\%/is)
			{
				# otherwise do not report the empty parenthesis when this is not a function
				$class->{sub_parts}{$z} = '(' . $class->{sub_parts}{$z} . ')';
			}
		}

		# Try to append aliases of subqueries in the from clause
		$statements[$i] = append_alias_clause($statements[$i]);

		$statements[$i] .= $class->{limit_clause};
		$class->{limit_clause} = '';

		# Apply translation on the full query
		$statements[$i] = translate_statement($class, $statements[$i]);

		$statements[$i] .= $class->{limit_clause};
		$class->{limit_clause} = '';

		# then restore subqueries code into the main query
		while ($statements[$i] =~ s/\%SUBQUERY(\d+)\%/$class->{sub_parts}{$1}/is) {};

		# Remove unnecessary offset to position 0 which is the default
		$statements[$i] =~ s/\s+OFFSET 0//igs;

	}

	map { s/[ ]+([\r\n]+)/$1/s; } @statements;
	map { s/[ ]+$//; } @statements;
	$str = join(';', @statements);

	# Rewrite some garbadged resulting from the transformation
	while ($str =~ s/(\s+AND)\s+AND\b/$1/is) {};
	while ($str =~ s/(\s+OR)\s+OR\b/$1/is) {};
	while ($str =~ s/\s+AND(\s+\%ORA2PG_COMMENT\d+\%\s+)+(AND)\b/$1$2/is) {};
	while ($str =~ s/\s+OR(\s+\%ORA2PG_COMMENT\d+\%\s+)+(OR)\b/$1$2/is) {};
	$str =~ s/\(\s*(AND|OR)\b/\(/igs;
	$str =~ s/(\s+WHERE)\s+(AND|OR)\b/$1/igs;
	$str =~ s/(\s+WHERE)(\s+\%ORA2PG_COMMENT\d+\%\s+)+(AND|OR)\b/$1$2/igs;

	# Attempt to remove some extra parenthesis in simple case only
	$str = remove_extra_parenthesis($str);

	# Remove cast in partition range
	$str =~ s/TIMESTAMP\s*('[^']+')/$1/igs;
	
	# Replace call to SQL%ROWCOUNT
	$str =~ s/([^\s]+)\s*:=\s*SQL\%ROWCOUNT/GET DIAGNOSTICS $1 = ROW_COUNT/igs;
	if ($str =~ s/(IF\s+)SQL\%ROWCOUNT/GET DIAGNOSTICS ora2pg_rowcount = ROW_COUNT;\n$1ora2pg_rowcount/igs) {
		$class->{get_diagnostics} = 'ora2pg_rowcount int;';
	} elsif ($str =~ s/;(\s+)([^;]+)SQL\%ROWCOUNT/;$1GET DIAGNOSTICS ora2pg_rowcount = ROW_COUNT;\n$1$2 ora2pg_rowcount/igs) {
		$class->{get_diagnostics} = 'ora2pg_rowcount int;';
	}
	# SQL%ROWCOUNT with concatenated string
	$str =~ s/(\s+)(GET DIAGNOSTICS )([^\s]+)( = ROW_COUNT)(\s+\|\|[^;]+);/$1$2$3$4;$1$3 := $3 $5;/;

	# Sometime variable used in FOR ... IN SELECT loop is not declared
	# Append its RECORD declaration in the DECLARE section.
	my $tmp_code = $str;
	while ($tmp_code =~ s/\bFOR\s+([^\s]+)\s+IN(.*?)LOOP//is)
	{
		my $varname = $1;
		my $clause = $2;
		my @code = split(/\bBEGIN\b/i, $str);
		if ($code[0] !~ /\bDECLARE\s+.*\b$varname\s+/is)
		{
			# When the cursor is refereing to a statement, declare
			# it as record otherwise it don't need to be replaced
			if ($clause =~ /\bSELECT\b/is)
			{
				# append variable declaration to declare section
				if ($str !~ s/\bDECLARE\b/DECLARE\n  $varname RECORD;/is)
				{
					# No declare section
					$str = "DECLARE\n  $varname RECORD;\n" . $str;
				}
			}
		}
	}

	# Rewrite direct call to function without out parameters using PERFORM
	$str = perform_replacement($class, $str);

	# Restore non converted outer join
	$str =~ s/\%OUTERJOIN\d+\%/\(\+\)/igs;

	return $str;
}

##############
# Rewrite direct call to function without out parameters using PERFORM
##############
sub perform_replacement
{
	my ($class, $str) = @_;

	if (uc($class->{type}) =~ /^(PACKAGE|FUNCTION|PROCEDURE|TRIGGER)$/)
	{
		foreach my $sch ( keys %{ $class->{function_metadata} })
		{
			foreach my $p ( keys %{ $class->{function_metadata}{$sch} })
			{
				foreach my $k (keys %{$class->{function_metadata}{$sch}{$p}})
				{
					my $fct_name = $class->{function_metadata}{$sch}{$p}{$k}{metadata}{fct_name} || '';
					next if (!$fct_name);
					next if ($p ne 'none' && $str !~ /\b$p\.$fct_name\b/is && $str !~ /(^|[^\.])\b$fct_name\b/is);
					next if ($p eq 'none' && $str !~ /\b$fct_name\b/is);
					my $call = 'PERFORM';
					if ($class->{pg_supports_procedure} && uc($class->{function_metadata}{$sch}{$p}{$k}{metadata}{type}) eq 'PROCEDURE') {
						$call = 'CALL';
					}

					if (!$class->{function_metadata}{$sch}{$p}{$k}{metadata}{inout})
					{
						if ($sch ne 'unknown' and $str =~ /\b$sch.$k\b/is)
						{
							# Look if we need to use $call to call the function
							$str =~ s/(BEGIN|LOOP|;)((?:\s*%ORA2PG_COMMENT\d+\%\s*|\s*\/\*(?:.*?)\*\/\s*)*\s*)($sch\.$k\s*[\(;])/$1$2$call $3/igs;
							while ($str =~ s/(EXCEPTION(?:(?!CASE|THEN).)*?THEN)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)($sch\.$k\s*[\(;])/$1$2$call $3/is) {};
							$str =~ s/(IF(?:(?!CASE|THEN).)*?THEN)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)($sch\.$k\s*[\(;])/$1$2$call $3/isg;
							$str =~ s/(IF(?:(?!CASE|ELSE).)*?ELSE)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)($sch\.$k\s*[\(;])/$1$2$call $3/isg;
							$str =~ s/($call $sch\.$k);/$1\(\);/igs;
						}
						elsif ($str =~ /\b($k|$fct_name)\b/is)
						{
							# Look if we need to use $call to call the function
							$str =~ s/(BEGIN|LOOP|CALL|;)((?:\s*%ORA2PG_COMMENT\d+\%\s*|\s*\/\*(?:.*?)\*\/\s*)*\s*)((?:$k|$fct_name)\s*[\(;])/$1$2$call $3/igs;
							while ($str =~ s/(EXCEPTION(?:(?!CASE).)*?THEN)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)((?:$k|$fct_name)\s*[\(;])/$1$2$call $3/is) {};
							$str =~ s/(IF(?:(?!CASE|THEN).)*?THEN)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)((?:$k|$fct_name)\s*[\(;])/$1$2$call $3/isg;
							$str =~ s/(IF(?:(?!CASE|ELSE).)*?ELSE)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)((?:$k|$fct_name)\s*[\(;])/$1$2$call $3/isg;
							$str =~ s/($call (?:$k|$fct_name));/$1\(\);/igs;
						}
					}
					else
					{
						# Recover call to function with OUT parameter with double affectation
						$str =~ s/([^:\s]+\s*:=\s*)[^:\s]*\s+:=\s*((?:[^\s\.]+\.)?\b$fct_name\s*\()/$1$2/isg;
					}
					# Remove package name and try to replace call to function name only
					if (!$class->{function_metadata}{$sch}{$p}{$k}{metadata}{inout} && $k =~ s/^[^\.]+\.// && lc($p) eq lc($class->{current_package}) )
					{
						if ($sch ne 'unknown' and $str =~ /\b$sch\.$k\b/is)
						{
							$str =~ s/(BEGIN|LOOP|;)((?:\s*%ORA2PG_COMMENT\d+\%\s*|\s*\/\*(?:.*?)\*\/\s*)*\s*)($sch\.$k\s*[\(;])/$1$2$call $3/igs;
							while ($str =~ s/(EXCEPTION(?:(?!CASE).)*?THEN)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)($sch\.$k\s*[\(;])/$1$2$call $3/is) {};
							$str =~ s/(IF(?:(?!CASE|THEN).)*?THEN)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)($sch\.$k\s*[\(;])/$1$2$call $3/isg;
							$str =~ s/(IF(?:(?!CASE|ELSE).)*?ELSE)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)($sch\.$k\s*[\(;])/$1$2$call $3/isg;
							$str =~ s/($call $sch\.$k);/$1\(\);/igs;
						}
						elsif ($str =~ /\b(?:$k|$fct_name)\b/is)
						{
							$str =~ s/(BEGIN|LOOP|CALL|;)((?:\s*%ORA2PG_COMMENT\d+\%\s*|\s*\/\*(?:.*?)\*\/\s*)*\s*)((?:$k|$fct_name)\s*[\(;])/$1$2$call $3/igs;
							while ($str =~ s/(EXCEPTION(?:(?!CASE).)*?THEN)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)((?:$k|$fct_name)\s*[\(;])/$1$2$call $3/is) {};
							$str =~ s/(IF(?:(?!CASE|THEN).)*?THEN)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)((?:$k|$fct_name)\s*[\(;])/$1$2$call $3/isg;
							$str =~ s/(IF(?:(?!CASE|ELSE).)*?ELSE)((?:\s*%ORA2PG_COMMENT\d+\%\s*)*\s*)((?:$k|$fct_name)\s*[\(;])/$1$2$call $3/isg;
							$str =~ s/($call (?:$k|$fct_name));/$1\(\);/igs;
						}
					}
				}
			}
		}
	}

	# Fix call to procedure changed above
	if ($class->{pg_supports_procedure}) {
		while ($str =~ s/\bCALL\s+(PERFORM|CALL)\s+/CALL /igs) {};
	} else {
		while ($str =~ s/\bCALL\s+(PERFORM|CALL)\s+/PERFORM /igs) {};
	}

	return $str;
}

sub translate_statement
{
	my ($class, $stmt, $is_subpart) = @_;

	# Divise code through UNION keyword marking a new query level
	my @q = split(/\b(UNION\s+ALL|UNION)\b/i, $stmt);
	for (my $j = 0; $j <= $#q; $j++) {
		next if ($q[$j] =~ /^UNION/);

		# Replace call to right outer join obsolete syntax
		$q[$j] = replace_outer_join($class, $q[$j], 'right');

		# Replace call to left outer join obsolete syntax
		$q[$j] = replace_outer_join($class, $q[$j], 'left');

		if ($q[$j] =~ /\bROWNUM\b/i)
		{
			# Replace ROWNUM after the WHERE clause by a LIMIT clause
			$q[$j] = replace_rownum_with_limit($class, $q[$j]);
			# Replace ROWNUM by row_number() when used in the target list
			$q[$j] =~ s/((?!WHERE\s.*|LIMIT\s.*)[\s,]+)ROWNUM([\s,]+)/$1row_number() OVER () AS rownum$2/is;
			# Aliases before =, <, >, +, -, ASC or DESC will generate an error
			$q[$j] =~ s/row_number\(\) OVER \(\) AS rownum\s*([=><+\-]|ASC|DESC)/row_number() OVER () $1/is;
			# Try to replace AS rownnum with alias if there is one already defined
			$q[$j] =~ s/(row_number\(\) OVER \(\) AS)\s+rownum\s+((?!FROM\s+|[,+\-]\s*)[^\s]+)/$1 $2/is;
			$q[$j] =~ s/\s+AS(\s+AS\s+)/$1/is;
			# The form "UPDATE mytbl SET col1 = ROWNUM;" is not yet translated
			# and mus be manually rewritten as follow:
			# WITH cte AS (SELECT *, ROW_NUMBER() OVER() AS rn FROM mytbl)
			# 	UPDATE mytbl SET col1 = (SELECT rn FROM cte WHERE cte.pk = mytbl.pk);
		}

	}
	$stmt = join("\n", @q);

	# Rewrite some invalid ending form after rewriting
	$stmt =~ s/(\s+WHERE)\s+AND/$1/igs;

	$stmt =~ s/(\s+)(?:WHERE|AND)\s+(LIMIT\s+)/$1$2/igs;
	$stmt =~ s/\s+WHERE\s*$//is;
	$stmt =~ s/\s+WHERE\s*\)/\)/is;

	# Remove unnecessary offset to position 0 which is the default
	$stmt =~ s/\s+OFFSET 0//igs;

	# Replacement of connect by with CTE
	$stmt = replace_connect_by($class, $stmt);

	return $stmt;
}

sub remove_extra_parenthesis
{
	my $str = shift;

	while ($str =~ s/\(\s*\(((?!\s*SELECT)[^\(\)]+)\)\s*\)/($1)/gs) {};
	my %store_clause = ();
	my $i = 0;
	while ($str =~ s/\(\s*\(([^\(\)]+)\)\s*AND\s*\(([^\(\)]+)\)\s*\)/\%PARENTHESIS$i\%/is) {
		$store_clause{$i} = find_or_parenthesis($1, $2);
		$i++
	}
	$str =~ s/\%PARENTHESIS(\d+)\%/$store_clause{$1}/gs;
	while ($str =~ s/\(\s*\(\s*\(([^\(\)]+\)[^\(\)]+\([^\(\)]+)\)\s*\)\s*\)/(($1))/gs) {};

	return $str;
}

# When the statement include OR keep parenthesisœ
sub find_or_parenthesis
{
	my ($left, $right) = @_;

	if ($left =~ /\s+OR\s+/i) {
		$left = "($left)";
	}
	if ($right =~ /\s+OR\s+/i) {
		$right = "($right)";
	}

	return "($left AND $right)";
}


sub extract_subpart
{
	my ($class, $str) = @_;

	while ($$str =~ s/\(([^\(\)]*)\)/\%SUBQUERY$class->{sub_parts_idx}\%/s) {
		$class->{sub_parts}{$class->{sub_parts_idx}} = $1;
		$class->{sub_parts_idx}++;
	}
	my @done = ();
	foreach my $k (sort { $b <=> $a } %{$class->{sub_parts}}) {
		if ($class->{sub_parts}{$k} =~ /\%OUTERJOIN\d+\%/ && $class->{sub_parts}{$k} !~ /\b(SELECT|FROM|WHERE)\b/i) {
			$$str =~ s/\%SUBQUERY$k\%/\($class->{sub_parts}{$k}\)/s;
			push(@done, $k);
		}
	}
	foreach (@done) {
		delete $class->{sub_parts}{$_};
	}
}

sub extract_subqueries
{
	my ($class, $str) = @_;

	return if ($class->{sub_queries_idx} == 100);

	my $cur_idx =  $class->{sub_queries_idx};
	if ($$str =~ s/\((\s*(?:SELECT|WITH).*)/\%SUBQUERY$class->{sub_queries_idx}\%/is) {
		my $stop_learning = 0;
		my $idx = 1;
		my $sub_query = '';
		foreach my $c (split(//, $1)) {
			$idx++ if (!$stop_learning && $c eq '(');
			$idx-- if (!$stop_learning && $c eq ')');
			if ($idx == 0) {
				# Do not copy last parenthesis in the output string
				$c = '' if (!$stop_learning);
				# Increment storage position for the next subquery
				$class->{sub_queries_idx}++ if (!$stop_learning);
				# Inform the loop that we don't want to process any charater anymore
				$stop_learning = 1;
				# We have reach the end of the subquery all next
				# characters must be restored to the final string.
				$$str .= $c;
			} elsif ($idx > 0) {
				# Append character to the current substring storage
				$class->{sub_queries}{$class->{sub_queries_idx}} .= $c;
			}
		}

		# Each subquery could have subqueries too, so call the
		# function recursively on each extracted subquery
		if ($class->{sub_queries}{$class->{sub_queries_idx}-1} =~ /\(\s*(?:SELECT|WITH)/is) {
				extract_subqueries($class, \$class->{sub_queries}{$class->{sub_queries_idx}-1});
		}
	}

}

sub replace_rownum_with_limit
{
	my ($class, $str) = @_;

	my $offset = '';
        if ($str =~ s/\s+(WHERE)\s+(?:\(\s*)?ROWNUM\s*=\s*([^\s\)]+)(\s*\)\s*)?([^;]*)/ $1 $3$4/is)
	{
		$offset = $2;
		($offset =~ /[^0-9]/) ? $offset = "($offset)" : $offset -= 1;
		$class->{limit_clause} = ' LIMIT 1 OFFSET ' . $offset;
		
        }
	if ($str =~ s/\s+AND\s+(?:\(\s*)?ROWNUM\s*=\s*([^\s\)]+)(\s*\)\s*)?([^;]*)/ $2$3/is)
	{
		$offset = $1;
		($offset =~ /[^0-9]/) ? $offset = "($offset)" : $offset -= 1;
		$class->{limit_clause} = ' LIMIT 1 OFFSET ' . $offset;
        }
	if ($str =~ s/\s+(WHERE)\s+(?:\(\s*)?ROWNUM\s*>=\s*([^\s\)]+)(\s*\)\s*)?([^;]*)/ $1 $3$4/is)
	{
		$offset = $2;
		($offset =~ /[^0-9]/) ? $offset = "($offset)" : $offset -= 1;
		$class->{limit_clause} = ' LIMIT ALL OFFSET ' . $offset;
        }
	if ($str =~ s/\s+(WHERE)\s+(?:\(\s*)?ROWNUM\s*>\s*([^\s\)]+)(\s*\)\s*)?([^;]*)/ $1 $3$4/is)
	{
		$offset = $2;
		$offset = "($offset)" if ($offset =~ /[^0-9]/);
		$class->{limit_clause} = ' LIMIT ALL OFFSET ' . $offset;
	}
	if ($str =~ s/\s+AND\s+(?:\(\s*)?ROWNUM\s*>=\s*([^\s\)]+)(\s*\)\s*)?([^;]*)/ $2$3/is)
	{
		$offset = $1;
		($offset =~ /[^0-9]/) ? $offset = "($offset)" : $offset -= 1;
		$class->{limit_clause} = ' LIMIT ALL OFFSET ' . $offset;
        }
	if ($str =~ s/\s+AND\s+(?:\(\s*)?ROWNUM\s*>\s*([^\s\)]+)(\s*\)\s*)?([^;]*)/ $2$3/is)
	{
		$offset = $1;
		$offset = "($offset)" if ($offset =~ /[^0-9]/);
		$class->{limit_clause} = ' LIMIT ALL OFFSET ' . $offset;
	}

	my $tmp_val = '';
	if ($str =~ s/\s+(WHERE)\s+(?:\(\s*)?ROWNUM\s*<=\s*([^\s\)]+)(\s*\)\s*)?([^;]*)/ $1 $3$4/is) {
		$tmp_val = $2;
	}
	if ($str =~ s/\s+(WHERE)\s+(?:\(\s*)?ROWNUM\s*<\s*([^\s\)]+)(\s*\)\s*)?([^;]*)/ $1 $3$4/is)
	{
		my $clause = $2;
		if ($clause =~ /\%SUBQUERY\d+\%/) {
			$tmp_val = $clause;
		} else {
			$tmp_val = $clause - 1;
		}
        }
	if ($str =~ s/\s+AND\s+(?:\(\s*)?ROWNUM\s*<=\s*([^\s\)]+)(\s*\)\s*)?([^;]*)/ $2$3/is) {
		$tmp_val = $1;
        }
	if ($str =~ s/\s+AND\s+(?:\(\s*)?ROWNUM\s*<\s*([^\s\)]+)(\s*\)\s*)?([^;]*)/ $2$3/is)
	{
		my $clause = $1;
		if ($clause =~ /\%SUBQUERY\d+\%/) {
			$tmp_val = $clause;
		} else {
			$tmp_val = $clause - 1;
		}
        }
	$str =~ s/\s+WHERE\s+ORDER\s+/ ORDER /is;

	if ($tmp_val)
	{
		if ($class->{limit_clause} =~ /LIMIT ALL OFFSET ([^\s]+)/is)
		{
			my $tmp_offset = $1;
			if ($tmp_offset !~ /[^0-9]/ && $tmp_val !~ /[^0-9]/) {
				$tmp_val -= $tmp_offset;
			} else {
				$tmp_val = "($tmp_val - $tmp_offset)";
			}
			$class->{limit_clause} =~ s/LIMIT ALL/LIMIT $tmp_val/is;
		}
		else
		{
			$tmp_val = "($tmp_val)" if ($tmp_val =~ /[^0-9]/);
			$class->{limit_clause} = ' LIMIT ' . $tmp_val;
		}
	}

	# Rewrite some invalid ending form after rewriting
	$str =~ s/(\s+WHERE)\s+AND/$1/igs;
	$str =~ s/\s+WHERE\s*$//is;
	$str =~ s/\s+WHERE\s*\)/\)/is;

	# Remove unnecessary offset to position 0 which is the default
	$str =~ s/\s+OFFSET 0//igs;

	return $str;
}

# Translation of REGEX_SUBSTR( string, pattern, [pos], [nth]) converted into
# (SELECT array_to_string(a, '') FROM regexp_matches(substr(string, pos), pattern, 'g') AS foo(a) LIMIT 1 OFFSET (nth - 1))";
# Optional fith parameter of match_parameter is appended to 'g' when present
sub convert_regex_substr
{
	($class, $str) = @_;

	my @params = split(/\s*,\s*/, $str);
	my $mod = '';
	if ($#params == 4) {
		# Restore constant string to look into date format
		while ($params[4] =~ s/\?TEXTVALUE(\d+)\?/$class->{text_values}{$1}/igs) {};
		#delete $class->{text_values}{$1};
		#
		$params[4] =~ s/'//g;
		$mod = $params[4] if ($params[4] ne 'g');
	}
	if ($#params < 2) {
		push(@params, 1, 1);
	} elsif ($#params < 3) {
		push(@params, 1);
	}
	if ($params[2] == 1) {
		$str = "(SELECT array_to_string(a, '') FROM regexp_matches($params[0], $params[1], 'g$mod') AS foo(a) LIMIT 1 OFFSET ($params[3] - 1))";
	} else {
		$str = "(SELECT array_to_string(a, '') FROM regexp_matches(substr($params[0], $params[2]), $params[1], 'g$mod') AS foo(a) LIMIT 1 OFFSET ($params[3] - 1))";
	}

	return $str;
}

sub convert_from_tz
{
	my ($class, $date) = @_;

	# Restore constant string to look into date format
	while ($date =~ s/\?TEXTVALUE(\d+)\?/$class->{text_values}{$1}/igs) {};
	#delete $class->{text_values}{$1};

	my $tz = '00:00';
	if ($date =~ /^[^']*'([^']+)'\s*,\s*'([^']+)'/) {
		$date = $1;
		$tz = $2;
		$date .= ' ';
		if ($tz =~ /^\d+:\d+$/) {
			$date .= '+' . $tz;
		} else {
			$date .= $tz;
		}
		$date = "'$date'";
	} elsif ($date =~ /^(.*),\s*'([^']+)'$/) {
		$date = $1;
		$tz = $2;
		if ($tz =~ /^\d+:\d+$/) {
			$tz .= '+' . $tz;
		}
		$date .= ' AT TIME ZONE ' . "'$tz'";
	}

	# Replace constant strings
	while ($date =~ s/('[^']+')/\?TEXTVALUE$class->{text_values_pos}\?/is) {
		$class->{text_values}{$class->{text_values_pos}} = $1;
		$class->{text_values_pos}++;
	}

	return $date;
}

sub convert_date_format
{
	my ($class, $fields, @strings) = @_;

	# Restore constant string to look into date format
	while ($fields =~ s/\?TEXTVALUE(\d+)\?/$class->{text_values}{$1}/igs) {};

	for ($i = 0; $i <= $#strings; $i++) {
		$fields =~ s/\%\%string$i\%\%/'$strings[$i]'/;
	}

	# Truncate time to microsecond
	$fields =~ s/(\d{2}:\d{2}:\d{2}[,\.]\d{6})\d{3}/$1/s;

	# Replace round year with two digit year format.
	$fields =~ s/RR/YY/sg;

	# Convert fractional seconds to milli (MS) or micro (US) seconds
	$fields =~ s/FF[123]/MS/s;
	$fields =~ s/FF\d*/US/s;

	# Remove any timezone format
	if ($class->{to_char_notimezone}) {
		$fields =~ s/\s*TZ[DHMR]//gs;
	}

	# Replace constant strings
	while ($str =~ s/('[^']+')/\?TEXTVALUE$class->{text_values_pos}\?/s)
	{
		$class->{text_values}{$class->{text_values_pos}} = $1;
		$class->{text_values_pos}++;
	}
	return $fields;
}


#------------------------------------------------------------------------------
# Set the correspondance between Oracle and PostgreSQL regexp modifiers
# Oracle default:
# 1) The default case sensitivity is determined by the NLS_SORT parameter.
#    Ora2pg assuming case sensitivy
# 2) A period (.) does not match the newline character.
# 3) The source string is treated as a single line.
# PostgreSQL default:
# 1) Default to case sensitivity
# 2) A period match the newline character.
# 3) The source string is treated as a single line.
# Oracle only supports the following modifiers
# 'i' specifies case-insensitive matching. Same for PG.
# 'c' specifies case-sensitive matching. Same for PG.
# 'x' Ignores whitespace characters in the search pattern. Same for PG.
# 'n' allows the period (.) to match the newline character. PG => s.
# 'm' treats the source string as multiple lines. PG => n.
#------------------------------------------------------------------------------
sub regex_flags
{
	my ($class, $modifier, $append) = @_;

	my $nconst = '';
	my $flags = $append || '';

	if ($modifier =~ /\?TEXTVALUE(\d+)\?/)
	{
		$nconst = $1;
		$modifier =~ s/\?TEXTVALUE$nconst\?/$class->{text_values}{$nconst}/igs;
	}
	# These flags have the same behavior
	if ($modifier =~ /([icx]+)/) {
		$flags .= $1;
	}
	# Oracle:
	# m : treats the source string as multiple lines.
	# SELECT '1' FROM DUAL WHERE REGEXP_LIKE('Hello'||CHR(10)||'world!', '^world!$', 'm'); => 1
	# PostgreSQL:
	# m : historical synonym for n => m : newline-sensitive matching
	# SELECT  regexp_match('Hello'||chr(10)||'world!', '^world!$', 'm'); => match
	if ($modifier =~ /m/) {
		$flags .= 'n';
	}
	# Oracle:
	# n: allows the period (.) to match the newline character. 
	# SELECT '1' FROM DUAL WHERE REGEXP_LIKE('a'||CHR(10)||'d', 'a.d', 'n'); => 1
	# SELECT '1' FROM DUAL WHERE REGEXP_LIKE('a'||CHR(10)||'d', '^d$', 'n'); => not match
	# PostgreSQL:
	# s: non-newline-sensitive matching (default)
	# SELECT regexp_match('a'||chr(10)||'d', 'a.d', 's'); => match
	# SELECT regexp_match('a'||chr(10)||'d', '^d$', 's'); => not match
	if ($modifier =~ /n/) {
		$flags .= 's';
	}

	# By default PG is non-newline-sensitive whereas Oracle is newline-sensitive
	# Oracle:
	# SELECT '1' FROM DUAL WHERE REGEXP_LIKE('a'||CHR(10)||'d', 'a.d'); => not match
	# PostgreSQL:
	# SELECT regexp_match('a'||chr(10)||'d', 'a.d'); => match
	# Add 'n' to force the same behavior like Oracle
	$flags .= 'n' if ($flags !~ /n|s/);

	if ($nconst ne '')
	{
		$class->{text_values}{$nconst} = "'$flags'";
		return "?TEXTVALUE$nconst?";
	}

	return "'$flags'";
}
	
sub replace_oracle_function
{
        my ($class, $str, @strings) = @_;

	my @xmlelt = ();
	my $field = '\s*([^\(\),]+)\s*';
	my $num_field = '\s*([\d\.]+)\s*';

	# Remove the SYS schema from calls
	$str =~ s/\bSYS\.//igs;

	#--------------------------------------------
	# PL/SQL to PL/PGSQL code conversion
	# Feel free to add your contribution here.
	#--------------------------------------------

	if ($class->{is_mysql}) {
		$str = mysql_to_plpgsql($class, $str);
	}

	# Change NVL to COALESCE
	$str =~ s/NVL\s*\(/coalesce(/is;
	$str =~ s/NVL2\s*\($field,$field,$field\)/(CASE WHEN $1 IS NOT NULL THEN $2 ELSE $3 END)/is;

	# Replace DEFAULT empty_blob() and empty_clob()
	my $empty = "''";
	$empty = 'NULL' if ($class->{empty_lob_null});
	$str =~ s/(empty_blob|empty_clob)\s*\(\s*\)/$empty/is;
	$str =~ s/(empty_blob|empty_clob)\b/$empty/is;

	# DBMS_LOB.GETLENGTH can be replaced by binary length.
	$str =~ s/DBMS_LOB.GETLENGTH/octet_length/igs;
	# DBMS_LOB.SUBSTR can be replaced by SUBSTR() with second and third parameter inversed
	$str =~ s/DBMS_LOB.SUBSTR\s*\($field,$field,$field\)/substr($1, $3, $2)/igs;
	# TO_CLOB(), we just remove it
	$str =~ s/TO_CLOB\s*\(/\(/igs;

	# Replace call to SYS_GUID() function
	$str =~ s/\bSYS_GUID\s*\(\s*\)/$class->{uuid_function}()/igs;
	$str =~ s/\bSYS_GUID\b/$class->{uuid_function}()/igs;

	# Rewrite TO_DATE formating call
	$str =~ s/TO_DATE\s*\(\s*('[^\']+')\s*,\s*('[^\']+')[^\)]*\)/to_date($1,$2)/igs;

	# When the date format is ISO and we have a constant we can remove the call to to_date()
	if ($class->{type} eq 'PARTITION' && $class->{pg_supports_partition}) {
		$str =~ s/to_date\(\s*('\s*\d+-\d+-\d+ \d+:\d+:\d+')\s*,\s*'[S]*YYYY-MM-DD HH24:MI:SS'[^\)]*\)/$1/igs;
	}

	# Translate to_timestamp_tz Oracle function
	$str =~ s/TO_TIMESTAMP_TZ\s*\((.*)\)/'to_timestamp(' . convert_date_format($class, $1, @strings) . ')'/iegs;

	# Translate from_tz Oracle function
	$str =~ s/FROM_TZ\s*\(\s*([^\)]+)\s*\)/'(' . convert_from_tz($class,$1) . ')::timestamp with time zone'/iegs;

	# Replace call to trim into btrim
	$str =~ s/\b(TRIM\s*\()\s+/$1/igs;
	$str =~ s/\bTRIM\s*\(((?!BOTH)[^\(\)]*)\)/trim(both $1)/igs;

	# Do some transformation when Orafce is not used
	if (!$class->{use_orafce})
	{
		# Replace to_nchar() without format by a simple cast to text
		$str =~ s/\bTO_NCHAR\s*\(\s*([^,\)]+)\)/($1)::varchar/igs;
		# Replace to_char() without format by a simple cast to text
		$str =~ s/\bTO_CHAR\s*\(\s*([^,\)]+)\)/($1)::varchar/igs;
		# Fix format for to_char() with format 
		$str =~ s/\b(TO_CHAR\s*\(\s*[^,\)]+\s*),(\s*[^,\)]+\s*)\)/"$1," . convert_date_format($class, $2, @strings) . ")"/iegs;
		if ($class->{type} ne 'TABLE') {
			$str =~ s/\(([^\s]+)\)(::varchar)/$1$2/igs;
		} else {
			$str =~ s/\(([^\s]+)\)(::varchar)/($1$2)/igs;
		}

		# Change trunc(date) to date_trunc('day', field)
		# Oracle has trunc(number) so there will have false positive
		# replacement but most of the time trunc(date) is encountered.
		$str =~ s/\bTRUNC\s*\($field\)/date_trunc('day', $1)/is;
		if ($str =~ s/\bTRUNC\s*\($field,$field\)/date_trunc($2, $1)/is ||
		    # Case where the parameters are obfuscated by function and string placeholders
		    $str =~ s/\bTRUNC\((\%\%REPLACEFCT\d+\%\%)\s*,\s*(\?TEXTVALUE\d+\?)\)/date_trunc($2, $1)/is
		)
		{
			if ($str =~ /date_trunc\(\?TEXTVALUE(\d+)\?/)
			{
				my $k = $1;
				$class->{text_values}{$k} =~ s/'(SYYYY|SYEAR|YEAR|[Y]+)'/'year'/is;
				$class->{text_values}{$k} =~ s/'Q'/'quarter'/is;
				$class->{text_values}{$k} =~ s/'(MONTH|MON|MM|RM)'/'month'/is;
				$class->{text_values}{$k} =~ s/'(IW|DAY|DY|D)'/'week'/is;
				$class->{text_values}{$k} =~ s/'(DDD|DD|J)'/'day'/is;
				$class->{text_values}{$k} =~ s/'(HH|HH12|HH24)'/'hour'/is;
				$class->{text_values}{$k} =~ s/'MI'/'minute'/is;
			}
		}

		# Convert the call to the Oracle function add_months() into Pg syntax
		$str =~ s/\bADD_MONTHS\s*\(([^,]+),\s*(\d+)\s*\)/$1 + '$2 month'::interval/si;
		$str =~ s/\bADD_MONTHS\s*\(([^,]+),\s*([^,\(\)]+)\s*\)/$1 + $2*'1 month'::interval/si;

		# Convert the call to the Oracle function add_years() into Pg syntax
		$str =~ s/\bADD_YEARS\s*\(([^,]+),\s*(\d+)\s*\)/$1 + '$2 year'::interval/si;
		$str =~ s/\bADD_YEARS\s*\(([^,]+),\s*([^,\(\)]+)\s*\)/$1 + $2*' year'::interval/si;

		# Translate numtodsinterval Oracle function
		$str =~ s/\b(?:NUMTODSINTERVAL|NUMTOYMINTERVAL)\s*\(\s*([^,]+)\s*,\s*([^\)]+)\s*\)/($1 * ('1'||$2)::interval)/is;

		# REGEX_LIKE( string, pattern, flags )
		$str =~ s/\bREGEXP_LIKE\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\)]+)\s*\)/"regexp_match($1, $2," . regex_flags($class, $3) . ") IS NOT NULL"/iges;
		# REGEX_LIKE( string, pattern )
		$str =~ s/\bREGEXP_LIKE\s*\(\s*([^,]+)\s*,\s*([^\)]+)\s*\)/"regexp_match($1, $2," . regex_flags($class, '') . ") IS NOT NULL"/iges;

		# REGEX_COUNT( string, pattern, position, flags )
		$str =~ s/\bREGEXP_COUNT\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*(\d+)\s*,\s*([^\)]+)\s*\)/"(SELECT count(*) FROM regexp_matches(substr($1, $3), $2, " . regex_flags($class, $4, 'g') . "))"/iges;
		# REGEX_COUNT( string, pattern, position )
		$str =~ s/\bREGEXP_COUNT\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*(\d+)\s*\)/(SELECT count(*) FROM regexp_matches(substr($1, $3), $2, 'g'))/igs;
		# REGEX_COUNT( string, pattern )
		$str =~ s/\bREGEXP_COUNT\s*\(\s*([^,]+)\s*,\s*([^\)]+)\s*\)/(SELECT count(*) FROM regexp_matches($1, $2, 'g'))/igs;
		# REGEX_SUBSTR( string, pattern, pos, num ) translation
		$str =~ s/\bREGEXP_SUBSTR\s*\(\s*([^\)]+)\s*\)/convert_regex_substr($class, $1)/iges;

		# Always append 'g' modifier to regexp_replace, this is the default with Oracle
		$str =~ s/\b(REGEXP_REPLACE\s*\(\s*[^\)]+)\s*\)/$1, 'g')/igs;

		# LAST_DAY( date ) translation
		$str =~ s/\bLAST_DAY\s*\(\s*([^\(\)]+)\s*\)/((date_trunc('month',($1)::timestamp + interval '1 month'))::date - 1)/igs;
	}
	else
	{
		s/([^\.])\b(GREATEST\()/$1oracle.$2/igs;
		s/([^\.])\b(LEAST\()/$1oracle.$2/igs;
	}

	# Replace INSTR by POSITION
	$str =~ s/\bINSTR\s*\(\s*([^,]+),\s*([^\),]+)\s*\)/position($2 in $1)/is;
	$str =~ s/\bINSTR\s*\(\s*([^,]+),\s*([^,]+)\s*,\s*1\s*\)/position($2 in $1)/is;

	# The to_number() function reclaim a second argument under postgres which is the format.
	# Replace to_number with a cast when no specific format is given
	if (lc($class->{to_number_conversion}) ne 'none')
	{
		if ($class->{to_number_conversion} =~ /(numeric|bigint|integer|int)/i)
		{
			my $cast = lc($1);
			if ($class->{type} ne 'TABLE') {
				$str =~ s/\bTO_NUMBER\s*\(\s*([^,\)]+)\s*\)\s?/($1)\:\:$cast /is;
			} else {
				$str =~ s/\bTO_NUMBER\s*\(\s*([^,\)]+)\s*\)\s?/(nullif($1, '')\:\:$cast) /is;
			}
		}
		else
		{
			$str =~ s/\bTO_NUMBER\s*\(\s*([^,\)]+)\s*\)/to_number\($1,'$class->{to_number_conversion}'\)/is;
		}
	}

	# Replace the UTC convertion with the PG syntaxe
	$str =~ s/SYS_EXTRACT_UTC\s*\(([^\)]+)\)/($1 AT TIME ZONE 'UTC')/is;

	# Remove call to XMLCDATA, there's no such function with PostgreSQL
	$str =~ s/XMLCDATA\s*\(([^\)]+)\)/'<![CDATA[' || $1 || ']]>'/is;
	# Remove call to getClobVal() or getStringVal, no need of that
	$str =~ s/\.(GETCLOBVAL|GETSTRINGVAL|GETNUMBERVAL|GETBLOBVAL)\s*\(\s*\)//is;
	# Add the name keyword to XMLELEMENT
	$str =~ s/XMLELEMENT\s*\(\s*/XMLELEMENT(name /is;
	# Replace XMLTYPE function
	$str =~ s/XMLTYPE\s*\(\s*([^,]+)\s*,[^\)]+\s*\)/xmlparse(DOCUMENT, convert_from($1, 'utf-8'))/igs;
	$str =~ s/XMLTYPE\.CREATEXML\s*\(\s*[^\)]+\s*\)/xmlparse(DOCUMENT, convert_from($1, 'utf-8'))/igs;

	# Cast round() call as numeric
	$str =~ s/round\s*\(([^,]+),([\s\d]+)\)/round\(($1)::numeric,$2\)/is;

	if ($str =~ /SDO_/is)
	{
		# Replace SDO_GEOM to the postgis equivalent
		$str = &replace_sdo_function($str);

		# Replace Spatial Operator to the postgis equivalent
		$str = &replace_sdo_operator($str);
	}

	# Rewrite replace(a,b) with three argument
	$str =~ s/REPLACE\s*\($field,$field\)/replace($1, $2, '')/is;

	# Replace Oracle substr(string, start_position, length) with
	# PostgreSQL substring(string from start_position for length)
	$str =~ s/\bsubstrb\s*\(/substr\(/igs;
	if (!$class->{pg_supports_substr})
	{
		$str =~ s/\bsubstr\s*\($field,$field,$field\)/substring($1 from $2 for $3)/is;
		$str =~ s/\bsubstr\s*\($field,$field\)/substring($1 from $2)/is;
	}

	# Replace call to function with out parameters
	$str = replace_out_param_call($class, $str);

	# Replace some sys_context call to the postgresql equivalent
	if ($str =~ /SYS_CONTEXT/is) {
		replace_sys_context($str);
	}

	return $str;
}

sub replace_out_param_call_internal
{
	my ($class, $fct_name, $str, $sch, $p, $k) = @_;

	my %replace_out_parm = ();
	my $idx = 0;
	while ($str =~ s/((?:[^\s\.]+\.)?\b$fct_name)\s*\(([^\(\)]+)\)/\%FCTINOUTPARAM$idx\%/is)
	{
		my $fname = $1;
		my $fparam = $2;
		if ($fname =~ /\./ && lc($fname) ne lc($k))
		{
			$replace_out_parm{$idx} = "$fname($fparam)";
			next;
		}
		$replace_out_parm{$idx} = "$fname(";
		# Extract position of out parameters
		my @params = split(/\s*,\s*/, $class->{function_metadata}{$sch}{$p}{$k}{metadata}{args});
		my @cparams = split(/\s*,\s*/s, $fparam);
		my $call_params = '';
		my @out_pos = ();
		my @out_fields = ();
		for (my $i = 0; $i <= $#params; $i++)
		{
			if (!$class->{is_mysql} && $params[$i] =~ /\s*([^\s]+)\s+(OUT|INOUT)\s/is)
			{
				push(@out_fields, $1);
				push(@out_pos, $i);
				$call_params .= $cparams[$i] if ($params[$i] =~ /\bINOUT\b/is);
			}
			elsif ($class->{is_mysql} && $params[$i] =~ /\s*(OUT|INOUT)\s+([^\s]+)\s/is)
			{
				push(@out_fields, $2);
				push(@out_pos, $i);
				$call_params .= $cparams[$i] if ($params[$i] =~ /\bINOUT\b/is);
			}
			else
			{
				$call_params .= $cparams[$i];
			}
			$call_params .= ', ' if ($i < $#params);
		}
		map { s/^\(//; } @out_fields;
		$call_params =~ s/(\s*,\s*)+$//s;
		while ($call_params =~ s/\s*,\s*,\s*/, /s) {};
		$call_params =~ s/^(\s*,\s*)+//s;
		$replace_out_parm{$idx} .= "$call_params)";
		my @out_param = ();
		foreach my $i (@out_pos) {
			push(@out_param, $cparams[$i]);
		}
		if ($class->{function_metadata}{$sch}{$p}{$k}{metadata}{inout} == 1)
		{
			map { s/[^\s=]+\s*=>\s*//; } @out_param;
			if ($#out_param == 0) {
				$replace_out_parm{$idx} = "$out_param[0] := $replace_out_parm{$idx}";
			} else {
				$replace_out_parm{$idx} = "SELECT * FROM $replace_out_parm{$idx} INTO " . join(', ', @out_param);
			}
		}
		elsif ($class->{function_metadata}{$sch}{$p}{$k}{metadata}{inout} > 1)
		{
			$class->{replace_out_params} = "_ora2pg_r RECORD;" if (!$class->{replace_out_params});
			$replace_out_parm{$idx} = "SELECT * FROM $replace_out_parm{$idx} INTO _ora2pg_r;\n";
			my $out_field_pos = 0;
			foreach my $parm (@out_param)
			{
				# remove use of named parameters
				$parm =~ s/.*=>\s*//;
				$replace_out_parm{$idx} .= " $parm := _ora2pg_r.$out_fields[$out_field_pos++];";
			}
			$replace_out_parm{$idx} =~ s/;$//s;
		}
		$idx++;
	}
	$str =~ s/\%FCTINOUTPARAM(\d+)\%/$replace_out_parm{$1}/gs;

	return $str;
}


##############
# Replace call to function with out parameters
##############
sub replace_out_param_call
{
	my ($class, $str) = @_;

	if (uc($class->{type}) =~ /^(PACKAGE|FUNCTION|PROCEDURE|TRIGGER)$/)
	{
		foreach my $sch (sort keys %{$class->{function_metadata}})
		{
			foreach my $p (sort keys %{$class->{function_metadata}{$sch}})
			{
				foreach my $k (sort keys %{$class->{function_metadata}{$sch}{$p}})
				{
					if ($class->{function_metadata}{$sch}{$p}{$k}{metadata}{inout})
					{
						my $fct_name = $class->{function_metadata}{$sch}{$p}{$k}{metadata}{fct_name} || '';
						next if (!$fct_name);
						next if ($p eq 'none' && $str !~ /\b$fct_name\b/is);
						next if ($p ne 'none' && $str !~ /\b$p\.$fct_name\b/is && $str !~ /(^|[^\.])\b$fct_name\b/is);

						# Prevent replacement with same function name from an other package
						next if ($class->{current_package} && lc($p) ne lc($class->{current_package}) && $str =~ /(^|[^\.])\b$fct_name\b/is);

						# Since PG14 procedures support OUT param should not be
						# changed, just add CALL at start of the function call
						if ($class->{pg_supports_outparam}
							&& $class->{function_metadata}{$sch}{$p}{$k}{metadata}{type} eq 'PROCEDURE')
						{
							$str =~ s/(^|\s+)($fct_name)\b/$1 CALL $2/igs;
							$str =~ s/\b($p\.$fct_name)\b/CALL $1/igs;
							next;
						}
						$str = &replace_out_param_call_internal($class, $fct_name, $str, $sch, $p, $k);
					}
				}
			}
		}

		# Replace regular procedur call (not proc from package)
		foreach my $sch (sort keys %{$class->{function_metadata}})
		{
			my $p = 'none';
			foreach my $k (sort keys %{$class->{function_metadata}{$sch}{$p}})
			{
				if ($class->{function_metadata}{$sch}{$p}{$k}{metadata}{inout})
				{
					my $fct_name = $class->{function_metadata}{$sch}{$p}{$k}{metadata}{fct_name} || '';
					next if (!$fct_name);

					# Prevent replacement with same function name from an other package
					next if ($str !~ /(^|[\.])\b$fct_name\b/is);

					# Since PG14 procedures support OUT param should not be
					# changed, just add CALL at start of the function call
					if ($class->{pg_supports_outparam}
						&& $class->{function_metadata}{$sch}{$p}{$k}{metadata}{type} eq 'PROCEDURE')
					{
						$str =~ s/(^|\s+)($fct_name)\b/$1 CALL $2/igs;
						$str =~ s/\b($p\.$fct_name)\b/CALL $1/igs;
						next;
					}
					$str = &replace_out_param_call_internal($class, $fct_name, $str, $sch, $p, $k);
				}
			}
		}
	}

	return $str;
}

# Replace decode("user_status",'active',"username",null)
# PostgreSQL (CASE WHEN "user_status"='ACTIVE' THEN "username" ELSE NULL END)
sub replace_decode
{
	my $str = shift;

	while ($str =~ s/\bDECODE\s*\((.*)$/\%DECODE\%/is) {
		my @decode_params = ('');
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
				# We have reach the end of the decode() parameter
				# next character must be restored to the final string.
				$str .= $c;
			} elsif ($idx > 0) {
				# We are parsing the decode() parameter part, append
				# the caracter to the right part of the param array.
				if ($c eq ',' && ($idx - 1) == 0) {
					# we are switching to a new parameter
					push(@decode_params, '');
				} elsif ($c ne "\n") {
					$decode_params[-1] .= $c;
				}
			}
		}
		my $case_str = 'CASE ';
		for (my $i = 1; $i <= $#decode_params; $i+=2) {
			$decode_params[$i] =~ s/^\s+//gs;
			$decode_params[$i] =~ s/\s+$//gs;
			if ($i < $#decode_params) {
				$case_str .= "WHEN $decode_params[0]=$decode_params[$i] THEN $decode_params[$i+1] ";
			} else {
				$case_str .= " ELSE $decode_params[$i] ";
			}
		}
		$case_str .= 'END ';
		$str =~ s/\%DECODE\%/$case_str/s;
	}

	return $str;
}

# Function to replace call to SYS_CONTECT('USERENV', ...)
# List of Oracle environment variables: http://docs.oracle.com/cd/B28359_01/server.111/b28286/functions172.htm
# Possibly corresponding PostgreSQL variables: http://www.postgresql.org/docs/current/static/functions-info.html
sub replace_sys_context
{
	my $str = shift;

	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'(OS_USER|SESSION_USER|AUTHENTICATED_IDENTITY)'\s*\)/session_user/is;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'BG_JOB_ID'\s*\)/pg_backend_pid()/is;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'(CLIENT_IDENTIFIER|PROXY_USER)'\s*\)/session_user/is;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'CURRENT_SCHEMA'\s*\)/current_schema/is;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'CURRENT_USER'\s*\)/current_user/is;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'(DB_NAME|DB_UNIQUE_NAME)'\s*\)/current_database/is;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'(HOST|IP_ADDRESS)'\s*\)/inet_client_addr()/is;
	$str =~ s/SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'SERVER_HOST'\s*\)/inet_server_addr()/is;

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
	# SDO_CS.TRANSFORM(geom, srid)
	$str =~ s/\bSDO_CS\.TRANSFORM\(/ST_Transform\(/igs;

	# Note that with ST_DumpPoints and :
	# TABLE(SDO_UTIL.GETVERTICES(C.GEOLOC)) T
	# T.X, T.Y, T.ID must be replaced manually as ST_X(T.geom) X, ST_Y(T.geom) Y, (T).path[1] ID
	my $field = '\s*[^\(\),]+\s*';
	my $num_field = '\s*[\d\.]+\s*';

	# SDO_GEOM.RELATE(geom1 IN SDO_GEOMETRY,mask IN VARCHAR2,geom2 IN SDO_GEOMETRY,tol IN NUMBER)
	$str =~ s/(ST_Relate\s*\($field),$field,($field),($field)\)/$1,$2\)/is;
	# SDO_GEOM.RELATE(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,mask IN VARCHAR2,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_Relate\s*\($field),$field,$field,($field),$field\)/$1,$2\)/is;
	# SDO_GEOM.SDO_AREA(geom IN SDO_GEOMETRY, tol IN NUMBER [, unit IN VARCHAR2])
	# SDO_GEOM.SDO_AREA(geom IN SDO_GEOMETRY,dim IN SDO_DIM_ARRAY [, unit IN VARCHAR2])
	$str =~ s/(ST_Area\s*\($field),[^\)]+\)/$1\)/is;
	# SDO_GEOM.SDO_BUFFER(geom IN SDO_GEOMETRY,dist IN NUMBER, tol IN NUMBER [, params IN VARCHAR2])
	$str =~ s/(ST_Buffer\s*\($field,$num_field),[^\)]+\)/$1\)/is;
	# SDO_GEOM.SDO_BUFFER(geom IN SDO_GEOMETRY,dim IN SDO_DIM_ARRAY,dist IN NUMBER [, params IN VARCHAR2])
	$str =~ s/(ST_Buffer\s*\($field),$field,($num_field)[^\)]*\)/$1,$2\)/is;
	# SDO_GEOM.SDO_CENTROID(geom1 IN SDO_GEOMETRY,tol IN NUMBER)
	# SDO_GEOM.SDO_CENTROID(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_Centroid\s*\($field),$field\)/$1\)/is;
	# SDO_GEOM.SDO_CONVEXHULL(geom1 IN SDO_GEOMETRY,tol IN NUMBER)
	# SDO_GEOM.SDO_CONVEXHULL(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_ConvexHull\s*\($field),$field\)/$1\)/is;
	# SDO_GEOM.SDO_DIFFERENCE(geom1 IN SDO_GEOMETRY,geom2 IN SDO_GEOMETRY,tol IN NUMBER)
	$str =~ s/(ST_Difference\s*\($field,$field),$field\)/$1\)/is;
	# SDO_GEOM.SDO_DIFFERENCE(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_Difference\s*\($field),$field,($field),$field\)/$1,$2\)/is;
	# SDO_GEOM.SDO_DISTANCE(geom1 IN SDO_GEOMETRY,geom2 IN SDO_GEOMETRY,tol IN NUMBER [, unit IN VARCHAR2])
	$str =~ s/(ST_Distance\s*\($field,$field),($num_field)[^\)]*\)/$1\)/is;
	# SDO_GEOM.SDO_DISTANCE(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY [, unit IN VARCHAR2])
	$str =~ s/(ST_Distance\s*\($field),$field,($field),($field)[^\)]*\)/$1,$2\)/is;
	# SDO_GEOM.SDO_INTERSECTION(geom1 IN SDO_GEOMETRY,geom2 IN SDO_GEOMETRY,tol IN NUMBER)
	$str =~ s/(ST_Intersection\s*\($field,$field),$field\)/$1\)/is;
	# SDO_GEOM.SDO_INTERSECTION(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_Intersection\s*\($field),$field,($field),$field\)/$1,$2\)/is;
	# SDO_GEOM.SDO_LENGTH(geom IN SDO_GEOMETRY, dim IN SDO_DIM_ARRAY [, unit IN VARCHAR2])
	# SDO_GEOM.SDO_LENGTH(geom IN SDO_GEOMETRY, tol IN NUMBER [, unit IN VARCHAR2])
	$str =~ s/(ST_Length\s*\($field),($field)[^\)]*\)/$1\)/is;
	# SDO_GEOM.SDO_POINTONSURFACE(geom1 IN SDO_GEOMETRY, tol IN NUMBER)
	# SDO_GEOM.SDO_POINTONSURFACE(geom1 IN SDO_GEOMETRY, dim1 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_PointOnSurface\s*\($field),$field\)/$1\)/is;
	# SDO_GEOM.SDO_UNION(geom1 IN SDO_GEOMETRY, geom2 IN SDO_GEOMETRY, tol IN NUMBER)
	$str =~ s/(ST_Union\s*\($field,$field),$field\)/$1\)/is;
	# SDO_GEOM.SDO_UNION(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_Union\s*\($field),$field,($field),$field\)/$1,$2\)/is;
	# SDO_GEOM.SDO_XOR(geom1 IN SDO_GEOMETRY,geom2 IN SDO_GEOMETRY, tol IN NUMBER)
	$str =~ s/(ST_SymDifference\s*\($field,$field),$field\)/$1\)/is;
	# SDO_GEOM.SDO_XOR(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_SymDifference\s*\($field),$field,($field),$field\)/$1,$2\)/is;
	# SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT(geom1 IN SDO_GEOMETRY, tol IN NUMBER)
	# SDO_GEOM.VALIDATE_GEOMETRY_WITH_CONTEXT(geom1 IN SDO_GEOMETRY, dim1 IN SDO_DIM_ARRAY)
	$str =~ s/(ST_IsValidReason\s*\($field),$field\)/$1\)/is;
	# SDO_GEOM.WITHIN_DISTANCE(geom1 IN SDO_GEOMETRY,dim1 IN SDO_DIM_ARRAY,dist IN NUMBER,geom2 IN SDO_GEOMETRY,dim2 IN SDO_DIM_ARRAY [, units IN VARCHAR2])
	$str =~ s/(ST_DWithin\s*\($field),$field,($field),($field),($field)[^\)]*\)/$1,$3,$2\)/is;
	# SDO_GEOM.WITHIN_DISTANCE(geom1 IN SDO_GEOMETRY,dist IN NUMBER,geom2 IN SDO_GEOMETRY, tol IN NUMBER [, units IN VARCHAR2])
	$str =~ s/(ST_DWithin\s*\($field)(,$field)(,$field),($field)[^\)]*\)/$1$3$2\)/is;

	return $str;
}

sub replace_sdo_operator
{
	my $str = shift;

	# SDO_CONTAINS(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_CONTAINS\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Contains($1)/is;
	$str =~ s/SDO_CONTAINS\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Contains($1)/is;
	$str =~ s/SDO_CONTAINS\s*\(([^\)]+)\)/ST_Contains($1)/is;
	# SDO_RELATE(geometry1, geometry2, param) = 'TRUE'
	$str =~ s/SDO_RELATE\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Relate($1)/is;
	$str =~ s/SDO_RELATE\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Relate($1)/is;
	$str =~ s/SDO_RELATE\s*\(([^\)]+)\)/ST_Relate($1)/is;
	# SDO_WITHIN_DISTANCE(geometry1, aGeom, params) = 'TRUE'
	$str =~ s/SDO_WITHIN_DISTANCE\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_DWithin($1)/is;
	$str =~ s/SDO_WITHIN_DISTANCE\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_DWithin($1)/is;
	$str =~ s/SDO_WITHIN_DISTANCE\s*\(([^\)]+)\)/ST_DWithin($1)/is;
	# SDO_TOUCH(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_TOUCH\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Touches($1)/is;
	$str =~ s/SDO_TOUCH\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Touches($1)/is;
	$str =~ s/SDO_TOUCH\s*\(([^\)]+)\)/ST_Touches($1)/is;
	# SDO_OVERLAPS(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_OVERLAPS\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Overlaps($1)/is;
	$str =~ s/SDO_OVERLAPS\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Overlaps($1)/is;
	$str =~ s/SDO_OVERLAPS\s*\(([^\)]+)\)/ST_Overlaps($1)/is;
	# SDO_INSIDE(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_INSIDE\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Within($1)/is;
	$str =~ s/SDO_INSIDE\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Within($1)/is;
	$str =~ s/SDO_INSIDE\s*\(([^\)]+)\)/ST_Within($1)/is;
	# SDO_EQUAL(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_EQUAL\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Equals($1)/is;
	$str =~ s/SDO_EQUAL\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Equals($1)/is;
	$str =~ s/SDO_EQUAL\s*\(([^\)]+)\)/ST_Equals($1)/is;
	# SDO_COVERS(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_COVERS\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Covers($1)/is;
	$str =~ s/SDO_COVERS\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Covers($1)/is;
	$str =~ s/SDO_COVERS\s*\(([^\)]+)\)/ST_Covers($1)/is;
	# SDO_COVEREDBY(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_COVEREDBY\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_CoveredBy($1)/is;
	$str =~ s/SDO_COVEREDBY\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_CoveredBy($1)/is;
	$str =~ s/SDO_COVEREDBY\s*\(([^\)]+)\)/ST_CoveredBy($1)/is;
	# SDO_ANYINTERACT(geometry1, geometry2) = 'TRUE'
	$str =~ s/SDO_ANYINTERACT\s*\((.*?)\)\s*=\s*[']+TRUE[']+/ST_Intersects($1)/is;
	$str =~ s/SDO_ANYINTERACT\s*\((.*?)\)\s*=\s*[']+FALSE[']+/NOT ST_Intersects($1)/is;
	$str =~ s/SDO_ANYINTERACT\s*\(([^\)]+)\)/ST_Intersects($1)/is;

	return $str;
}

# Function used to rewrite dbms_output.put, dbms_output.put_line and
# dbms_output.new_line by a plpgsql code
sub raise_output
{
	my ($class, $str) = @_;

	my @strings = split(/\s*\|\|\s*/s, $str);

	my @params = ();
	my @pattern = ();
	foreach my $el (@strings) {
		$el =~ s/\?TEXTVALUE(\d+)\?/$class->{text_values}{$1}/igs;
		$el =~ s/ORA2PG_ESCAPE2_QUOTE/''/gs;
		$el =~ s/ORA2PG_ESCAPE1_QUOTE'/\\'/gs;
		if ($el =~ /^\s*'(.*)'\s*$/s) {
			push(@pattern, $1);
		} else {
			push(@pattern, '%');
			push(@params, $el);
		}
	}
	#my $ret = "RAISE NOTICE '$pattern'";
	my $ret = "'" . join('', @pattern) . "'";
	$ret =~ s/\%\%/\% \%/gs;
	if ($#params >= 0) {
		$ret .= ', ' . join(', ', @params);
	}

	return 'RAISE NOTICE ' . $ret;
}

sub replace_sql_type
{
        my ($str, $pg_numeric_type, $default_numeric, $pg_integer_type, $varchar_to_text, %data_type) = @_;

	# Remove the SYS schema from type name
	$str =~ s/\bSYS\.//igs;

	$str =~ s/with local time zone/with time zone/igs;
	$str =~ s/([A-Z])\%ORA2PG_COMMENT/$1 \%ORA2PG_COMMENT/igs;

	# Replace MySQL type UNSIGNED in cast
	$str =~ s/\bTINYINT\s+UNSIGNED\b/smallint/igs;
	$str =~ s/\bSMALLINT\s+UNSIGNED\b/integer/igs;
	$str =~ s/\bMEDIUMINT\s+UNSIGNED\b/integer/igs;
	$str =~ s/\bBIGINT\s+UNSIGNED\b/numeric/igs;
	$str =~ s/\bINT\s+UNSIGNED\b/bigint/igs;

	# Remove precision for RAW|BLOB as type modifier is not allowed for type "bytea"
	$str =~ s/\b(RAW|BLOB)\s*\(\s*\d+\s*\)/$1/igs;

	# Replace type with precision
	my @ora_type = keys %data_type;
	map { s/\(/\\\(/; s/\)/\\\)/; } @ora_type;
	my $oratype_regex = join('|', @ora_type);

	while ($str =~ /(.*)\b($oratype_regex)\s*\(([^\)]+)\)/i)
	{
		my $backstr = $1;
		my $type = uc($2);
		my $args = $3;
		# Remove extra CHAR or BYTE information from column type
		$args =~ s/\s*(CHAR|BYTE)\s*$//i;
		if ($backstr =~ /_$/)
		{
		    $str =~ s/\b($oratype_regex)\s*\(([^\)]+)\)/$1\%\|$2\%\|\%/is;
		    next;
		}

		my ($precision, $scale) = split(/\s*,\s*/, $args);
		$precision = 38 if ($precision eq '*'); # case of NUMBER(*,10) or NUMBER(*)
		$len = $precision if ($len eq '*');
		$scale ||= 0;
		my $len = $precision || 0;
		$len =~ s/\D//;
		if ( $type =~ /CHAR|STRING/i )
		{
			# Type CHAR have default length set to 1
			# Type VARCHAR(2) must have a specified length
			$len = 1 if (!$len && (($type eq "CHAR") || ($type eq "NCHAR")));
			$str =~ s/\b$type\b\s*\([^\)]+\)/$data_type{$type}\%\|$len\%\|\%/is;
		}
		elsif ($type =~ /TIMESTAMP/i)
		{
			$len = 6 if ($len > 6);
			$str =~ s/\b$type\b\s*\([^\)]+\)/timestamp\%\|$len%\|\%/is;
 		}
		elsif ($type =~ /INTERVAL/i)
		{
 			# Interval precision for year/month/day is not supported by PostgreSQL
 			$str =~ s/(INTERVAL\s+YEAR)\s*\(\d+\)/$1/is;
 			$str =~ s/(INTERVAL\s+YEAR\s+TO\s+MONTH)\s*\(\d+\)/$1/is;
 			$str =~ s/(INTERVAL\s+DAY)\s*\(\d+\)/$1/is;
			# maximum precision allowed for seconds is 6
			if ($str =~ /INTERVAL\s+DAY\s+TO\s+SECOND\s*\((\d+)\)/)
			{
				if ($1 > 6) {
					$str =~ s/(INTERVAL\s+DAY\s+TO\s+SECOND)\s*\(\d+\)/$1(6)/i;
				}
			}
		}
		elsif ($type eq "NUMBER")
		{
			# This is an integer
			if (!$scale)
			{
				if ($precision)
				{
					if ($pg_integer_type)
					{
						if ($precision < 5) {
							$str =~ s/\b$type\b\s*\([^\)]+\)/smallint/is;
						} elsif ($precision <= 9) {
							$str =~ s/\b$type\b\s*\([^\)]+\)/integer/is;
						} elsif ($precision <= 19) {
							$str =~ s/\b$type\b\s*\([^\)]+\)/bigint/is;
						} else {
							$str =~ s/\b$type\b\s*\([^\)]+\)/numeric($precision)/is;
						}
					} else {
						$str =~ s/\b$type\b\s*\([^\)]+\)/numeric\%\|$precision\%\|\%/i;
					}
				}
				elsif ($pg_integer_type)
				{
					my $tmp = $default_numeric || 'bigint';
					$str =~ s/\b$type\b\s*\([^\)]+\)/$tmp/is;
				}
			}
			else
			{
				if ($pg_numeric_type)
				{
					if ($precision eq '') {
						$str =~ s/\b$type\b\s*\([^\)]+\)/decimal(38, $scale)/is;
					} elsif ($precision <= 6) {
						$str =~ s/\b$type\b\s*\([^\)]+\)/real/is;
					} else {
						$str =~ s/\b$type\b\s*\([^\)]+\)/double precision/is;
					}
				}
				else
				{
					if ($precision eq '') {
						$str =~ s/\b$type\b\s*\([^\)]+\)/decimal(38, $scale)/is;
					} else {
						$str =~ s/\b$type\b\s*\([^\)]+\)/decimal\%\|$precision,$scale\%\|\%/is;
					}
				}
			}
		}
		elsif ($type eq "NUMERIC") {
			$str =~ s/\b$type\b\s*\([^\)]+\)/numeric\%\|$args\%\|\%/is;
		} elsif ( ($type eq "DEC") || ($type eq "DECIMAL") ) {
			$str =~ s/\b$type\b\s*\([^\)]+\)/decimal\%\|$args\%\|\%/is;
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

        # Replace datatype without precision
	my $number = $data_type{'NUMBER'};
	$number = $default_numeric if ($pg_integer_type);
	$str =~ s/\bNUMBER\b/$number/igs;

	# Set varchar without length to text
	$str =~ s/\bVARCHAR2\b/VARCHAR/igs;
	$str =~ s/\bSTRING\b/VARCHAR/igs;
	if ($varchar_to_text) {
		$str =~ s/\bVARCHAR\b(\s*(?!\())/text$1/igs;
	} else {
		$str =~ s/\bVARCHAR\b(\s*(?!\())/varchar$1/igs;
	}

	foreach my $t ('DATE','LONG RAW','LONG','NCLOB','CLOB','BLOB','BFILE','RAW','ROWID','UROWID','FLOAT','DOUBLE PRECISION','INTEGER','INT','REAL','SMALLINT','BINARY_FLOAT','BINARY_DOUBLE','BINARY_INTEGER','BOOLEAN','XMLTYPE','SDO_GEOMETRY','PLS_INTEGER','NUMBER')
	{
		if ($t eq 'DATE') {
			$str =~ s/\b$t\s*\(\d\)/$data_type{$t}/igs;
		}
		elsif ($t eq 'NUMBER')
		{
			if ($pg_integer_type)
			{
				my $tmp = $default_numeric || 'bigint';
				$str =~ s/\b$t\b/$tmp/igs;
				next;
			}
		}
		$str =~ s/\b$t\b/$data_type{$t}/igs;
	}

	# Translate cursor declaration
	$str = replace_cursor_def($str);

	# Remove remaining %ROWTYPE in other prototype declaration
	#$str =~ s/\%ROWTYPE//isg;

	$str =~ s/;[ ]+/;/gs;

        return $str;
}

sub replace_cursor_def
{
	my $str = shift;

	# Remove IN information from cursor declaration
	while ($str =~ s/(\bCURSOR\b[^\(]+)\(([^\)]+\bIN\b[^\)]+)\)/$1\(\%\%CURSORREPLACE\%\%\)/is) {
		my $args = $2;
		$args =~ s/\bIN\b//igs;
		$str =~ s/\%\%CURSORREPLACE\%\%/$args/is;
	}

	# Replace %ROWTYPE ref cursor
	$str =~ s/\bTYPE\s+([^\s]+)\s+(IS\s+REF\s+CURSOR|REFCURSOR)\s+RETURN\s+[^\s\%]+\%ROWTYPE;/$1 REFCURSOR;/isg;


	# Replace local type ref cursor
	my %locatype = ();
	my $i = 0;
	while ($str =~ s/\bTYPE\s+([^\s]+)\s+(IS\s+REF\s+CURSOR|REFCURSOR)\s*;/\%LOCALTYPE$i\%/is)
	{
		$localtype{$i} = "TYPE $1 IS REF CURSOR;";
		my $local_type = $1;
		if ($str =~ s/\b([^\s]+)\s+$local_type\s*;/$1 REFCURSOR;/igs) {
			$str =~ s/\%LOCALTYPE$i\%//igs;
		}
		$i++;
	}
	$str =~ s/\%LOCALTYPE(\d+)\%/$localtype{$1}/gs;

	# Retrieve cursor names
	#my @cursor_names = $str =~ /\bCURSOR\b\s*([A-Z0-9_\$]+)/isg;
	# Reorder cursor declaration
	$str =~ s/\bCURSOR\b\s*([A-Z0-9_\$]+)/$1 CURSOR/isg;

	# Replace call to cursor type if any
	#foreach my $c (@cursor_names) {
	#	$str =~ s/\b$c\%ROWTYPE/RECORD/isg;
	#}

	# Replace REF CURSOR as Pg REFCURSOR
	$str =~ s/\bIS(\s*)REF\s+CURSOR/REFCURSOR/isg;
	$str =~ s/\bREF\s+CURSOR/REFCURSOR/isg;

	# Replace SYS_REFCURSOR as Pg REFCURSOR
	$str =~ s/\bSYS_REFCURSOR\b/REFCURSOR/isg;

	# Replace CURSOR IS SELECT by CURSOR FOR SELECT
	$str =~ s/\bCURSOR(\s+)IS([\s\(]*)(\%ORA2PG_COMMENT\d+\%)?([\s\(]*)SELECT/CURSOR$1FOR$2$3$4SELECT/isg;
	# Replace CURSOR (param) IS SELECT by CURSOR FOR SELECT
	$str =~ s/\bCURSOR(\s*\([^\)]+\)\s*)IS([\s\(]*)(\%ORA2PG_COMMENT\d+\%)?([\s\(]*)SELECT/CURSOR$1FOR$2$3$4SELECT/isg;

	# Replace REF CURSOR as Pg REFCURSOR
	$str =~ s/\bIS(\s*)REF\s+CURSOR/REFCURSOR/isg;
	$str =~ s/\bREF\s+CURSOR/REFCURSOR/isg;

	# Replace SYS_REFCURSOR as Pg REFCURSOR
	$str =~ s/\bSYS_REFCURSOR\b/REFCURSOR/isg;

	# Replace OPEN cursor FOR with dynamic query
	$str =~ s/(OPEN\s+(?:[^;]+?)\s+FOR)((?:[^;]+?)\bUSING\b)/$1 EXECUTE$2/isg;
	$str =~ s/(OPEN\s+(?:[^;]+?)\s+FOR)\s+([^\s]+\s*;)/$1 EXECUTE $2/isg;
	$str =~ s/(OPEN\s+(?:[^;]+?)\s+FOR)\s+(?!(\s+|SELECT|EXECUTE|WITH|\%ORA2PG_COMMENT))/$1 EXECUTE /isg;

	# Remove empty parenthesis after an open cursor
	$str =~ s/(OPEN\s+[^\(\s;]+)\s*\(\s*\)/$1/isg;

	# Invert FOR CURSOR call
	$str =~ s/\bFOR\s+CURSOR\s*\(([^;]+)?\);/CURSOR FOR $1;/igs;
	$str =~ s/\bFOR\s+CURSOR(\s+)/CURSOR FOR$1/igs;

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
	if ($type eq 'QUERY' || $type eq 'VIEW') {
		$cost = 0;
	}
	$cost_details{'TEST'} = $cost;

	# Set cost following code length
	my $cost_size = int(length($str)/$SIZE_SCORE) || 1;
	# When evaluating queries size must not be included here
	if ($type eq 'QUERY' || $type eq 'VIEW') {
		$cost_size = 0;
	}
	$cost += $cost_size;
	$cost_details{'SIZE'} = $cost_size;

	# Try to figure out the manual work
	my $n = () = $str =~ m/\bIS\s+TABLE\s+OF\b/igs;
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
	$n = () = $str =~ m/\bUROWID\b/igs;
	$cost_details{'UROWID'} += $n;
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
	$n = () = $str =~ m/PLUNIT/igs;
	$cost_details{'PLUNIT'} += $n;
	if (!$class->{use_orafce})
	{
		$n = () = $str =~ m/ADD_MONTHS/igs;
		$cost_details{'ADD_MONTHS'} += $n;
		$n = () = $str =~ m/LAST_DAY/igs;
		$cost_details{'LAST_DAY'} += $n;
		$n = () = $str =~ m/NEXT_DAY/igs;
		$cost_details{'NEXT_DAY'} += $n;
		$n = () = $str =~ m/MONTHS_BETWEEN/igs;
		$cost_details{'MONTHS_BETWEEN'} += $n;
		$n = () = $str =~ m/DBMS_OUTPUT\.put\(/igs;
		$cost_details{'DBMS_OUTPUT.put'} += $n;
		$n = () = $str =~ m/DBMS_OUTPUT\.(put_line|new_line|put)/igs;
		$cost_details{'DBMS_'} -= $n;
		$n = () = $str =~ m/\bTRUNC\s*\(/igs;
		$cost_details{'TRUNC'} += $n;
		$n = () = $str =~ m/REGEXP_LIKE/igs;
		$cost_details{'REGEXP_LIKE'} += $n;
		$n = () = $str =~ m/REGEXP_SUBSTR/igs;
		$cost_details{'REGEXP_SUBSTR'} += $n;
		$n = () = $str =~ m/REGEXP_COUNT/igs;
		$cost_details{'REGEXP_COUNT'} += $n;
		$n = () = $str =~ m/REGEXP_INSTR/igs;
		$cost_details{'REGEXP_INSTR'} += $n;
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
	}
	else
	{
		$n = () = $str =~ m/UTL_FILE/igs;
		$cost_details{'UTL_'} -= $n;
		$n = () = $str =~ m/DBMS_PIPE/igs;
		$cost_details{'DBMS_'} -= $n;
		$n = () = $str =~ m/DBMS_ALERT/igs;
		$cost_details{'DBMS_'} -= $n;
		$n = () = $str =~ m/DMS_UTILITY.FORMAT_CALL_STACK/igs;
		$cost_details{'DBMS_'} -= $n;
		$n = () = $str =~ m/DBMS_ASSERT/igs;
		$cost_details{'DBMS_'} -= $n;
		$n = () = $str =~ m/DBMS_STRING/igs;
		$cost_details{'DBMS_'} -= $n;
		$n = () = $str =~ m/PLUNIT.ASSERT/igs;
		$cost_details{'PLUNIT'} -= $n;
		$n = () = $str =~ m/DBMS_SQL/igs;
		$cost_details{'DBMS_'} -= $n;
		$n = () = $str =~ m/DBMS_RANDOM/igs;
		$cost_details{'DBMS_'} -= $n;
	}
	$n = () = $str =~ m/\b(INSERTING|DELETING|UPDATING)\b/igs;
	$cost_details{'TG_OP'} += $n;
	$n = () = $str =~ m/REF\s*CURSOR/igs;
	$cost_details{'CURSOR'} += $n;
	$n = () = $str =~ m/ORA_ROWSCN/igs;
	$cost_details{'ORA_ROWSCN'} += $n;
	$n = () = $str =~ m/SAVEPOINT/igs;
	$cost_details{'SAVEPOINT'} += $n;
	$n = () = $str =~ m/(FROM|EXEC)((?!WHERE).)*\b[\w\_]+\@[\w\_]+\b/igs;
	$cost_details{'DBLINK'} += $n;
	$n = () = $str =~ m/\%ISOPEN\b/igs;
	$cost_details{'ISOPEN'} += $n;
	$n = () = $str =~ m/\%ROWCOUNT\b/igs;
	$cost_details{'ROWCOUNT'} += $n;
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
	$n = () = $str =~ m/\bCONTAINS\(/igs;
	$cost_details{'CONTAINS'} += $n;
	$n = () = $str =~ m/\bSCORE\((?:.*)?\bCONTAINS\(/igs;
	$cost_details{'SCORE'} += $n;
	$n = () = $str =~ m/CONTAINS\((?:.*)?\bFUZZY\(/igs;
	$cost_details{'FUZZY'} += $n;
	$n = () = $str =~ m/CONTAINS\((?:.*)?\bNEAR\(/igs;
	$cost_details{'NEAR'} += $n;
	$n = () = $str =~ m/TO_CHAR\([^,\)]+\)/igs;
	$cost_details{'TO_CHAR'} += $n;
	$n = () = $str =~ m/TO_NCHAR\([^,\)]+\)/igs;
	$cost_details{'TO_NCHAR'} += $n;
	$n = () = $str =~ m/\s+ANYDATA/igs;
	$cost_details{'ANYDATA'} += $n;
	$n = () = $str =~ m/\|\|/igs;
	$cost_details{'CONCAT'} += $n;
	$n = () = $str =~ m/TIMEZONE_(REGION|ABBR)/igs;
	$cost_details{'TIMEZONE'} += $n;
	$n = () = $str =~ m/IS\s+(NOT)?\s*JSON/igs;
	$cost_details{'JSON'} += $n;
	$n = () = $str =~ m/TO_CLOB\([^,\)]+\)/igs;
	$cost_details{'TO_CLOB'} += $n;
	$n = () = $str =~ m/XMLTYPE\(/igs;
	$cost_details{'XMLTYPE'} += $n;
	$n = () = $str =~ m/CREATENONSCHEMABASEDXML\(/igs;
	$cost_details{'CREATENONSCHEMABASEDXML'} += $n;
	$n = () = $str =~ m/CREATESCHEMABASEDXML\(/igs;
	$cost_details{'CREATESCHEMABASEDXML'} += $n;
	$n = () = $str =~ m/CREATEXML\(/igs;
	$cost_details{'CREATEXML'} += $n;
	$n = () = $str =~ m/EXISTSNODE\(/igs;
	$cost_details{'EXISTSNODE'} += $n;
	$n = () = $str =~ m/EXTRACT\(/igs;
	$cost_details{'EXTRACT'} += $n;
	$n = () = $str =~ m/GETNAMESPACE\(/igs;
	$cost_details{'GETNAMESPACE'} += $n;
	$n = () = $str =~ m/GETROOTELEMENT\(/igs;
	$cost_details{'GETROOTELEMENT'} += $n;
	$n = () = $str =~ m/GETSCHEMAURL\(/igs;
	$cost_details{'GETSCHEMAURL'} += $n;
	$n = () = $str =~ m/ISFRAGMENT\(/igs;
	$cost_details{'ISFRAGMENT'} += $n;
	$n = () = $str =~ m/ISSCHEMABASED\(/igs;
	$cost_details{'ISSCHEMABASED'} += $n;
	$n = () = $str =~ m/ISSCHEMAVALID\(/igs;
	$cost_details{'ISSCHEMAVALID'} += $n;
	$n = () = $str =~ m/ISSCHEMAVALIDATED\(/igs;
	$cost_details{'ISSCHEMAVALIDATED'} += $n;
	$n = () = $str =~ m/SCHEMAVALIDATE\(/igs;
	$cost_details{'SCHEMAVALIDATE'} += $n;
	$n = () = $str =~ m/SETSCHEMAVALIDATED\(/igs;
	$cost_details{'SETSCHEMAVALIDATED'} += $n;
	$n = () = $str =~ m/TOOBJECT\(/igs;
	$cost_details{'TOOBJECT'} += $n;
	$n = () = $str =~ m/TRANSFORM\(/igs;
	$cost_details{'TRANSFORM'} += $n;

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

	# remove FROM DUAL
	$str =~ s/FROM\s+DUAL//igs;

	# Simply remove this as not supported
	$str =~ s/\bDEFAULT\s+NULL\b//igs;

	# Change mysql variable affectation 
	$str =~ s/\bSET\s+([^\s:=]+\s*)=([^;\n]+;)/$1:=$2/igs;

	# remove declared handler
	$str =~ s/[^\s]+\s+HANDLER\s+FOR\s+[^;]+;//igs;

	# Fix call to unsigned
	$str =~ s/\bTINYINT\s+UNSIGNED\b/smallint/igs;
	$str =~ s/\bSMALLINT\s+UNSIGNED\b/integer/igs;
	$str =~ s/\bMEDIUMINT\s+UNSIGNED\b/integer/igs;
	$str =~ s/\bBIGINT\s+UNSIGNED\b/numeric/igs;
	$str =~ s/\bINT\s+UNSIGNED\b/bigint/igs;

	# Drop temporary doesn't exist in PostgreSQL
	$str =~ s/DROP\s+TEMPORARY/DROP/gs;

	# Private temporary table doesn't exist in PostgreSQL
	$str =~ s/PRIVATE\s+TEMPORARY/TEMPORARY/igs;
	$str =~ s/ON\s+COMMIT\s+PRESERVE\s+DEFINITION/ON COMMIT PRESERVE ROWS/igs;
	$str =~ s/ON\s+COMMIT\s+DROP\s+DEFINITION/ON COMMIT DROP/igs;

	# Remove extra parenthesis in join in some possible cases
	# ... INNER JOIN(services s) ON ...
	$str =~ s/\bJOIN\s*\(([^\s]+\s+[^\s]+)\)/JOIN $1/igs;

	# Rewrite MySQL JOIN with WHERE clause instead of ON
	$str =~ s/\((\s*[^\s]+(?:\s+[^\s]+)?\s+JOIN\s+[^\s]+(?:\s+[^\s]+)?\s*)\)\s+WHERE\s+/$1 ON /igs;

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
	# Replace call to CURRENT_TIMESTAMP() to special variable
	$str =~ s/\bCURRENT_TIMESTAMP\s*\(\)/CURRENT_TIMESTAMP/igs;

	# Replace EXTRACT() with unit not supported by PostgreSQL
	if ($class->{mysql_internal_extract_format})
	{
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
	$str =~ s/\bstatementSYSDATE\(\s*\)/timeofday()::timestamp(0) without time zone/igs;
	$str =~ s/\bUNIX_TIMESTAMP\(\s*\)/floor(extract(epoch from CURRENT_TIMESTAMP::timestamp with time zone))/igs;
	$str =~ s/\bUNIX_TIMESTAMP\(\s*([^\)]+)\s*\)/floor(extract(epoch from ($1)::timestamp with time zone))/igs;
	$str =~ s/\bUTC_DATE\(\s*\)/(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date/igs;
	$str =~ s/\bUTC_TIME\(\s*\)/(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::time(0)/igs;
	$str =~ s/\bUTC_TIMESTAMP\(\s*\)/(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::timestamp(0)/igs;

	$str =~ s/\bCONVERT_TZ\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\(\),]+)\s*\)/(($1)::timestamp without time zone AT TIME ZONE ($2)::text) AT TIME ZONE ($3)::text/igs;
	$str =~ s/\bDATEDIFF\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/extract(day from (date_trunc('day', ($1)::timestamp) - date_trunc('day', ($2)::timestamp)))/igs;
	$str =~ s/\bDATE_FORMAT\(\s*(.*?)\s*,\s*('[^'\(\)]+'|\?TEXTVALUE\d+\?)\s*\)/_mysql_dateformat_to_pgsql($class, $1, $2)/iges;
	$str =~ s/\b(?:ADDDATE|DATE_ADD)\(\s*(.*?)\s*,\s*INTERVAL\s*([^\(\),]+)\s*\)/"($1)::timestamp " . _replace_dateadd($2)/iges;
	$str =~ s/\bADDDATE\(\s*([^,]+)\s*,\s*(\d+)\s*\)/($1)::timestamp + ($2 * interval '1 day')/igs;
	$str =~ s/\bADDTIME\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/($1)::timestamp + ($2)::interval/igs;


	$str =~ s/\b(DAY|DAYOFMONTH)\(\s*([^\(\)]+)\s*\)/extract(day from date($1))::integer/igs;
	$str =~ s/\bDAYNAME\(\s*([^\(\)]+)\s*\)/to_char(($1)::date, 'FMDay')/igs;
	$str =~ s/\bDAYOFWEEK\(\s*([^\(\)]+)\s*\)/extract(dow from date($1))::integer + 1/igs; # start on sunday = 1
	$str =~ s/\bDAYOFYEAR\(\s*([^\(\)]+)\s*\)/extract(doy from date($1))::integer/igs;
	$str =~ s/\bFORMAT\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/to_char(round($1, $2), 'FM999,999,999,999,999,999,990'||case when $2 > 0 then '.'||repeat('0', $2) else '' end)/igs;
	$str =~ s/\bFROM_DAYS\(\s*([^\(\)]+)\s*\)/'0001-01-01bc'::date + ($1)::integer/igs;
	$str =~ s/\bFROM_UNIXTIME\(\s*([^\(\),]+)\s*\)/to_timestamp($1)::timestamp without time zone/igs;
	$str =~ s/\bFROM_UNIXTIME\(\s*(.*?)\s*,\s*('[^\(\)]+'|\?TEXTVALUE\d+\?)\s*\)/FROM_UNIXTIME2(to_timestamp($1), $2)/igs;
	$str =~ s/\bFROM_UNIXTIME2\(\s*(.*?)\s*,\s*('[^'\(\)]+'|\?TEXTVALUE\d+\?)\s*\)/_mysql_dateformat_to_pgsql($class, $1, $2)/eigs;
	$str =~ s/\bGET_FORMAT\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/_mysql_getformat_to_pgsql($1, $2)/eigs;
	$str =~ s/\bHOUR\(\s*([^\(\)]+)\s*\)/extract(hour from ($1)::interval)::integer/igs;
	$str =~ s/\bLAST_DAY\(\s*([^\(\)]+)\s*\)/((date_trunc('month',($1)::timestamp + interval '1 month'))::date - 1)/igs;
	$str =~ s/\bMAKEDATE\(\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/(date($1||'-01-01') + ($2 - 1) * interval '1 day')::date/igs;
	$str =~ s/\bMAKETIME\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\(\)]+)\s*\)/($1 * interval '1 hour' + $2 * interval '1 min' + $3 * interval '1 sec')/igs;
	$str =~ s/\bMICROSECOND\(\s*([^\(\)]+)\s*\)/extract(microsecond from ($1)::time)::integer/igs;
	$str =~ s/\bMINUTE\(\s*([^\(\)]+)\s*\)/extract(minute from ($1)::time)::integer/igs;
	$str =~ s/\bMONTH\(\s*([^\(\)]+)\s*\)/extract(month from date($1))::integer/igs;
	$str =~ s/\bMONTHNAME\(\s*([^\(\)]+)\s*\)/to_char(($1)::date, 'FMMonth')/igs;
	$str =~ s/\bQUARTER\(\s*([^\(\)]+)\s*\)/extract(quarter from date($1))::integer/igs;
	$str =~ s/\bSECOND\(\s*([^\(\)]+)\s*\)/extract(second from ($1)::interval)::integer/igs;
	$str =~ s/\bSEC_TO_TIME\(\s*([^\(\)]+)\s*\)/($1 * interval '1 second')/igs;
	$str =~ s/\bSTR_TO_DATE\(\s*(.*?)\s*,\s*('[^'\(\),]+'|\?TEXTVALUE\d+\?)\s*\)/_mysql_strtodate_to_pgsql($class, $1, $2)/eigs;
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
	$str =~ s/\bTIME_FORMAT\(\s*(.*?)\s*,\s*('[^'\(\),]+'|\?TEXTVALUE\d+\?)\s*\)/_mysql_timeformat_to_pgsql($class, $1, $2)/eigs;
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
	if (!$class->{pg_supports_substr}) {
		$str =~ s/\bLOCATE\(\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/position($1 in substring ($2 from $3)) + $3 - 1/igs;
		$str =~ s/\bMID\(/substring\(/igs;
	} else {
		$str =~ s/\bLOCATE\(\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/position($1 in substr($2, $3)) + $3 - 1/igs;
		$str =~ s/\bMID\(/substr\(/igs;
	}
	$str =~ s/\bLOCATE\(\s*([^\(\),]+)\s*,\s*([^\(\),]+)\s*\)/position($1 in $2)/igs;
	$str =~ s/\bLCASE\(/lower\(/igs;
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
	$str =~ s/\bUUID\(/$class->{uuid_function}\(/igs;
	$str =~ s/\bNOT REGEXP BINARY/\!\~/igs;
	$str =~ s/\bREGEXP BINARY/\~/igs;
	$str =~ s/\bNOT REGEXP/\!\~\*/igs;
	$str =~ s/\bREGEXP/\~\*/igs;

	$str =~ s/\bGET_LOCK/pg_advisory_lock/igs;
	$str =~ s/\bIS_USED_LOCK/pg_try_advisory_lock/igs;
	$str =~ s/\bRELEASE_LOCK/pg_advisory_unlock/igs;

	# GROUP_CONCAT doesn't exist, it must be replaced by calls to array_to_string() and array_agg() functions
	$str =~ s/GROUP_CONCAT\((.*?)\s+ORDER\s+BY\s+([^\s]+)\s+(ASC|DESC)\s+SEPARATOR\s+(\?TEXTVALUE\d+\?|'[^']+')\s*\)/array_to_string(array_agg($1 ORDER BY $2 $3), $4)/igs;
	$str =~ s/GROUP_CONCAT\((.*?)\s+ORDER\s+BY\s+([^\s]+)\s+SEPARATOR\s+(\?TEXTVALUE\d+\?|'[^']+')\s*\)/array_to_string(array_agg($1 ORDER BY $2 ASC), $3)/igs;
	$str =~ s/GROUP_CONCAT\((.*?)\s+SEPARATOR\s+(\?TEXTVALUE\d+\?|'[^']+')\s*\)/array_to_string(array_agg($1), $2)/igs;
	$str =~ s/GROUP_CONCAT\((.*?)\s+ORDER\s+BY\s+([^\s]+)\s+(ASC|DESC)\s*\)/array_to_string(array_agg($1 ORDER BY $2 $3), ',')/igs;
	$str =~ s/GROUP_CONCAT\((.*?)\s+ORDER\s+BY\s+([^\s]+)\s*\)/array_to_string(array_agg($1 ORDER BY $2), ',')/igs;
	$str =~ s/GROUP_CONCAT\(([^\)]+)\)/array_to_string(array_agg($1), ',')/igs;

	# Replace IFNULL() MySQL function in a query
	while ($str =~ s/\bIFNULL\(\s*([^,]+)\s*,\s*([^\)]+\s*)\)/COALESCE($1, $2)/is) {};

	# Rewrite while loop
	$str =~ s/\bWHILE\b(.*?)\bEND\s+WHILE\s*;/WHILE $1END LOOP;/igs;
	$str =~ s/\bWHILE\b(.*?)\bDO\b/WHILE $1LOOP/igs;

	# Rewrite REPEAT loop
	my %repl_repeat = ();
	$i = 0;
	while ($str =~ s/\bREPEAT\s+(.*?)\bEND REPEAT\s*;/%REPREPEATLBL$i%/igs) {
		my $code = $1;
		$code =~ s/\bUNTIL(.*)//;
		$repl_repeat{$i} = "LOOP ${code}EXIT WHEN $1;\nEND LOOP;";
	}
	foreach $i (keys %repl_repeat) {
		$str =~ s/\%REPREPEATLBL$i\%/$repl_repeat{$i}/gs;
	}
	%repl_repeat = ();

	# Fix some charset encoding call in cast function
	#$str =~ s/(CAST\s*\((?:.*?)\s+AS\s+(?:[^\s]+)\s+)CHARSET\s+([^\s\)]+)\)/$1) COLLATE "\U$2\E"/igs;
	$str =~ s/(CAST\s*\((?:.*?)\s+AS\s+(?:[^\s]+)\s+)(CHARSET|CHARACTER\s+SET)\s+([^\s\)]+)\)/$1)/igs;
	$str =~ s/CONVERT\s*(\((?:[^,]+)\s+,\s+(?:[^\s]+)\s+)(CHARSET|CHARACTER\s+SET)\s+([^\s\)]+)\)/CAST$1)/igs;
	$str =~ s/CONVERT\s*\((.*?)\s+USING\s+([^\s\)]+)\)/CAST($1 AS text)/igs;
	# Set default UTF8 collation to postgreSQL equivalent C.UTF-8
	#$str =~ s/COLLATE "UTF8"/COLLATE "C.UTF-8"/gs;
	$str =~ s/\bCHARSET(\s+)/COLLATE$1/igs;

	# Remove call to start transaction
	$str =~ s/\sSTART\s+TRANSACTION\s*;/-- START TRANSACTION;/igs;

	# Comment call to COMMIT or ROLLBACK in the code if allowed
	if ($class->{comment_commit_rollback}) {
		$str =~ s/\b(COMMIT|ROLLBACK)\s*;/-- $1;/igs;
		$str =~ s/(ROLLBACK\s+TO\s+[^;]+);/-- $1;/igs;
	}

	# Translate call to CREATE TABLE ... SELECT
	$str =~ s/CREATE\s+PRIVATE\s+TEMPORARY/CREATE TEMPORARY/;
	$str =~ s/(CREATE(?:\s+TEMPORARY)?\s+TABLE\s+[^\s]+)(\s+SELECT)/$1 AS $2/igs;
	$str =~ s/ON\s+COMMIT\s+PRESERVE\s+DEFINITION/ON COMMIT PRESERVE ROWS/igs;
	$str =~ s/ON\s+COMMIT\s+DROP\s+DEFINITION/ON COMMIT DROP/igs;

	# Remove @ from variables and rewrite SET assignement in QUERY mode
	if ($class->{type} eq 'QUERY') {
		$str =~ s/\@([^\s]+)\b/$1/gs;
		$str =~ s/:=/=/gs;
	}

	# Replace spatial related lines
	$str = replace_mysql_spatial($str);

	# Rewrite direct call to function without out parameters using PERFORM
	$str = perform_replacement($class, $str);

	# Remove CALL from all statements if not supported
	if (!$class->{pg_supports_procedure}) {
		$str =~ s/\bCALL\s+//igs;
	}

	return $str;
}

sub _replace_dateadd
{
	my $str = shift;
	my $dd = shift;

	my $op = '+';
	if ($str =~ s/^\-[\s]*//) {
		$op = '-';
	}
	if ($str =~ s/^(\d+)\s+([^\(\),\s]+)$/ $op $1*interval '1 $2'/s) {
		return $str;
	} elsif ($str =~ s/^([^\s]+)\s+([^\(\),\s]+)$/ $op $1*interval '1 $2'/s) {
		return $str;
	} elsif ($str =~ s/^([^\(\),]+)$/ $op interval '$1'/s) {
		return $str;
	}

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
	my ($class, $datetime, $format) = @_;

	my $str = _mysql_dateformat_to_pgsql($class, $datetime, $format, 1);

	return $str;
}

sub _mysql_timeformat_to_pgsql
{
	my ($class, $datetime, $format) = @_;

	my $str = _mysql_dateformat_to_pgsql($class, $datetime, $format, 0, 1);

	return $str;
}


sub _mysql_dateformat_to_pgsql
{
	my ($class, $datetime, $format, $todate, $totime) = @_;

# Not supported:
# %X	Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V

	$format =~ s/\?TEXTVALUE(\d+)\?/$class->{text_values}{$1}/igs;

	$format =~ s/\%a/Dy/g;
	$format =~ s/\%b/Mon/g;
	$format =~ s/\%c/FMMM/g;
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
	$format =~ s/\%(\d+)/$1/g;

	# Replace constant strings
	if ($format =~ s/('[^']+')/\?TEXTVALUE$class->{text_values_pos}\?/is) {
		$class->{text_values}{$class->{text_values_pos}} = $1;
		$class->{text_values_pos}++;
	}

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

sub replace_outer_join
{
	my ($class, $str, $type) = @_;

	# Remove comments in the from clause. They need to be removed because the
	# entire FROM clause will be rewritten and we don't know where to restore.
	while ($str =~ s/(\s+FROM\s+(?:.*)?)\%ORA2PG_COMMENT\d+\%((?:.*)?WHERE\s+)/$1$2/is) {};

	if (!grep(/^$type$/, 'left', 'right')) {
		die "FATAL: outer join type must be 'left' or 'right' in call to replace_outer_join().\n";
	}

	# When we have a right outer join, just rewrite it as a left join to simplify the translation work
	if ($type eq 'right') {
		$str =~ s/(\s+)([^\s]+)\s*(\%OUTERJOIN\d+\%)\s*(!=|<>|>=|<=|=|>|<|NOT LIKE|LIKE)\s*([^\s]+)/$1$5 $4 $2$3/isg;
		return $str;
	}

	my $regexp1 = qr/((?:!=|<>|>=|<=|=|>|<|NOT LIKE|LIKE)\s*[^\s]+\s*\%OUTERJOIN\d+\%)/is;
	my $regexp2 = qr/\%OUTERJOIN\d+\%\s*(?:!=|<>|>=|<=|=|>|<|NOT LIKE|LIKE)/is;

	# process simple form of outer join
	my $nbouter = $str =~ $regexp1;

	# Check that we don't have right outer join too
	if ($nbouter >= 1 && $str !~ $regexp2)
	{
		# Extract tables in the FROM clause
		$str =~ s/(.*)\bFROM\s+(.*?)\s+WHERE\s+(.*?)$/$1FROM FROM_CLAUSE WHERE $3/is;
		my $from_clause = $2;
		$from_clause =~ s/"//gs;
		my @tables = split(/\s*,\s*/, $from_clause);

		# Set a hash for alias to table mapping
		my %from_clause_list = ();
		my %from_order = ();
		my $fidx = 0;
		foreach my $table (@tables)
		{
			$table =~ s/^\s+//s;
			$table =~ s/\s+$//s;
			my $cmt = '';
			while ($table =~ s/(\s*\%ORA2PG_COMMENT\d+\%\s*)//is) {
				$cmt .= $1;
			}
			my ($t, $alias, @others) = split(/\s+/, lc($table));
			$alias = $others[0] if (uc($alias) eq 'AS');
			$alias = "$t" if (!$alias);
			$from_clause_list{$alias} = "$cmt$t";
			$from_order{$alias} = $fidx++;
		}

		# Extract all Oracle's outer join syntax from the where clause
		my @outer_clauses = ();
		my %final_outer_clauses = ();
		my %final_from_clause = ();
		my @tmp_from_list = ();
		my $start_query = '';
		my $end_query = '';
		if ($str =~ s/^(.*FROM FROM_CLAUSE WHERE)//is) {
			$start_query = $1;
		}
		if ($str =~ s/\s+((?:START WITH|CONNECT BY|ORDER SIBLINGS BY|GROUP BY|ORDER BY).*)$//is) {
			$end_query = $1;
		}

		# Extract predicat from the WHERE clause
		my @predicat = split(/\s*(\bAND\b|\bOR\b|\%ORA2PG_COMMENT\d+\%)\s*/i, $str);
		my $id = 0;
		my %other_join_clause = ();
		# Process only predicat with a obsolete join syntax (+) for now
		for (my $i = 0; $i <= $#predicat; $i++)
		{
			next if ($predicat[$i] !~ /\%OUTERJOIN\d+\%/i);
			my $where_clause = $predicat[$i];
			$where_clause =~ s/"//gs;
			$where_clause =~ s/^\s+//s;
			$where_clause =~ s/[\s;]+$//s;
			$where_clause =~ s/\s*(\%OUTERJOIN\d+\%)//gs;

			$predicat[$i] = "WHERE_CLAUSE$id ";

			# Split the predicat to retrieve left part, operator and right part
			my ($l, $o, $r) = split(/\s*(!=|>=|<=|=|<>|<|>|NOT LIKE|LIKE)\s*/i, $where_clause);

			# NEW / OLD pseudo table in triggers can not be part of a join
			# clause. Move them int to the WHERE clause.
			if ($l =~ /^(NEW|OLD)\./is)
			{
				$predicat[$i] =~ s/WHERE_CLAUSE$id / $l $o $r /s;
				next;
			}
			$id++;

			# Extract the tablename part of the left clause
			my $lbl1 = '';
			my $table_decl1 = $l;
			if ($l =~ /^([^\.\s]+\.[^\.\s]+)\..*/ || $l =~ /^([^\.\s]+)\..*/)
			{
				$lbl1 = lc($1);
				# If the table/alias is not part of the from clause
				if (!exists $from_clause_list{$lbl1}) {
					$from_clause_list{$lbl1} = $lbl1;
					$from_order{$lbl1} = $fidx++;
				}
				$table_decl1 = $from_clause_list{$lbl1};
				$table_decl1 .= " $lbl1" if ($lbl1 ne $from_clause_list{$lbl1});
			}
			elsif ($l =~ /\%SUBQUERY(\d+)\%/)
			{
				# Search for table.column in the subquery or function code
				my $tmp_str = $l;
				while ($tmp_str =~ s/\%SUBQUERY(\d+)\%/$class->{sub_parts}{$1}/is)
				{
					if ($tmp_str =~ /\b([^\.\s]+\.[^\.\s]+)\.[^\.\s]+/
						|| $tmp_str =~ /\b([^\.\s]+)\.[^\.\s]+/)
					{
						$lbl1 = lc($1);
						# If the table/alias is not part of the from clause
						if (!exists $from_clause_list{$lbl1})
						{
							$from_clause_list{$lbl1} = $lbl1;
							$from_order{$lbl1} = $fidx++;
						}
						$table_decl1 = $from_clause_list{$lbl1};
						$table_decl1 .= " $lbl1" if ($lbl1 ne $from_clause_list{$lbl1});
						last;
					}
				}
			}

			# Extract the tablename part of the right clause
			my $lbl2 = '';
			my $table_decl2 = $r;
			if ($r =~ /^([^\.\s]+\.[^\.\s]+)\..*/ || $r =~ /^([^\.\s]+)\..*/)
			{
				$lbl2 = lc($1);
				if (!$lbl1) {
					push(@{$other_join_clause{$lbl2}}, "$l $o $r");
					next;
				}
				# If the table/alias is not part of the from clause
				if (!exists $from_clause_list{$lbl2}) {
					$from_clause_list{$lbl2} = $lbl2;
					$from_order{$lbl2} = $fidx++;
				}
				$table_decl2 = $from_clause_list{$lbl2};
				$table_decl2 .= " $lbl2" if ($lbl2 ne $from_clause_list{$lbl2});
			}
			elsif ($lbl1)
			{
				# Search for table.column in the subquery or function code
				my $tmp_str = $r;
				while ($tmp_str =~ s/\%SUBQUERY(\d+)\%/$class->{sub_parts}{$1}/is)
				{
					if ($tmp_str =~ /\b([^\.\s]+\.[^\.\s]+)\.[^\.\s]+/
						|| $tmp_str =~ /\b([^\.\s]+)\.[^\.\s]+/)
					{
						$lbl2 = lc($1);
						# If the table/alias is not part of the from clause
						if (!exists $from_clause_list{$lbl2})
						{
							$from_clause_list{$lbl2} = $lbl2;
							$from_order{$lbl2} = $fidx++;
						}
						$table_decl2 = $from_clause_list{$lbl2};
						$table_decl2 .= " $lbl2" if ($lbl2 ne $from_clause_list{$lbl2});
					}
				}
				if (!$lbl2 )
				{
					push(@{$other_join_clause{$lbl1}}, "$l $o $r");
					next;
				}
			}

			# When this is the first join parse add the left tablename
			# first then the outer join with the right table
			if (scalar keys %final_from_clause == 0)
			{
				$from_clause = $table_decl1;
				$table_decl1 =~ s/\s*\%ORA2PG_COMMENT\d+\%\s*//igs;
				push(@outer_clauses, (split(/\s/, $table_decl1))[1] || $table_decl1);
				$final_from_clause{"$lbl1;$lbl2"}{position} = $i;
				push(@{$final_from_clause{"$lbl1;$lbl2"}{clause}{$table_decl2}{predicat}}, "$l $o $r");
			}
			else
			{
				$final_from_clause{"$lbl1;$lbl2"}{position} = $i;
				push(@{$final_from_clause{"$lbl1;$lbl2"}{clause}{$table_decl2}{predicat}}, "$l $o $r");
				if (!exists $final_from_clause{"$lbl1;$lbl2"}{clause}{$table_decl2}{$type}) {
					$final_from_clause{"$lbl1;$lbl2"}{clause}{$table_decl2}{$type} = $table_decl1;
				}
			}
			if ($type eq 'left') {
				$final_from_clause{"$lbl1;$lbl2"}{clause}{$table_decl2}{position} = $i;
			} else {
				$final_from_clause{"$lbl1;$lbl2"}{clause}{$table_decl1}{position} = $i;
			}
		}
		$str = $start_query . join(' ', @predicat) . ' ' . $end_query;

		# Remove part from the WHERE clause that will be moved into the FROM clause
		$str =~ s/\s*(AND\s+)?WHERE_CLAUSE\d+ / /igs;
		$str =~ s/WHERE\s+(AND|OR)\s+/WHERE /is;
		$str =~ s/WHERE[\s;]+$//i;
		$str =~ s/(\s+)WHERE\s+(ORDER|GROUP)\s+BY/$1$2 BY/is;
		$str =~ s/\s+WHERE(\s+)/\nWHERE$1/igs;

		my %associated_clause = ();
		foreach my $t (sort { $final_from_clause{$a}{position} <=> $final_from_clause{$b}{position} } keys %final_from_clause)
		{
			foreach my $j (sort { $final_from_clause{$t}{clause}{$a}{position} <=> $final_from_clause{$t}{clause}{$b}{position} } keys %{$final_from_clause{$t}{clause}})
			{
				next if ($#{$final_from_clause{$t}{clause}{$j}{predicat}} < 0);

				if (exists $final_from_clause{$t}{clause}{$j}{$type} && $j !~ /\%SUBQUERY\d+\%/i && $from_clause !~ /\b\Q$final_from_clause{$t}{clause}{$j}{$type}\E\b/)
				{
					$from_clause .= ",$final_from_clause{$t}{clause}{$j}{$type}";
					push(@outer_clauses, (split(/\s/, $final_from_clause{$t}{clause}{$j}{$type}))[1] || $final_from_clause{$t}{clause}{$j}{$type});
				}
				my ($l,$r) = split(/;/, $t);
				my $tbl = $j;
				$tbl =~ s/\s*\%ORA2PG_COMMENT\d+\%\s*//isg;
				$from_clause .= "\n\U$type\E OUTER JOIN $tbl ON (" .  join(' AND ', @{$final_from_clause{$t}{clause}{$j}{predicat}}) . ")";
				push(@{$final_outer_clauses{$l}{join}},  "\U$type\E OUTER JOIN $tbl ON (" .  join(' AND ', @{$final_from_clause{$t}{clause}{$j}{predicat}}, @{$other_join_clause{$r}}) . ")");
				push(@{$final_outer_clauses{$l}{position}},  $final_from_clause{$t}{clause}{$j}{position});
				push(@{$associated_clause{$l}}, $r);
			}
		}

		$from_clause = '';
		my @clause_done = ();
		foreach my $c (sort { $from_order{$a} <=> $from_order{$b} } keys %from_order)
		{
			next if (!grep(/^\Q$c\E$/i, @outer_clauses));
			my @output = ();
			for (my $j = 0; $j <= $#{$final_outer_clauses{$c}{join}}; $j++) {
				push(@output, $final_outer_clauses{$c}{join}[$j]);
			}

			find_associated_clauses($c, \@output, \%associated_clause, \%final_outer_clauses);

			if (!grep(/\QJOIN $from_clause_list{$c} $c \E/is, @clause_done))
			{
				$from_clause .= "\n, $from_clause_list{$c}";
				$from_clause .= " $c" if ($c ne $from_clause_list{$c});
			}
			foreach (@output) { 
				$from_clause .= "\n" . $_;
			}
			push(@clause_done, @output);
			delete $from_order{$c};
			delete $final_outer_clauses{$c};
			delete $associated_clause{$c};
		}
		$from_clause =~ s/^\s*,\s*//s;

		# Append tables to from clause that was not involved into an outer join
		foreach my $a (sort keys %from_clause_list)
		{
			my $table_decl = "$from_clause_list{$a}";
			$table_decl .= " $a" if ($a ne $from_clause_list{$a});
			# Remove comment before searching it inside the from clause
			my $tmp_tbl = $table_decl;
			my $comment = '';
			while ($tmp_tbl =~ s/(\s*\%ORA2PG_COMMENT\d+\%\s*)//is) {
				$comment .= $1;
			}

			if ($from_clause !~ /(^|\s|,)\Q$tmp_tbl\E\b/is) {
				$from_clause = "$table_decl, " . $from_clause;
			} elsif ($comment) {
				 $from_clause = "$comment " . $from_clause;
			}
		}
		$from_clause =~ s/\b(new|old)\b/\U$1\E/gs;
		$from_clause =~ s/,\s*$/ /s;
		$str =~ s/FROM FROM_CLAUSE/FROM $from_clause/s;
	}

	return $str;
}

sub find_associated_clauses
{
	my ($c, $output, $associated_clause, $final_outer_clauses) = @_;

	foreach my $f (@{$associated_clause->{$c}}) {
		for (my $j = 0; $j <= $#{$final_outer_clauses->{$f}{join}}; $j++) {
			push(@$output, $final_outer_clauses->{$f}{join}[$j]);
		}
		delete $final_outer_clauses->{$f};
		if (scalar keys %{ $final_outer_clauses }) {
			find_associated_clauses($f, $output, $associated_clause, $final_outer_clauses);
		}
	}
	delete $associated_clause->{$c};
}


sub replace_connect_by
{
	my ($class, $str) = @_;

	return $str if ($str !~ /\bCONNECT\s+BY\b/is);

	my $final_query = "WITH RECURSIVE cte AS (\n";

	# Remove NOCYCLE, not supported at now
	$str =~ s/\s+NOCYCLE//is;

	# Remove SIBLINGS keywords and enable siblings rewrite 
	my $siblings = 0;
	if ($str =~ s/\s+SIBLINGS//is) {
		$siblings = 1;
	}

	# Extract UNION part of the query to past it at end
	my $union = '';
	if ($str =~ s/(CONNECT BY.*)(\s+UNION\s+.*)/$1/is) {
		$union = $2;
	}
	
	# Extract order by to past it to the query at end
	my $order_by = '';
	if ($str =~ s/\s+ORDER BY(.*)//is) {
		$order_by = $1;
	}

	# Extract group by to past it to the query at end
	my $group_by = '';
	if ($str =~ s/(\s+GROUP BY.*)//is) {
		$group_by = $1;
	}

	# Extract the starting node or level of the tree 
	my $where_clause = '';
	my $start_with = '';
	if ($str =~ s/WHERE\s+(.*?)\s+START\s+WITH\s*(.*?)\s+CONNECT BY\s*//is) {
		$where_clause = " WHERE $1";
		$start_with = $2;
	} elsif ($str =~ s/WHERE\s+(.*?)\s+CONNECT BY\s+(.*?)\s+START\s+WITH\s*(.*)/$2/is) {
		$where_clause = " WHERE $1";
		$start_with = $3;
	} elsif ($str =~ s/START\s+WITH\s*(.*?)\s+CONNECT BY\s*//is) {
		$start_with = $1;
	} elsif ($str =~ s/\s+CONNECT BY\s+(.*?)\s+START\s+WITH\s*(.*)/ $1 /is) {
		$start_with = $2;
	} else {
		$str =~ s/CONNECT BY\s*//is;
	}

	# remove alias from where clause
	$where_clause =~ s/\b[^\.]\.([^\s]+)\b/$1/gs;

	# Extract the CONNECT BY clause in the hierarchical query
	my $prior_str = '';
	my @prior_clause = '';
	if ($str =~ s/([^\s]+\s*=\s*PRIOR\s+.*)//is) {
		$prior_str =  $1;
	} elsif ($str =~ s/(\s*PRIOR\s+.*)//is) {
		$prior_str =  $1;
	} else {
		# look inside subqueries if we have a prior clause
		my @ids = $str =~ /\%SUBQUERY(\d+)\%/g;
		my $sub_prior_str = '';
		foreach my $i (@ids) {
			if ($class->{sub_parts}{$i} =~ s/([^\s]+\s*=\s*PRIOR\s+.*)//is) {
				$sub_prior_str =  $1;
				$str =~ s/\%SUBQUERY$i\%//;
			} elsif ($class->{sub_parts}{$i} =~ s/(\s*PRIOR\s+.*)//is) {
				$sub_prior_str =  $1;
				$str =~ s/\%SUBQUERY$i\%//;
			}
			$sub_prior_str =~ s/^\(//;
			$sub_prior_str =~ s/\)$//;
			($prior_str ne '' || $sub_prior_str eq '') ? $prior_str .= ' ' . $sub_prior_str : $prior_str = $sub_prior_str;
		}
	}
	if ($prior_str) {
		# Try to extract the prior clauses
		my @tmp_prior = split(/\s*AND\s*/, $prior_str);
		$tmp_prior[-1] =~ s/\s*;\s*//s;
		my @tmp_prior2 = ();
		foreach my $p (@tmp_prior) {
			if ($p =~ /\bPRIOR\b/is) {
				push(@prior_clause, split(/\s*=\s*/i, $p));
			} else {
				$where_clause .= " AND $p";
			}
		}
		if ($siblings) {
			if ($prior_clause[-1] !~ /PRIOR/i) {
				$siblings = $prior_clause[-1];
			} else {
				$siblings = $prior_clause[-2];
			}
			$siblings =~ s/\s+//g;
		}
		shift(@prior_clause) if ($prior_clause[0] eq '');
		my @rebuild_prior = ();
		# Place PRIOR in the left part if necessary
		for (my $i = 0; $i < $#prior_clause; $i+=2) {
			if ($prior_clause[$i+1] =~ /PRIOR\s+/i) {
				my $tmp = $prior_clause[$i];
				$prior_clause[$i] = $prior_clause[$i+1];
				$prior_clause[$i+1] = $tmp;
			}
			push(@rebuild_prior, "$prior_clause[$i] = $prior_clause[$i+1]");
		}
		@prior_clause = @rebuild_prior;
		# Remove table aliases from prior clause
		map { s/\s*PRIOR\s*//s; s/[^\s\.=<>!]+\.//s; } @prior_clause;
	}

	my $bkup_query = $str;
	# Construct the initialization query
	$str =~ s/(SELECT\s+)(.*?)(\s+FROM)/$1COLUMN_ALIAS$3/is;
	my @columns = split(/\s*,\s*/, $2);
	# When the pseudo column LEVEL is used in the where clause
	# and not used in columns list, add the pseudo column
	if ($where_clause =~ /\bLEVEL\b/is && !grep(/\bLEVEL\b/i, @columns)) {
		push(@columns, 'level');
	}
	my @tabalias = ();
	my %connect_by_path = ();
	for (my $i = 0; $i <= $#columns; $i++) {
		my $found = 0;
		while ($columns[$i] =~ s/\%SUBQUERY(\d+)\%/$class->{sub_parts}{$1}/is) {
			# Get out of here next run when a call to SYS_CONNECT_BY_PATH is found
			# This will prevent opening too much subquery in the function parameters
			last if ($found);
			$found = 1 if ($columns[$i]=~ /SYS_CONNECT_BY_PATH/is);
		};
		# Replace LEVEL call by a counter, there is no direct equivalent in PostgreSQL
		if (lc($columns[$i]) eq 'level') {
			$columns[$i] = "1 as level";
		} elsif ($columns[$i] =~ /\bLEVEL\b/is) {
			$columns[$i] =~ s/\bLEVEL\b/1/is;
		}
		# Replace call to SYS_CONNECT_BY_PATH by the right concatenation string
		if ($columns[$i] =~ s/SYS_CONNECT_BY_PATH\s*[\(]*\s*([^,]+),\s*([^\)]+)\s*\)/$1/is) {
			my $col = $1;
			$connect_by_path{$col}{sep} = $2;
			# get the column alias
			if ($columns[$i] =~ /\s+([^\s]+)\s*$/s) {
				$connect_by_path{$col}{alias} = $1;
			}
		}
		if ($columns[$i] =~ /([^\.]+)\./s) {
			push(@tabalias, $1) if (!grep(/^\Q$1\E$/i, @tabalias));
		}
		extract_subpart($class, \$columns[$i]);

		# Append parenthesis on new subqueries values
		foreach my $z (sort {$a <=> $b } keys %{$class->{sub_parts}}) {
			next if ($class->{sub_parts}{$z} =~ /^\(/ || $class->{sub_parts}{$z} =~ /^TABLE[\(\%]/i);
			# If subpart is not empty after transformation
			if ($class->{sub_parts}{$z} =~ /\S/is) { 
				# add open and closed parenthesis 
				$class->{sub_parts}{$z} = '(' . $class->{sub_parts}{$z} . ')';
			} elsif ($statements[$i] !~ /\s+(WHERE|AND|OR)\s*\%SUBQUERY$z\%/is) {
				# otherwise do not report the empty parenthesis when this is not a function
				$class->{sub_parts}{$z} = '(' . $class->{sub_parts}{$z} . ')';
			}
		}
	}

	# Extraction of the table aliases in the FROM clause
	my $cols = join(',', @columns);
	$str =~ s/COLUMN_ALIAS/$cols/s;
	if ($str =~ s/(\s+FROM\s+)(.*)/$1FROM_CLAUSE/is) {
		my $from_clause = $2;
		$str =~ s/FROM_CLAUSE/$from_clause/;
	}

	# Now append the UNION ALL query that will be called recursively
	if ($str =~ s/^(\s*BEGIN\s+(?:.*)?(?:\s+))(SELECT\s+)/$2/is) {
		$final_query = "$1$final_query";
		$final_query .= $str;
		$bkup_query =~ s/^(\s*BEGIN\s+(?:.*)?(?:\s+))(SELECT\s+)/$2/is;
	} else {
		$final_query .= $str;
	}
	$final_query .= ' WHERE ' . $start_with . "\n" if ($start_with);
	#$where_clause =~ s/^\s*WHERE\s+/ AND /is;
	#$final_query .= $where_clause . "\n";
	$final_query .= "  UNION ALL\n";
	if ($siblings && !$order_by) {
		$final_query =~ s/(\s+FROM\s+)/,ARRAY[ row_number() OVER (ORDER BY $siblings) ] as hierarchy$1/is;
	} elsif ($siblings) {

		$final_query =~ s/(\s+FROM\s+)/,ARRAY[ row_number() OVER (ORDER BY $order_by) ] as hierarchy$1/is;
	}
	$bkup_query =~ s/(SELECT\s+)(.*?)(\s+FROM)/$1COLUMN_ALIAS$3/is;
	@columns = split(/\s*,\s*/, $2);
	# When the pseudo column LEVEL is used in the where clause
	# and not used in columns list, add the pseudo column
	if ($where_clause =~ /\bLEVEL\b/is && !grep(/\bLEVEL\b/i, @columns)) {
		push(@columns, 'level');
	}
	for (my $i = 0; $i <= $#columns; $i++) {
		my $found = 0;
		while ($columns[$i] =~ s/\%SUBQUERY(\d+)\%/$class->{sub_parts}{$1}/is) {
			# Get out of here when a call to SYS_CONNECT_BY_PATH is found
			# This will prevent opening subquery in the function parameters
			last if ($found);
			$found = 1 if ($columns[$i]=~ /SYS_CONNECT_BY_PATH/is);
		};
		if ($columns[$i] =~ s/SYS_CONNECT_BY_PATH\s*[\(]*\s*([^,]+),\s*([^\)]+)\s*\)/$1/is) {
			$columns[$i] = "c.$connect_by_path{$1}{alias} || $connect_by_path{$1}{sep} || " . $columns[$i];
		}
		if ($columns[$i] !~ s/\b[^\.]+\.LEVEL\b/(c.level+1)/igs) {
			$columns[$i] =~ s/\bLEVEL\b/(c.level+1)/igs;
		}
		extract_subpart($class, \$columns[$i]);

		# Append parenthesis on new subqueries values
		foreach my $z (sort {$a <=> $b } keys %{$class->{sub_parts}}) {
			next if ($class->{sub_parts}{$z} =~ /^\(/ || $class->{sub_parts}{$z} =~ /^TABLE[\(\%]/i);
			# If subpart is not empty after transformation
			if ($class->{sub_parts}{$z} =~ /\S/is) { 
				# add open and closed parenthesis 
				$class->{sub_parts}{$z} = '(' . $class->{sub_parts}{$z} . ')';
			} elsif ($statements[$i] !~ /\s+(WHERE|AND|OR)\s*\%SUBQUERY$z\%/is) {
				# otherwise do not report the empty parenthesis when this is not a function
				$class->{sub_parts}{$z} = '(' . $class->{sub_parts}{$z} . ')';
			}
		}
	}
	$cols = join(',', @columns);
	$bkup_query =~ s/COLUMN_ALIAS/$cols/s;
	my $prior_alias = '';
	if ($bkup_query =~ s/(\s+FROM\s+)(.*)/$1FROM_CLAUSE/is) {
		my $from_clause = $2;
		if ($from_clause =~ /\b[^\s]+\s+(?:AS\s+)?([^\s]+)\b/) {
			my $a = $1;
			$prior_alias = "$a." if (!grep(/\b$a\.[^\s]+$/, @prior_clause));
		}
		$bkup_query =~ s/FROM_CLAUSE/$from_clause/;
	}

	# Remove last subquery alias in the from clause to put our own 
	$bkup_query =~ s/(\%SUBQUERY\d+\%)\s+[^\s]+\s*$/$1/is;
	if ($siblings && $order_by) {
		$bkup_query =~ s/(\s+FROM\s+)/, array_append(c.hierarchy, row_number() OVER (ORDER BY $order_by))  as hierarchy$1/is;
	} elsif ($siblings) {
		$bkup_query =~ s/(\s+FROM\s+)/, array_append(c.hierarchy, row_number() OVER (ORDER BY $siblings))  as hierarchy$1/is;
	}
	$final_query .= $bkup_query;
	map { s/^\s*(.*?)(=\s*)(.*)/c\.$1$2$prior_alias$3/s; } @prior_clause;
	map { s/\s+$//s; s/^\s+//s; } @prior_clause;
	$final_query .= " JOIN cte c ON (" . join(' AND ', @prior_clause) . ")\n";
	if ($siblings) {
		$order_by = " ORDER BY hierarchy";
	} elsif ($order_by) {
		$order_by =~ s/^, //s;
		$order_by = " ORDER BY $order_by";
	}
	$final_query .= "\n) SELECT * FROM cte$where_clause$union$group_by$order_by";

	return $final_query;
}

sub replace_without_function
{
	my ($class, $str) = @_;

	# Code disabled because it break other complex GROUP BY clauses
	# Keeping it just in case some light help me to solve this problem
	# Reported in issue #496
	# Remove text constant in GROUP BY clause, this is not allowed
	# GROUP BY ?TEXTVALUE10?, %%REPLACEFCT1%%, DDI.LEGAL_ENTITY_ID
	#if ($str =~ s/(\s+GROUP\s+BY\s+)(.*?)((?:(?=\bUNION\b|\bORDER\s+BY\b|\bLIMIT\b|\bINTO\s+|\bFOR\s+UPDATE\b|\bPROCEDURE\b).)+|$)/$1\%GROUPBY\% $3/is) {
	#	my $tmp = $2;
	#	$tmp =~ s/\?TEXTVALUE\d+\?[,]*\s*//gs;
	#	$tmp =~ s/(\s*,\s*),\s*/$1/gs;
	#	$tmp =~ s/\s*,\s*$//s;
	#	$str =~ s/\%GROUPBY\%/$tmp/s;
	#}

	return $str;
}

1;

__END__


=head1 AUTHOR

Gilles Darold <gilles@darold.net>


=head1 COPYRIGHT

Copyright (c) 2000-2022 Gilles Darold - All rights reserved.

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

