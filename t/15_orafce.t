use utf8;
use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use DblinkTest qw(convert norm);

binmode(Test::More->builder->$_, ':utf8') for qw(output failure_output todo_output);

# Everything here runs with USE_ORAFCE 1: orafce provides the functions, ora2pg must
# emit calls that really work against orafce 4.17 (checked on PG 18).
sub conv { my ($s, %o) = @_; my ($out) = convert($s, use_orafce => 1, %o); return norm($out); }

# GREATEST/LEAST are SQL keywords in PG (they skip NULLs); Oracle returns NULL,
# oracle.greatest/least do too, so the schema prefix is mandatory.
like(conv('x := GREATEST(a, b);'), qr/\bx := oracle\.GREATEST\(a, b\)/i, 'greatest prefixed');
like(conv('x := least(a, b, c);'), qr/\boracle\.least\(a, b, c\)/i, 'least prefixed');
unlike(conv('x := oracle.greatest(a, b);'), qr/oracle\.oracle\./i, 'no double prefix');
unlike(conv('x := my_greatest(a, b);'), qr/oracle\./i, 'identifier ending in greatest untouched');

# SYS_CONTEXT: string literals are masked at this stage
like(conv(q{x := SYS_CONTEXT('USERENV','CURRENT_SCHEMA');}), qr/x := current_schema\b/, 'sys_context current_schema');
like(conv(q{x := SYS_CONTEXT('USERENV', 'IP_ADDRESS');}), qr/x := inet_client_addr\(\)/, 'sys_context ip_address');
like(conv(q{x := lower(SYS_CONTEXT('USERENV','SESSION_USER')) || lower(SYS_CONTEXT('USERENV','DB_NAME'));}),
	qr/lower\(session_user\) \|\| lower\(current_database\(\)\)/, 'two sys_context in one statement (/g)');
like(conv(q{x := SYS_CONTEXT('MYNS','PARAM');}), qr/SYS_CONTEXT\('MYNS','PARAM'\)/, 'unknown namespace left alone');

# RR / RRRR: PG and orafce do not know them (to_char prints a literal RR)
like(conv(q{x := TO_CHAR(d, 'dd.mm.rr');}), qr/TO_CHAR\(d, 'dd\.mm\.YY'\)/, 'to_char rr -> YY');
like(conv(q{x := TO_CHAR(d, 'DD.MM.RRRR');}), qr/'DD\.MM\.YYYY'/, 'to_char rrrr -> YYYY');
like(conv(q{x := TO_DATE(s, 'dd.mm.rr');}), qr/TO_DATE\(s, ?'dd\.mm\.YY'\)/i, 'to_date rr -> YY');
like(conv(q{x := TO_CHAR(d, 'DD.MM.YYYY');}), qr/'DD\.MM\.YYYY'/, 'plain format untouched');
like(conv(q{x := TO_CHAR(d, 'DD "ERROR" YYYY');}), qr/'DD "ERROR" YYYY'/, 'RR inside a word untouched');

# NLS_DATE_LANGUAGE=RUSSIAN: names go through TM* so lc_time decides the language
like(conv(q{x := TO_CHAR(d, 'DAY', 'NLS_DATE_LANGUAGE=RUSSIAN');}), qr/TO_CHAR\(d, 'TMDAY'\)/, 'DAY -> TMDAY');
like(conv(q{x := TO_CHAR(d, 'Day, DD Month YYYY', 'NLS_DATE_LANGUAGE=RUSSIAN');}), qr/'TMDay, DD TMMonth YYYY'/, 'Day + Month in one mask');
like(conv(q{x := TO_CHAR(d, 'DY', 'NLS_DATE_LANGUAGE=RUSSIAN');}), qr/'TMDY'/, 'DY -> TMDY');
like(conv(q{x := TO_CHAR(d, 'DAY');}), qr/TO_CHAR\(d, 'DAY'\)/, 'no NLS argument: left alone');

# orafce functions without arguments need () in PL/pgSQL
like(conv('NEW.cs := dbms_utility.format_call_stack;'), qr/dbms_utility\.format_call_stack\(\);?$/i, 'format_call_stack()');
like(conv('t := dbms_utility.get_time;'), qr/get_time\(\)/i, 'get_time()');
unlike(conv('s := dbms_utility.format_call_stack();'), qr/\(\)\(\)/, 'no double parentheses');

# procedures of orafce packages used as statements need PERFORM; functions in expressions do not
like(conv("BEGIN\n dbms_pipe.pack_message(x);\n dbms_pipe.purge(c);\nEND;"), qr/PERFORM dbms_pipe\.pack_message\(x\);.*PERFORM dbms_pipe\.purge\(c\);/i, 'pipe procedures get PERFORM');
unlike(conv("IF dbms_pipe.send_message(c, 3) > 0 THEN null; END IF;"), qr/PERFORM/i, 'function in expression untouched');
unlike(conv("s := dbms_pipe.receive_message(c, 5);"), qr/PERFORM/i, 'assignment untouched');
like(conv("BEGIN dbms_alert.signal('a', 'b'); END;"), qr/PERFORM dbms_alert\.signal/i, 'alert signal');
like(conv("BEGIN dbms_pipe.unpack_message(v_info); END;"), qr/v_info := dbms_pipe\.unpack_message_text\(\).*TODO/i, 'unpack OUT form -> assignment + TODO');

# DBMS_OUTPUT.PUT_LINE accepts only text in orafce
like(conv('dbms_output.put_line(n);'), qr/PERFORM dbms_output\.put_line\(\(n\)::text\)/i, 'variable cast to text');
like(conv(q{dbms_output.put_line('a'||n);}), qr/PERFORM dbms_output\.put_line\('a'\|\|n\)/i, 'concatenation not cast');
like(conv(q{dbms_output.put_line('plain');}), qr/PERFORM dbms_output\.put_line\('plain'\)/i, 'literal not cast');
like(conv(q{dbms_output.put_line(to_char(a||b));}), qr/put_line\(\(to_char\(a\|\|b\)\)::text\)/i, 'inner || does not hide a non-text result');

# LNNVL must survive the NVL rule
like(conv('IF LNNVL(a > 1) THEN null; END IF;'), qr/\bLNNVL\(/i, 'lnnvl kept');
like(conv('x := NVL(a, b);'), qr/coalesce\(a, ?b\)/, 'nvl still coalesce');

# integer literal division (Oracle NUMBER is never integer): 14/24 must not be 0
like(conv('x := trunc(d) + 14/24;'), qr{\+ 14/24\.0}, 'literal/literal -> numeric');
like(conv('x := (30/60)/24;'), qr{\(30/60\.0\)/24}, 'only literal/literal is rewritten');
unlike(conv('x := a/24;'), qr{24\.0}, 'column/literal untouched');
unlike(conv('x := 14/24.0;'), qr{24\.0\.0}, 'already numeric: idempotent');
unlike(conv('x := 1.5/2;'), qr{1\.5/2\.0}, 'decimal numerator untouched');

# to_date with a time mask: pg_catalog.to_date (first in search_path) returns date and loses the time
like(conv(q{x := TO_DATE(s, 'DD.MM.YYYY HH24:MI:SS');}), qr/oracle\.to_date\(s, ?'DD\.MM\.YYYY HH24:MI:SS'\)/i, 'time mask -> oracle.to_date');
like(conv(q{x := to_date(s, 'dd.mm.yyyy hh24:mi');}), qr/oracle\.to_date/i, 'lower case mask');
unlike(conv(q{x := TO_DATE(s, 'DD.MM.YYYY');}), qr/oracle\./i, 'date-only mask keeps plain to_date');
unlike(conv(q{x := TO_DATE(s, 'DD.MM.YYYY');}), qr/oracle\.oracle/i, 'no double prefix');

# fix_rr_date_format on an unmasked literal (callers normally pass masked ones)
{ my $o = bless { text_values => {} }, 'Ora2Pg';
  is(Ora2Pg::PLSQL::fix_rr_date_format($o, q{TO_CHAR(d, 'dd.mm.rr')}), q{TO_CHAR(d, 'dd.mm.YY')}, 'unmasked rr'); }

# exact integer division stays integer (substr/lpad arguments must not become numeric)
like(conv('x := substr(s, 20/2);'), qr{20/2\)}, 'exact division untouched');

done_testing;
