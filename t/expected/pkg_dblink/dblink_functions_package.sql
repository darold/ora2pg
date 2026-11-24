CREATE SCHEMA IF NOT EXISTS fdw_rdb1_rsch_rem_iface_pkg;
CREATE OR REPLACE FUNCTION fdw_rdb1_rsch_rem_iface_pkg.verify_state(p_ref text)
RETURNS text LANGUAGE sql AS $fn$
  SELECT oracle_scalar('rdb1', $ora$SELECT rsch.rem_iface_pkg.verify_state(:1) FROM dual$ora$,
    ARRAY[$1::text])::text
$fn$;
REVOKE ALL ON FUNCTION fdw_rdb1_rsch_rem_iface_pkg.verify_state(text) FROM PUBLIC;
