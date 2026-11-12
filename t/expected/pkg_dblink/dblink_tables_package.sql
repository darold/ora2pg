-- TODO dblink RDB1: no columns for RSCH.ITEM_LOG

CREATE SCHEMA IF NOT EXISTS fdw_rdb1_rsch;
CREATE FOREIGN TABLE IF NOT EXISTS fdw_rdb1_rsch.rem_accounts (
	key_code varchar(30) OPTIONS (key 'true'),
	key_name varchar(240),
	active numeric,
	amt decimal(12,2)
) SERVER rdb1 OPTIONS (schema 'RSCH', table 'REM_ACCOUNTS');
