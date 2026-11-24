BEGIN
  SYS.DBMS_SCHEDULER.CREATE_JOB
    (
       job_name        => 'APP.DEMO_DAILY_JOB'
      ,repeat_interval => 'FREQ=DAILY;BYHOUR=3;BYMINUTE=0;BYSECOND=0'
      ,job_type        => 'PLSQL_BLOCK'
      ,job_action      => 'BEGIN demo_pkg.refresh(''x''); END;'
    );
END;
/
