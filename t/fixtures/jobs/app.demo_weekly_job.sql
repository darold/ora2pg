BEGIN
  SYS.DBMS_SCHEDULER.CREATE_JOB
    (
       job_name        => 'APP.DEMO_WEEKLY_JOB'
      ,repeat_interval => 'FREQ=WEEKLY;BYDAY=MON'
      ,job_type        => 'STORED_PROCEDURE'
      ,job_action      => 'demo_pkg.weekly'
    );
END;
/
