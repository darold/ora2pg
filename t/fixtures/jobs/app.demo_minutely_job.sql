BEGIN
  SYS.DBMS_SCHEDULER.CREATE_JOB
    (
       job_name        => 'APP.DEMO_MINUTELY_JOB'
      ,repeat_interval => 'freq=minutely;interval=30; bysecond=0;'
      ,job_type        => 'STORED_PROCEDURE'
      ,job_action      => 'demo_pkg.run_check'
    );
END;
/
