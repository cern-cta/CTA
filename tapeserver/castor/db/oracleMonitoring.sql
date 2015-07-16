/*******************************************************************
 * PL/SQL code for stager monitoring
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* CastorMon Package Specification */
CREATE OR REPLACE PACKAGE CastorMon AS
  -- Backlog statistics for migrations, grouped by TapePool
  PROCEDURE waitTapeMigrationStats;
  -- Backlog statistics for recalls, grouped by SvcClass
  PROCEDURE waitTapeRecallStats;
END CastorMon;
/


/* CastorMon Package Body */
CREATE OR REPLACE PACKAGE BODY CastorMon AS

  /**
   * PL/SQL method implementing waitTapeMigrationStats
   * See the castorMon package specification for documentation.
   */
  PROCEDURE waitTapeMigrationStats AS
    CURSOR rec IS
      SELECT tapePool tapePool,
             nvl(sum(fileSize), 0) totalFileSize, 
             count(fileSize) nbFiles
      FROM (
        SELECT
               TapePool.name tapePool,
               MigrationJob.filesize fileSize
        FROM MigrationJob, TapePool
        WHERE TapePool.id = MigrationJob.tapePool(+) -- Left outer join to have zeroes when there is no migrations
      )
      GROUP BY tapePool;
  BEGIN
    FOR r in rec LOOP
      logToDLF(NULL, dlf.LVL_SYSTEM, 'waitTapeMigrationStats', 0, '', 'stagerd', 
            'TapePool="' || r.tapePool ||
            '" totalFileSize="' || r.totalFileSize || 
            '" nbFiles="' || r.nbFiles || '"');
    END LOOP;
  END waitTapeMigrationStats;

  /**
   * PL/SQL method implementing waitTapeRecallStats
   * See the castorMon package specification for documentation.
   */
  PROCEDURE waitTapeRecallStats AS
    CURSOR rec IS
      SELECT svcClass svcClass,
             nvl(sum(fileSize), 0) totalFileSize,
             count(fileSize) nbFiles
      FROM (
        SELECT
               SvcClass.name svcClass,
               RecallJob.filesize fileSize
        FROM RecallJob, SvcClass
        WHERE SvcClass.id = RecallJob.SvcClass(+) -- Left outer join to have zeroes when there is no recall
      )
      GROUP BY svcClass;
  BEGIN
    FOR r in rec LOOP
      logToDLF(NULL, dlf.LVL_SYSTEM, 'waitTapeRecallStats', 0, '', 'stagerd', 
            'SvcClass="' || r.svcClass ||
            '" totalFileSize="' || r.totalFileSize || 
            '" nbFiles="' || r.nbFiles || '"');
    END LOOP;
  END waitTapeRecallStats;

END CastorMon;
/


/* Database jobs */
BEGIN
  -- Drop all monitoring jobs
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name LIKE ('MONITORINGJOB_%'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Recreate monitoring jobs
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'monitoringJob_1min',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN CastorMon.waitTapeMigrationStats(); CastorMon.waitTapeRecallStats(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/3600,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Generation of monitoring information about migrations and recalls backlog');
END;
/

