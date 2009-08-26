/*******************************************************************
 * @(#)$RCSfile: oracleMonitoring.sql,v $ $Revision: 1.8 $ $Date: 2009/07/05 13:46:14 $ $Author: waldron $
 * PL/SQL code for stager monitoring
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* CastorMon Package Specification */
CREATE OR REPLACE PACKAGE CastorMon AS

  /**
   * This procedure generates statistics on the status of all diskcopies. The
   * results are grouped by DiskServer, MountPoint and DiskCopy status.
   *
   * @param interval The frequency at which the data is refreshed.
   */
  PROCEDURE diskCopyStats(interval IN NUMBER);

  /**
   * This procedure generates statistics on files which are waiting to be
   * migrated to tape.
   * @param interval The frequency at which the data is refreshed.
   */
  PROCEDURE waitTapeMigrationStats(interval IN NUMBER);

  /**
   * This procedure generates statistics on files which are waiting to be
   * recalled from tape.
   * @param interval The frequency at which the data is refreshed.
   */
  PROCEDURE waitTapeRecallStats(interval IN NUMBER);

END CastorMon;
/


/* CastorMon Package Body */
CREATE OR REPLACE PACKAGE BODY CastorMon AS

  /**
   * PL/SQL method implementing diskCopyStats
   * See the castorMon package specification for documentation.
   */
  PROCEDURE diskCopyStats(interval IN NUMBER) AS
  BEGIN
    -- Truncate the MonDiskCopyStats table
    EXECUTE IMMEDIATE 'DELETE FROM MonDiskCopyStats';
    -- Populate the MonDiskCopyStats table
    INSERT INTO MonDiskCopyStats
      (timestamp, interval, diskServer, mountPoint, dsStatus, fsStatus,
       available, status, totalFileSize, nbFiles)
      -- Gather data
      SELECT sysdate timestamp, interval, a.name diskServer, a.mountPoint,
             a.dsStatus, a.fsStatus, a.available, a.statusName status,
             nvl(b.totalFileSize, 0) totalFileSize, nvl(b.nbFiles, 0) nbFiles
        FROM (
          -- Produce a matrix of all possible diskservers, filesystems and
          -- diskcopy states.
          SELECT DiskServer.name, FileSystem.mountPoint, FileSystem.id,
                 (SELECT statusName FROM ObjStatus
                   WHERE object = 'DiskServer'
                     AND field = 'status'
                     AND statusCode = DiskServer.status) dsStatus,
                 (SELECT statusName FROM ObjStatus
                   WHERE object = 'FileSystem'
                     AND field = 'status'
                     AND statusCode = FileSystem.status) fsStatus,
                 decode(DiskServer.status, 2, 'N',
                   decode(FileSystem.status, 2, 'N', 'Y')) available,
                 ObjStatus.statusName
            FROM DiskServer, FileSystem, ObjStatus
           WHERE FileSystem.diskServer = DiskServer.id
             AND ObjStatus.object(+) = 'DiskCopy'
             AND ObjStatus.field = 'status'
             AND ObjStatus.statusCode IN (0, 4, 5, 6, 7, 9, 10, 11)) a
        -- Attach the aggregation information for all filesystems to the results
        -- extracted above.
        LEFT JOIN (
          SELECT DiskCopy.fileSystem, ObjStatus.statusName,
                 sum(DiskCopy.diskCopySize) totalFileSize, count(*) nbFiles
            FROM DiskCopy, ObjStatus
           WHERE DiskCopy.status = ObjStatus.statusCode
             AND ObjStatus.object = 'DiskCopy'
             AND ObjStatus.field = 'status'
             AND DiskCopy.status IN (0, 4, 5, 6, 7, 9, 10, 11)
           GROUP BY DiskCopy.fileSystem, ObjStatus.statusName) b
          ON (a.id = b.fileSystem AND a.statusName = b.statusName)
       ORDER BY a.name, a.mountPoint, a.statusName;
  END diskCopyStats;


  /**
   * PL/SQL method implementing waitTapeMigrationStats
   * See the castorMon package specification for documentation.
   */
  PROCEDURE waitTapeMigrationStats(interval IN NUMBER) AS
  BEGIN
    -- Truncate the MonWaitTapeMigrationStats table
    EXECUTE IMMEDIATE 'DELETE FROM MonWaitTapeMigrationStats';
    -- Populate the MonWaitTapeMigrationStats table
    INSERT INTO MonWaitTapeMigrationStats
      (timestamp, interval, svcClass, status, minWaitTime, maxWaitTime,
       avgWaitTime, minFileSize, maxFileSize, avgFileSize, bin_LT_1,
       bin_1_To_6, bin_6_To_12, bin_12_To_24, bin_24_To_48, bin_GT_48,
       totalFileSize, nbFiles)
      -- Gather data
      SELECT sysdate timestamp, interval, b.svcClass, nvl(b.status, '-') status,
             -- File age statistics
             round(nvl(min(a.waitTime), 0), 0) minWaitTime,
             round(nvl(max(a.waitTime), 0), 0) maxWaitTime,
             round(nvl(avg(a.waitTime), 0), 0) avgWaitTime,
             -- File size statistics
             round(nvl(min(a.diskCopySize), 0), 0) minFileSize,
             round(nvl(max(a.diskCopySize), 0), 0) maxFileSize,
             round(nvl(avg(a.diskCopySize), 0), 0) avgFileSize,
             -- Wait time stats
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 1     AND 3600
                      THEN 1 ELSE 0 END) BIN_LT_1,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 3600  AND 21600
                      THEN 1 ELSE 0 END) BIN_1_TO_6,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 21600 AND 43200
                      THEN 1 ELSE 0 END) BIN_6_TO_12,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 43200 AND 86400
                      THEN 1 ELSE 0 END) BIN_12_TO_24,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 86400 AND 172800
                      THEN 1 ELSE 0 END) BIN_24_TO_48,
             sum(CASE WHEN nvl(a.waitTime, 0) > 172800
                      THEN 1 ELSE 0 END) BIN_GT_48,
             -- Summary values
             nvl(sum(a.diskCopySize), 0) totalFileSize, nvl(sum(a.found), 0) nbFiles
        FROM (
          -- Determine the service class of all tapecopies and their associated
          -- status.
          SELECT /*+ USE_NL(TapeCopy DiskCopy CastorFile) */ 
                 SvcClass.name svcClass,
                 decode(sign(TapeCopy.status - 2), -1, 'PENDING',
                   decode(TapeCopy.status, 6, 'FAILED', 'SELECTED')) status,
                 (getTime() - DiskCopy.creationTime) waitTime,
                 DiskCopy.diskCopySize, 1 found
            FROM DiskCopy, CastorFile, TapeCopy, SvcClass 
           WHERE DiskCopy.castorFile = CastorFile.id
             AND CastorFile.id = TapeCopy.castorFile
             AND CastorFile.svcClass = SvcClass.id
             AND decode(DiskCopy.status, 10, DiskCopy.status, NULL) = 10  -- CANBEMIGR
             AND TapeCopy.status IN (0, 1, 2, 3, 6) -- CREATED, TOBEMIGRATED, WAITINSTREAMS,
                                                    -- SELECTED, FAILED
        ) a
        -- Attach a list of all service classes and possible states (PENDING,
        -- SELECTED) to the results above.
        RIGHT JOIN (
          SELECT SvcClass.name svcClass, a.status
            FROM SvcClass,
             (SELECT 'PENDING'  status FROM Dual UNION ALL
              SELECT 'SELECTED' status FROM Dual UNION ALL
              SELECT 'FAILED'   status FROM Dual) a) b
           ON (a.svcClass = b.svcClass AND a.status = b.status)
       GROUP BY GROUPING SETS (b.svcClass, b.status), (b.svcClass)
       ORDER BY b.svcClass, b.status;
  END waitTapeMigrationStats;


  /**
   * PL/SQL method implementing waitTapeRecallStats
   * See the castorMon package specification for documentation.
   */
  PROCEDURE waitTapeRecallStats(interval IN NUMBER) AS
  BEGIN
    -- Truncate the MonWaitTapeRecallStats table
    EXECUTE IMMEDIATE 'DELETE FROM MonWaitTapeRecallStats';
    -- Populate the MonWaitTapeRecallStats table
    INSERT INTO MonWaitTapeRecallStats
      (timestamp, interval, svcClass, minWaitTime, maxWaitTime, avgWaitTime,
       minFileSize, maxFileSize, avgFileSize, bin_LT_1, bin_1_To_6, bin_6_To_12,
       bin_12_To_24, bin_24_To_48, bin_GT_48, totalFileSize, nbFiles)
      -- Gather data
      SELECT sysdate timestamp, interval, SvcClass.name svcClass,
             -- File age statistics
             round(nvl(min(a.waitTime), 0), 0) minWaitTime,
             round(nvl(max(a.waitTime), 0), 0) maxWaitTime,
             round(nvl(avg(a.waitTime), 0), 0) avgWaitTime,
             -- File size statistics
             round(nvl(min(a.fileSize), 0), 0) minFileSize,
             round(nvl(max(a.fileSize), 0), 0) maxFileSize,
             round(nvl(avg(a.fileSize), 0), 0) avgFileSize,
             -- Wait time stats
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 1     AND 3600
                      THEN 1 ELSE 0 END) BIN_LT_1,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 3600  AND 21600
                      THEN 1 ELSE 0 END) BIN_1_TO_6,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 21600 AND 43200
                      THEN 1 ELSE 0 END) BIN_6_TO_12,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 43200 AND 86400
                      THEN 1 ELSE 0 END) BIN_12_TO_24,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 86400 AND 172800
                      THEN 1 ELSE 0 END) BIN_24_TO_48,
             sum(CASE WHEN nvl(a.waitTime, 0) > 172800
                      THEN 1 ELSE 0 END) BIN_GT_48,
             -- Summary values
             nvl(sum(a.fileSize), 0) totalFileSize, nvl(sum(a.found), 0) nbFiles
        FROM (
          -- Determine the list of ongoing a. We need to join with the
          -- SubRequest and Request tables here to work out the service class
          -- as the DiskCopy filesystem pointer is 0 until the file is
          -- successfully recalled.
          SELECT Request.svcClassName svcClass, CastorFile.fileSize,
                 (getTime() - DiskCopy.creationTime) waitTime, 1 found
            FROM DiskCopy, SubRequest, CastorFile,
              (SELECT id, svcClassName FROM StageGetRequest UNION ALL
               SELECT id, svcClassName FROM StagePrepareToGetRequest UNION ALL
               SELECT id, svcClassName FROM StageUpdateRequest UNION ALL
               SELECT id, svcClassName FROM StageRepackRequest) Request
           WHERE DiskCopy.id = SubRequest.diskCopy
             AND DiskCopy.status = 2  -- DISKCOPY_WAITTAPERECALL
             AND DiskCopy.castorFile = CastorFile.id
             AND SubRequest.status = 4  -- SUBREQUEST_WAITTAPERECALL
             AND SubRequest.parent = 0
             AND SubRequest.request = Request.id
        ) a
        -- Attach a list of all service classes
        RIGHT JOIN SvcClass
           ON SvcClass.name = a.svcClass
        GROUP BY SvcClass.name
        ORDER BY SvcClass.name;
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
      JOB_NAME        => 'monitoringJob_60mins',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'DECLARE
                            interval NUMBER := 3600;
                          BEGIN
                            castorMon.diskCopyStats(interval);
                            castorMon.waitTapeMigrationStats(interval);
                            castorMon.waitTapeRecallStats(interval);
                          END;',
      JOB_CLASS       => 'CASTOR_MON_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=60',
      ENABLED         => TRUE,
      COMMENTS        => 'Generation of monitoring information');
END;
/

