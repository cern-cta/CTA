/******************************************************************************
 *              oracleTrailer.sql
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)RCSfile: oracleCreate.sql,v  Release: 1.2  Release Date: 2009/03/26 13:14:27  Author: waldron
 *
 * This script create a new Monitoring schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/


/* Update the schema version */
UPDATE CastorVersion SET schemaVersion = '2_1_8_10';


/***** EXISTING/OLD MONITORING *****/

/* PL/SQL method implementing statsLatency
 *
 * Provides statistics on the amount of time a user has had to wait since their
 * request was entered into the system and it actually being served. The
 * returned data is broken down by request type and protocol.
 */
CREATE OR REPLACE PROCEDURE statsLatency (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: LatencyStats
  -- Frequency: 5 minutes
  INSERT INTO LatencyStats
    (timestamp, interval, type, protocol, started, minTime, maxTime, avgTime,
     stddevTime, medianTime)
    SELECT now - 5/1440 timestamp, interval,
           nvl(type, 'StageDiskCopyReplicaRequest') type,  protocol,
           count(*) started, min(waitTime) min, max(waitTime) max,
           avg(waitTime) avg, stddev_pop(waitTime) stddev, median(waitTime) median
      FROM (
        SELECT waitTime,
               max(decode(params.name, 'Type',     params.value, NULL)) type,
               max(decode(params.name, 'Protocol', params.value, NULL)) protocol
          FROM (
            -- Extract the totalWaitTime for all stagerJobs or diskCopyTransfers
            -- which have started.
            SELECT params.id, params.value waitTime
              FROM &dlfschema..dlf_messages messages,
                   &dlfschema..dlf_num_param_values params
             WHERE messages.id = params.id
               AND messages.severity = 8 -- System
               AND ((messages.facility = 26 AND messages.msg_no = 20)  -- Job started
                OR  (messages.facility = 23 AND messages.msg_no = 25)) -- DiskCopyTransfer started
               AND messages.timestamp >  sysdate - 10/1440
               AND messages.timestamp <= sysdate - 5/1440
               AND params.name = 'TotalWaitTime'
               AND params.timestamp >  sysdate - 10/1440
               AND params.timestamp <= sysdate - 5/1440
          ) results
      -- For facility 23 (DiskCopyTransfer) we can assume that the request type
      -- associated with the transfer is 133 (StageDiskCopyReplicaRequest).
      -- However, for normal jobs we must parse the Arguments attribute of the
      -- start message to determine the request type. Hence the left join,
      -- NULL's are 133!!
      LEFT JOIN &dlfschema..dlf_str_param_values params
        ON results.id = params.id
       AND params.name IN ('Type', 'Protocol')
       AND params.timestamp >  sysdate - 10/1440
       AND params.timestamp <= sysdate - 5/1440
     GROUP BY waitTime)
     GROUP BY type, protocol
     ORDER BY type, protocol;
END;
/


/* PL/SQL method implementing statsQueueTime
 *
 * Provides statistics on the queue time of requests in LSF broken down by
 * request type and service class.
 */
CREATE OR REPLACE PROCEDURE statsQueueTime (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: QueueTimeStats
  -- Frequency: 5 minutes
  INSERT INTO QueueTimeStats
    (timestamp, interval, type, svcClass, dispatched, minTime, maxTime, avgTime,
     stddevTime, medianTime)
    SELECT now - 5/1440 timestamp, interval, type, svcclass,
           count(*) dispatched,
           nvl(min(params.value), 0) min,
           nvl(max(params.value), 0) max,
           nvl(avg(params.value), 0) avg,
           nvl(stddev_pop(params.value), 0) stddev,
           nvl(median(params.value), 0) median
         FROM (
         -- Extract the type and service class for all jobs dispatched by LSF
         SELECT messages.id,
                max(decode(params.name, 'Type',     params.value, NULL)) type,
                max(decode(params.name, 'SvcClass', params.value, NULL)) svcclass
           FROM &dlfschema..dlf_messages messages,
                &dlfschema..dlf_str_param_values params
          WHERE messages.id = params.id
            AND messages.severity = 8 -- System
            AND messages.facility = 9 -- Scheduler
            AND messages.msg_no = 34  -- Wrote notification file
            AND messages.timestamp >  now - 10/1440
            AND messages.timestamp <= now - 5/1440
            AND params.name IN ('Type', 'SvcClass')
            AND params.timestamp >  now - 10/1440
            AND params.timestamp <= now - 5/1440
          GROUP BY messages.id
       ) results
      -- Attach the QueueTime attribute to the results previously collected.
      -- After this we will have a line for each started job detailing the
      -- service class the job is destined for, the request type and the number
      -- of seconds it spent queued.
      INNER JOIN &dlfschema..dlf_num_param_values params
         ON results.id = params.id
        AND params.name = 'QueueTime'
        AND params.timestamp >  now - 10/1440
        AND params.timestamp <= now - 5/1440
      GROUP BY type, svcclass
      ORDER BY type, svcclass;
END;
/


/* PL/SQL method implementing statsGarbageCollection
 *
 * Provides an overview of the garbage collection process which includes the
 * number of files removed during the last interval, the total volume of
 * reclaimed space and statistical information.
 */
CREATE OR REPLACE PROCEDURE statsGarbageCollection (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: GarbageCollectionStats
  -- Frequency: 5 minutes
  INSERT INTO GarbageCollectionStats
    (timestamp, interval, diskServer, type, deleted, totalSize, minFileAge,
     maxFileAge, avgFileAge, stddevFileAge, medianFileAge, minFileSize,
     maxFileSize, avgFileSize, stddevFileSize, medianFileSize)
    SELECT now - 5/1440 timestamp, interval, diskserver, type,
           count(*) deleted,
           sum(fileSize) totalSize,
           -- File age statistics
           min(fileAge) minFileAge,
           max(fileAge) maxFileAge,
           avg(fileAge) avgFileAge,
           stddev_pop(fileAge) stddevFileAge,
           median(fileAge) medianFileAge,
           -- File size statistics
           min(fileSize) minFileSize,
           max(fileSize) maxFileSize,
           avg(fileSize) avgFileSize,
           stddev_pop(fileSize) stddevFileSize,
           median(fileSize) medianFileSize
      FROM (
        SELECT hostname diskserver, results.fileAge, results.fileSize,
               replace(params.value, ' ', '_') type
          FROM (
            -- Extract the file age, size and service class of all files successfully
            -- removed across all diskservers
            SELECT messages.id, messages.hostid,
                   max(decode(params.name, 'FileAge',  params.value, NULL)) fileAge,
                   max(decode(params.name, 'FileSize', params.value, NULL)) fileSize
              FROM &dlfschema..dlf_messages messages,
                   &dlfschema..dlf_num_param_values params
             WHERE messages.id = params.id
               AND messages.severity = 8 -- System
               AND messages.facility = 8 -- GC
               AND (messages.msg_no = 11 OR -- Removed file successfully
                    messages.msg_no = 27 OR -- Deleting ... nameserver
                    messages.msg_no = 36)   -- Deleting ... stager catalog
               AND messages.timestamp >  now - 10/1440
               AND messages.timestamp <= now - 5/1440
               AND params.name IN ('FileAge', 'FileSize')
               AND params.timestamp >  now - 10/1440
               AND params.timestamp <= now - 5/1440
             GROUP BY messages.id, messages.hostid
          ) results
        -- Attach the gctye to the results collected above
        INNER JOIN &dlfschema..dlf_str_param_values params
           ON results.id = params.id
          AND params.name = 'GcType'
        -- Resolve the hostid to names
        INNER JOIN &dlfschema..dlf_host_map hosts
           ON results.hostid = hosts.hostid)
    GROUP BY diskserver, type
    ORDER BY diskserver, type;
END;
/


/* PL/SQL method implementing statsRequest
 *
 * Provides statistical information on the types of requests recorded by the
 * request handler, the total for all users and a break down of the top 10 users
 * per request type
 */
CREATE OR REPLACE PROCEDURE statsRequest (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: RequestStats
  -- Frequency: 5 minutes
  INSERT INTO RequestStats
    (timestamp, interval, type, hostname, euid, requests)
    SELECT now - 5/1440 timestamp, interval, type, hostname, euid, requests FROM (
      -- For each request type display the top 5 users + the total number of
      -- requests
      SELECT type, euid, hostname, requests,
             RANK() OVER (PARTITION BY type ORDER BY requests DESC, euid ASC) rank
        FROM(
          SELECT params.value type, hosts.hostname, '-' euid, count(*) requests
            FROM &dlfschema..dlf_messages messages,
                 &dlfschema..dlf_str_param_values params,
                 &dlfschema..dlf_host_map hosts
           WHERE messages.id = params.id
             AND messages.severity = 10 -- Monitoring
             AND messages.facility = 4  -- RequestHandler
             AND messages.msg_no = 10   -- Reply sent to client
             AND messages.timestamp >  now - 10/1440
             AND messages.timestamp <= now - 5/1440
             AND params.name = 'Type'
             AND params.timestamp >  now - 10/1440
             AND params.timestamp <= now - 5/1440
             AND messages.hostid = hosts.hostid
           GROUP BY params.value, hosts.hostname
          -- Join the user and summary/aggregate level breakdowns together.
          -- Note: this could probably be done using an analytical function or
          -- grouping set!!!
           UNION
          -- Determine the number of requests made for each request type and
          -- per user over the last sampling period. This gives us a user level
          -- breakdown.
          SELECT results.value type, hostname, TO_CHAR(params.value) euid, count(*) requests
            FROM (
              SELECT params.id, params.value, hosts.hostname
                FROM &dlfschema..dlf_messages messages,
                     &dlfschema..dlf_str_param_values params,
                     &dlfschema..dlf_host_map hosts
               WHERE messages.id = params.id
                 AND messages.severity = 10 -- Monitoring
                 AND messages.facility = 4  -- RequestHandler
                 AND messages.msg_no = 10   -- Reply sent to client
                 AND messages.timestamp >  now - 10/1440
                 AND messages.timestamp <= now - 5/1440
                 AND params.name = 'Type'
                 AND params.timestamp >  now - 10/1440
                 AND params.timestamp <= now - 5/1440
                 AND messages.hostid = hosts.hostid
            ) results
          -- Determine the uid of the user associated with each request
          INNER JOIN &dlfschema..dlf_num_param_values params
             ON results.id = params.id
            AND params.name = 'Euid'
            AND params.timestamp >  now - 10/1440
            AND params.timestamp <= now - 5/1440
          GROUP BY results.value, hostname, params.value)
      ) WHERE rank < 11
     ORDER BY type, requests DESC;
END;
/


/* PL/SQL method implementing statsDiskCachEfficiency
 *
 * Provides an overview of how effectively the disk cache is performing.
 */
CREATE OR REPLACE PROCEDURE statsDiskCacheEfficiency (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: DiskCacheEfficiencyStats
  -- Frequency: 5 minutes

  -- Collect a list of request ids that we are interested in. We dump this list
  -- into a temporary table so that the execution plan of the query afterwards
  -- is optimized
  INSERT INTO CacheEfficiencyHelper
    SELECT messages.reqid
      FROM &dlfschema..dlf_messages messages
     WHERE messages.severity = 10 -- Monitoring
       AND messages.facility = 4  -- RequestHandler
       AND messages.msg_no = 10   -- Reply sent to client
       AND messages.timestamp >  now - 10/1440
       AND messages.timestamp <= now - 5/1440;

  -- Record results
  INSERT INTO DiskCacheEfficiencyStats
    (timestamp, interval, type, svcClass, wait, d2d, recall, staged, waitPerc,
     d2dPerc, recallPerc, stagedPerc, total)
    SELECT now - 5/1440 timestamp, interval, results.*,
           -- Percentages
           (results.Wait   / results.Total) * 100 WaitPerc,
           (results.D2D    / results.Total) * 100 D2DPerc,
           (results.Recall / results.Total) * 100 RecallPerc,
           (results.Staged / results.Total) * 100 StagedPerc
        FROM (
        SELECT type, svcclass,
               nvl(sum(decode(msg_no, 53, requests, 0)), 0) Wait,
               nvl(sum(decode(msg_no, 56, requests, 0)), 0) D2D,
               nvl(sum(decode(msg_no, 57, requests, 0)), 0) Recall,
               nvl(sum(decode(msg_no, 60, requests, 0)), 0) Staged,
               nvl(sum(requests), 0) Total
          FROM (
            SELECT type, svcclass, msg_no, count(*) requests FROM (
              SELECT * FROM (
                -- Get the first message issued for all subrequests of interest.
                -- This will indicate to us whether the request was a hit or a miss
                SELECT msg_no, type, svcclass,
                       RANK() OVER (PARTITION BY subreqid
                              ORDER BY timestamp ASC, timeusec ASC) rank
                  FROM (
                    -- Extract all subrequests processed by the stager that resulted
                    -- in read type access
                    SELECT messages.reqid, messages.subreqid, messages.timestamp,
                           messages.timeusec, messages.msg_no,
                           max(decode(params.name, 'Type',     params.value, NULL)) type,
                           max(decode(params.name, 'SvcClass', params.value, 'default')) svcclass
                      FROM &dlfschema..dlf_messages messages,
                           &dlfschema..dlf_str_param_values params
                     WHERE messages.id = params.id
                       AND messages.severity = 8  -- System
                       AND messages.facility = 22 -- Stager
                       AND messages.msg_no IN (53, 56, 57, 60)
                       AND messages.timestamp >  now - 10/1440
                       AND messages.timestamp <= now - 3/1440
                       AND params.name IN ('Type', 'SvcClass')
                       AND params.timestamp >  now - 10/1440
                       AND params.timestamp <= now - 3/1440
                     GROUP BY messages.reqid, messages.subreqid, messages.timestamp,
                              messages.timeusec, messages.msg_no
                  ) results
                 -- Filter the subrequests so that we only process requests which
                 -- entered the system through the request handler in the last
                 -- sampling interval. This stops us from recounting subrequests
                 -- that were restarted
                 WHERE results.reqid IN
                   (SELECT /*+ CARDINALITY(helper 1000) */ *
                      FROM CacheEfficiencyHelper helper))
             ) WHERE rank = 1
           GROUP BY type, svcclass, msg_no)
         GROUP BY type, svcclass) results;
END;
/


/* PL/SQL method implementing statsMigratedFiles
 *
 * Provides statistical information on the number of files migrated to tape and
 * the total data volume transferred broken down by service class and tape pool.
 */
CREATE OR REPLACE PROCEDURE statsMigratedFiles (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: FilesMigratedStats
  -- Frequency: 5 minutes
  INSERT INTO FilesMigratedStats
    (timestamp, interval, svcClass, tapePool, totalFiles, totalSize)
    SELECT now - 5/1440 timestamp, interval, svcclass, nvl(tapepool, '-') tapepool,
           count(*) files, sum(params.value) totalsize
      FROM (
        -- Extract the messages to indicate when a file has been migrated
        SELECT messages.id,
               max(decode(params.name, 'SVCCLASS', params.value, NULL)) svcclass,
               max(decode(params.name, 'TAPEPOOL', params.value, NULL)) tapepool
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_str_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 8 -- System
           AND messages.facility = 1 -- migrator
           AND messages.msg_no = 55  -- File staged
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name IN ('SVCCLASS', 'TAPEPOOL')
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440
         GROUP BY messages.id
      ) results
      -- Attach the filesize to the previously collected information
     INNER JOIN &dlfschema..dlf_num_param_values params
        ON results.id = params.id
       AND params.name = 'FILESIZE'
       AND params.timestamp >  now - 10/1440
       AND params.timestamp <= now - 5/1440
     GROUP BY GROUPING SETS (svcclass, tapepool), (svcclass);
END;
/


/* PL/SQL method implementing statsReplication
 *
 * Provides statistical information on disk copy replication requests both
 * across service classes and internally within the same service class.
 */
CREATE OR REPLACE PROCEDURE statsReplication (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: ReplicationStats
  -- Frequency: 5 minutes
  INSERT INTO ReplicationStats
    (timestamp, interval, sourceSvcClass, destSvcClass, transferred, totalSize,
     minSize, maxSize, avgSize, stddevSize, medianSize)
    SELECT now - 5/1440 timestamp, interval, src, dest, count(*) transferred,
           sum(params.value) totalsize, min(params.value) min, max(params.value) max,
           avg(params.value) avg, stddev_pop(params.value) stddev, median(params.value) median
      FROM (
        SELECT params.id,
               substr(params.value, 0, instr(params.value, '->', 1) - 2) src,
               substr(params.value, instr(params.value, '->', 1) + 3) dest
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_str_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 8  -- System
           AND messages.facility = 23 -- DiskCopyTransfer
           AND messages.msg_no = 39   -- DiskCopy Transfer successful
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name = 'Direction'
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440
      ) results
     -- Attach the size of the file to each replication request. As a result of
     -- this we will have one line per request detailing the direction of the
     -- transfer and the amount of data transferred
     INNER JOIN &dlfschema..dlf_num_param_values params
        ON results.id = params.id
       AND params.name = 'FileSize'
       AND params.timestamp >  now - 10/1440
       AND params.timestamp <= now - 5/1440
     GROUP BY src, dest;
END;
/


/* PL/SQL method implementing statsTapeRecalled
 *
 * Provides statistical information on who triggered a tape recall, how many
 * files were requested, the status of the tape as the request was processed and
 * the type of request that triggered the recall.
 *
 * Example output:
 *   Type                     Username Groupname TapeVID TapeStatus   Files MountsPerDay
 *   StagePrepareToGetRequest waldron  c3        I10488	 TAPE_PENDING 10    0
 *   StagePrepareToGetRequest waldron  c3        I10487	 TAPE_PENDING 8     0
 *   StagePrepareToGetRequest waldron  c3        I10486	 TAPE_PENDING 2     0
 *   StagePrepareToGetRequest waldron  c3        I06983	 TAPE_PENDING 854   0
 */
CREATE OR REPLACE PROCEDURE statsTapeRecalled (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: TapeRecalledStats
  -- Frequency: 5 minutes

  -- Populate the TapeMountsHelper table with a line for every recaller that
  -- has been started in the last sampling interval. This will act as a summary
  -- table
  INSERT INTO TapeMountsHelper
    SELECT messages.timestamp, messages.tapevid
      FROM &dlfschema..dlf_messages messages
     WHERE messages.severity = 8 -- System
       AND messages.facility = 2 -- Recaller
       AND messages.msg_no = 13  -- Recaller started
       AND messages.subreqid <> '00000000-0000-0000-0000-000000000000'
       AND messages.timestamp >  now - 10/1440
       AND messages.timestamp <= now - 5/1440;

  -- Record results
  INSERT INTO TapeRecalledStats
    (timestamp, interval, type, username, groupname, tapeVID, tapeStatus, files,
     totalSize, mountsPerDay)
    SELECT now - 5/1440 timestamp, interval, type, username, groupname, results.tapevid,
           tapestatus, count(*) files, sum(params.value) totalsize,
           max(nvl(mounts.mounted, 0)) mounted
      FROM (
        -- Extract all requests from the stager which triggered a tape recall
        -- including the request type, username and groupname associated with
        -- that request
        SELECT messages.id, messages.tapevid,
               max(decode(params.name, 'Type',       params.value, NULL)) type,
               max(decode(params.name, 'Username',   params.value, NULL)) username,
               max(decode(params.name, 'Groupname',  params.value, NULL)) groupname,
               max(decode(params.name, 'TapeStatus', params.value, NULL)) tapestatus
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_str_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 8  -- System
           AND messages.facility = 22 -- Stager
           AND messages.msg_no = 57   -- Triggering Tape Recall
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name IN ('Type', 'Username', 'Groupname', 'TapeStatus')
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440
         GROUP BY messages.id, messages.tapevid
      ) results
     -- Attach the file size to be recalled
     INNER JOIN &dlfschema..dlf_num_param_values params
        ON results.id = params.id
       AND params.name = 'FileSize'
       AND params.timestamp >  now - 10/1440
       AND params.timestamp <= now - 5/1440
     -- Attached the number of mounts which took place for the tape over the
     -- last 24 hours
     LEFT JOIN (
       SELECT helper.tapevid, count(*) mounted
         FROM TapeMountsHelper helper
        WHERE helper.timestamp > (now - 1) - 5/1440
        GROUP BY helper.tapevid) mounts
       ON results.tapevid = mounts.tapevid
     GROUP BY type, username, groupname, results.tapevid, tapestatus;
END;
/


/* PL/SQL method implementing statsProcessingTime
 *
 * Provides statistics on the processing time in seconds of requests in the
 * Stager and RequestHandler daemons
 */
CREATE OR REPLACE PROCEDURE statsProcessingTime (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: ProcessingTimeStats
  -- Frequency: 5 minutes
  INSERT INTO ProcessingTimeStats
    (timestamp, interval, daemon, type, requests, minTime, maxTime, avgTime,
     stddevTime, medianTime)
    SELECT now - 5/1440 timestamp, interval, facility.fac_name daemon, params.value type,
           count(*) requests,
           min(results.value) min,
           max(results.value) max,
           avg(results.value) avg,
           stddev_pop(results.value) stddev,
           median(results.value) median
      FROM (
        -- Extract all the processing time values for the Stager
        SELECT messages.id, messages.facility, params.value
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_num_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 10 -- Monitoring
           AND messages.facility = 22 -- Stager
           AND messages.msg_no = 25   -- Request processed
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name = 'ProcessingTime'
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440
         UNION
        -- Extract all the processing time values for the RequestHandler
        SELECT messages.id, messages.facility, params.value
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_num_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 10 -- Monitoring
           AND messages.facility = 4  -- RequestHandler
           AND messages.msg_no = 10   -- Reply sent to client
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name = 'ElapsedTime'
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440
      ) results
     -- Attach the request type
     INNER JOIN &dlfschema..dlf_str_param_values params
        ON results.id = params.id
       AND params.name = 'Type'
       AND params.timestamp >  now - 10/1440
       AND params.timestamp <= now - 5/1440
     -- Resolve the facility number to a name
     INNER JOIN &dlfschema..dlf_facilities facility
        ON results.facility = facility.fac_no
     GROUP BY facility.fac_name, params.value;
END;
/


/* PL/SQL method implementing statsClientVersion
 *
 * Provides statistics on the different client versions seen by the
 * RequestHandler
 */
CREATE OR REPLACE PROCEDURE statsClientVersion (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: ClientVersionStats
  -- Frequency: 5 minutes
  INSERT INTO ClientVersionStats
    (timestamp, interval, clientVersion, requests)
    SELECT now - 5/1440 timestamp, interval, clientVersion, count(*) requests
      FROM (
        SELECT nvl(params.value, 'Unknown') clientVersion
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_str_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 10 -- Monitoring
           AND messages.facility = 4  -- RequestHandler
           AND messages.msg_no = 10   -- Reply sent to client
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name = 'ClientVersion'
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440)
     GROUP BY clientVersion;
END;
/


/***** NEW MONITORING *****/

/* PL/SQL method implementing statsTotalLat */
CREATE OR REPLACE PROCEDURE statsTotalLat(maxTimeStamp IN DATE)
AS
BEGIN
  -- Extract total latency info from job started summary message
  INSERT INTO TotalLatency (subReqId, timestamp, nsfileid, totalLatency)
    (SELECT DISTINCT a.subReqId, a.timestamp, a.nsfileid, b.value
       FROM &dlfschema..dlf_messages a, &dlfschema..dlf_num_param_values b
      WHERE a.id = b.id
        AND a.facility = 26 -- Job
        AND a.msg_no = 20   -- Job Started
        AND a.timestamp >= maxTimeStamp
        AND b.timestamp >= maxTimeStamp
        AND a.timestamp < maxTimeStamp + 5/1440
        AND b.timestamp < maxTimeStamp + 5/1440)
  LOG ERRORS INTO Err_Latency REJECT LIMIT 100000;
END;
/


/* PL/SQL method implementing statsDiskCopy */
CREATE OR REPLACE PROCEDURE statsDiskCopy(maxTimeStamp IN DATE)
AS
BEGIN
  -- Info about external file replication: source - target SvcClass
  INSERT INTO DiskCopy
    (nsfileid, timestamp, originalPool, targetPool, srcHost, destHost)
    SELECT * FROM (
      SELECT /*+ index(a I_Messages_Facility) index(a I_Messages_NSFileid)*/
             a.nsfileid nsfileid,
             min(a.timestamp) timestamp,
             max(decode(b.name, 'Direction',
                        substr(b.value, 0, instr(b.value, '->', 1) - 2),
                        NULL)) src,
             max(decode(b.name, 'Direction',
                        substr(b.value, instr(b.value, '->', 1) + 3),
                        NULL)) dest,
             max(decode(b.name, 'SourcePath',
                        substr(b.value, 0, instr(b.value, ':', 1) - 1),
                        NULL)) src_host,
             max(decode(b.name, 'SourcePath', c.hostname, NULL)) dest_host
        FROM &dlfschema..dlf_messages a, &dlfschema..dlf_str_param_values b,
             &dlfschema..dlf_host_map c
       WHERE a.id = b.id
         AND a.facility = 23      -- DiskCopy
         AND a.msg_no IN (39, 28) -- DiskCopy Transfer Successful, Transfer information
         AND a.hostid = c.hostid
         AND a.timestamp >= maxTimeStamp
         AND b.timestamp >= maxTimeStamp
         AND a.timestamp < maxTimeStamp + 5/1440
         AND b.timestamp < maxTimeStamp + 5/1440
       GROUP BY nsfileid
    ) temp
   WHERE temp.src <> temp.dest
     AND temp.dest IS NOT NULL
     AND temp.src IS NOT NULL
     AND temp.dest_host IS NOT NULL
     AND temp.src_host IS NOT NULL;
END;
/


/* PL/SQL method implementing statsInternalDiskCopy */
CREATE OR REPLACE PROCEDURE statsInternalDiskCopy(maxTimeStamp IN DATE)
AS
BEGIN
  -- Info about external file replication: source - target SvcClass
  INSERT ALL INTO InternalDiskCopy (timestamp, svcclass, copies)
  VALUES (maxTimeStamp, src, copies)
    SELECT src, count(*) copies
     FROM (
       SELECT a.nsfileid nsfileid,
              substr(b.value, 0, instr(b.value, '->', 1) - 2) src,
              substr(b.value, instr(b.value, '->', 1) + 3) dest
         FROM &dlfschema..dlf_messages a, &dlfschema..dlf_str_param_values b
        WHERE a.id = b.id
          AND a.facility = 23  -- DiskCopy
          AND a.msg_no = 39    -- DiskCopy Transfer Successful
          AND a.timestamp >= maxTimeStamp
          AND b.timestamp >= maxTimeStamp
          AND a.timestamp < maxTimeStamp + 5/1440
          AND b.timestamp < maxTimeStamp + 5/1440
     ) temp
    WHERE temp.src = temp.dest
    GROUP BY src;
END;
/


/* PL/SQL method implementing statsGCFiles */
CREATE OR REPLACE PROCEDURE statsGCFiles(maxTimeStamp IN DATE)
AS
BEGIN
  INSERT INTO GcFiles
    (timestamp, nsfileid, fileSize, fileAge, lastAccessTime, nbAccesses,
     gcType, svcclass)
    SELECT /*+ index(mes I_Messages_NSFileid) index(num I_Num_Param_Values_id) index(str I_Str_Param_Values_id) */
           mes.timestamp,
           mes.nsfileid,
           max(decode(num.name, 'FileSize', num.value, NULL)) fileSize,
           max(decode(num.name, 'FileAge', num.value, NULL)) fileAge,
           max(decode(num.name, 'LastAccessTime', num.value, NULL)) lastAccessTime,
           max(decode(num.name, 'NbAccesses', num.value, NULL)) nb_accesses,
           max(decode(str.name, 'GcType', str.value, NULL)) gcType,
           max(decode(str.name, 'SvcClass', str.value, NULL)) svcclass
      FROM &dlfschema..dlf_messages mes,
           &dlfschema..dlf_num_param_values num,
           &dlfschema..dlf_str_param_values str
     WHERE mes.facility = 8 -- GC
       AND mes.msg_no = 11  -- Removed file successfully
       AND mes.id = num.id
       AND num.id = str.id
       AND num.name IN ('FileSize', 'FileAge', 'LastAccessTime', 'NbAccesses')
       AND str.name IN ('SvcClass', 'GcType')
       AND mes.timestamp >= maxTimeStamp
       AND num.timestamp >= maxTimeStamp
       AND str.timestamp >= maxTimeStamp
       AND mes.timestamp < maxTimeStamp + 5/1440
       AND num.timestamp < maxTimeStamp + 5/1440
       AND str.timestamp < maxTimeStamp + 5/1440
     GROUP BY mes.timestamp, mes.nsfileid;
END;
/


/* PL/SQL method implementing statsTapeRecall */
CREATE OR REPLACE PROCEDURE statsTapeRecall(maxTimeStamp IN DATE)
AS
BEGIN
  -- Info about tape recalls: Tape Volume Id - Tape Status
  INSERT INTO TapeRecall (timestamp, subReqId, tapeId, tapeMountState)
    (SELECT /*+ index(mes I_Messages_Facility) index(str I_Str_Param_Values_id) */
            mes.timestamp, mes.subreqid, mes.tapevid, str.value
       FROM &dlfschema..dlf_messages mes, &dlfschema..dlf_str_param_values str
      WHERE mes.id = str.id
        AND mes.facility = 22 -- Stager
        AND mes.msg_no = 57   -- Triggering Tape Recall
        AND str.name = 'TapeStatus'
        AND mes.timestamp >= maxTimeStamp
        AND str.timestamp >= maxTimeStamp
        AND mes.timestamp < maxTimeStamp + 5/1440
        AND str.timestamp < maxTimeStamp + 5/1440)
  LOG ERRORS INTO Err_Taperecall REJECT LIMIT 100000;
  -- Insert info about size of recalled file
  FOR a IN (SELECT /*+ index(mes I_Messages_Facility) index(num I_Num_Param_Values_id) */
                   mes.subreqid, num.value
              FROM &dlfschema..dlf_messages mes, &dlfschema..dlf_num_param_values num
             WHERE mes.id = num.id
               AND mes.facility = 22  -- Stager
               AND mes.msg_no = 57    -- Triggering Tape Recall
               AND num.name = 'FileSize'
               AND mes.timestamp >= maxTimeStamp
               AND num.timestamp >= maxTimeStamp
               AND mes.timestamp < maxTimeStamp + 5/1440
               AND num.timestamp < maxTimeStamp + 5/1440)
  LOOP
    UPDATE Requests
       SET filesize = to_number(a.value)
     WHERE subreqid = a.subReqId;
  END LOOP;
END;
/


/* PL/SQL method implementing statsMigs */
CREATE OR REPLACE PROCEDURE statsMigs(maxTimeStamp IN DATE)
AS
BEGIN
  INSERT INTO Migration
    (timestamp, reqid, subreqid, nsfileid, type, svcclass, username, filename)
    SELECT /*+ index(mes I_Messages_NSFileid) index(str I_Str_Param_Values_id) */
           mes.timestamp,mes.reqid,
           mes.subreqid, mes.nsfileid,
           max(decode(str.name, 'Type',     str.value, NULL)) type,
           max(decode(str.name, 'SvcClass', str.value, NULL)) svcclass,
           max(decode(str.name, 'Username', str.value, NULL)) username,
           max(decode(str.name, 'Filename', str.value, NULL)) filename
      FROM &dlfschema..dlf_messages mes, &dlfschema..dlf_str_param_values str
     WHERE mes.id = str.id
       AND str.id IN
         (SELECT id
            FROM &dlfschema..dlf_str_param_values
           WHERE name = 'Type'
             AND value LIKE 'Stage%'
             AND timestamp >= maxTimeStamp
             AND timestamp < maxTimeStamp + 5/1440)
       AND mes.facility = 22  -- Stager
       AND mes.msg_no = 58    -- Recreating CastorFile
       AND str.name IN ('SvcClass', 'Username', 'Groupname', 'Type', 'Filename')
       AND mes.timestamp >= maxTimeStamp
       AND mes.timestamp < maxTimeStamp + 5/1440
       AND str.timestamp >= maxTimeStamp
       AND str.timestamp < maxTimeStamp + 5/1440
     GROUP BY mes.timestamp, mes.id, mes.reqid, mes.subreqid, mes.nsfileid
     ORDER BY mes.timestamp
  LOG ERRORS INTO Err_Migration REJECT LIMIT 100000;
END;
/


/* PL/SQL method implementing statsReqs */
CREATE OR REPLACE PROCEDURE statsReqs(maxtimestamp IN DATE)
AS
BEGIN
  INSERT INTO Requests
    (timestamp, reqId, subReqId, nsfileid, type, svcclass, username, state, filename)
    SELECT /*+ index(mes I_Messages_Facility) index(str I_Str_Param_Values_id) */
           mes.timestamp, mes.reqid, mes.subreqid, mes.nsfileid,
           max(decode(str.name, 'Type', str.value, NULL)) type,
           max(decode(str.name, 'SvcClass', str.value, NULL)) svcclass,
           max(decode(str.name, 'Username', str.value, NULL)) username,
           max(decode(mes.msg_no, 60, 'DiskHit', 56, 'DiskCopy', 57, 'TapeRecall', NULL)) state,
           max(decode(str.name, 'Filename', str.value, NULL)) filename
      FROM &dlfschema..dlf_messages mes , &dlfschema..dlf_str_param_values str
     WHERE mes.id = str.id
       AND str.id IN
         (SELECT id
            FROM &dlfschema..dlf_str_param_values
           WHERE name = 'Type'
             AND value LIKE 'Stage%'
             AND timestamp >= maxtimestamp
             AND timestamp < maxtimestamp + 5/1440)
       AND mes.facility = 22           -- Stager
       AND mes.msg_no IN (56, 57, 60)  -- Triggering internal DiskCopy replication, Triggering Tape Recall, Diskcopy available, scheduling job
       AND str.name IN ('SvcClass', 'Username', 'Groupname', 'Type', 'Filename')
       AND mes.timestamp >= maxtimestamp
       AND mes.timestamp < maxtimestamp + 5/1440
       AND str.timestamp >= maxtimestamp
       AND str.timestamp < maxtimestamp + 5/1440
     GROUP BY mes.timestamp, mes.id ,mes.reqid, mes.subreqid, mes.nsfileid
     ORDER BY mes.timestamp
  LOG ERRORS INTO Err_Requests REJECT LIMIT 1000000;
END;
/


/***** COMMON CODE *****/

/* PL/SQL method implementing createPartition */
CREATE OR REPLACE PROCEDURE createPartitions
AS
  username VARCHAR2(2048);
  partitionMax NUMBER;
  tableSpaceName VARCHAR2(2048);
  highValue DATE;
  cnt NUMBER;
BEGIN
  -- Set the nls_date_format
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = "DD-MON-YYYY"';

  -- Extract the name of the current user running the PL/SQL procedure. This
  -- name will be used within the tablespace names.
  SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER')
    INTO username
    FROM dual;

  -- Loop over all partitioned tables
  FOR a IN (SELECT DISTINCT(table_name)
              FROM user_tab_partitions
             ORDER BY table_name)
  LOOP
    -- Determine the high value on which to split the MAX_VALUE partition of
    -- all tables
    SELECT max(substr(partition_name, 3, 10))
      INTO partitionMax
      FROM user_tab_partitions
     WHERE partition_name <> 'MAX_VALUE'
       AND table_name = a.table_name;

    partitionMax := TO_NUMBER(
      TO_CHAR(TRUNC(TO_DATE(partitionMax, 'YYYYMMDD') + 1), 'YYYYMMDD'));

    -- If this is the first execution there will be no high value, so set it to
    -- today
    IF partitionMax IS NULL THEN
      partitionMax := TO_NUMBER(TO_CHAR(SYSDATE, 'YYYYMMDD'));
    END IF;

    -- Create partition
    FOR b IN (SELECT TO_DATE(partitionMax, 'YYYYMMDD') + rownum - 1 value
                FROM all_objects
               WHERE rownum <=
                     TO_DATE(TO_CHAR(SYSDATE + 7, 'YYYYMMDD'), 'YYYYMMDD') -
                     TO_DATE(partitionMax, 'YYYYMMDD') + 1)
    LOOP
      -- To improve data management each daily partition has its own tablespace
      -- http://www.oracle.com/technology/oramag/oracle/06-sep/o56partition.html

      -- Check if a new tablespace is required before creating the partition
      tableSpaceName := 'MON_'||TO_CHAR(b.value, 'YYYYMMDD')||'_'||username;
      SELECT count(*) INTO cnt
        FROM user_tablespaces
       WHERE tablespace_name = tableSpaceName;

      IF cnt = 0 THEN
        EXECUTE IMMEDIATE 'CREATE TABLESPACE '||tableSpaceName||'
                           DATAFILE SIZE 100M
                           AUTOEXTEND ON NEXT 200M
                           MAXSIZE 30G
                           EXTENT MANAGEMENT LOCAL
                           SEGMENT SPACE MANAGEMENT AUTO';
      END IF;

      -- If the tablespace is read only, alter its status to read write for this
      -- operation.
      FOR d IN (SELECT tablespace_name FROM user_tablespaces
                 WHERE tablespace_name = tableSpaceName
                   AND status = 'READ ONLY')
      LOOP
        EXECUTE IMMEDIATE 'ALTER TABLESPACE '||d.tablespace_name||' READ WRITE';
      END LOOP;

      highValue := TRUNC(b.value + 1);
      EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                         SPLIT PARTITION MAX_VALUE
                         AT    ('''||TO_CHAR(highValue, 'DD-MON-YYYY')||''')
                         INTO  (PARTITION P_'||TO_CHAR(b.value, 'YYYYMMDD')||'
                                TABLESPACE '||tableSpaceName||',
                                PARTITION MAX_VALUE)
                         UPDATE INDEXES';

      -- Move indexes to the correct tablespace
      FOR c IN (SELECT index_name
                  FROM user_indexes
                 WHERE table_name = a.table_name
                   AND partitioned = 'YES')
      LOOP
        EXECUTE IMMEDIATE 'ALTER INDEX '||c.index_name||'
                           REBUILD PARTITION P_'||TO_CHAR(b.value, 'YYYYMMDD')||'
                           TABLESPACE '||tableSpaceName;
      END LOOP;
    END LOOP;
  END LOOP;
END;
/


/* PL/SQL method implementing archiveData */
CREATE OR REPLACE PROCEDURE archiveData (expiry IN NUMBER)
AS
  username VARCHAR2(2048);
  expiryTime NUMBER;
BEGIN
  -- Set the nls_date_format
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = "DD-MON-YYYY"';

  -- Extract the name of the current user running the PL/SQL procedure. This
  -- name will be used within the tablespace names.
  SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER')
    INTO username
    FROM dual;

  -- Extract configurable expiry time
  expiryTime := expiry;
  IF expiryTime = -1 THEN
    SELECT expiry INTO expiryTime
      FROM ConfigSchema;
  END IF;

  -- Drop partitions across all tables
  FOR a IN (SELECT *
              FROM user_tab_partitions
             WHERE partition_name < concat('P_', TO_CHAR(SYSDATE - expiryTime, 'YYYYMMDD'))
               AND partition_name <> 'MAX_VALUE'
             ORDER BY partition_name DESC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                       DROP PARTITION '||a.partition_name;
  END LOOP;

  -- Drop tablespaces
  FOR a IN (SELECT tablespace_name
              FROM user_tablespaces
             WHERE status <> 'OFFLINE'
               AND tablespace_name LIKE 'MON_%_'||username
               AND tablespace_name < 'MON_'||TO_CHAR(SYSDATE - expiryTime, 'YYYYMMDD')||'_'||username
             ORDER BY tablespace_name ASC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||a.tablespace_name||' OFFLINE';
    EXECUTE IMMEDIATE 'DROP TABLESPACE '||a.tablespace_name||'
                       INCLUDING CONTENTS AND DATAFILES';
  END LOOP;

  -- Set the status of tablespaces older then 2 days to read only.
  FOR a IN (SELECT tablespace_name
              FROM user_tablespaces
             WHERE status <> 'OFFLINE'
               AND status <> 'READ ONLY'
               AND tablespace_name LIKE 'MON_%_'||username
               AND tablespace_name < 'MON_'||TO_CHAR(SYSDATE - 2, 'YYYYMMDD')||'_'||username
             ORDER BY tablespace_name ASC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||a.tablespace_name||' READ ONLY';
  END LOOP;
END;
/


/* Scheduler jobs */
BEGIN
  -- Remove scheduler jobs before recreation
  FOR a IN (SELECT job_name FROM user_scheduler_jobs)
  LOOP
    DBMS_SCHEDULER.DROP_JOB(a.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every day and create new partitions
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'partitionCreationJob',
      JOB_TYPE        => 'STORED_PROCEDURE',
      JOB_ACTION      => 'createPartitions',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => TRUNC(SYSDATE) + 1/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily partitioning creation');

  -- Create a db job to be run every day and drop old data from the database
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'archiveDataJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN
                            archiveData(-1);
                            FOR a IN (SELECT table_name FROM user_tables
                                       WHERE table_name LIKE ''ERR_%'')
                            LOOP
                              EXECUTE IMMEDIATE ''TRUNCATE TABLE ''||a.table_name;
                            END LOOP;
                          END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => TRUNC(SYSDATE) + 2/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily data archiving');

  -- Create a job to execute the procedures that create statistical information (OLD)
  DBMS_SCHEDULER.CREATE_JOB (
      JOB_NAME        => 'statisticJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'DECLARE
                            now DATE := SYSDATE;
                            interval NUMBER := 300;
                          BEGIN
                            statsLatency(now, interval);
                            statsQueueTime(now, interval);
                            statsGarbageCollection(now, interval);
                            statsRequest(now, interval);
                            statsDiskCacheEfficiency(now, interval);
                            statsMigratedFiles(now, interval);
                            statsReplication(now, interval);
                            statsTapeRecalled(now, interval);
                            statsProcessingTime(now, interval);
                            statsClientVersion(now, interval);
                          END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'CASTOR2 Monitoring Statistics (OLD) (5 Minute Frequency)');

  -- Create a job to execute the procedures that create statistical information (NEW)
  DBMS_SCHEDULER.CREATE_JOB (
      JOB_NAME        => 'populateJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'DECLARE
                            maxTimeStamp DATE;
                          BEGIN
                            EXECUTE IMMEDIATE ''TRUNCATE TABLE ERR_Requests'';
                            SELECT runmaxtime INTO maxTimeStamp FROM ConfigSchema;

                            statsReqs(maxTimeStamp);
                            statsDiskCopy(maxTimeStamp);
                            statsInternalDiskCopy(maxTimeStamp);
                            statsTapeRecall(maxTimeStamp);
                            statsMigs(maxTimeStamp);
                            statsTotalLat(maxTimeStamp);
                            statsGcFiles(maxTimeStamp);

                            UPDATE ConfigSchema
                               SET runmaxtime = runmaxtime + 5/1440;
                          END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => SYSDATE + 10/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'CASTOR2 Monitoring Statistics (NEW) (5 Minute Frequency)');

END;
/


/* Trigger the initial creation of partitions */
BEGIN
  createPartitions();
END;
/
