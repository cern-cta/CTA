/*******************************************************************
 *
 * @(#)$RCSfile: oracleCommon.sql,v $ $Revision: 1.650 $ $Date: 2008/05/20 11:20:08 $ $Author: waldron $
 *
 * This file contains all schema definitions which are not generated automatically
 * and some common PL/SQL utilities, appended at the end of the generated code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* A small table used to cross check code and DB versions */
UPDATE CastorVersion SET schemaVersion = '2_1_7_5';

/* Sequence for indices */
CREATE SEQUENCE ids_seq CACHE 300;

/* SQL statements for object types */
CREATE TABLE Id2Type (id INTEGER CONSTRAINT I_Id2Type_Id PRIMARY KEY, type NUMBER) ENABLE ROW MOVEMENT;

/* SQL statements for requests status */
/* Partitioning enables faster response (more than indexing) for the most frequent queries - credits to Nilo Segura */
CREATE TABLE newRequests (type NUMBER(38) NOT NULL, id NUMBER(38) NOT NULL, creation DATE NOT NULL, CONSTRAINT I_NewRequests_Type_Id PRIMARY KEY (type, id))
organization index
compress
partition by list (type)
 (
  partition type_16 values (16)  tablespace stager_data,
  partition type_21 values (21)  tablespace stager_data,
  partition type_33 values (33)  tablespace stager_data,
  partition type_34 values (34)  tablespace stager_data,
  partition type_35 values (35)  tablespace stager_data,
  partition type_36 values (36)  tablespace stager_data,
  partition type_37 values (37)  tablespace stager_data,
  partition type_38 values (38)  tablespace stager_data,
  partition type_39 values (39)  tablespace stager_data,
  partition type_40 values (40)  tablespace stager_data,
  partition type_41 values (41)  tablespace stager_data,
  partition type_42 values (42)  tablespace stager_data,
  partition type_43 values (43)  tablespace stager_data,
  partition type_44 values (44)  tablespace stager_data,
  partition type_45 values (45)  tablespace stager_data,
  partition type_46 values (46)  tablespace stager_data,
  partition type_48 values (48)  tablespace stager_data,
  partition type_49 values (49)  tablespace stager_data,
  partition type_50 values (50)  tablespace stager_data,
  partition type_51 values (51)  tablespace stager_data,
  partition type_60 values (60)  tablespace stager_data,
  partition type_64 values (64)  tablespace stager_data,
  partition type_65 values (65)  tablespace stager_data,
  partition type_66 values (66)  tablespace stager_data,
  partition type_67 values (67)  tablespace stager_data,
  partition type_78 values (78)  tablespace stager_data,
  partition type_79 values (79)  tablespace stager_data,
  partition type_80 values (80)  tablespace stager_data,
  partition type_84 values (84)  tablespace stager_data,
  partition type_90 values (90)  tablespace stager_data,
  partition type_142 values (142)  tablespace stager_data,	
  partition type_144 values (144)  tablespace stager_data,
  partition type_147 values (147)  tablespace stager_data,
  partition type_149 values (149)  tablespace stager_data,
  partition notlisted values (default) tablespace stager_data
 );

/* Redefinition of table SubRequest to make it partitioned by status */
/* Unfortunately it has already been defined, so we drop and recreate it */
/* Note that if the schema changes, this part has to be updated manually! */
DROP TABLE SubRequest;
CREATE TABLE SubRequest
  (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048),
   xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER,
   modeBits NUMBER, creationTime INTEGER, lastModificationTime INTEGER,
   answered NUMBER, errorCode NUMBER, errorMessage VARCHAR2(2048), id NUMBER NOT NULL,
   diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER,
   request INTEGER, getNextStatus INTEGER, requestedFileSystems VARCHAR2(2048)
  )
  PCTFREE 50 PCTUSED 40 INITRANS 50
  ENABLE ROW MOVEMENT
  PARTITION BY LIST (STATUS)
   (
    PARTITION P_STATUS_0_1_2   VALUES (0, 1, 2),      -- *START
    PARTITION P_STATUS_3_13_14 VALUES (3, 13, 14),    -- *SCHED
    PARTITION P_STATUS_4       VALUES (4),
    PARTITION P_STATUS_5       VALUES (5),
    PARTITION P_STATUS_6       VALUES (6),
    PARTITION P_STATUS_7       VALUES (7),
    PARTITION P_STATUS_8       VALUES (8),
    PARTITION P_STATUS_9_10    VALUES (9, 10),        -- FAILED_*
    PARTITION P_STATUS_11      VALUES (11),
    PARTITION P_STATUS_12      VALUES (12),
    PARTITION P_STATUS_OTHER   VALUES (DEFAULT)
   );

ALTER TABLE SUBREQUEST ADD CONSTRAINT I_SUBREQUEST_PK PRIMARY KEY (ID);

/* Indexes related to most used entities */
CREATE UNIQUE INDEX I_DiskServer_name on DiskServer (name);

CREATE UNIQUE INDEX I_CastorFile_FileIdNsHost on CastorFile (fileId, nsHost);
CREATE INDEX I_CastorFile_LastKnownFileName on CastorFile (lastKnownFileName);
CREATE INDEX I_CastorFile_SvcClass on CastorFile (svcClass);

CREATE INDEX I_DiskCopy_Castorfile on DiskCopy (castorFile);
CREATE INDEX I_DiskCopy_FileSystem on DiskCopy (fileSystem);
CREATE INDEX I_DiskCopy_Status on DiskCopy (status);
CREATE INDEX I_DiskCopy_GCWeight on DiskCopy (gcWeight);
CREATE INDEX I_DiskCopy_FS_Status_10 on DiskCopy (fileSystem,decode(status,10,status,NULL));

CREATE INDEX I_TapeCopy_Castorfile on TapeCopy (castorFile);
CREATE INDEX I_TapeCopy_Status on TapeCopy (status);
CREATE INDEX I_TapeCopy_CF_Status_2 on TapeCopy (castorFile,decode(status,2,status,null));

CREATE INDEX I_FileSystem_DiskPool on FileSystem (diskPool);
CREATE INDEX I_FileSystem_DiskServer on FileSystem (diskServer);

CREATE INDEX I_SubRequest_Castorfile on SubRequest (castorFile);
CREATE INDEX I_SubRequest_DiskCopy on SubRequest (diskCopy);
CREATE INDEX I_SubRequest_Request on SubRequest (request);
CREATE INDEX I_SubRequest_Parent on SubRequest (parent);
CREATE INDEX I_SubRequest_SubReqId on SubRequest (subReqId);

CREATE INDEX I_StagePTGRequest_ReqId on StagePrepareToGetRequest (reqId);
CREATE INDEX I_StagePTPRequest_ReqId on StagePrepareToPutRequest (reqId);
CREATE INDEX I_StagePTURequest_ReqId on StagePrepareToUpdateRequest (reqId);
CREATE INDEX I_StageGetRequest_ReqId on StageGetRequest (reqId);
CREATE INDEX I_StagePutRequest_ReqId on StagePutRequest (reqId);
CREATE INDEX I_StageRepackRequest_ReqId on StageRepackRequest (reqId);

/* A primary key index for better scan of Stream2TapeCopy */
CREATE UNIQUE INDEX I_pk_Stream2TapeCopy ON Stream2TapeCopy (parent, child);

/* Some index on the GCFile table to speed up garbage collection */
CREATE INDEX I_GCFILE_REQUEST ON GCFILE(REQUEST);

/* Indexing segments by Tape */
CREATE INDEX I_SEGMENT_TAPE ON SEGMENT (TAPE);

/* Some constraints */
ALTER TABLE FileSystem ADD CONSTRAINT diskserver_fk FOREIGN KEY (diskServer) REFERENCES DiskServer(id);
ALTER TABLE FileSystem MODIFY (status NOT NULL);
ALTER TABLE FileSystem MODIFY (diskServer NOT NULL);
ALTER TABLE DiskServer MODIFY (status NOT NULL);
ALTER TABLE SvcClass MODIFY (name NOT NULL);

/* An index to speed up queries in FileQueryRequest, FindRequestRequest, RequestQueryRequest */
CREATE INDEX I_QueryParameter_Query on QueryParameter (query);

/* An index to speed the queries on Segments by copy */
CREATE INDEX I_Segment_Copy on Segment (copy);

/* Constraint on FileClass name */
ALTER TABLE FileClass ADD CONSTRAINT I_FileClass_Name UNIQUE (name); 

/* Add unique constraint on tapes */
ALTER TABLE Tape ADD CONSTRAINT I_Tape_VIDSideTpMode UNIQUE (VID, side, tpMode);

/* Add unique constraint on svcClass name */
ALTER TABLE SvcClass ADD CONSTRAINT I_SvcClass_Name UNIQUE (NAME); 

/* The primary key in this table allows for materialized views */
ALTER TABLE DiskPool2SvcClass ADD CONSTRAINT I_DiskPool2SvcCla_ParentChild PRIMARY KEY (parent, child);

/* Global temporary table to handle output of the filesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcOutput
  (fileid NUMBER, nshost VARCHAR2(2048))
  ON COMMIT PRESERVE ROWS;

/* Global temporary table to store castor file ids temporarily in the filesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcHelper
  (cfId NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to handle output of the nsFilesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE NsFilesDeletedOrphans
  (fileid NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to handle output of the stgFilesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE StgFilesDeletedOrphans
  (diskCopyId NUMBER)
  ON COMMIT DELETE ROWS;
  
/* Global temporary table for the filesClearedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesClearedProcHelper
  (cfId NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table for dropped out of date StageOut diskCopies */
CREATE GLOBAL TEMPORARY TABLE OutOfDateStageOutDropped
  (fileId NUMBER, nsHost VARCHAR2(2048))
  ON COMMIT DELETE ROWS;

/* Global temporary table for out of date StageOut diskCopies
   where putdone was issued */
CREATE GLOBAL TEMPORARY TABLE OutOfDateStageOutPutDone
  (fileId NUMBER, nsHost VARCHAR2(2048))
  ON COMMIT DELETE ROWS;

/* Temporary table to handle removing of priviledges */
CREATE GLOBAL TEMPORARY TABLE removePrivilegeTmpTable
  (svcClass VARCHAR2(2048),
   euid NUMBER,
   egid NUMBER,
   reqType NUMBER)
  ON COMMIT DELETE ROWS;


/**
  * Black and while list mechanism
  * In order to be able to enter a request for a given service class, you need :
  *   - to be in the white list for this service class
  *   - to not be in the black list for this services class
  * Being in a list means :
  *   - either that your uid,gid is explicitely in the list
  *   - or that your gid is in the list with null uid (that is group wildcard)
  *   - or there is an entry with null uid and null gid (full wild card)
  * The permissions can also have a request type. Default is null, that is everything.
  * By default anybody can do anything
  */
CREATE TABLE WhiteList (svcClass VARCHAR2(2048), euid NUMBER, egid NUMBER, reqType NUMBER);
CREATE TABLE BlackList (svcClass VARCHAR2(2048), euid NUMBER, egid NUMBER, reqType NUMBER);

INSERT INTO WhiteList VALUES ('*', NULL, NULL, NULL);
INSERT INTO WhiteList VALUES (NULL, NULL, NULL, NULL);


/* Define the service handlers for the appropriate sets of stage request objects */
UPDATE Type2Obj SET svcHandler = 'JobReqSvc' WHERE type in (35, 40, 44);
UPDATE Type2Obj SET svcHandler = 'PrepReqSvc' WHERE type in (36, 37, 38, 119);
UPDATE Type2Obj SET svcHandler = 'StageReqSvc' WHERE type in (39, 42, 95);
UPDATE Type2Obj SET svcHandler = 'QueryReqSvc' WHERE type in (33, 34, 41, 103, 131);
UPDATE Type2Obj SET svcHandler = 'JobSvc' WHERE type in (60, 64, 65, 67, 78, 79, 80, 93, 144, 147);
UPDATE Type2Obj SET svcHandler = 'GCSvc' WHERE type in (73, 74, 83, 142, 149);

/* Set default values for the StageDiskCopyReplicaRequest table */
ALTER TABLE StageDiskCopyReplicaRequest MODIFY flags DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY euid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY egid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY mask DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY pid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY machine DEFAULT 'stager';

COMMIT;

/****************************************************************/
/* NbTapeCopiesInFS to work around ORACLE missing optimizations */
/****************************************************************/

/* This table keeps track of the number of TapeCopy waiting for migration
 * which have a diskCopy on this fileSystem. This table only exist
 * because Oracle is not able to optimize the following query :
 * SELECT max(A) from A, B where A.pk = B.fk;
 * Such a query is needed in bestTapeCopyForStream in order to
 * select filesystems effectively having tapecopies.
 * As a work around, we keep track of the tapecopies for each
 * filesystem. The cost is an increase of complexity and especially
 * of the number of triggers ensuring consistency of the whole database */
CREATE TABLE NbTapeCopiesInFS (FS NUMBER, Stream NUMBER, NbTapeCopies NUMBER);
CREATE UNIQUE INDEX I_NbTapeCopiesInFS_FSStream on NbTapeCopiesInFS(FS, Stream);
CREATE INDEX I_NbTapeCopiesInFS_Stream on NbTapeCopiesInFS(Stream);


/*******************************************************************/
/* LockTable to implement proper locking for bestTapeCopyForStream */
/*******************************************************************/

/* This table is needed to insure that bestTapeCopyForStream works Ok.
 * It basically serializes the queries ending to the same diskserver.
 * This is only needed because of lack of functionnality in ORACLE.
 * The original goal was to lock the selected filesystem in the first
 * query of bestTapeCopyForStream. But a SELECT FOR UPDATE was not enough
 * because it does not revalidate the inner select and we were thus selecting
 * n times the same filesystem when n queries were processed in parallel.
 * (take care, they were processed sequentially due to the lock, but they
 * were still locking the same filesystem). Thus an UPDATE was needed but
 * UPDATE cannot use joins. Thus it was impossible to lock DiskServer
 * and FileSystem at the same time (we need to avoid the DiskServer to
 * be chosen again (with a different filesystem) before we update the
 * weight of all filesystems). Locking the diskserver only was fine but
 * was introducing a possible deadlock with a place where the FileSystem
 * is locked before the DiskServer. Thus this table..... */
CREATE TABLE LockTable (DiskServerId NUMBER PRIMARY KEY, TheLock NUMBER);
INSERT INTO LockTable SELECT id, id FROM DiskServer;


/*********************************************************************/
/* FileSystemsToCheck used to optimise the processing of filesystems */
/* when they change status                                           */
/*********************************************************************/
CREATE TABLE FileSystemsToCheck (FileSystem NUMBER PRIMARY KEY, ToBeChecked NUMBER);
INSERT INTO FileSystemsToCheck SELECT id, 0 FROM FileSystem;


/*********************/
/* FileSystem rating */
/*********************/

/* Computes a 'rate' for the filesystem which is an agglomeration
   of weight and fsDeviation. The goal is to be able to classify
   the fileSystems using a single value and to put an index on it */
CREATE OR REPLACE FUNCTION fileSystemRate
(readRate IN NUMBER,
 writeRate IN NUMBER,
 nbReadStreams IN NUMBER,
 nbWriteStreams IN NUMBER,
 nbReadWriteStreams IN NUMBER,
 nbMigratorStreams IN NUMBER,
 nbRecallerStreams IN NUMBER)
RETURN NUMBER DETERMINISTIC IS
BEGIN
  RETURN - nbReadStreams - nbWriteStreams - nbReadWriteStreams - nbMigratorStreams - nbRecallerStreams;
END;


/* FileSystem index based on the rate. */
CREATE INDEX I_FileSystem_Rate
    ON FileSystem(fileSystemRate(readRate, writeRate,
	          nbReadStreams,nbWriteStreams, nbReadWriteStreams, nbMigratorStreams, nbRecallerStreams));


/* Get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
BEGIN
  RETURN (SYSDATE - to_date('01-jan-1970 01:00:00','dd-mon-yyyy HH:MI:SS')) * (24*60*60);
END;

/* Generate a universally unique id (UUID) */
CREATE OR REPLACE FUNCTION uuidGen RETURN VARCHAR2 IS
  ret VARCHAR2(36);
BEGIN
  -- Note: the guid generator provided by ORACLE produces sequential uuid's, not
  -- random ones. The reason for this is because random uuid's are not good for
  -- indexing!
  SELECT lower(regexp_replace(sys_guid(), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5'))
    INTO ret FROM Dual;
  RETURN ret;
END;

/* compute the impact of a file's size in its gcweight */
CREATE OR REPLACE FUNCTION size2gcweight(s NUMBER) RETURN NUMBER IS
BEGIN
  IF s < 1073741824 THEN
    RETURN 1073741824/(s+1)*86400 + getTime();  -- 1GB/filesize (days) + current time as lastAccessTime
  ELSE
    RETURN 86400 + getTime();  -- the value for 1G file. We do not make any difference for big files and privilege FIFO
  END IF;
END;


/* PL/SQL method deleting tapecopies (and segments) of a castorfile */
CREATE OR REPLACE PROCEDURE deleteTapeCopies(cfId NUMBER) AS
BEGIN
  -- loop over the tapecopies
  FOR t IN (SELECT id FROM TapeCopy WHERE castorfile = cfId) LOOP
    FOR s IN (SELECT id FROM Segment WHERE copy = t.id) LOOP
    -- Delete the segment(s)
      DELETE FROM Id2Type WHERE id = s.id;
      DELETE FROM Segment WHERE id = s.id;
    END LOOP;
    -- Delete from Stream2TapeCopy
    DELETE FROM Stream2TapeCopy WHERE child = t.id;
    -- Delete the TapeCopy
    DELETE FROM Id2Type WHERE id = t.id;
    DELETE FROM TapeCopy WHERE id = t.id;
  END LOOP;
END;


/* PL/SQL method canceling a given recall */
CREATE OR REPLACE PROCEDURE cancelRecall
(cfId NUMBER, dcId NUMBER, newSubReqStatus NUMBER) AS
  srIds "numList";
  unused NUMBER;
BEGIN
  -- Lock the CastorFile
  SELECT id INTO unused FROM CastorFile 
   WHERE id = cfId FOR UPDATE;
  -- Cancel the recall
  deleteTapeCopies(cfId);
  -- Delete the DiskCopy
  UPDATE DiskCopy SET status = 7 WHERE id = dcId; -- INVALID
  -- Look for request associated to the recall and fail
  -- it and all the waiting ones
  UPDATE SubRequest SET status = newSubReqStatus
   WHERE diskCopy = dcId RETURNING id BULK COLLECT INTO srIds;
  UPDATE SubRequest
     SET status = newSubReqStatus, parent = 0 -- FAILED
   WHERE status = 5 -- WAITSUBREQ
     AND parent IN (SELECT * FROM TABLE(srIds))
     AND castorfile = cfId;
END;


/* PL/SQL method to delete a CastorFile only when no Disk|TapeCopies are left for it */
/* Internally used in filesDeletedProc, putFailedProc and deleteOutOfDateDiskCopies */
CREATE OR REPLACE PROCEDURE deleteCastorFile(cfId IN NUMBER) AS
  nb NUMBER;
BEGIN
  -- See whether the castorfile has no other DiskCopy
  SELECT count(*) INTO nb FROM DiskCopy
   WHERE castorFile = cfId;
  -- If any DiskCopy, give up
  IF nb = 0 THEN
    -- See whether the castorfile has any TapeCopy
    SELECT count(*) INTO nb FROM TapeCopy
     WHERE castorFile = cfId AND status != 6; -- FAILED
    -- If any TapeCopy, give up
    IF nb = 0 THEN
      -- See whether the castorfile has any pending SubRequest
      SELECT count(*) INTO nb FROM SubRequest
       WHERE castorFile = cfId
         AND status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 13, 14);   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
      -- If any SubRequest, give up
      IF nb = 0 THEN
        DECLARE
          fid NUMBER;
          fc NUMBER;
          nsh VARCHAR2(2048);
        BEGIN
          -- Delete the failed TapeCopies
          deleteTapeCopies(cfId);
          -- Delete the CastorFile
          DELETE FROM id2Type WHERE id = cfId;
          DELETE FROM CastorFile WHERE id = cfId
            RETURNING fileId, nsHost, fileClass
            INTO fid, nsh, fc;
          -- check whether this file potentially had TapeCopies
          SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
          IF nb = 0 THEN
            -- This castorfile was created with no TapeCopy
            -- So removing it from the stager means erasing
            -- it completely. We should thus also remove it
            -- from the name server
            INSERT INTO FilesDeletedProcOutput VALUES (fid, nsh);
          END IF;
        END;
      END IF;
    END IF;
  END IF;
END;
