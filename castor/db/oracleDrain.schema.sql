/*******************************************************************
 * Schema creation code for Draining FileSystems Logic
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement for the creation of the DrainingFileSystem table */
CREATE TABLE DrainingFileSystem
  (userName       VARCHAR2(30) CONSTRAINT NN_DrainingFs_UserName NOT NULL,
   machine        VARCHAR2(500) CONSTRAINT NN_DrainingFs_Machine NOT NULL,
   creationTime   NUMBER DEFAULT 0,
   startTime      NUMBER DEFAULT 0,
   lastUpdateTime NUMBER DEFAULT 0,
   fileSystem     NUMBER CONSTRAINT NN_DrainingFs_FileSystem NOT NULL,
   status         NUMBER DEFAULT 0,
   svcClass       NUMBER CONSTRAINT NN_DrainingFs_SvcClass NOT NULL,
   /* Flag to indicate whether files should be invalidated so that they can be
    * removed by the garbage collection process after a file is replicated to
    * another diskserver.
    */
   autoDelete     NUMBER DEFAULT 0,
   /* Column to indicate which files should be replicated. Valid values are:
    *   0 -- ONTAPE & DISKONLY,
    *   1 -- NOTONTAPE
    *   2 -- ALL
    */
   fileMask       NUMBER DEFAULT 1,
   /* The maximum number of current transfers (job slots) available for draining
    * the filesystem.
    */
   maxTransfers   NUMBER DEFAULT 50,
   totalFiles     NUMBER DEFAULT 0,
   totalBytes     NUMBER DEFAULT 0,
   comments       VARCHAR2(50) DEFAULT 'N/A' CONSTRAINT NN_DrainingFs_Comments NOT NULL)
  /* Allow shrink operations */
  ENABLE ROW MOVEMENT;
BEGIN
  setObjStatusName('DrainingFileSystem', 'status', 0, 'CREATED');
  setObjStatusName('DrainingFileSystem', 'status', 1, 'INITIALIZING');
  setObjStatusName('DrainingFileSystem', 'status', 2, 'RUNNING');
  setObjStatusName('DrainingFileSystem', 'status', 3, 'INTERRUPTED');
  setObjStatusName('DrainingFileSystem', 'status', 4, 'FAILED');
  setObjStatusName('DrainingFileSystem', 'status', 5, 'COMPLETED');
  setObjStatusName('DrainingFileSystem', 'status', 6, 'DELETING');
  setObjStatusName('DrainingFileSystem', 'status', 7, 'RESTART');
END;
/

/* SQL statement for primary key constraint on DrainingFileSystem */
ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT PK_DrainingFs_FileSystem
  PRIMARY KEY (fileSystem);

/* SQL statements for check constraints on the DrainingFileSystem table */
ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT CK_DrainingFs_Status
  CHECK (status IN (0, 1, 2, 3, 4, 5, 6, 7));

ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT CK_DrainingFs_FileMask
  CHECK (fileMask IN (0, 1, 2));

ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT CK_DrainingFs_AutoDelete
  CHECK (autoDelete IN (0, 1));

ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT CK_DrainingFs_MaxTransfers
  CHECK (maxTransfers > 0);

/* SQL statements for foreign key constraints on DrainingFileSystem */
ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT FK_DrainingFs_SvcClass
  FOREIGN KEY (svcClass)
  REFERENCES SvcClass (id);

ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT FK_DrainingFs_FileSystem
  FOREIGN KEY (fileSystem)
  REFERENCES FileSystem (id);

/* SQL statements for indexes on DrainingFileSystem table */
CREATE INDEX I_DrainingFileSystem_SvcClass
  ON DrainingFileSystem (svcClass);


/* SQL statements for the creation of the DrainingDiskCopy table
 *
 * The way the logic for draining a filesystems works is to essentially create
 * a list of all the files that need to be replicated to other diskservers and
 * to process that list until all files have been replicated.
 *
 * This list/queue could have been done with Oracle Advanced Queuing (AQ).
 * However, due to the complexities of setting it up and the lack of prior
 * experience on behalf of the CASTOR developers and CERN DBA's we create a
 * simple queue using a standard table.
 */
CREATE TABLE DrainingDiskCopy
  (fileSystem     NUMBER CONSTRAINT NN_DrainingDCs_FileSystem NOT NULL,
   /* Status of the diskcopy to be replicated. Note: this is not the same as
    * the status of the diskcopy i.e. VALID, STAGEOUT. It is an internal
    * status assigned to each diskcopy (file) as a means of tracking how far the
    * file is in the lifecycle of draining a filesystem.
    * PROCESSING is a transient state.
    */
   status         NUMBER DEFAULT 0 CONSTRAINT NN_DrainingDCs_Status NOT NULL,
   /* A link to the diskcopy. Note: this is deliberately not enforced with a
    * foreign key constraint!!!
    */
   diskCopy       NUMBER CONSTRAINT NN_DrainingDCs_DiskCopy NOT NULL,
   parent         NUMBER DEFAULT 0 CONSTRAINT NN_DrainingDCs_Parent NOT NULL,
   creationTime   NUMBER DEFAULT 0,
   priority       NUMBER DEFAULT 0,
   fileSize       NUMBER DEFAULT 0 CONSTRAINT NN_DrainingDCs_FileSize NOT NULL,
   comments       VARCHAR2(2048) DEFAULT NULL)
  /* Allow shrink operations */
  ENABLE ROW MOVEMENT;
BEGIN
  setObjStatusName('DrainingDiskCopy', 'status', 0, 'CREATED');
  setObjStatusName('DrainingDiskCopy', 'status', 2, 'PROCESSING');
  setObjStatusName('DrainingDiskCopy', 'status', 3, 'WAITD2D');
  setObjStatusName('DrainingDiskCopy', 'status', 4, 'FAILED');
END;
/

/* SQL statement for primary key constraint on DrainingDiskCopy */
ALTER TABLE DrainingDiskCopy
  ADD CONSTRAINT PK_DrainingDCs_DiskCopy
  PRIMARY KEY (diskCopy);

/* SQL statement for check constraints on the DrainingDiskCopy table */
ALTER TABLE DrainingDiskCopy
  ADD CONSTRAINT CK_DrainingDCs_Status
  CHECK (status IN (0, 1, 2, 3, 4));

/* SQL statement for foreign key constraints on DrainingDiskCopy */
ALTER TABLE DrainingDiskCopy
  ADD CONSTRAINT FK_DrainingDCs_FileSystem
  FOREIGN KEY (fileSystem)
  REFERENCES DrainingFileSystem (fileSystem);

/* SQL statements for indexes on DrainingDiskCopy table */
CREATE INDEX I_DrainingDCs_FileSystem
  ON DrainingDiskCopy (fileSystem);

CREATE INDEX I_DrainingDCs_Status
  ON DrainingDiskCopy (status);

/* For the in-order processing, see drainFileSystem */
CREATE INDEX I_DrainingDCs_FSStPrioTimeDC
  ON DrainingDiskCopy (fileSystem, status, priority, creationTime, diskCopy);

CREATE INDEX I_DrainingDCs_Parent
  ON DrainingDiskCopy (parent);
