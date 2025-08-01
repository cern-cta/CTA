CREATE TYPE ARCHIVE_JOB_STATUS AS ENUM (
  'AJS_ToTransferForUser',
  'AJS_ToReportToUserForSuccess',
  'AJS_Complete',
  'AJS_ToReportToUserForFailure',
  'AJS_Failed',
  'AJS_Abandoned',
  'AJS_ToTransferForRepack',
  'AJS_ToReportToRepackForFailure',
  'ReadyForDeletion');
CREATE TYPE RETRIEVE_JOB_STATUS AS ENUM (
  'RJS_ToTransfer',
  'RJS_ToReportToUserForSuccess',
  'RJS_ToReportToUserForFailure',
  'RJS_Failed',
  'RJS_Complete',
  'RJS_ToReportToRepackForSuccess',
  'RJS_ToReportToRepackForFailure',
  'ReadyForDeletion');
CREATE TYPE REPACK_REQ_STATUS AS ENUM (
  'RRS_Pending',
  'RRS_ToExpand',
  'RRS_Starting',
  'RRS_Running',
  'RRS_Complete',
  'RRS_Failed' );
CREATE TABLE CTA_SCHEDULER(
  SCHEMA_VERSION_MAJOR    NUMERIC(20, 0) CONSTRAINT CTA_SCHEDULER_SVM1_NN NOT NULL,
  SCHEMA_VERSION_MINOR    NUMERIC(20, 0) CONSTRAINT CTA_SCHEDULER_SVM2_NN NOT NULL,
  NEXT_SCHEMA_VERSION_MAJOR NUMERIC(20, 0),
  NEXT_SCHEMA_VERSION_MINOR NUMERIC(20, 0),
  STATUS                  VARCHAR(100),
  IS_PRODUCTION           CHAR(1)         DEFAULT '0' CONSTRAINT CTA_SCHEDULER_IP_NN NOT NULL,
  CONSTRAINT CTA_SCHEDULER_IP_BOOL_CK     CHECK(IS_PRODUCTION IN ('0','1'))
);
CREATE TABLE ARCHIVE_ACTIVE_QUEUE(
/* Common part with RETRIEVE table - request related info */
  JOB_ID BIGSERIAL PRIMARY KEY,
/* Common part with RETRIEVE and REPACK table - request related info */
  ARCHIVE_REQUEST_ID BIGINT,
  REQUEST_JOB_COUNT SMALLINT,
  STATUS ARCHIVE_JOB_STATUS CONSTRAINT AJQ_S_NN NOT NULL,
  CREATION_TIME BIGINT,
/* TIMESTAMP NOT NULL, */
  MOUNT_POLICY VARCHAR(100) CONSTRAINT AJQ_MPN_NN NOT NULL,
  TAPE_POOL VARCHAR(100) CONSTRAINT AJQ_TPN_NN NOT NULL,
  VID VARCHAR(20),
/* Common part with RETRIEVE table - request related info */
  MOUNT_ID BIGINT,
  DRIVE VARCHAR(100),
  HOST VARCHAR(100),
  MOUNT_TYPE VARCHAR(100),
  LOGICAL_LIBRARY VARCHAR(100),
  START_TIME BIGINT,
  PRIORITY SMALLINT CONSTRAINT AJQ_MPAP_NN NOT NULL,
  STORAGE_CLASS VARCHAR(100),
  MIN_ARCHIVE_REQUEST_AGE INTEGER CONSTRAINT AJQ_MPAMR_NN NOT NULL,
  COPY_NB NUMERIC(3, 0),
  SIZE_IN_BYTES BIGINT,
  ARCHIVE_FILE_ID BIGINT,
  CHECKSUMBLOB BYTEA,
  REQUESTER_NAME VARCHAR(100),
  REQUESTER_GROUP VARCHAR(100),
  SRC_URL VARCHAR(2000),
  DISK_INSTANCE VARCHAR(100),
  DISK_FILE_PATH VARCHAR(2000),
  DISK_FILE_ID VARCHAR(100),
  DISK_FILE_GID INTEGER,
  DISK_FILE_OWNER_UID INTEGER,
 /* REPACK_REQID BIGINT, */
 /* IS_REPACK CHAR(1), */
  ARCHIVE_ERROR_REPORT_URL VARCHAR(2000),
  ARCHIVE_REPORT_URL VARCHAR(2000),
/* ARCHIVE specific columns */
/* REPACK_DEST_VID VARCHAR(20), */
  TOTAL_RETRIES SMALLINT,
  MAX_TOTAL_RETRIES SMALLINT,
  RETRIES_WITHIN_MOUNT SMALLINT,
  MAX_RETRIES_WITHIN_MOUNT SMALLINT,
  LAST_MOUNT_WITH_FAILURE BIGINT,
  IS_REPORTING BOOLEAN DEFAULT FALSE,
/* ARCHIVE specific columns */
  FAILURE_LOG TEXT DEFAULT '',
  LAST_UPDATE_TIME INTEGER DEFAULT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER),
  REPORT_FAILURE_LOG TEXT DEFAULT '',
  TOTAL_REPORT_RETRIES SMALLINT,
  MAX_REPORT_RETRIES SMALLINT
/* REPACK_FILEBUF_URL VARCHAR(2000), */
/* REPACK_FSEQ NUMERIC(20, 0) */
/* PARTITION BY RANGE (CREATION_TIME) */
/* CREATE TABLE ARCHIVE_ACTIVE_QUEUE_DEFAULT PARTITION OF ARCHIVE_ACTIVE_QUEUE DEFAULT */
/* ALTER TABLE ARCHIVE_ACTIVE_QUEUE ADD CONSTRAINT UNIQUE_ARCHIVE_COPY UNIQUE (ARCHIVE_FILE_ID) */
);
CREATE TABLE ARCHIVE_PENDING_QUEUE (LIKE ARCHIVE_ACTIVE_QUEUE INCLUDING ALL);
CREATE INDEX IDX_ARCHIVE_PENDING_QUEUE_FILTER_SORT ON ARCHIVE_PENDING_QUEUE (TAPE_POOL, STATUS, PRIORITY DESC, JOB_ID);
CREATE INDEX IDX_ARCHIVE_PENDING_QUEUE_MOUNT_ID_NULL ON ARCHIVE_PENDING_QUEUE (MOUNT_ID) WHERE MOUNT_ID IS NULL;
CREATE INDEX IDX_ARCHIVE_ACTIVE_QUEUE_COMPOSITE ON ARCHIVE_ACTIVE_QUEUE (TAPE_POOL, STATUS);
CREATE INDEX IDX_ARCHIVE_ACTIVE_QUEUE_COMPOSITE_ORDERING ON ARCHIVE_ACTIVE_QUEUE (PRIORITY DESC, JOB_ID);
/* CREATE INDEX IDX_ARCHIVE_ACTIVE_QUEUE_COMPOSITE ON ARCHIVE_ACTIVE_QUEUE (TAPE_POOL, PRIORITY, JOB_ID) */
CREATE TABLE ARCHIVE_FAILED_QUEUE (LIKE ARCHIVE_ACTIVE_QUEUE INCLUDING ALL);

CREATE TABLE RETRIEVE_SESSION_STATS
(
    MOUNT_ID         BIGSERIAL PRIMARY KEY,
    LAST_UPDATE_TIME INTEGER DEFAULT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER),
    VID              BIGINT,
    DRIVE            VARCHAR(100),
    INSERTED_JOBS    BIGINT,
    DELETED_JOBS     BIGINT,
    REQUEUED_JOBS    BIGINT,
    QUEUED_JOBS      BIGINT
);
CREATE TABLE ARCHIVE_SESSION_STATS
(
    MOUNT_ID         BIGSERIAL PRIMARY KEY,
    LAST_UPDATE_TIME INTEGER DEFAULT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER),
    TAPE_POOL        BIGINT,
    DRIVE            VARCHAR(100),
    INSERTED_JOBS    BIGINT,
    DELETED_JOBS     BIGINT,
    REQUEUED_JOBS    BIGINT,
    QUEUED_JOBS      BIGINT
);
CREATE TABLE RETRIEVE_ACTIVE_QUEUE(
  JOB_ID BIGSERIAL PRIMARY KEY,
  RETRIEVE_REQUEST_ID BIGINT,
  REQUEST_JOB_COUNT SMALLINT,
  STATUS RETRIEVE_JOB_STATUS CONSTRAINT RJQ_S_NN NOT NULL,
  ARCHIVE_FILE_ID BIGINT,
  CREATION_TIME BIGINT,
  STORAGE_CLASS VARCHAR(100),
  SIZE_IN_BYTES BIGINT,
  CHECKSUMBLOB BYTEA,
  FSEQ BIGINT,
  BLOCK_ID BIGINT,
  DISK_INSTANCE VARCHAR(100),
  DISK_FILE_PATH VARCHAR(2000),
  DISK_FILE_ID VARCHAR(100),
  DISK_FILE_GID INTEGER,
  DISK_FILE_OWNER_UID INTEGER,
  TAPE_POOL VARCHAR(100),
  MOUNT_POLICY VARCHAR(100) CONSTRAINT RJQ_MPN_NN NOT NULL,
  VID VARCHAR(20) CONSTRAINT RJQ_TPN_NN NOT NULL,
  ALTERNATE_COPY_NBS VARCHAR(20),
  ALTERNATE_FSEQS VARCHAR(256),
  ALTERNATE_BLOCK_IDS VARCHAR(256),
  ALTERNATE_VIDS VARCHAR(128),
  MOUNT_ID BIGINT,
  DRIVE VARCHAR(100),
  HOST VARCHAR(100),
  LOGICAL_LIBRARY VARCHAR(100),
  START_TIME BIGINT,
  PRIORITY SMALLINT CONSTRAINT RJQ_MPAP_NN NOT NULL,
  MIN_RETRIEVE_REQUEST_AGE INTEGER CONSTRAINT RJQ_MPAMR_NN NOT NULL,
  COPY_NB NUMERIC(3, 0),
  REQUESTER_NAME VARCHAR(100),
  REQUESTER_GROUP VARCHAR(100),
  DST_URL VARCHAR(2000),
  RETRIEVE_ERROR_REPORT_URL VARCHAR(2000),
  RETRIEVE_REPORT_URL VARCHAR(2000),
  TOTAL_RETRIES SMALLINT,
  MAX_TOTAL_RETRIES SMALLINT,
  RETRIES_WITHIN_MOUNT SMALLINT,
  MAX_RETRIES_WITHIN_MOUNT SMALLINT,
  LAST_MOUNT_WITH_FAILURE BIGINT,
  IS_REPORTING BOOLEAN DEFAULT FALSE,
  FAILURE_LOG TEXT DEFAULT '',
  LAST_UPDATE_TIME INTEGER DEFAULT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER),
  REPORT_FAILURE_LOG TEXT DEFAULT '',
  TOTAL_REPORT_RETRIES SMALLINT,
  MAX_REPORT_RETRIES SMALLINT,
  ACTIVITY VARCHAR(100),
  SRR_USERNAME VARCHAR(100),
  SRR_HOST VARCHAR(100),
  SRR_TIME BIGINT,
  SRR_MOUNT_POLICY VARCHAR(100),
  SRR_ACTIVITY VARCHAR(100),
/* User is_verify from schedulerretrieverequest */
  IS_VERIFY_ONLY BOOLEAN DEFAULT FALSE,
/*  IS_REPACK CHAR(1), */
  LIFECYCLE_CREATION_TIME BIGINT,
  LIFECYCLE_FIRST_SELECTED_TIME BIGINT,
  LIFECYCLE_COMPLETED_TIME BIGINT,
  DISK_SYSTEM_NAME VARCHAR(100)
);
/* ALTER TABLE RETRIEVE_ACTIVE_QUEUE ADD CONSTRAINT UNIQUE_RETRIEVE_COPY UNIQUE (ARCHIVE_FILE_ID) */
CREATE TABLE RETRIEVE_FAILED_QUEUE (LIKE RETRIEVE_ACTIVE_QUEUE INCLUDING ALL);
CREATE TABLE RETRIEVE_PENDING_QUEUE (LIKE RETRIEVE_ACTIVE_QUEUE INCLUDING ALL);
CREATE INDEX IDX_RETRIEVE_ACTIVE_QUEUE_COMPOSITE ON RETRIEVE_ACTIVE_QUEUE (VID, STATUS);
CREATE INDEX IDX_RETRIEVE_ACTIVE_QUEUE_COMPOSITE_ORDERING ON RETRIEVE_ACTIVE_QUEUE (PRIORITY DESC, JOB_ID);
CREATE INDEX IDX_RETRIEVE_PENDING_QUEUE_FILTER_SORT ON RETRIEVE_PENDING_QUEUE (VID, STATUS, PRIORITY DESC, JOB_ID);
CREATE INDEX IDX_RETRIEVE_PENDING_QUEUE_MOUNT_ID_NULL ON RETRIEVE_PENDING_QUEUE (MOUNT_ID) WHERE MOUNT_ID IS NULL;

CREATE TABLE REPACK_ACTIVE_QUEUE(
  REPACK_REQID BIGSERIAL,
  VID VARCHAR(20),
  BUFFER_URL VARCHAR(2000),
  STATUS REPACK_REQ_STATUS,
  IS_ADD_COPIES CHAR(1),
  IS_MOVE CHAR(1),
  TOTAL_FILES_TO_RETRIEVE BIGINT,
  TOTAL_BYTES_TO_RETRIEVE BIGINT,
  TOTAL_FILES_TO_ARCHIVE BIGINT,
  TOTAL_BYTES_TO_ARCHIVE BIGINT,
  USER_PROVIDED_FILES BIGINT,
  USER_PROVIDED_BYTES BIGINT,
  RETRIEVED_FILES BIGINT,
  RETRIEVED_BYTES BIGINT,
  ARCHIVED_FILES BIGINT,
  ARCHIVED_BYTES BIGINT,
  FAILED_TO_RETRIEVE_FILES BIGINT,
  FAILED_TO_RETRIEVE_BYTES BIGINT,
  FAILED_TO_CREATE_ARCHIVE_REQ BIGINT,
  FAILED_TO_ARCHIVE_FILES BIGINT,
  FAILED_TO_ARCHIVE_BYTES BIGINT,
  LAST_EXPANDED_FSEQ BIGINT,
  IS_EXPAND_FINISHED CHAR(1),
  IS_EXPAND_STARTED  CHAR(1),
  MOUNT_POLICY VARCHAR(100),
  IS_COMPLETE CHAR(1),
  IS_NO_RECALL CHAR(1),
  SUBREQ_PB BYTEA,
  DESTINFO_PB BYTEA,
  CREATE_USERNAME VARCHAR(100),
  CREATE_HOST VARCHAR(100),
  CREATE_TIME BIGINT,
  REPACK_FINISHED_TIME BIGINT
);
CREATE TABLE TAPE_MOUNTS(
  MOUNT_ID BIGSERIAL,
  CREATION_TIME TIMESTAMPTZ DEFAULT NOW(),
  OWNER VARCHAR(100)
);
CREATE TABLE ARCHIVE_QUEUE_SUMMARY_INCREMENTAL(
    MOUNT_ID                 BIGINT PRIMARY KEY,
    TAPE_POOL                VARCHAR(100) CONSTRAINT AJQ_TPN_NN NOT NULL,
    MOUNT_POLICY             VARCHAR(100) CONSTRAINT AJQ_MPN_NN NOT NULL,
    JOB_COUNT               BIGINT,
    JOB_TOTAL_SIZE          BIGINT,
    OLDEST_JOB_START_TIME    BIGINT,
    MAX_PRIORITY                 SMALLINT,
    MIN_ARCHIVE_REQUEST_AGE  INTEGER,
    LAST_UPDATE_TIME         INTEGER DEFAULT (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::INTEGER)
);
CREATE VIEW ARCHIVE_QUEUE_SUMMARY AS (SELECT STATUS,
    TAPE_POOL,
    MOUNT_POLICY,
    MOUNT_ID,
    COUNT(*) AS JOBS_COUNT,
    SUM(SIZE_IN_BYTES) AS JOBS_TOTAL_SIZE,
    MIN(START_TIME) AS OLDEST_JOB_START_TIME,
    MAX(PRIORITY) AS ARCHIVE_PRIORITY,
    MIN(MIN_ARCHIVE_REQUEST_AGE) AS ARCHIVE_MIN_REQUEST_AGE,
    MAX(LAST_UPDATE_TIME) AS LAST_JOB_UPDATE_TIME
        FROM ARCHIVE_PENDING_QUEUE WHERE MOUNT_ID IS NULL GROUP BY STATUS, TAPE_POOL, MOUNT_POLICY, MOUNT_ID
    );

CREATE VIEW RETRIEVE_QUEUE_SUMMARY AS (SELECT
    VID,
    MOUNT_POLICY,
    ACTIVITY,
    MAX(PRIORITY) AS PRIORITY,
    COUNT(*) AS JOBS_COUNT,
    SUM(SIZE_IN_BYTES) AS JOBS_TOTAL_SIZE,
    MIN(START_TIME) AS OLDEST_JOB_START_TIME,
    MAX(START_TIME) AS YOUNGEST_JOB_START_TIME,
    MIN(MIN_RETRIEVE_REQUEST_AGE) AS RETRIEVE_MIN_REQUEST_AGE,
    MAX(LAST_UPDATE_TIME) AS LAST_JOB_UPDATE_TIME
        FROM RETRIEVE_PENDING_QUEUE WHERE MOUNT_ID IS NULL GROUP BY VID, MOUNT_POLICY, ACTIVITY
    );
CREATE SEQUENCE MOUNT_ID_SEQ
    INCREMENT BY 1
    START WITH 1
    NO MAXVALUE
    MINVALUE 1
    NO CYCLE
    CACHE 1;
CREATE SEQUENCE ARCHIVE_REQUEST_ID_SEQ
    INCREMENT BY 1
    START WITH 1
    NO MAXVALUE
    MINVALUE 1
    NO CYCLE
    CACHE 1;
CREATE SEQUENCE RETRIEVE_REQUEST_ID_SEQ
    INCREMENT BY 1
    START WITH 1
    NO MAXVALUE
    MINVALUE 1
    NO CYCLE
    CACHE 1;