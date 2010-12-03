/*******************************************************************
 *
 * @(#)$RCSfile$ $Revision$ $Date$ $Author$
 *
 * This file contains is an SQL script used to test polices of the tape gateway
 * on a disposable stager schema, which is supposed to be initially empty.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/
 
 /*  W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    */
 /*
  * D O   N O    U S E    O N    A   P R O D   O R   D E V    S T A G E R
  *
  * Only intended for throwaway schema on devdb10
  */
   /*  W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    */

DROP PACKAGE TG_POLICY_TESTING;
DROP TRIGGER TG_test_policies_logs_insert;
DROP TABLE TG_test_policies_logs;
DROP SEQUENCE TG_test_policies_seq;
CREATE TABLE TG_test_policies_logs (id NUMBER, Line VARCHAR2(2048));
CREATE SEQUENCE TG_test_policies_seq START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE  
TRIGGER TG_test_policies_logs_insert 
BEFORE INSERT ON TG_test_policies_logs 
FOR EACH ROW
BEGIN
  SELECT TG_test_policies_seq.nextval INTO :new.id FROM DUAL;
END; /* TRIGGER TG_test_policies_logs_insert */
/

CREATE OR REPLACE PACKAGE TG_POLICY_TESTING AS
	PROCEDURE ClearLog;
	PROCEDURE LogLine (Line IN VARCHAR2);
	PROCEDURE ClearData;
	PROCEDURE PopulateData;
	PROCEDURE LoopGetNextFile;
	PROCEDURE Test1;
END TG_POLICY_TESTING;
/


/* Create object types package for cleaner coding */
/* Generated from:
  perl -p -e 'if (/OBJ_([^\s=]*)\s*=\s*(\d+)/) { $_ = "\t$1 CONSTANT NUMBER := $2;\n"; } else { $_ = ""; }' < castor/Constants.hpp
  */
CREATE OR REPLACE PACKAGE castorObjTypes 
IS
        INVALID CONSTANT NUMBER := 0;
        Ptr CONSTANT NUMBER := 1;
        CastorFile CONSTANT NUMBER := 2;
        Cuuid CONSTANT NUMBER := 4;
        DiskCopy CONSTANT NUMBER := 5;
        DiskFile CONSTANT NUMBER := 6;
        DiskPool CONSTANT NUMBER := 7;
        DiskServer CONSTANT NUMBER := 8;
        FileClass CONSTANT NUMBER := 10;
        FileSystem CONSTANT NUMBER := 12;
        IClient CONSTANT NUMBER := 13;
        MessageAck CONSTANT NUMBER := 14;
        ReqIdRequest CONSTANT NUMBER := 16;
        Request CONSTANT NUMBER := 17;
        Segment CONSTANT NUMBER := 18;
        StageGetNextRequest CONSTANT NUMBER := 21;
        Stream CONSTANT NUMBER := 26;
        SubRequest CONSTANT NUMBER := 27;
        SvcClass CONSTANT NUMBER := 28;
        Tape CONSTANT NUMBER := 29;
        TapeCopy CONSTANT NUMBER := 30;
        TapePool CONSTANT NUMBER := 31;
        StageFileQueryRequest CONSTANT NUMBER := 33;
        StageGetRequest CONSTANT NUMBER := 35;
        StagePrepareToGetRequest CONSTANT NUMBER := 36;
        StagePrepareToPutRequest CONSTANT NUMBER := 37;
        StagePrepareToUpdateRequest CONSTANT NUMBER := 38;
        StagePutDoneRequest CONSTANT NUMBER := 39;
        StagePutRequest CONSTANT NUMBER := 40;
        StageRmRequest CONSTANT NUMBER := 42;
        StageUpdateRequest CONSTANT NUMBER := 44;
        FileRequest CONSTANT NUMBER := 45;
        QryRequest CONSTANT NUMBER := 46;
        StagePutNextRequest CONSTANT NUMBER := 48;
        StageUpdateNextRequest CONSTANT NUMBER := 49;
        StageAbortRequest CONSTANT NUMBER := 50;
        StageReleaseFilesRequest CONSTANT NUMBER := 51;
        DiskCopyForRecall CONSTANT NUMBER := 58;
        TapeCopyForMigration CONSTANT NUMBER := 59;
        GetUpdateStartRequest CONSTANT NUMBER := 60;
        BaseAddress CONSTANT NUMBER := 62;
        Disk2DiskCopyDoneRequest CONSTANT NUMBER := 64;
        MoverCloseRequest CONSTANT NUMBER := 65;
        StartRequest CONSTANT NUMBER := 66;
        PutStartRequest CONSTANT NUMBER := 67;
        IObject CONSTANT NUMBER := 69;
        IAddress CONSTANT NUMBER := 70;
        QueryParameter CONSTANT NUMBER := 71;
        DiskCopyInfo CONSTANT NUMBER := 72;
        Files2Delete CONSTANT NUMBER := 73;
        FilesDeleted CONSTANT NUMBER := 74;
        GCLocalFile CONSTANT NUMBER := 76;
        GetUpdateDone CONSTANT NUMBER := 78;
        GetUpdateFailed CONSTANT NUMBER := 79;
        PutFailed CONSTANT NUMBER := 80;
        GCFile CONSTANT NUMBER := 81;
        GCFileList CONSTANT NUMBER := 82;
        FilesDeletionFailed CONSTANT NUMBER := 83;
        TapeRequest CONSTANT NUMBER := 84;
        ClientIdentification CONSTANT NUMBER := 85;
        TapeServer CONSTANT NUMBER := 86;
        TapeDrive CONSTANT NUMBER := 87;
        DeviceGroupName CONSTANT NUMBER := 88;
        ErrorHistory CONSTANT NUMBER := 89;
        TapeDriveDedication CONSTANT NUMBER := 90;
        TapeAccessSpecification CONSTANT NUMBER := 91;
        TapeDriveCompatibility CONSTANT NUMBER := 92;
        SetFileGCWeight CONSTANT NUMBER := 95;
        RepackRequest CONSTANT NUMBER := 96;
        RepackSubRequest CONSTANT NUMBER := 97;
        RepackSegment CONSTANT NUMBER := 98;
        RepackAck CONSTANT NUMBER := 99;
        DiskServerDescription CONSTANT NUMBER := 101;
        FileSystemDescription CONSTANT NUMBER := 102;
        DiskPoolQuery CONSTANT NUMBER := 103;
        EndResponse CONSTANT NUMBER := 104;
        FileResponse CONSTANT NUMBER := 105;
        StringResponse CONSTANT NUMBER := 106;
        Response CONSTANT NUMBER := 107;
        IOResponse CONSTANT NUMBER := 108;
        AbortResponse CONSTANT NUMBER := 109;
        GetUpdateStartResponse CONSTANT NUMBER := 113;
        BasicResponse CONSTANT NUMBER := 114;
        StartResponse CONSTANT NUMBER := 115;
        GCFilesResponse CONSTANT NUMBER := 116;
        FileQryResponse CONSTANT NUMBER := 117;
        DiskPoolQueryResponse CONSTANT NUMBER := 118;
        StageRepackRequest CONSTANT NUMBER := 119;
        DiskServerStateReport CONSTANT NUMBER := 120;
        DiskServerMetricsReport CONSTANT NUMBER := 121;
        FileSystemStateReport CONSTANT NUMBER := 122;
        FileSystemMetricsReport CONSTANT NUMBER := 123;
        DiskServerAdminReport CONSTANT NUMBER := 124;
        FileSystemAdminReport CONSTANT NUMBER := 125;
        StreamReport CONSTANT NUMBER := 126;
        FileSystemStateAck CONSTANT NUMBER := 127;
        MonitorMessageAck CONSTANT NUMBER := 128;
        Client CONSTANT NUMBER := 129;
        JobSubmissionRequest CONSTANT NUMBER := 130;
        VersionQuery CONSTANT NUMBER := 131;
        VersionResponse CONSTANT NUMBER := 132;
        StageDiskCopyReplicaRequest CONSTANT NUMBER := 133;
        RepackResponse CONSTANT NUMBER := 134;
        RepackFileQry CONSTANT NUMBER := 135;
        CnsInfoMigrationPolicy CONSTANT NUMBER := 136;
        DbInfoMigrationPolicy CONSTANT NUMBER := 137;
        CnsInfoRecallPolicy CONSTANT NUMBER := 138;
        DbInfoRecallPolicy CONSTANT NUMBER := 139;
        DbInfoStreamPolicy CONSTANT NUMBER := 140;
        PolicyObj CONSTANT NUMBER := 141;
        NsFilesDeleted CONSTANT NUMBER := 142;
        NsFilesDeletedResponse CONSTANT NUMBER := 143;
        Disk2DiskCopyStartRequest CONSTANT NUMBER := 144;
        Disk2DiskCopyStartResponse CONSTANT NUMBER := 145;
        ThreadNotification CONSTANT NUMBER := 146;
        FirstByteWritten CONSTANT NUMBER := 147;
        VdqmTape CONSTANT NUMBER := 148;
        StgFilesDeleted CONSTANT NUMBER := 149;
        StgFilesDeletedResponse CONSTANT NUMBER := 150;
        VolumePriority CONSTANT NUMBER := 151;
        ChangePrivilege CONSTANT NUMBER := 152;
        BWUser CONSTANT NUMBER := 153;
        RequestType CONSTANT NUMBER := 154;
        ListPrivileges CONSTANT NUMBER := 155;
        Privilege CONSTANT NUMBER := 156;
        ListPrivilegesResponse CONSTANT NUMBER := 157;
        PriorityMap CONSTANT NUMBER := 158;
        VectorAddress CONSTANT NUMBER := 159;
        Tape2DriveDedication CONSTANT NUMBER := 160;
        TapeRecall CONSTANT NUMBER := 161;
        FileMigratedNotification CONSTANT NUMBER := 162;
        FileRecalledNotification CONSTANT NUMBER := 163;
        FileToMigrateRequest CONSTANT NUMBER := 164;
        FileToMigrate CONSTANT NUMBER := 165;
        FileToRecallRequest CONSTANT NUMBER := 166;
        FileToRecall CONSTANT NUMBER := 167;
        VolumeRequest CONSTANT NUMBER := 168;
        Volume CONSTANT NUMBER := 169;
        TapeGatewayRequest CONSTANT NUMBER := 170;
        DbInfoRetryPolicy CONSTANT NUMBER := 171;
        EndNotification CONSTANT NUMBER := 172;
        NoMoreFiles CONSTANT NUMBER := 173;
        NotificationAcknowledge CONSTANT NUMBER := 174;
        FileErrorReport CONSTANT NUMBER := 175;
        BaseFileInfo CONSTANT NUMBER := 176;
        RmMasterReport CONSTANT NUMBER := 178;
        EndNotificationErrorReport CONSTANT NUMBER := 179;
        TapeGatewaySubRequest CONSTANT NUMBER := 180;
        GatewayMessage CONSTANT NUMBER := 181;
        DumpNotification CONSTANT NUMBER := 182;
        PingNotification CONSTANT NUMBER := 183;
        DumpParameters CONSTANT NUMBER := 184;
        DumpParametersRequest CONSTANT NUMBER := 185;
        RecallPolicyElement CONSTANT NUMBER := 186;
        MigrationPolicyElement CONSTANT NUMBER := 187;
        StreamPolicyElement CONSTANT NUMBER := 188;
        RetryPolicyElement CONSTANT NUMBER := 189;
        VdqmTapeGatewayRequest CONSTANT NUMBER := 190;
        StageQueryResult CONSTANT NUMBER := 191;
END castorobjtypes;
/

/* Create object status package for cleaner coding */
/* Generated from:
  find Devplayground/Workspace/v2_1_9_7_gateway/ -name *StatusCode*.hpp | xargs cat | perl -e 'my $in_enum=0; while (<>) { if ($in_enum) { if (/^\s*(\w+)\s*=\s*(\d+)/) { print "\t$1 CONSTANT NUMBER := $2;\n"; } elsif (/\}\;/) { $in_enum =0; } } else { if (/enum.*StatusCode/) { $in_enum = 1; } } }' > Devplayground/raw_states.txt
  */
CREATE OR REPLACE PACKAGE CastorStatus 
IS
	ADMIN_NONE CONSTANT NUMBER := 0;
	ADMIN_FORCE CONSTANT NUMBER := 1;
	ADMIN_RELEASE CONSTANT NUMBER := 2;
	ADMIN_DELETED CONSTANT NUMBER := 3;
	RSUBREQUEST_TOBECHECKED CONSTANT NUMBER := 0;
	RSUBREQUEST_TOBESTAGED CONSTANT NUMBER := 1;
	RSUBREQUEST_ONGOING CONSTANT NUMBER := 2;
	RSUBREQUEST_TOBECLEANED CONSTANT NUMBER := 3;
	RSUBREQUEST_DONE CONSTANT NUMBER := 4;
	RSUBREQUEST_FAILED CONSTANT NUMBER := 5;
	RSUBREQUEST_TOBEREMOVED CONSTANT NUMBER := 6;
	RSUBREQUEST_TOBERESTARTED CONSTANT NUMBER := 7;
	RSUBREQUEST_ARCHIVED CONSTANT NUMBER := 8;
	RSUBREQUEST_ONHOLD CONSTANT NUMBER := 9;
	DISKCOPY_STAGED CONSTANT NUMBER := 0;
	DISKCOPY_WAITDISK2DISKCOPY CONSTANT NUMBER := 1;
	DISKCOPY_WAITTAPERECALL CONSTANT NUMBER := 2;
	DISKCOPY_DELETED CONSTANT NUMBER := 3;
	DISKCOPY_FAILED CONSTANT NUMBER := 4;
	DISKCOPY_WAITFS CONSTANT NUMBER := 5;
	DISKCOPY_STAGEOUT CONSTANT NUMBER := 6;
	DISKCOPY_INVALID CONSTANT NUMBER := 7;
	DISKCOPY_BEINGDELETED CONSTANT NUMBER := 9;
	DISKCOPY_CANBEMIGR CONSTANT NUMBER := 10;
	DISKCOPY_WAITFS_SCHEDULING CONSTANT NUMBER := 11;
	DISKSERVER_PRODUCTION CONSTANT NUMBER := 0;
	DISKSERVER_DRAINING CONSTANT NUMBER := 1;
	DISKSERVER_DISABLED CONSTANT NUMBER := 2;
	FILESYSTEM_PRODUCTION CONSTANT NUMBER := 0;
	FILESYSTEM_DRAINING CONSTANT NUMBER := 1;
	FILESYSTEM_DISABLED CONSTANT NUMBER := 2;
	SEGMENT_UNPROCESSED CONSTANT NUMBER := 0;
	SEGMENT_FILECOPIED CONSTANT NUMBER := 5;
	SEGMENT_FAILED CONSTANT NUMBER := 6;
	SEGMENT_SELECTED CONSTANT NUMBER := 7;
	SEGMENT_RETRIED CONSTANT NUMBER := 8;
	STREAM_PENDING CONSTANT NUMBER := 0;
	STREAM_WAITDRIVE CONSTANT NUMBER := 1;
	STREAM_WAITMOUNT CONSTANT NUMBER := 2;
	STREAM_RUNNING CONSTANT NUMBER := 3;
	STREAM_WAITSPACE CONSTANT NUMBER := 4;
	STREAM_CREATED CONSTANT NUMBER := 5;
	STREAM_STOPPED CONSTANT NUMBER := 6;
	STREAM_WAITPOLICY CONSTANT NUMBER := 7;
	GETNEXTSTATUS_NOTAPPLICABLE CONSTANT NUMBER := 0;
	GETNEXTSTATUS_FILESTAGED CONSTANT NUMBER := 1;
	GETNEXTSTATUS_NOTIFIED CONSTANT NUMBER := 2;
	SUBREQUEST_START CONSTANT NUMBER := 0;
	SUBREQUEST_RESTART CONSTANT NUMBER := 1;
	SUBREQUEST_RETRY CONSTANT NUMBER := 2;
	SUBREQUEST_WAITSCHED CONSTANT NUMBER := 3;
	SUBREQUEST_WAITTAPERECALL CONSTANT NUMBER := 4;
	SUBREQUEST_WAITSUBREQ CONSTANT NUMBER := 5;
	SUBREQUEST_READY CONSTANT NUMBER := 6;
	SUBREQUEST_FAILED CONSTANT NUMBER := 7;
	SUBREQUEST_FINISHED CONSTANT NUMBER := 8;
	SUBREQUEST_FAILED_FINISHED CONSTANT NUMBER := 9;
	SUBREQUEST_FAILED_ANSWERING CONSTANT NUMBER := 10;
	SUBREQUEST_ARCHIVED CONSTANT NUMBER := 11;
	SUBREQUEST_REPACK CONSTANT NUMBER := 12;
	SUBREQUEST_READYFORSCHED CONSTANT NUMBER := 13;
	SUBREQUEST_BEINGSCHED CONSTANT NUMBER := 14;
	TAPECOPY_CREATED CONSTANT NUMBER := 0;
	TAPECOPY_TOBEMIGRATED CONSTANT NUMBER := 1;
	TAPECOPY_WAITINSTREAMS CONSTANT NUMBER := 2;
	TAPECOPY_SELECTED CONSTANT NUMBER := 3;
	TAPECOPY_TOBERECALLED CONSTANT NUMBER := 4;
	TAPECOPY_STAGED CONSTANT NUMBER := 5;
	TAPECOPY_FAILED CONSTANT NUMBER := 6;
	TAPECOPY_WAITPOLICY CONSTANT NUMBER := 7;
	TAPECOPY_REC_RETRY CONSTANT NUMBER := 8;
	TAPECOPY_MIG_RETRY CONSTANT NUMBER := 9;
	TAPE_UNUSED CONSTANT NUMBER := 0;
	TAPE_PENDING CONSTANT NUMBER := 1;
	TAPE_WAITDRIVE CONSTANT NUMBER := 2;
	TAPE_WAITMOUNT CONSTANT NUMBER := 3;
	TAPE_MOUNTED CONSTANT NUMBER := 4;
	TAPE_FINISHED CONSTANT NUMBER := 5;
	TAPE_FAILED CONSTANT NUMBER := 6;
	TAPE_UNKNOWN CONSTANT NUMBER := 7;
	TAPE_WAITPOLICY CONSTANT NUMBER := 8;
	UNIT_UP CONSTANT NUMBER := 0;
	UNIT_STARTING CONSTANT NUMBER := 1;
	UNIT_ASSIGNED CONSTANT NUMBER := 2;
	VOL_MOUNTED CONSTANT NUMBER := 3;
	FORCED_UNMOUNT CONSTANT NUMBER := 4;
	UNIT_DOWN CONSTANT NUMBER := 5;
	WAIT_FOR_UNMOUNT CONSTANT NUMBER := 6;
	STATUS_UNKNOWN CONSTANT NUMBER := 7;
	REQUEST_PENDING CONSTANT NUMBER := 0;
	REQUEST_MATCHED CONSTANT NUMBER := 1;
	REQUEST_BEINGSUBMITTED CONSTANT NUMBER := 2;
	REQUEST_SUBMITTED CONSTANT NUMBER := 3;
	REQUEST_FAILED CONSTANT NUMBER := 4;
	TAPESERVER_ACTIVE CONSTANT NUMBER := 0;
	TAPESERVER_INACTIVE CONSTANT NUMBER := 1;
END castorstatus;
/