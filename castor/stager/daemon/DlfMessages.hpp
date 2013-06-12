/*********************************************************************************/
/* File including the constants to represent the DLF Messages for the new */
/*******************************************************************************/

#ifndef STAGER_DLF_MESSAGES_HPP
#define STAGER_DLF_MESSAGES_HPP 1


namespace castor{
  namespace stager{
    namespace daemon{

      enum StgDlfMessages{

        /***************************************/
        /* Daemon: To DLF_LVL_DEBUG */
        /*************************************/

        STAGER_DAEMON_START = 1, /*  Daemon started" */
        STAGER_DAEMON_EXECUTION = 2, /* " Daemon execution" */
        STAGER_DAEMON_ERROR_CONFIG = 3, /* " Daemon configuration error" */
        STAGER_DAEMON_EXCEPTION = 4, /* " Daemon Exception" */
        STAGER_DAEMON_POOLCREATION = 7, /* " Daemon Pool creation" */


        STAGER_CONFIGURATION = 8, /* "Got wrong configuration, using default" */
        STAGER_CONFIGURATION_ERROR = 9, /* "Impossible to get (right) configuration" */

        /*******************************************************************************************************/
        /* Constants related with the DBService SvcThreads: JobRequestSvc, PreRequestSvc, StgRequestSvc */
        /*****************************************************************************************************/
        /* mainly for DLF_LVL_DEBUG */
        /***************************/
        STAGER_JOBREQSVC_CREATION = 21, /* "Created new JobRequestSvc Thread" */
        STAGER_PREREQSVC_CREATION = 22, /* "Created new PreRequestSvc Thread" */
        STAGER_STGREQSVC_CREATION = 23, /* "Created new StgRequestSvc Thread" */
        STAGER_SUBREQ_SELECTED = 24,
        STAGER_REQ_PROCESSED = 25,

        /* JobRequestSvcThread */
        STAGER_GET = 31, /*Get Request" */
        STAGER_UPDATE = 32, /* Update Request" */
        STAGER_PUT = 33, /* "Put Request" */

        /************************/
        /* PreRequestSvcThread */
        STAGER_REPACK = 34, /* "Repack Request" */
        STAGER_PREPARETOGET = 35, /* "PrepareToGet Request" */
        STAGER_PREPARETOUPDATE = 36, /* "PrepareToUpdate Request" */
        STAGER_PREPARETOPUT = 37, /* "PrepareToPut Request" */

        /*************************/
        /* StgRequestSvcThread  */
        STAGER_SETGC = 38, /* "SetGC Request" */
        STAGER_RM = 40, /* Rm Request" */
        STAGER_RMPERFORMED = 41, /* Rm performed" */
        STAGER_PUTDONE = 42, /* "PutDone Request" */
        STAGER_CHGPRIV = 43, /* "ChangePrivilege Request" */

        /***********************/
        /* BulkStageSvcThread  */
        STAGER_BLKSTGSVC_ABORT = 152, /* Abort processed */
        STAGER_BLKSTGSVC_REPACK = 155, /* Repack initiated */
        STAGER_BLKSTGSVC_UNKREQ = 153, /* "Unknown request processed" */

        /******************/
        /*  SYSTEM LEVEL */
        /****************/
        /* after calling the corresponding stagerService function, to show the decision taken */
        STAGER_WAITSUBREQ = 53, /* Request moved to Wait" */
        STAGER_REPACK_MIGRATION = 54, /* Starting Repack Migration" */
        STAGER_TAPE_RECALL = 57, /* Triggering Tape Recall" */
        STAGER_SCHEDULINGJOB = 60, /* Diskcopy available, scheduling job */

        /* DLF_LVL_ERROR */
        STAGER_SERVICES_EXCEPTION = 71, /*Impossible to get the Service" */
        STAGER_SVCCLASS_EXCEPTION = 72, /*Impossible to get the SvcClass" */
        STAGER_USER_INVALID = 73, /*Invalid user" */
        STAGER_USER_PERMISSION = 74, /*User doenst have the right permission" */
        STAGER_USER_NONFILE = 75, /*User asking for a Non Existing File" */
        STAGER_INVALID_FILESYSTEM = 76, /*Invalid fileSystem" */
        STAGER_UNABLETOPERFORM = 77, /* Unable to perform request, notifying user */
        STAGER_CNS_EXCEPTION = 79, /* Error on the name server */
        STAGER_SUBREQUESTUUID_EXCEPTION = 80, /* Impossible to get the subrequest Uuid */
        STAGER_REQUESTUUID_EXCEPTION = 81, /* Impossible to get the request Uuid */
        STAGER_CASTORFILE_EXCEPTION = 82, /* Impossible to get the CastorFile */
        STAGER_INVALID_TYPE = 83,  /* Request type not valid for this thread pool */

        /*******************/
        /* QueryRequestSvc */
        STAGER_QRYSVC_GETSVC = 91, /* Could not get QuerySvc" */
        STAGER_QRYSVC_EXCEPT = 92, /*Unexpected exception caught" */
        STAGER_QRYSVC_NOCLI = 93, /* "No client associated with request ! Cannot answer !" */
        STAGER_QRYSVC_INVSC = 94, /* "Invalid ServiceClass name" */
        STAGER_QRYSVC_UNKREQ = 95, /* Unknown request type */
        STAGER_QRYSVC_INVARG = 96, /* Invalid argument */
        STAGER_QRYSVC_FQNOPAR = 97, /* "StageFileQueryRequest has no parameters" */
        STAGER_QRYSVC_FQUERY = 98, /* "Processing File Query by fileName" */
        STAGER_QRYSVC_IQUERY = 99, /* "Processing File Query by fileId" */
        STAGER_QRYSVC_RQUERY = 100, /* "Processing File Query by Request" */
        STAGER_QRYSVC_DSQUERY = 101, /* "Processing DiskPoolQuery by SvcClass" */
        STAGER_QRYSVC_DDQUERY = 102, /* "Processing DiskPoolQuery by DiskPool" */
        STAGER_QRYSVC_DFAILED = 103, /* "Failed to process DiskPoolQuery" */
        STAGER_QRYSVC_VQUERY = 149, /* Processing VersionQuery */
        STAGER_QRYSVC_CPRIV = 150, /* Processing ChangePrivilege */
        STAGER_QRYSVC_LPRIV = 151, /* Processing ListPrivieleg */

        /*********/
        /* GcSvc */
        STAGER_GCSVC_GETSVC = 111, /* "Could not get GCSvc" */
        STAGER_GCSVC_EXCEPT = 112, /* "Unexpected exception caught" */
        STAGER_GCSVC_NOCLI = 113, /* "No client associated with request ! Cannot answer !" */
        STAGER_GCSVC_UNKREQ = 114, /* "Unknown Request type" */
        STAGER_GCSVC_FDELOK = 115, /* "Invoking filesDeleted" */
        STAGER_GCSVC_FDELFAIL = 116, /* "Invoking filesDeletionFailed" */
        STAGER_GCSVC_SELF2DEL = 117, /* "Invoking selectFiles2Delete" */
        STAGER_GCSVC_FSEL4DEL = 118, /* "File selected for deletion" */
        STAGER_GCSVC_NSFILDEL = 119, /* "Invoking nsFilesDeleted" */
        STAGER_GCSVC_FNSDEL = 120, /* "File deleted since it disappeared from nameserver" */
        STAGER_GCSVC_STGFILDEL = 125, /* "Invoking stgFilesDeleted" */
        STAGER_GCSVC_FSTGDEL = 126 , /* "File to be unlinked since it dissapeared from the stager" */

        /************/
        /* ErrorSvc */
        STAGER_ERRSVC_GETSVC = 121, /* "Could not get Svc" */
        STAGER_ERRSVC_EXCEPT = 122, /* "Unexpected exception caught" */
        STAGER_ERRSVC_NOREQ = 123, /* "No request associated with subrequest ! Cannot answer !" */
        STAGER_ERRSVC_NOCLI = 124, /* "No client associated with request ! Cannot answer !" */

        /**********/
        /* JobSvc */
        STAGER_JOBSVC_GETSVC = 131, /* "Could not get JobSvc" */
        STAGER_JOBSVC_EXCEPT = 132, /* "Unexpected exception caught" */
        STAGER_JOBSVC_NOSREQ = 133, /* "Could not find subrequest associated to Request" */
        STAGER_JOBSVC_BADSRT = 134, /* "Expected SubRequest in Request but found another type" */
        STAGER_JOBSVC_GETUPDS = 136, /* "Invoking getUpdateStart" */
        STAGER_JOBSVC_PUTS = 137, /* "Invoking putStart" */
        STAGER_JOBSVC_PFMIG = 140, /* "Invoking prepareForMigration" */
        STAGER_JOBSVC_GETUPDO = 141, /* "Invoking getUpdateDone" */
        STAGER_JOBSVC_GETUPFA = 142, /* "Invoking getUpdateFailed" */
        STAGER_JOBSVC_PUTFAIL = 143, /* "Invoking putFailed" */
        STAGER_JOBSVC_NOCLI = 144, /* "No client associated with request ! Cannot answer !" */
        STAGER_JOBSVC_UNKREQ = 145, /* "Unknown Request type" */
        STAGER_JOBSVC_1STBWR = 147, /* "Invoking firstByteWritten"*/
        STAGER_JOBSVC_DELWWR = 148, /* "File was removed by another user while being modified" */
        STAGER_JOBSVC_CHKMISMATCH = 154, /* "Preset checksum mismatch detected, invoking putFailed" */

        /**************/
        /* LoggingSvc */
        STAGER_LOGGING_DONE = 127,/* "Dump of the DB logs completed, dropping db connection" */
        STAGER_LOGGING_EXCEPT = 156 /* "Unexpected exception caught" */

      };
    }// end namespace daemon
  }// end namespace stager
}// end namespace castor

#endif

