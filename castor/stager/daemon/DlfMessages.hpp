/*********************************************************************************/
/* File including the constants to represent the DLF Messages for the new Stager*/
/*******************************************************************************/

#ifndef STAGER_DLF_MESSAGES_HPP
#define STAGER_DLF_MESSAGES_HPP 1



namespace castor{
  namespace stager{
    namespace dbService{

      
   

   enum StgDlfMessages{


     /***************************************/
     /* StagerMainDaemon: To DLF_LVL_DEBUG */
     /*************************************/
     
     STAGER_DAEMON_START= 1, /* Stager Daemon started" */
     STAGER_DAEMON_EXECUTION= 2, /* "Stager Daemon execution" */
     STAGER_DAEMON_ERROR_CONFIG= 3, /* "Stager Daemon configuration error" */
     STAGER_DAEMON_EXCEPTION= 4, /* "Stager Daemon Exception" */
     STAGER_DAEMON_EXCEPTION_GENERAL= 5, /* "Stager Daemon General Exception" */
     STAGER_DAEMON_FINISHED= 6, /* "Stager Daemon successfully finished" */  	  
     STAGER_DAEMON_POOLCREATION= 7, /* "Stager Daemon Pool creation" */
     
     
     STAGER_CONFIGURATION= 8, /* "Got wrong configuration, using default" */ /* DLF_LVL_USAGE */
     STAGER_CONFIGURATION_ERROR= 9, /* "Impossible to get (right) configuration" */ /* DLF_LVL_ERROR */

     /*******************************************************************************************************/
     /* Constants related with the StagerDBService SvcThreads: JobRequestSvc, PreRequestSvc, StgRequestSvc */
     /*****************************************************************************************************/
     /* mainly for DLF_LVL_DEBUG */
     /***************************/     
     STAGER_JOBREQSVC_CREATION= 21, /* "Created new JobRequestSvc Thread" */
     STAGER_PREREQSVC_CREATION= 22, /* "Created new PreRequestSvc Thread" */   
     STAGER_STGREQSVC_CREATION= 23, /* "Created new StgRequestSvc Thread" */
    
     /* JobRequestSvcThread */     
     STAGER_GET= 31, /*Get Request" */
     STAGER_UPDATE= 32, /* Update Request" */
     STAGER_PUT= 33, /* "Put Request" */     
     
     /************************/
     /* PreRequestSvcThread */  
     STAGER_REPACK= 34, /* "Repack Request" */
     STAGER_PREPARETOGET= 35, /* "PrepareToGet Request" */
     STAGER_PREPARETOUPDATE= 36, /* "PrepareToUpdate Request" */
     STAGER_PREPARETOPUT= 37, /* "PrepareToPut Request" */
     
     
     /*************************/
     /* StgRequestSvcThread  */
     STAGER_SETGC= 38, /* "SetGC Request" */
     STAGER_SETGC_DETAILS= 39, /* "SetGC details" *//* SYSTEM LEVEL ALSO */
     STAGER_RM= 40, /* Rm Request" */
     STAGER_RM_DETAILS= 41, /* Rm details" *//* SYSTEM LEVEL ALSO */
     STAGER_PUTDONE= 42, /* "PutDone Request" */
	  

     /******************/
     /*  SYSTEM LEVEL */
     /****************/
     /* after calling the corresponding stagerService function, to show the decision taken */
     STAGER_ARCHIVE_SUBREQ= 51, /* Archiving subrequest" */
     STAGER_NOTHING_TOBEDONE= 52, /* Nothing to be done (STAGED)" */
     STAGER_WAIT_SUBREQ= 53, /* Request moved to Wait" */
     STAGER_REPACK_MIGRATION= 54, /* Starting Repack Migration" */
     STAGER_DISKTODISK_COPY= 55, /* Triggering Disk2Disk Copy" */
     STAGER_TAPE_RECALL= 56, /* Triggering Tape Recall" */
     STAGER_CASTORFILE_RECREATION= 57, /*Recreating CastorFile" */
   
     /* DLF_LVL_ERROR */
     STAGER_SERVICES_EXCEPTION= 71, /*Impossible to get the Service" */
     STAGER_SVCCLASS_EXCEPTION= 72, /*Impossible to get the SvcClass" */
     STAGER_USER_INVALID= 73, /*Invalid user" */
     STAGER_USER_PERMISSION= 74, /*User doenst have the right permission" */
     STAGER_USER_NONFILE= 75, /*User asking for a Non Existing File" */
     STAGER_INVALID_FILESYSTEM= 76, /*Invalid fileSystem" */
     STAGER_PUTDONE_WITHOUT_PUT= 77, /*PutDone without a Put" */
     
     
     /*******************/
     /* QueryRequestSvc */
     STAGER_QRYSVC_GETSVC= 91, /* Could not get QuerySvc" */
     STAGER_QRYSVC_EXCEPT= 92, /*Unexpected exception caught" */
     STAGER_QRYSVC_NOCLI= 93, /* "No client associated with request ! Cannot answer !" */
     STAGER_QRYSVC_INVSC= 94, /* "Invalid ServiceClass name" */
     STAGER_QRYSVC_UNKREQ=95, /*Unknown Request type" */
     STAGER_QRYSVC_FQNOPAR= 96, /* "StageFileQueryRequest has no parameters" */
     STAGER_QRYSVC_FQUERY= 97, /* "Processing File Query by fileName" */
     STAGER_QRYSVC_IQUERY= 98, /* "Processing File Query by fileId" */
     STAGER_QRYSVC_RQUERY= 99, /* "Processing File Query by Request" */
     
     /*********/
     /* GcSvc */
     STAGER_GCSVC_GETSVC= 111, /* "Could not get GCSvc" */
     STAGER_GCSVC_EXCEPT= 112, /* "Unexpected exception caught" */
     STAGER_GCSVC_NOCLI= 113, /* "No client associated with request ! Cannot answer !" */
     STAGER_GCSVC_UNKREQ= 114, /* "Unknown Request type" */
     STAGER_GCSVC_FDELOK= 115, /* "Invoking filesDeleted" */
     STAGER_GCSVC_FDELFAIL= 116, /*"Invoking filesDeletionFailed" */
     
     /************/
     /* ErrorSvc */
     STAGER_ERRSVC_GETSVC= 121, /* "Could not get StagerSvc" */
     STAGER_ERRSVC_EXCEPT= 122, /* "Unexpected exception caught" */
     STAGER_ERRSVC_NOREQ= 123, /* "No request associated with subrequest ! Cannot answer !" */
     STAGER_ERRSVC_NOCLI= 124, /* "No client associated with request ! Cannot answer !" */
     
     /**********/
     /* JobSvc */
     STAGER_JOBSVC_GETSVC=131, /* "Could not get JobSvc" */
     STAGER_JOBSVC_EXCEPT= 132, /* "Unexpected exception caught" */
     STAGER_JOBSVC_NOSREQ= 133, /* "Could not find subrequest associated to Request" */
     STAGER_JOBSVC_BADSRT= 134, /* "Expected SubRequest in Request but found another type" */
     STAGER_JOBSVC_NOFSOK= 135, /* "Could not find suitable filesystem" */
     STAGER_JOBSVC_GETUPDS= 136, /* "Invoking getUpdateStart" */
     STAGER_JOBSVC_PUTS= 137, /* "Invoking PutStart" */
     STAGER_JOBSVC_D2DCBAD=138 , /* "Invoking disk2DiskCopyFailed" */
     STAGER_JOBSVC_D2DCOK= 139, /* "Invoking disk2DiskCopyDone" */
     STAGER_JOBSVC_PFMIG= 140, /* "Invoking PrepareForMigration" */
     STAGER_JOBSVC_GETUPDO= 141, /* "Invoking getUpdateDone" */
     STAGER_JOBSVC_GETUPFA=142, /* "Invoking getUpdateFailed" */
     STAGER_JOBSVC_PUTFAIL= 143, /* "Invoking putFailed" */
     STAGER_JOBSVC_NOCLI= 144, /* "No client associated with request ! Cannot answer !" */
     STAGER_JOBSVC_UNKREQ= 145 /* "Unknown Request type" */
    
      };
      

    }// end namespace dbService
  }// end namespace stager
}// end namespace castor



#endif

