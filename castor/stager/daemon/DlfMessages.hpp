/*********************************************************************************/
/* File including the constants to represent the DLF Messages for the new Stager*/
/*******************************************************************************/

#ifndef STAGER_DLF_MESSAGES_HPP
#define STAGER_DLF_MESSAGES_HPP 1



namespace castor{
  namespace stager{
    namespace dbService{

      
   

   enum StgDlfMessages{

     /* Constants related with the Stager (Main) Daemon */
     /* to be added: related with the keyboard interruption, timeout and so on ?!?!?! */
     STAGER_DAEMON_START =1,
     STAGER_DAEMON_EXECUTION =2,
     STAGER_DAEMON_ERROR_CONFIG =3,
     STAGER_DAEMON_EXCEPTION = 4,
     STAGER_DAEMON_EXCEPTION_GENERAL =5,
     STAGER_DAEMON_FINISHED = 6,
     
     
     STAGER_DAEMON_POOLCREATION = 7,
     
     
     
     /*******************************************************************************************************/
     /* Constants related with the StagerDBService SvcThreads: JobRequestSvc, PreRequestSvc, StgRequestSvc */
     /*****************************************************************************************************/
     
     /************************/
     /* JobRequestSvcThread */
     
     /* JobRequestSvc flow */
     STAGER_JOBREQSVC_CREATION = 11,
     STAGER_JOBREQSVC_SELECT = 12,
     STAGER_JOBREQSVC_PROCESS = 13,
     STAGER_JOBREQSVC_EXCEPTION = 14,
     STAGER_JOBREQSVC_EXCEPTION_GENERAL = 15,
     


     /* StagerGetHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper} */
     STAGER_GET = 21,
     STAGER_GET_EXCEPTION = 22,
     STAGER_GET_EXCEPTION_GENERAL = 23,
     
     /* StagerUpdateHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper} */
     STAGER_UPDATE = 26,
     STAGER_UPDATE_EXCEPTION = 27,
     STAGER_UPDATE_EXCEPTION_GENERAL = 28,
     
     /* StagerPutHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper} */
     STAGER_PUT = 31,
     STAGER_PUT_EXCEPTION = 32,
     STAGER_PUT_EXCEPTION_GENERAL = 33,
     

     /************************/
     /* PreRequestSvcThread */
     
     /* PreRequestSvc flow */
     STAGER_PREREQSVC_CREATION = 41,
     STAGER_PREREQSVC_SELECT = 42,
     STAGER_PREREQSVC_PROCESS = 43,
     STAGER_PREREQSVC_EXCEPTION = 44,
     STAGER_PREREQSVC_EXCEPTION_GENERAL = 45, 
     

     /* StagerRepackHandler */
     STAGER_REPACK = 51,
     STAGER_REPACK_EXCEPTION = 52,
     STAGER_REPACK_EXCEPTION_GENERAL = 53,
     
     /* StagerPrepareToGetHandler */
     STAGER_PREPARETOGET = 56, 
     STAGER_PREPARETOGET_EXCEPTION = 57,
     STAGER_PREPARETOGET_EXCEPTION_GENERAL = 58,
     
     /* StagerPrepareToUpdateHandler */
     STAGER_PREPARETOUPDATE = 61,
     STAGER_PREPARETOUPDATE_EXCEPTION = 62,
     STAGER_PREPARETOUPDATE_EXCEPTION_GENERAL = 63,
     
     /* StagerPrepareToPutHandler */
     STAGER_PREPARETOPUT = 66,
     STAGER_PREPARETOPUT_EXCEPTION = 67,
     STAGER_PREPARETOPUT_EXCEPTION_GENERAL = 68,
     

     /*************************/
     /* StgRequestSvcThread  */
     
     /* StgRequestSvc flow */
     STAGER_STGREQSVC_CREATION = 71,
     STAGER_STGREQSVC_SELECT = 72,
     STAGER_STGREQSVC_PROCESS = 73,
     STAGER_STGREQSVC_EXCEPTION = 74,
     STAGER_STGREQSVC_EXCEPTION_GENERAL = 75,
     
     
     /* StagerSetGCHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper, stgReplyHelper} */
     STAGER_SETGC = 81,
     STAGER_SETGC_EXCEPTION = 82,
     STAGER_SETGC_EXCEPTION_GENERAL = 83,
     
     
     /* StagerRmHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper, stgReplyHelper} */
     STAGER_RM = 86,
     STAGER_RM_EXCEPTION = 87,
     STAGER_RM_EXCEPTION_GENERAL = 88,




     /* StagerPutDoneHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper, stgReplyHelper} */
     STAGER_PUTDONE = 91,
     STAGER_PUTDONE_EXCEPTION = 92,
     STAGER_PUTDONE_EXCEPTION_GENERAL = 93,
     
     
     /*************************/
     /* StagerRequestHandler */
     STAGER_REQHANDLER_METHOD = 101,
     STAGER_REQHANDLER_EXCEPTION = 102,
     STAGER_REQHANDLER_EXCEPTION_GENERAL = 103,
     
     /****************************/
     /* StagerJobRequestHandler */
     STAGER_JOBREQHANDLER_METHOD = 106,
     STAGER_JOBREQHANDLER_EXCEPTION = 107,
     STAGER_JOBREQHANDLER_EXCEPTION_GENERAL = 108,
     
     
     /************************/
     /* StagerRequestHelper */
     STAGER_REQHELPER_CONSTRUCTOR = 111,
     STAGER_REQHELPER_METHOD = 112,
     STAGER_REQHELPER_EXCEPTION = 113,
     STAGER_REQHELPER_EXCEPTION_GENERAL = 114,
     
     
     /********************/
     /* StagerCnsHelper */
     STAGER_CNSHELPER_CONSTRUCTOR = 116,
     STAGER_CNSHELPER_METHOD = 117,
     STAGER_CNSHELPER_EXCEPTION = 118,
     STAGER_CNSHELPER_EXCEPTION_GENERAL = 119,

     
     
     /*********************/
     /* StagerReplyHelper*/
     STAGER_REPLYHELPER_CONSTRUCTOR = 121,
     STAGER_REPLYHELPER_METHOD = 122,
     STAGER_REPLYHELPER_EXCEPTION = 123,
     STAGER_REPLYHELPER_EXCEPTION_GENERAL = 124,
     

     /*******************/
     /* QueryRequestSvc */
     STAGER_QRYSVC_GETSVC,
     STAGER_QRYSVC_EXCEPT,
     STAGER_QRYSVC_NOCLI,
     STAGER_QRYSVC_INVSC,
     STAGER_QRYSVC_UNKREQ,
     STAGER_QRYSVC_FQNOPAR,
     STAGER_QRYSVC_FQUERY,
     STAGER_QRYSVC_IQUERY,
     STAGER_QRYSVC_RQUERY,

     /*********/
     /* GcSvc */
     STAGER_GCSVC_GETSVC,
     STAGER_GCSVC_EXCEPT,
     STAGER_GCSVC_NOCLI,
     STAGER_GCSVC_UNKREQ,
     STAGER_GCSVC_FDELOK,
     STAGER_GCSVC_FDELFAIL,

     /************/
     /* ErrorSvc */
     STAGER_ERRSVC_GETSVC,
     STAGER_ERRSVC_EXCEPT,
     STAGER_ERRSVC_NOREQ,
     STAGER_ERRSVC_NOCLI,

     /**********/
     /* JobSvc */
     STAGER_JOBSVC_GETSVC,
     STAGER_JOBSVC_EXCEPT,
     STAGER_JOBSVC_NOSREQ,
     STAGER_JOBSVC_BADSRT,
     STAGER_JOBSVC_NOFSOK,
     STAGER_JOBSVC_GETUPDS,
     STAGER_JOBSVC_PUTS,
     STAGER_JOBSVC_D2DCBAD,
     STAGER_JOBSVC_D2DCOK,
     STAGER_JOBSVC_PFMIG,
     STAGER_JOBSVC_GETUPDO,
     STAGER_JOBSVC_GETUPFA,
     STAGER_JOBSVC_PUTFAIL,
     STAGER_JOBSVC_NOCLI,
     STAGER_JOBSVC_UNKREQ

     /*************************************************************************************************/
     
     
     /* Constants related with the QueryRequestSvcThread */
     
     
     /* Constants related with the ErrorSvcThread */


      /* Constants related with the JobSvcThread */


      /* Constants related with the GCSvcThread */
      
      };
      

    }// end namespace dbService
  }// end namespace stager
}// end namespace castor



#endif

