/*********************************************************************************/
/* File including the constants to represent the DLF Messages for the new Stager*/
/*******************************************************************************/

#ifndef STAGER_DLF_MESSAGES_HPP
#define STAGER_DLF_MESSAGES_HPP 1



namespace castor{
  namespace stager{
    namespace dbService{

      /* Constants related with the Stager (Main) Daemon */
      /* to be added: related with the keyboard interruption, timeout and so on ?!?!?! */

#define STAGER_DAEMON_START 1
#define STAGER_DAEMON_FLOW 2
#define STAGER_DAEMON_ERROR_CONFIG 3
#define STAGER_DAEMON_EXCEPTION 4
#define STAGER_DAEMON_EXCEPTION_GENERAL 5
#define STAGER_DAEMON_FINISHED 6


#define STAGER_DAEMON_POOLCREATION 7

    

      /*******************************************************************************************************/
      /* Constants related with the StagerDBService SvcThreads: JobRequestSvc, PreRequestSvc, StgRequestSvc */
      /*****************************************************************************************************/

      /************************/
      /* JobRequestSvcThread */

      /* JobRequestSvc flow */
#define STAGER_JOBREQSVC_CREATION 8
#define STAGER_JOBREQSVC_SELECT 9
#define STAGER_JOBREQSVC_PROCESS 10
#define STAGER_JOBREQSVC_EXCEPTION 11
#define STAGER_JOBREQSVC_EXCEPTION_GENERAL 12



      /* StagerGetHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper} */
#define STAGER_GET 13
#define STAGER_GET_EXCEPTION 14
#define STAGER_GET_EXCEPTION_GENERAL 15

      /* StagerUpdateHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper} */
#define STAGER_UPDATE 16
#define STAGER_UPDATE_EXCEPTION 17
#define STAGER_UPDATE_EXCEPTION_GENERAL 18

      /* StagerPutHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper} */
#define STAGER_PUT 19
#define STAGER_PUT_EXCEPTION 20
#define STAGER_PUT_EXCEPTION_GENERAL 21


      /************************/
      /* PreRequestSvcThread */

      /* PreRequestSvc flow */
#define STAGER_PREREQSVC_CREATION 22
#define STAGER_PREREQSVC_SELECT 23
#define STAGER_PREREQSVC_PROCESS 24
#define STAGER_PREREQSVC_EXCEPTION 25
#define STAGER_PREREQSVC_EXCEPTION_GENERAL 26


      /* StagerRepackHandler */
#define STAGER_REPACK 27
#define STAGER_REPACK_EXCEPTION 28
#define STAGER_REPACK_EXCEPTION_GENERAL 29

      /* StagerPrepareToGetHandler */
#define STAGER_PREPARETOGET 30
#define STAGER_PREPARETOGET_EXCEPTION 31
#define STAGER_PREPARETOGET_EXCEPTION_GENERAL 32

      /* StagerPrepareToUpdateHandler */
#define STAGER_PREPARETOUPDATE 33
#define STAGER_PREPARETOUPDATE_EXCEPTION 34
#define STAGER_PREPARETOUPDATE_EXCEPTION_GENERAL 35

      /* StagerPrepareToPutHandler */
#define STAGER_PREPARETOPUT 36
#define STAGER_PREPARETOPUT_EXCEPTION 37
#define STAGER_PREPARETOPUT_EXCEPTION_GENERAL 38


      /*************************/
      /* StgRequestSvcThread  */

      /* StgRequestSvc flow */
#define STAGER_STGREQSVC_CREATION 39
#define STAGER_STGREQSVC_SELECT 40 
#define STAGER_STGREQSVC_PROCESS 41
#define STAGER_STGREQSVC_EXCEPTION 42
#define STAGER_STGREQSVC_EXCEPTION_GENERAL 43


      /* StagerSetGCHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper, stgReplyHelper} */
#define STAGER_SETGC 44
#define STAGER_SETGC_EXCEPTION 45
#define STAGER_SETGC_EXCEPTION_GENERAL 46


      /* StagerRmHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper, stgReplyHelper} */
#define STAGER_RM 47
#define STAGER_RM_EXCEPTION 48
#define STAGER_RM_EXCEPTION_GENERAL 49




      /* StagerPutDoneHandler: {stgGetHandler flow, stgRequestHelper, stgCnsHelper, stgReplyHelper} */
#define STAGER_PUTDONE 50
#define STAGER_PUTDONE_EXCEPTION 51
#define STAGER_PUTDONE_EXCEPTION_GENERAL 52


      /*************************/
      /* StagerRequestHandler */
#define STAGER_REQHANDLER_METHOD 53
#define STAGER_REQHANDLER_EXCEPTION 54
#define STAGER_REQHANDLER_EXCEPTION_GENERAL 55

      /****************************/
      /* StagerJobRequestHandler */
#define STAGER_JOBREQHANDLER_METHOD 56
#define STAGER_JOBREQHANDLER_EXCEPTION 57
#define STAGER_JOBREQHANDLER_EXCEPTION_GENERAL 58


      /************************/
      /* StagerRequestHelper */
#define STAGER_REQHELPER_CONSTRUCTOR 59
#define STAGER_REQHELPER_METHOD 60
#define STAGER_REQHELPER_EXCEPTION 61
#define STAGER_REQHELPER_EXCEPTION_GENERAL 62


      /********************/
      /* StagerCnsHelper */
#define STAGER_CNSHELPER_CONSTRUCTOR 63
#define STAGER_CNSHELPER_METHOD 64
#define STAGER_CNSHELPER_EXCEPTION 65
#define STAGER_CNSHELPER_EXCEPTION_GENERAL 66



      /*********************/
      /* StagerReplyHelper*/
#define STAGER_REPLYHELPER_CONSTRUCTOR 67
#define STAGER_REPLYHELPER_METHOD 68
#define STAGER_REPLYHELPER_EXCEPTION 69
#define STAGER_REPLYHELPER_EXCEPTION_GENERAL 70






      /*************************************************************************************************/


      /* Constants related with the QueryRequestSvcThread */


      /* Constants related with the ErrorSvcThread */


      /* Constants related with the JobSvcThread */


      /* Constants related with the GCSvcThread */

      

    }// end namespace dbService
  }// end namespace stager
}// end namespace castor



#endif

