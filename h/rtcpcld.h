/*
 * Copyright (C) 2003 by CERN IT-ADC
 * All rights reserved
 */


/*
 * rtcpcld.h,v 1.5 2004/04/30 14:25:36 CERN IT-ADC Olof Barring
 */

/*
 * rtcpcld.h - rtcopy client daemon specific definitions
 */

#ifndef RTCPCLD_H
#define RTCPCLD_H

/* headers */
#include "castor/stager/TapeCopy.h"
#include "castor/stager/ITapeSvc.h"

#define LOG_SYSCALL_ERR(func) { \
    int _save_serrno = serrno; \
    (void)dlf_write( \
                    (inChild == 0 ? mainUuid : childUuid), \
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL), \
                    (struct Cns_fileid *)NULL, \
                    RTCPCLD_NB_PARAMS+2, \
                    "SYSCALL", \
                    DLF_MSG_PARAM_STR, \
                    func, \
                    "ERROR_STRING", \
                    DLF_MSG_PARAM_STR, \
                    sstrerror(serrno), \
                    RTCPCLD_LOG_WHERE \
                    ); \
    serrno = _save_serrno;}

#define LOG_DBCALL_ERR(func,dbMsg) { \
    char *_dbErr = NULL; \
    int _save_serrno = serrno; \
    (void)dlf_write( \
                    (inChild == 0 ? mainUuid : childUuid), \
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_DBSVC), \
                    (struct Cns_fileid *)NULL, \
                    RTCPCLD_NB_PARAMS+3, \
                    "DBCALL", \
                    DLF_MSG_PARAM_STR, \
                    func, \
                    "ERROR_STRING", \
                    DLF_MSG_PARAM_STR, \
                    sstrerror(serrno), \
                    "DB_ERROR", \
                    DLF_MSG_PARAM_STR, \
                    (_dbErr = rtcpcld_fixStr(dbMsg)), \
                    RTCPCLD_LOG_WHERE \
                    ); \
    if ( _dbErr != NULL ) free(_dbErr); \
    serrno = _save_serrno;}

#define LOG_DBCALLANDKEY_ERR(func,dbMsg,dbKey) { \
    char *_dbErr = NULL; \
    int _save_serrno = serrno; \
    (void)dlf_write( \
                    (inChild == 0 ? mainUuid : childUuid), \
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_DBSVC), \
                    (struct Cns_fileid *)NULL, \
                    RTCPCLD_NB_PARAMS+4, \
                    "DBCALL", \
                    DLF_MSG_PARAM_STR, \
                    func, \
                    "DBKEY", \
                    DLF_MSG_PARAM_INT64, \
                    dbKey, \
                    "ERROR_STRING", \
                    DLF_MSG_PARAM_STR, \
                    sstrerror(serrno), \
                    "DB_ERROR", \
                    DLF_MSG_PARAM_STR, \
                    (_dbErr = rtcpcld_fixStr(dbMsg)), \
                    RTCPCLD_LOG_WHERE \
                    ); \
    if ( _dbErr != NULL ) free(_dbErr); \
    serrno = _save_serrno;}



#define LOG_SECURITY_ERR(func) {\
    int _save_serrno = serrno; \
    (void)dlf_write( \
                    (inChild == 0 ? mainUuid : childUuid), \
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SECURITY), \
                    (struct Cns_fileid *)NULL, \
                    RTCPCLD_NB_PARAMS+2, \
                    "SYSCALL", \
                    DLF_MSG_PARAM_STR, \
                    func, \
                    "ERROR_STRING", \
                    DLF_MSG_PARAM_STR, \
                    sstrerror(serrno), \
                    RTCPCLD_LOG_WHERE \
                    ); \
    serrno = _save_serrno;}

#define LOG_CALL_TRACE(statement) {\
    int _save_serrno = serrno; \
    char *_dbErr = NULL; \
    (void)dlf_write( \
                    (inChild == 0 ? mainUuid : childUuid), \
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_CALL_TRACE), \
                    (struct Cns_fileid *)NULL, \
                    RTCPCLD_NB_PARAMS+1, \
                    "BEGIN", \
                    DLF_MSG_PARAM_STR, \
                    (_dbErr = rtcpcld_fixStr(#statement)), \
                    RTCPCLD_LOG_WHERE \
                    ); \
    if ( _dbErr != NULL ) free(_dbErr); \
    _dbErr = NULL; \
    serrno = _save_serrno; \
    statement; \
    _save_serrno = serrno; \
    (void)dlf_write( \
                    childUuid, \
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_CALL_TRACE), \
                    (struct Cns_fileid *)NULL, \
                    RTCPCLD_NB_PARAMS+1, \
                    "END", \
                    DLF_MSG_PARAM_STR, \
                    (_dbErr = rtcpcld_fixStr(#statement)), \
                    RTCPCLD_LOG_WHERE \
                    ); \
    if ( _dbErr != NULL ) free(_dbErr); \
    serrno = _save_serrno;}

#define ID_TYPE u_signed64
enum NotificationState {
  NOT_NOTIFIED,
  NOTIFIED,
  DONT_NOTIFY
};

#if defined(CASTOR_SERVICES_H)
int rtcpcld_getDbSvc (
		      struct C_Services_t ***
		      );
#endif /* CASTOR_SERVICES_H */

#if defined(CASTOR_ITAPESVC_H)
int rtcpcld_getStgSvc (
		       struct Cstager_ITapeSvc_t ***
		       );
#endif /* CASTOR_ITAPESVC_H */

int rtcpcld_doCommit (
		      void
		      );

int rtcpcld_InitNW  (
		     SOCKET **
		     );
int rtcp_Listen  (
		  SOCKET, SOCKET *,
		  int, int
		  );
int rtcp_CleanUp  (
		   SOCKET **, 
		   int
		   );
int rtcpcld_Listen (
		    SOCKET *, 
		    SOCKET *, 
		    int
		    );
int rtcpcld_initLogging (
			 char *
			 );
void rtcpcld_extlog (
		     int,
		     const char *,
		     ...
		     );
int rtcpcld_initNotify (
			SOCKET **
			);
int rtcpcld_initNotifyByPort (
			      SOCKET **,
			      int *
			      );
int rtcpcld_getNotifyWithAddr (
			       SOCKET,
			       char *
			       );
int rtcpcld_getNotify (
		       SOCKET
		       );
int rtcpcld_sendNotify (
			char *
			);
int rtcpcld_sendNotifyByAddr (
			      char *,
			      int
			      );
int rtcpcld_notifyRtcpclientd (
			       void
			       );
int rtcpcld_validPosition (
			   int,
			   unsigned char *
			   );
int rtcpcld_validPath (
		       char *
		       );
int rtcpcld_validFilereq (
			  rtcpFileRequest_t *
			  );
int rtcpcld_findFile (
		      tape_list_t *,
		      rtcpFileRequest_t *,
		      file_list_t **
		      );
int rtcpcld_nextFileToProcess (
			       tape_list_t *,
			       file_list_t **
			       );
int rtcpcld_runWorker (
		       tape_list_t *,
		       SOCKET *,
		       int (*) (void *(*)(void *), void *),
		       int (*) (rtcpTapeRequest_t *, 
				rtcpFileRequest_t *)
		       );
                              
int rtcpcld_parseWorkerCmd (
			    int,
			    char **,
			    tape_list_t *,
			    SOCKET *
			    );
int rtcpcld_workerFinished (
			    tape_list_t *,
			    int,
			    u_signed64,
			    int,
			    int,
			    int
			    );
int rtcpcld_initLocks (
		       tape_list_t *
		       );
int rtcpcld_setTapeFseq (
			 int
			 );
int rtcpcld_getTapeFseq (
			 void
			 );
int rtcpcld_getAndIncrementTapeFseq (
				     void
				     );
int rtcpcld_lockTape (
		      void
		      );
int rtcpcld_unlockTape (
			void
			);
int rtcpcld_myDispatch (
			void *(*) (void *),
			void *
			);
int rtcpcld_initThreadPool (
			    int
			    );
void rtcpcld_cleanupTape (
			  tape_list_t *
			  );
void rtcpcld_cleanupFile (
			  file_list_t *
			  );
int rtcpcld_getTapesToDo (
			  tape_list_t ***,
			  int *
			  );
int rtcpcld_getSegmentToDo (
			    tape_list_t *,
			    file_list_t **
			    );
int rtcpcld_getTapeCopyNumber (
			       file_list_t *,
			       int *
			       );
int rtcpcld_anyReqsForTape (
			    tape_list_t *
			    );
int rtcpcld_returnStream (
			  tape_list_t *
			  );
int rtcpcld_restoreSelectedTapeCopies (
				       tape_list_t *
				       );
int rtcpcld_restoreSelectedSegments (
				     tape_list_t *
				     );
int rtcpcld_setVidWorkerAddress (
				 tape_list_t *,
				 int
				 );
int rtcpcld_checkFileForRepack (
				struct Cns_fileid *,
				char **
				);

#if defined(CASTOR_STAGER_TAPESTATUSCODES_H) && defined(CASTOR_STAGER_SEGMENTSTATUSCODES_H)
int rtcpcld_updateTapeStatus (
			      tape_list_t *, 
			      enum Cstager_TapeStatusCodes_t,
			      enum Cstager_TapeStatusCodes_t
			      );
int rtcpcld_updateVIDFileStatus (
				 tape_list_t *,
				 enum Cstager_SegmentStatusCodes_t,
				 enum Cstager_SegmentStatusCodes_t
				 );

int rtcpcld_setFileStatus (
			   tape_list_t *,
			   file_list_t *,
			   enum Cstager_SegmentStatusCodes_t,
			   int
			   );
#endif /* CASTOR_STAGER_TAPESTATUSCODES_H && CASTOR_STAGER_SEGMENTSTATUSCODES_H */

int rtcpcld_setVIDFailedStatus (
				tape_list_t *
				);

char *rtcpcld_fixStr (
		      const char *
		      );
int rtcpcld_updcMigrFailed (
			    tape_list_t *,
			    file_list_t *
			    );
int rtcpcld_updcRecallFailed (
			      tape_list_t *,
			      file_list_t *,
			      int del_file
			      );
int rtcpcld_updcFileMigrated (
			      tape_list_t *,
			      file_list_t *
			      );
int rtcpcld_updcFileRecalled (
			      tape_list_t *,
			      file_list_t *
			      );
int rtcpcld_getMigrSvcClassName (
				 file_list_t *,
				 char **
				 );
int rtcpcld_getLastModificationTime (
				     file_list_t *,
				     time_t *
				     );
int rtcpcld_getTapePoolName (
			     tape_list_t *,
			     char **
			     );

#ifdef CASTOR_STAGER_TAPECOPY_H
int rtcpcld_putFailed (
		       struct Cstager_TapeCopy_t *,
		       int
		       );
#endif /* CASTOR_STAGER_TAPECOPY_H */

int rtcpcld_initNsInterface (
			     void
			     );
int rtcpcld_initCatalogueInterface (
				    void
				    );
int rtcpcld_updateNsSegmentAttributes (
				       tape_list_t *,
				       file_list_t *,
				       int,
				       struct Cns_fileid*,
				       time_t
				       );
int rtcpcld_checkNsSegment (
			    tape_list_t *,
			    file_list_t *
			    );
int rtcpcld_checkSegment (
			  tape_list_t *,
			  file_list_t *
			  );

int rtcpcld_checkDualCopies (
			     file_list_t *
			     );
char *rtcpcld_tapeStatusStr (
			     int
			     );
int rtcpcld_getFileId (
		       file_list_t *,
		       struct Cns_fileid **
		       );
int rtcpcld_getOwner (
		      struct Cns_fileid *,
		      int *,
		      int *
		      );
int rtcpcld_gettape (
		     char *,
		     u_signed64,
		     tape_list_t **
		     );
int rtcpcld_tapeOK (
		    tape_list_t *
		    );
int rtcpcld_handleTapeError (
			     tape_list_t *,
			     file_list_t *
			     );
int rtcpcld_updateTape (
			tape_list_t *,
			file_list_t *,
			int,
			int,
			int *
			);
int rtcpcld_segmentOK (
		       struct Cns_segattrs *
		       );
int rtcpcld_cleanup (
		     void
		     );
void rtcpcld_sendStreamReport (
			       struct Cstager_ITapeSvc_t* tpSvc,
			       char *file_path,
			       int direction,
			       int created,
			       char* tape_vid,
			       Cuuid_t uuid
			       );

#endif /* RTCPCLD_H */
