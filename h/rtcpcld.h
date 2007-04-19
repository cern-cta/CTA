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
int rtcpcld_getDbSvc _PROTO((
                             struct C_Services_t ***
                             ));
#endif /* CASTOR_SERVICES_H */

#if defined(CASTOR_ITAPESVC_H)
int rtcpcld_getStgSvc _PROTO((
                              struct Cstager_ITapeSvc_t ***
                              ));
#endif /* CASTOR_ITAPESVC_H */

int rtcpcld_doCommit _PROTO((
                             void
                             ));

int rtcpcld_InitNW  _PROTO((
                            SOCKET **
                            ));
int rtcp_Listen  _PROTO((
                         SOCKET, SOCKET *,
                         int, int
                         ));
int rtcp_CleanUp  _PROTO((
                          SOCKET **, 
                          int
                          ));
int rtcpcld_Listen _PROTO((
                           SOCKET *, 
                           SOCKET *, 
                           int
                           ));
int rtcpcld_initLogging _PROTO((
                                char *
                                ));
void rtcpcld_extlog _PROTO((
                            int,
                            CONST char *,
                            ...
                            ));
int rtcpcld_initNotify _PROTO((
                               SOCKET **
                               ));
int rtcpcld_initNotifyByPort _PROTO((
                                     SOCKET **,
                                     int *
                                     ));
int rtcpcld_getNotifyWithAddr _PROTO((
                                      SOCKET,
                                      char *
                                      ));
int rtcpcld_getNotify _PROTO((
                              SOCKET
                              ));
int rtcpcld_sendNotify _PROTO((
                               char *
                               ));
int rtcpcld_sendNotifyByAddr _PROTO((
                                     char *,
                                     int
                                     ));
int rtcpcld_notifyRtcpclientd _PROTO((
                                      void
                                      ));
int rtcpcld_validPosition _PROTO((
                                  int,
                                  unsigned char *
                                  ));
int rtcpcld_validPath _PROTO((
                              char *
                              ));
int rtcpcld_validFilereq _PROTO((
                                 rtcpFileRequest_t *
                                 ));
int rtcpcld_findFile _PROTO((
                             tape_list_t *,
                             rtcpFileRequest_t *,
                             file_list_t **
                             ));
int rtcpcld_nextFileToProcess _PROTO((
                                      tape_list_t *,
                                      file_list_t **
                                      ));
int rtcpcld_runWorker _PROTO((
                              tape_list_t *,
                              SOCKET *,
                              int (*) _PROTO((void *(*)(void *), void *)),
                              int (*) _PROTO((rtcpTapeRequest_t *, 
                                              rtcpFileRequest_t *))
                              ));
                              
int rtcpcld_parseWorkerCmd _PROTO((
                                   int,
                                   char **,
                                   tape_list_t *,
                                   SOCKET *
                                   ));
int rtcpcld_workerFinished _PROTO((
                                   tape_list_t *,
                                   int,
                                   u_signed64,
                                   int,
                                   int,
                                   int
                                   ));
int rtcpcld_initLocks _PROTO((
                              tape_list_t *
                              ));
int rtcpcld_setTapeFseq _PROTO((
                                int
                                ));
int rtcpcld_getTapeFseq _PROTO((
                                void
                                ));
int rtcpcld_getAndIncrementTapeFseq _PROTO((
                                            void
                                            ));
int rtcpcld_lockTape _PROTO((
                             void
                             ));
int rtcpcld_unlockTape _PROTO((
                               void
                               ));
int rtcpcld_myDispatch _PROTO((
                               void *(*) _PROTO((void *)),
                               void *
                               ));
int rtcpcld_initThreadPool _PROTO((
                                   int
                                   ));
void rtcpcld_cleanupTape _PROTO((
                                tape_list_t *
                                ));
void rtcpcld_cleanupFile _PROTO((
                                 file_list_t *
                                 ));
int rtcpcld_getTapesToDo _PROTO((
                                 tape_list_t ***,
                                 int *
                                 ));
int rtcpcld_getSegmentToDo _PROTO((
                                   tape_list_t *,
                                   file_list_t **
                                   ));
int rtcpcld_getTapeCopyNumber _PROTO((
                                      file_list_t *,
                                      int *
                                      ));
int rtcpcld_anyReqsForTape _PROTO((
                                  tape_list_t *
                                  ));
int rtcpcld_returnStream _PROTO((
                                 tape_list_t *
                                 ));
int rtcpcld_restoreSelectedTapeCopies _PROTO((
                                              tape_list_t *
                                              ));
int rtcpcld_restoreSelectedSegments _PROTO((
                                            tape_list_t *
                                            ));
int rtcpcld_setVidWorkerAddress _PROTO((
                                        tape_list_t *,
                                        int
                                        ));
int rtcpcld_checkFileForRepack _PROTO((
                                       struct Cns_fileid *,
                                       char **
                                       ));

#if defined(CASTOR_STAGER_TAPESTATUSCODES_H) && defined(CASTOR_STAGER_SEGMENTSTATUSCODES_H)
int rtcpcld_updateTapeStatus _PROTO((
                                     tape_list_t *, 
                                     enum Cstager_TapeStatusCodes_t,
                                     enum Cstager_TapeStatusCodes_t
                                     ));
int rtcpcld_updateVIDFileStatus _PROTO((
                                        tape_list_t *,
                                        enum Cstager_SegmentStatusCodes_t,
                                        enum Cstager_SegmentStatusCodes_t
                                        ));

int rtcpcld_setFileStatus _PROTO((
                                  tape_list_t *,
                                  file_list_t *,
                                  enum Cstager_SegmentStatusCodes_t,
                                  int
                                  ));
#endif /* CASTOR_STAGER_TAPESTATUSCODES_H && CASTOR_STAGER_SEGMENTSTATUSCODES_H */

int rtcpcld_setVIDFailedStatus _PROTO((
                                      tape_list_t *
                                      ));

char *rtcpcld_fixStr _PROTO((
                             CONST char *
                             ));
int rtcpcld_updcMigrFailed _PROTO((
                                   tape_list_t *,
                                   file_list_t *
                                   ));
int rtcpcld_updcRecallFailed _PROTO((
                                     tape_list_t *,
                                     file_list_t *
                                     ));
int rtcpcld_updcFileMigrated _PROTO((
                                     tape_list_t *,
                                     file_list_t *
                                   ));
int rtcpcld_updcFileRecalled _PROTO((
                                     tape_list_t *,
                                     file_list_t *
                                     ));
int rtcpcld_getMigrSvcClassName _PROTO((
                                        file_list_t *,
                                        char **
                                        ));
int rtcpcld_getTapePoolName _PROTO((
                                    tape_list_t *,
                                    char **
                                    ));

#ifdef CASTOR_STAGER_TAPECOPY_H
int rtcpcld_putFailed _PROTO((
                              struct Cstager_TapeCopy_t *,
                              int
                              ));
#endif /* CASTOR_STAGER_TAPECOPY_H */

int rtcpcld_initNsInterface _PROTO((
                                    void
                                    ));
int rtcpcld_initCatalogueInterface _PROTO((
                                           void
                                           ));
int rtcpcld_updateNsSegmentAttributes _PROTO((
                                              tape_list_t *,
                                              file_list_t *,
                                              int,
					      struct Cns_fileid*
                                              ));
int rtcpcld_checkNsSegment _PROTO((
                                   tape_list_t *,
                                   file_list_t *
                                   ));
int rtcpcld_checkSegment _PROTO((
                                 tape_list_t *,
                                 file_list_t *
                                 ));

int rtcpcld_checkDualCopies _PROTO((
                                    file_list_t *
                                    ));
char *rtcpcld_tapeStatusStr _PROTO((
                                    int
                                    ));
int rtcpcld_getFileId _PROTO((
                              file_list_t *,
                              struct Cns_fileid **
                              ));
int rtcpcld_getOwner _PROTO((
                             struct Cns_fileid *,
                             int *,
                             int *
                             ));
int rtcpcld_gettape _PROTO((
                            char *,
                            u_signed64,
                            tape_list_t **
                            ));
int rtcpcld_tapeOK _PROTO((
                           tape_list_t *
                           ));
int rtcpcld_handleTapeError _PROTO((
                                    tape_list_t *,
                                    file_list_t *
                                    ));
int rtcpcld_updateTape _PROTO((
                               tape_list_t *,
                               file_list_t *,
                               int,
                               int
                               ));
int rtcpcld_segmentOK _PROTO((
                              struct Cns_segattrs *
                              ));

void rtcpcld_sendStreamReport _PROTO((
				      struct Cstager_ITapeSvc_t* tpSvc,
				      char *file_path,
				      int direction,
				      int created,
				      char* tape_vid,
				      Cuuid_t uuid
                              ));

#endif /* RTCPCLD_H */
