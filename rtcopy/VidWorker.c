/*
 *
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: VidWorker.c,v $ $Revision: 1.25 $ $Release$ $Date: 2004/09/17 15:39:36 $ $Author: obarring $
 *
 *
 *
 * @author Olof Barring
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: VidWorker.c,v $ $Revision: 1.25 $ $Release$ $Date: 2004/09/17 15:39:36 $ Olof Barring";
#endif /* not lint */

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
WSADATA wsadata;
#else /* _WIN32 */
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <sys/time.h>
#endif /* _WIN32 */
#include <sys/stat.h>
#include <errno.h>
#include <patchlevel.h>
#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <osdep.h>
#include <trace.h>
#include <serrno.h>
#include <common.h>
#include <Cgetopt.h>
#include <Cinit.h>
#include <Cuuid.h>
#include <Cpwd.h>
#include <Cnetdb.h>
#include <Cpool_api.h>
#include <dlf_api.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <vmgr_api.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld.h>
#include <rtcpcld_messages.h>
#include "castor/Constants.h"
extern int rtcpc_runReq_ext _PROTO((
                                    rtcpHdr_t *,
                                    rtcpc_sockets_t **,
                                    tape_list_t *,
                                    int (*)(void *(*)(void *), void *)
                                    ));
extern int (*rtcpc_ClientCallback) _PROTO((
                                           rtcpTapeRequest_t *, 
                                           rtcpFileRequest_t *
                                           ));

static int callbackThreadPool = -1;
static void *segmCountLock = NULL;
static int segmSubmitted = 0, segmCompleted = 0, segmFailed = 0;
static void *abortLock = NULL;
static int requestAborted = 0;
static int segmentFailed = 0;

Cuuid_t childUuid, mainUuid;
tape_list_t *vidChildTape = NULL;

int inChild = 1;

#define LOG_SYSCALL_ERR(func) { \
    int _save_serrno = serrno; \
    (void)dlf_write( \
                    childUuid, \
                    DLF_LVL_ERROR, \
                    RTCPCLD_MSG_SYSCALL, \
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

static int initLocks() 
{
  int rc;
  /*
   * Segment processing count lock
   */
  rc = Cthread_mutex_lock(&segmSubmitted);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock()");
    return(-1);
  }
  rc = Cthread_mutex_unlock(&segmSubmitted);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock()");
    return(-1);
  }  
  segmCountLock = Cthread_mutex_lock_addr(&segmSubmitted);
  if ( segmCountLock == NULL ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock_addr()");
    return(-1);
  }

  /*
   * Abort lock
   */
  rc = Cthread_mutex_lock(&requestAborted);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock()");
    return(-1);
  }
  rc = Cthread_mutex_unlock(&requestAborted);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock()");
    return(-1);
  }
  
  abortLock = Cthread_mutex_lock_addr(&requestAborted);
  if ( segmCountLock == NULL ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock_addr()");
    return(-1);
  }
  
  return(0);
}

static int updateSegmCount(
                           submitted,
                           completed,
                           failed
                           )
     int submitted,completed, failed;
{
  int rc;
  rc = Cthread_mutex_lock_ext(segmCountLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock_ext()");
    return(-1);
  }
  segmSubmitted += submitted;
  segmCompleted += completed;
  segmFailed += failed;
  (void)dlf_write(
                  childUuid,
                  DLF_LVL_DEBUG,
                  RTCPCLD_MSG_SEGMCNTS,
                  (struct Cns_fileid *)NULL,
                  RTCPCLD_NB_PARAMS+3,
                  "SUBM",
                  DLF_MSG_PARAM_INT,
                  segmSubmitted,
                  "COMPL",
                  DLF_MSG_PARAM_INT,
                  segmCompleted,
                  "FAILED",
                  DLF_MSG_PARAM_INT,
                  segmFailed,
                  RTCPCLD_LOG_WHERE
                  );
  rc = Cthread_mutex_unlock_ext(segmCountLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock_ext()");
    return(-1);
  }
  return(0);
}

static int nbRunningSegms() 
{
  int rc, nbRunning;
  rc = Cthread_mutex_lock_ext(segmCountLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock_ext()");
    return(-1);
  }
  if ( segmFailed > 0 ) nbRunning = 0;
  else nbRunning = segmSubmitted - segmCompleted;
  (void)dlf_write(
                  childUuid,
                  DLF_LVL_DEBUG,
                  RTCPCLD_MSG_SEGMCNTS,
                  (struct Cns_fileid *)NULL,
                  RTCPCLD_NB_PARAMS+3,
                  "SUBM",
                  DLF_MSG_PARAM_INT,
                  segmSubmitted,
                  "COMPL",
                  DLF_MSG_PARAM_INT,
                  segmCompleted,
                  "FAILED",
                  DLF_MSG_PARAM_INT,
                  segmFailed,
                  RTCPCLD_LOG_WHERE
                  );
  rc = Cthread_mutex_unlock_ext(segmCountLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock_ext()");
    return(-1);
  }
  return(nbRunning);
}

static int checkAborted(
                      segmFailed
                      )
     int *segmFailed;
{
  int aborted = 0, rc;
  rc = Cthread_mutex_lock_ext(abortLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock_ext()");
    return(-1);
  }
  aborted = requestAborted;
  if ( segmFailed != NULL ) *segmFailed = segmentFailed;

  rc = Cthread_mutex_unlock_ext(abortLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock_ext()");
    return(-1);
  }
  return(aborted);
}

static void setAborted(
                       segmFailed
                       )
     int segmFailed;
{
  int rc;
  rc = Cthread_mutex_lock_ext(abortLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_lock_ext()");
  }
  requestAborted = 1;
  if ( segmFailed > segmentFailed ) segmentFailed = segmFailed;

  rc = Cthread_mutex_unlock_ext(abortLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock_ext()");
  }
  return;
}

static int processTapePositionCallback(
                                       tapereq,
                                       filereq
                                       )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  int rc = 0;

  if ( (filereq != NULL) && 
       (filereq->cprc == -1) ) {
    rc = updateSegmCount(0,0,1);
  }
  return(rc);
}

static int processFileCopyCallback(
                                   tapereq,
                                   filereq
                                   )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  int rc = 0;
  if ( (filereq != NULL) && 
       (filereq->cprc == 0) && 
       (filereq->proc_status == RTCP_FINISHED) ) {
    rc = updateSegmCount(0,1,0);
  } else if ( filereq->cprc == -1 ) {
    rc = updateSegmCount(0,0,1);
  }
  
  return(rc);
}


static int processGetMoreWorkCallback(
                                      tapereq,
                                      filereq
                                      )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  /*
   * Since we do not have access to the full request list we need a
   * static variable for remembering the number of requests we have
   * already sent off for processing. This is OK even if the
   * FILE request callbacks are multithreaded because RTCOPY will
   * always serialize the RQST_REQUEST_MORE_WORK callbacks.
   */
  static int nbInProgress = 0;
  int rc, save_serrno, found, allFinished = 0, totalWaittime = 0;
  time_t tBefore, tNow;
  char *p;
  file_list_t *fl = NULL;

  /*
   * skip callback if we are in the process of aborting
   */
  if ( checkAborted(NULL) == 1) return(-1);
  if ( tapereq == NULL || filereq == NULL ) {
    serrno = EINVAL;
    return(-1);
  }  

  while ( allFinished == 0 ) {
    rc = nbRunningSegms();
    if ( rc <= 0 ) allFinished = 1;

    found = FALSE;
    CLIST_ITERATE_BEGIN(vidChildTape->file,fl) {
      if ( fl->filereq.proc_status == RTCP_WAITING ) {
        found = TRUE;
        break;
      }
    } CLIST_ITERATE_END(vidChildTape->file,fl);

    if ( found == FALSE ) {
      /*
       * Internal list empty or has no waiting requests (however that
       * happened?). Get more file requests from Catalogue.
       */
      tBefore = time(NULL);
      for (;;) {
        if ( checkAborted(NULL) != 0 ) return(-1);
        rc = rtcpcld_getReqsForVID(
                                   vidChildTape
                                   );
        if ( rc == 0 ) break;
        if ( rc == -1 ) {
          save_serrno = serrno;
          if ( serrno == EAGAIN ) {
            if ( nbInProgress > 0 ) break; /* There was already something todo */
            tNow = time(NULL);
            totalWaittime += (int)(tNow-tBefore);
            tBefore = tNow;
            (void)dlf_write(
                            childUuid,
                            DLF_LVL_SYSTEM,
                            RTCPCLD_MSG_WAITSEGMS,
                            (struct Cns_fileid *)NULL,
                            1,
                            "WAITTIME",
                            DLF_MSG_PARAM_INT,
                            totalWaittime
                            );
            if ( totalWaittime > (RTCP_NETTIMEOUT*3)/4 ) {
              (void)dlf_write(
                              childUuid,
                              DLF_LVL_SYSTEM,
                              RTCPCLD_MSG_WAITTIMEOUT,
                              (struct Cns_fileid *)NULL,
                              1,
                              "WAITTIME",
                              DLF_MSG_PARAM_INT,
                              totalWaittime
                              );
              filereq->proc_status = RTCP_FINISHED;
              return(0);
            }
            sleep(2);
            continue;
          }
          
          if ( serrno == ENOENT ) {
            (void)dlf_write(
                            childUuid,
                            DLF_LVL_SYSTEM,
                            RTCPCLD_MSG_NOMORESEGMS,
                            (struct Cns_fileid *)NULL,
                            0
                            );
            break;
          }
          
          (void)dlf_write(
                          childUuid,
                          DLF_LVL_ERROR,
                          RTCPCLD_MSG_SYSCALL,
                          (struct Cns_fileid *)NULL,
                          RTCPCLD_NB_PARAMS+2,
                          "SYSCALL",
                          DLF_MSG_PARAM_STR,
                          "rtcpcld_getReqsForVID()",
                          "ERROR_STRING",
                          DLF_MSG_PARAM_STR,
                          sstrerror(serrno),
                          RTCPCLD_LOG_WHERE
                          );
          serrno = save_serrno;
          return(-1);
        }
      }
      /*
       * Check if any new requests were found in Catalogue
       */
      CLIST_ITERATE_BEGIN(vidChildTape->file,fl) {
        if ( fl->filereq.proc_status == RTCP_WAITING ) {
          found = TRUE;
          break;
        }
      } CLIST_ITERATE_END(vidChildTape->file,fl);
    }
    
    if ( (found == TRUE) && (fl != NULL) ) {
      /*
       * Pop off request by request until all has been passed back 
       * through callback. Make sure to retain the original tape
       * path, given by server 
       */
      strncpy(
              fl->filereq.tape_path,
              filereq->tape_path,
              sizeof(fl->filereq.tape_path)-1
              );
      if ( fl->filereq.proc_status == RTCP_WAITING ) {
        *filereq = fl->filereq;
        nbInProgress++;
      }
    
      CLIST_DELETE(vidChildTape->file,fl);
      free(fl);
      (void)updateSegmCount(1,0,0);
      break;
    } else {
      if ( nbInProgress > 0 ) {
        /*
         * There is already something to do. Go ahead with the processing
         * and we will be called again once it has finished. Reset the
         * counter for next callback.
         */
        nbInProgress = 0;
        break;
      } else {
        /*
         * If there are still segments running we can wait a while and
         * see if client wants to add more files to the request. Make
         * sure that we don't expire the server timeout (wait up to 3/4
         * of server timeout).
         * Otherwise flag the file request as finished in order to stop the
         * processing (any value different from RTCP_REQUEST_MORE_WORK or
         * RTCP_WAITING would do the job).
         */
        if ( (allFinished == 1) || (totalWaittime > (RTCP_NETTIMEOUT*3)/4) ) {
          if ( allFinished != 1 ) (void)dlf_write(
                                                  childUuid,
                                                  DLF_LVL_SYSTEM,
                                                  RTCPCLD_MSG_WAITTIMEOUT,
                                                  (struct Cns_fileid *)NULL,
                                                  1,
                                                  "WTIME",
                                                  DLF_MSG_PARAM_INT,
                                                  totalWaittime
                                                  );
          filereq->proc_status = RTCP_FINISHED;
          break;
        } else {
          (void)dlf_write(
                          childUuid,
                          DLF_LVL_SYSTEM,
                          RTCPCLD_MSG_WAITNEWSEGMS,
                          (struct Cns_fileid *)NULL,
                          0
                          );
          totalWaittime += 2;
          sleep(2);
          continue;
        }
      }
    }
  }
  
  return(0);
}
  
  
/** rtcpcld_Callback() - VidWorker processing of RTCOPY callbacks
 *
 */
int rtcpcld_Callback(
                     tapereq,
                     filereq
                     )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  char *func, *vid = "", *blkid = NULL, *disk_path = ".", *errmsgtxt = "";
  int fseq = -1, proc_status = -1, msgNo = -1, rc, status, getMoreWork = 0;
  int cprc = -1, save_serrno, severity = RTCP_OK, errorcode=0;
  struct Cns_fileid fileId;
  Cuuid_t stgUuid, rtcpUuid;

  if ( tapereq == NULL || filereq == NULL ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Invalid parameter",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = EINVAL;
    return(-1);
  }

  if ( tapereq != NULL ) {
    vid = tapereq->vid;
    rtcpUuid = tapereq->rtcpReqId;
  }

  if ( filereq != NULL ) {
    if ( filereq->proc_status == RTCP_POSITIONED ) {
      msgNo = RTCPCLD_MSG_CALLBACK_POS;
      func = "processTapePositionCallback";
      rc = processTapePositionCallback(
                                       tapereq,
                                       filereq
                                       );
    } else if ( filereq->proc_status == RTCP_REQUEST_MORE_WORK ) {
      getMoreWork = 1;
      msgNo = RTCPCLD_MSG_CALLBACK_GETW;
      func = "processGetMoreWorkCallback";
      if ( filereq->cprc == 0 ) rc = processGetMoreWorkCallback(
                                                                tapereq,
                                                                filereq
                                                                );
    } else {
      msgNo = RTCPCLD_MSG_CALLBACK_CP;
      func = "processFileCopyCallback";
      rc = processFileCopyCallback(
                                       tapereq,
                                       filereq
                                       );
    }
    if ( rc == -1 ) {
      save_serrno = serrno;
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      func,
                      "ERROR_STRING",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      RTCPCLD_LOG_WHERE
                      );
      serrno = save_serrno;
      return(-1);
    }

    /*
     * Always update catalogue upon error or for real FILE callbacks. Note
     * that for a RTCP_REQUSET_MORE_WORK callback, the status
     * is changed to RTCP_FINISHED when there is nothing more
     * to do.
     */
    if ( filereq->cprc == -1 || 
         (getMoreWork == 0 && filereq->proc_status == RTCP_FINISHED) ) {
      if ( filereq->cprc == 0 ) status = SEGMENT_FILECOPIED;
      else {
        status = SEGMENT_FAILED;
        setAborted(1);
      }
      rc = rtcpcld_setFileStatus(
                                 filereq,
                                 status,
                                 (status == SEGMENT_FAILED ? 1 : 0)
                                 );
      if ( rc == -1 ) {
        save_serrno = serrno;
        (void)dlf_write(
                        childUuid,
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_SYSCALL,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+2,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "rtcpcld_setFileStatus()",
                        "ERROR_STRING",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        RTCPCLD_LOG_WHERE
                        );
        serrno = save_serrno;
        return(-1);
      }
    }

    fseq = filereq->tape_fseq;
    blkid = rtcp_voidToString(
                              (void *)filereq->blockid,
                              sizeof(filereq->blockid)
                              );
    if ( blkid == NULL ) blkid = strdup("unknown");
    fileId.fileid = filereq->castorSegAttr.castorFileId;
    strncpy(
            fileId.server,
            filereq->castorSegAttr.nameServerHostName,
            sizeof(fileId.server)-1
            );
    stgUuid = filereq->stgReqId;
    proc_status = filereq->proc_status;
    disk_path = filereq->file_path;
    cprc = filereq->cprc;
    errmsgtxt = filereq->err.errmsgtxt;
    errorcode = filereq->err.errorcode;
    severity = filereq->err.severity;
  } /* if ( filereq != NULL ) */

  if ( msgNo > 0 ) {
    if ( cprc == 0 ) {
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_SYSTEM,
                      msgNo,
                      &fileId,
                      7,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      vid,
                      "",
                      DLF_MSG_PARAM_UUID,
                      rtcpUuid,
                      "",
                      DLF_MSG_PARAM_UUID,
                      stgUuid,
                      "FSEQ",
                      DLF_MSG_PARAM_INT,
                      fseq,
                      "BLKID",
                      DLF_MSG_PARAM_STR,
                      (blkid != NULL ? blkid : "unknown"),
                      "STATUS",
                      DLF_MSG_PARAM_INT,
                      proc_status,
                      "CPRC",
                      DLF_MSG_PARAM_INT,
                      cprc
                      );
    } else {
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_SYSTEM,
                      msgNo,
                      &fileId,
                      10,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      vid,
                      "",
                      DLF_MSG_PARAM_UUID,
                      rtcpUuid,
                      "",
                      DLF_MSG_PARAM_UUID,
                      stgUuid,
                      "FSEQ",
                      DLF_MSG_PARAM_INT,
                      fseq,
                      "BLKID",
                      DLF_MSG_PARAM_STR,
                      (blkid != NULL ? blkid : "unknown"),
                      "STATUS",
                      DLF_MSG_PARAM_INT,
                      proc_status,
                      "CPRC",
                      DLF_MSG_PARAM_INT,
                      cprc,
                      "ERROR_CODE",
                      DLF_MSG_PARAM_INT,
                      errorcode,
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      errmsgtxt,
                      "RTCP_SEVERITY",
                      DLF_MSG_PARAM_INT,
                      severity
                      );
    }
  }
  if ( blkid != NULL ) free(blkid);
  return(0);
}

/** myDispatch() - FILE request dispatch function to be used by RTCOPY API
 *
 * @param func - function to be dispatched
 * @param arg - pointer to opaque function argument
 *
 * myDispatch() creates a thread for dispatching the processing of FILE request
 * callbacks in the extended RTCOPY API. This allows for handling weakly
 * blocking callbacks like tppos to not be serialized with potentially strongly
 * blocking callbascks like filcp or request for more work.
 * 
 * Note that the Cthread_create_detached() API cannot be passed directly in
 * the RTCOPY API because it is a macro and not a proper function prototype.
 *
 * @return -1 = error otherwise the Cthread id (>=0)
 */
int myDispatch(
               func,
               arg
               )
     void *(*func)(void *);
     void *arg;
{
  return(Cpool_assign(
                      callbackThreadPool,
                      func,
                      arg,
                      -1
                      ));  
}

static int initThreadPool() 
{
  int poolsize;
  char *p;

  if ( (p = getenv("RTCPD_THREAD_POOL")) != (char *)NULL ) {
    poolsize = atoi(p);
  } else if ( ( p = getconfent("RTCPD","THREAD_POOL",0)) != (char *)NULL ) {
    poolsize = atoi(p);
  } else {
#if defined(RTCPD_THREAD_POOL)
    poolsize = RTCPD_THREAD_POOL;
#else /* RTCPD_THREAD_POOL */
    poolsize = 3;         /* Set some reasonable default */
#endif /* RTCPD_TRHEAD_POOL */
  }
  callbackThreadPool = Cpool_create(poolsize,&poolsize);
  if ( (callbackThreadPool == -1) || (poolsize <= 0) ) return(-1);
  return(0);
}

static int updateClientInfo(
                            origSocket,
                            port,
                            tape
                            )
     SOCKET *origSocket;
     int port;
     tape_list_t *tape;
{
  vdqmVolReq_t volReq;
  vdqmDrvReq_t drvReq;
  struct passwd *pw;
  int rc, save_serrno;
  uid_t myUid;
  gid_t myGid;

  if ( origSocket == NULL || 
       *origSocket == INVALID_SOCKET || 
       port < 0 || 
       tape == NULL ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "invalid parameter",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = EINVAL;
    return(-1);
  }
  memset(&volReq,'\0',sizeof(volReq));
  memset(&drvReq,'\0',sizeof(drvReq));

  errno = serrno = 0;
  myUid = geteuid();
  myGid = getegid();
  pw = Cgetpwuid(myUid);
  if ( pw == NULL ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_PWUID,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = SESYSERR;
    return(-1);
  }
  strncpy(
          volReq.client_name,
          pw->pw_name,
          sizeof(volReq.client_name)-1
          );
  volReq.clientUID = myUid;
  volReq.clientGID = myGid;
  (void)gethostname(
                    volReq.client_host,
                    sizeof(volReq.client_host)-1
                    );
  volReq.client_port = port;
  strncpy(
          drvReq.dgn,
          tape->tapereq.dgn,
          sizeof(drvReq.dgn)-1
          );
  volReq.VolReqID = tape->tapereq.VolReqID;
  strncpy(
          drvReq.drive,
          tape->tapereq.unit,
          sizeof(drvReq.drive)-1
          );
  errno = serrno = 0;
  rc = vdqm_SendToRTCP(
                       *origSocket,
                       &volReq,
                       &drvReq
                       );
  if ( rc == -1 ) {
    save_serrno = serrno;
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "vdqm_SendToRTCP()",
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }
  closesocket(
              *origSocket
              );
  *origSocket = INVALID_SOCKET;
  return(0);
}



static int vidWorker(
                     origSocket
                     )
     SOCKET *origSocket;
{
  rtcpc_sockets_t *socks;
  rtcpHdr_t hdr;
  tape_list_t *tapeCopy = NULL;
  file_list_t *placeHolderFile = NULL;
  int rc, port = -1, reqId, save_serrno;

  if ( vidChildTape == NULL || 
       *vidChildTape->tapereq.vid == '\0' ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Invalid parameter",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = EINVAL;
    return(-1);
  }

  (void)dlf_write(
                  mainUuid,
                  DLF_LVL_SYSTEM,
                  RTCPCLD_MSG_VIDWORKER_STARTED,
                  (struct Cns_fileid *)NULL,
                  5,
                  "",
                  DLF_MSG_PARAM_UUID,
                  childUuid,
                  "",
                  DLF_MSG_PARAM_TPVID,
                  vidChildTape->tapereq.vid,
                  "MODE",
                  DLF_MSG_PARAM_INT,
                  vidChildTape->tapereq.mode,
                  "VDQMID",
                  DLF_MSG_PARAM_INT,
                  vidChildTape->tapereq.VolReqID
                  );

  /*
   * Check that there still are any requests for the VID
   */
  errno = serrno = 0;
  rc = rtcpcld_anyReqsForVID(
                             vidChildTape
                             );
  if ( rc == -1 ) {
    save_serrno = serrno;
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "rtcpcld_anyReqsForVID()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(save_serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }

  if ( rc == 0 ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_SYSTEM,
                    RTCPCLD_MSG_NOREQS,
                    (struct Cns_fileid *)NULL,
                    0
                    );
    return(0);
  }

  rc = rtcp_NewTapeList(&tapeCopy,NULL,vidChildTape->tapereq.mode);
  if ( (rc != -1) && tapeCopy != NULL ) {
    tapeCopy->tapereq = vidChildTape->tapereq;
    rc = rtcp_NewFileList(
                          &tapeCopy,
                          &placeHolderFile,
                          vidChildTape->tapereq.mode
                          );
    if ( (rc != -1) && (placeHolderFile != NULL) ) {
      placeHolderFile->filereq.concat = NOCONCAT;
      placeHolderFile->filereq.convert = ASCCONV;
      strcpy(placeHolderFile->filereq.recfm,"F");
      placeHolderFile->filereq.proc_status = RTCP_REQUEST_MORE_WORK;
    }
  }
  
  if ( (rc == -1) || tapeCopy == NULL || tapeCopy->file == NULL ) {
    save_serrno = serrno;
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    (tapeCopy == NULL ? "rtcp_NewTapeList()" : "rtcp_NewFileList()"),
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(save_serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }

  /*
   * Check the request and initialise our private listen socket
   * that we will use for further communication with rtcpd
   */
  errno = serrno = 0;
  rc = rtcpc_InitReq(
                     &socks,
                     &port,
                     tapeCopy
                     );
  if ( rc == -1 ) {
    save_serrno = serrno;
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "rtcpc_InitReq()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(save_serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }

  if ( origSocket != NULL ) {
    /*
     * Send the new address to the rtcpd server. The original vdqm request
     * contained the rtcpclientd port, which we cannot use.
     */
    errno = serrno = 0;
    rc = updateClientInfo(
                          origSocket,
                          port,
                          tapeCopy
                          );
    if ( rc == -1 ) return(-1);  /* error already logged */
  } else {
    /*
     * VDQM not yet called when running in standalone mode.
     */
    rc = vdqm_SendVolReq(
                         NULL,
                         &(vidChildTape->tapereq.VolReqID),
                         vidChildTape->tapereq.vid,
                         vidChildTape->tapereq.dgn,
                         NULL,
                         NULL,
                         vidChildTape->tapereq.mode,
                         port
                         );
    if ( rc == -1 ) {
      save_serrno = serrno;
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_VDQM,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "vdqm_SendVolReq()",
                      "ERROR_STR", 
                      DLF_MSG_PARAM_STR,
                      sstrerror(save_serrno),
                      RTCPCLD_LOG_WHERE
                      );
      serrno = save_serrno;
      return(-1);
    }
  }

  /*
   * Put the vwAddress in the database to allow client to send abort
   */
  (void)rtcpcld_setVidWorkerAddress(vidChildTape,port);
  
  /*
   * Run the request;
   */
  rtcpc_ClientCallback = rtcpcld_Callback;
  rc = initThreadPool();
  if ( rc == -1 ) return(-1);

  reqId = vidChildTape->tapereq.VolReqID;
  rc = rtcpc_SelectServer(
                          &socks,
                          tapeCopy,
                          NULL,
                          port,
                          &reqId
                          );
  if ( rc == -1 ) return(-1);
  hdr.magic = RTCOPY_MAGIC;
  rc = rtcpc_sendReqList(
                         &hdr,
                         &socks,
                         tapeCopy
                         );
  if ( rc == -1 ) return(-1);
  rc = rtcpc_sendEndOfReq(
                          &hdr,
                          &socks,
                          tapeCopy
                          );
  if ( rc == -1 ) return(-1);
  rc = rtcpc_runReq_ext(
                        &hdr,
                        &socks,
                        tapeCopy,
                        myDispatch
                        );
  return(rc);
}

static int checkArgs(
                     vid,
                     dgn,
                     lbltype,
                     density,
                     unit,
                     vdqmVolReqID
                     )
     char *vid, *dgn, *lbltype, *density, *unit;
     int vdqmVolReqID;
{
  int rc = 0;

  if ( vid == NULL || strlen(vid) > CA_MAXVIDLEN ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "VID wrongly specified",
                    "VALUE",
                    DLF_MSG_PARAM_STR,
                    (vid != NULL ? vid : "(null)"),
                    RTCPCLD_LOG_WHERE
                    );
    rc = -1;
  }
  if ( (dgn == NULL && vdqmVolReqID > 0) || 
       (dgn != NULL && strlen(dgn) > CA_MAXDGNLEN) ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "DGN wrongly specified",
                    "VALUE",
                    DLF_MSG_PARAM_STR,
                    (dgn != NULL ? dgn : "(null)"),
                    RTCPCLD_LOG_WHERE
                    );
    rc = -1;
  }
  if ( (lbltype == NULL && vdqmVolReqID > 0) || 
       (lbltype != NULL && strlen(lbltype) > CA_MAXLBLTYPLEN) ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Label type wrongly specified",
                    "VALUE",
                    DLF_MSG_PARAM_STR,
                    (lbltype != NULL ? lbltype : "(null)"),
                    RTCPCLD_LOG_WHERE
                    );
    rc = -1;
  }
  if ( (density == NULL && vdqmVolReqID > 0) || 
       (density != NULL && strlen(density) > CA_MAXDENLEN) ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Density wrongly specified",
                    "VALUE",
                    DLF_MSG_PARAM_STR,
                    (density != NULL ? density : "(null)"),
                    RTCPCLD_LOG_WHERE
                    );
    rc = -1;
  }

  if ( (unit == NULL && vdqmVolReqID > 0) || 
       (unit != NULL && strlen(unit) > CA_MAXUNMLEN) ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Tape unit wrongly specified",
                    "VALUE",
                    DLF_MSG_PARAM_STR,
                    (unit != NULL ? unit : "(null)"),
                    RTCPCLD_LOG_WHERE
                    );
    rc = -1;
  }

  return(rc);
}

static int callVmgr(
                    vid,
                    side,
                    dgn,
                    density,
                    lbltype
                    )
     char *vid, **dgn, **density, **lbltype;
     int side;
{
  char vmgr_error_buffer[512], tmpdgn[CA_MAXDGNLEN+1];
  struct vmgr_tape_info vmgr_tapeinfo;
  int rc;
  
  vmgr_seterrbuf(
                 vmgr_error_buffer,
                 sizeof(vmgr_error_buffer)
                 );
  memset(
         &vmgr_tapeinfo,
         '\0',
         sizeof(vmgr_tapeinfo)
         );
  rc = vmgr_querytape (
                       vid,
                       side,
                       &vmgr_tapeinfo,
                       tmpdgn
                       );
  if ( ((vmgr_tapeinfo.status & DISABLED) == DISABLED) ||
       ((vmgr_tapeinfo.status & EXPORTED) == EXPORTED) ||
       ((vmgr_tapeinfo.status & ARCHIVED) == ARCHIVED) ) {
    serrno = EACCES;
    return(-1);
  }

  if ( *dgn == NULL ) *dgn = strdup(tmpdgn);
  else strcpy(*dgn,tmpdgn);
  if ( *density == NULL ) *density = strdup(vmgr_tapeinfo.density);
  else strcpy(*density,vmgr_tapeinfo.density);
  if ( *lbltype == NULL ) *lbltype = strdup(vmgr_tapeinfo.lbltype);
  else strcpy(*lbltype,vmgr_tapeinfo.lbltype);
  return(0);
}

static int initTapeReq(
                       vid,
                       side,
                       mode,
                       _dgn,
                       _density,
                       _lbltype,
                       unit,
                       vdqmVolReqID
                       )
     char *vid, *_dgn, *_density, *_lbltype, *unit;
     int side, mode, vdqmVolReqID;
{
  char *dgn, *density, *lbltype;
  int rc;
  
  dgn = _dgn;
  density = _density;
  lbltype = _lbltype;

  if ( vid == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  vidChildTape = NULL;
  rc = rtcp_NewTapeList(
                        &vidChildTape,
                        NULL,
                        mode
                        );
  if ( rc == -1 || vidChildTape == NULL ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "rtcp_NewTapeList()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    return(-1);
  }

  if ( dgn == NULL || density == NULL || lbltype == NULL ) {
    /*
     * Get missing tape parameters from VMGR
     */
    rc = callVmgr(
                  vid,
                  side,
                  &dgn,
                  &density,
                  &lbltype
                  );
    if ( rc == -1 || dgn == NULL || density == NULL || lbltype == NULL ) {
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "callVmgr()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      RTCPCLD_LOG_WHERE
                      );
      return(-1);
    }
  }

  strncpy(
          vidChildTape->tapereq.vid,
          vid,
          sizeof(vidChildTape->tapereq.vid)-1
          );
  strncpy(
          vidChildTape->tapereq.dgn,
          dgn,
          sizeof(vidChildTape->tapereq.dgn)-1
          );
  strncpy(
          vidChildTape->tapereq.density,
          density,
          sizeof(vidChildTape->tapereq.density)-1
          );
  strncpy(
          vidChildTape->tapereq.label,
          lbltype,
          sizeof(vidChildTape->tapereq.label)-1
          );
  if ( unit != NULL ) {
    strncpy(
            vidChildTape->tapereq.unit,
            unit,
            sizeof(vidChildTape->tapereq.unit)-1
            );
  }

  vidChildTape->tapereq.VolReqID = vdqmVolReqID;
  
  return(0);
}

static void setErrorInfo(
                         tape,
                         errorCode,
                         errMsgTxt
                         )
     tape_list_t *tape;
     char *errMsgTxt;
     int errorCode;
{
  file_list_t *fl;
  int rc, segmFailed = 0;
  
  if ( tape == NULL ) return;
  
  rc = checkAborted(&segmFailed);
  if ( (rc == 1) && (segmFailed == 0) ) {
    if ( tape->tapereq.tprc == 0 ) tape->tapereq.tprc = -1;
    if ( tape->tapereq.err.errorcode == 0 ) 
      tape->tapereq.err.errorcode = errorCode;
    if ( *tape->tapereq.err.errmsgtxt == '\0' ) {
      if ( errMsgTxt != NULL ) {
        strncpy(tape->tapereq.err.errmsgtxt,
                errMsgTxt,
                sizeof(tape->tapereq.err.errmsgtxt)-1);
      } else {
        strncpy(tape->tapereq.err.errmsgtxt,
                sstrerror(errorCode),
                sizeof(tape->tapereq.err.errmsgtxt)-1);
      }
    }
    if ( (tape->tapereq.err.severity == RTCP_OK) ||
         (tape->tapereq.err.severity == 0 ) ) {
      tape->tapereq.err.severity = RTCP_UNERR|RTCP_FAILED;
    }
  }
  return;  
}

  

int main(
         argc,
         argv
         )
     int argc;
     char **argv;
{
  char *vid = NULL, *dgn = NULL, *lbltype = NULL, *density = NULL;
  char  *unit = NULL, *mainUuidStr = NULL, *shiftMsg, optstr[3];
  char *vidChildFacility = RTCPCLIENTD_FACILITY_NAME, cmdline[CA_MAXLINELEN+1];
  int i, vdqmVolReqID = -1, c, rc, mode = WRITE_DISABLE, modeSet = 0, side = 0;
  int save_serrno, retval, tStartRequest = 0;
  ID_TYPE key = 0;
  /*
   * If we are started by the rtcpclientd, the main accept socket has been
   * duplicated to file descriptor 0
   */
  SOCKET origSocket = 0;

  /* Initializing the C++ log */
  /* Necessary at start of program and after any fork */
  C_BaseObject_initLog("NewStagerLog", SVC_NOMSG);
  
  Coptind = 1;
  Copterr = 1;
  vid = dgn = lbltype = density = NULL;

  Cuuid_create(
               &childUuid
               );
  /*
   * Initialise DLF
   */
  (void)rtcpcld_initLogging(
                            vidChildFacility
                            );
  cmdline[0] = '\0';
  c=0;
  for (i=0; (i<CA_MAXLINELEN) && (c<argc);) {
    strcat(cmdline,argv[c++]);
    strcat(cmdline," ");
    i = strlen(cmdline)+1;
  }
  
  (void)dlf_write(
                  childUuid,
                  DLF_LVL_SYSTEM,
                  RTCPCLD_MSG_VIDWORKER_STARTED,
                  (struct Cns_fileid *)NULL,
                  1,
                  "COMMAND",
                  DLF_MSG_PARAM_STR,
                  cmdline
                  );

  while ( (c = Cgetopt(argc, argv, "V:g:i:k:l:d:s:S:U:u:T:rw")) != -1 ) {
    switch (c) {
    case 'V':
      /*
       * Tape VID. This argument is required
       */
      vid = Coptarg;
      break;
    case 'g':
      /* 
       * Device group name. If not provided, it will be taken from VMGR
       */
      dgn = Coptarg;
      break;
    case 'i':
      /*
       * VDQM Volume Request ID. This argument is passed by the rtcpclientd
       * daemon, which starts the VidWorker only when the tape request has
       * started on the tape server. If not provided, the VidWorker will
       * submit its own request to VDQM. This allows for stand-alone running
       * (and debugging) of the VidWorker program.
       */
      vdqmVolReqID = atoi(Coptarg);
      break;
    case 'k':
      key = atol(Coptarg);
      rtcpcld_setTapeKey(key);
      break;
    case 'l':
      /*
       * Tape label type. If not provided, it will be taken from VMGR
       */
      lbltype = Coptarg;
      break;
    case 'd':
      /*
       * Tape media density. If not provided, it will be taken from VMGR
       */
      density = Coptarg;
      break;
    case 's':
      /*
       * Media side (forseen for future support for two sided medias).
       * Defaults to 0
       */
      side = atoi(Coptarg);
      break;
    case 'S':
      /*
       * Parent decided to use a different socket than 0 .
       * Useful when running with Insure...
       */
      origSocket = atoi(Coptarg);
      break;
    case 'U':
      /*
       * UUID of the rtcpclientd daemon that started this VidWorker.
       * Used only for logging of UUID association.
       */
      mainUuidStr = Coptarg;
      serrno = 0;
      rc = rtcp_stringToVoid(
                             mainUuidStr,
                             &mainUuid,
                             sizeof(mainUuid)
                             );
      if ( rc == -1 ) {
        if ( serrno == 0 ) serrno = errno;
        (void)dlf_write(
                        childUuid,
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_SYSCALL,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+2,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "rtcp_stringToVoid()",
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        RTCPCLD_LOG_WHERE
                        );
        return(1);
      }
      break;
    case 'u':
      unit = Coptarg;
      break;
    case 'T':
      tStartRequest = atoi(Coptarg);
      break;
    case 'r':
      /*
       * Read mode switch. Default if no tape access mode is specified
       */
      if ( modeSet != 0 ) {
        (void)dlf_write(
                        childUuid,
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_INTERNAL,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+1,
                        "ERROR_STRING",
                        DLF_MSG_PARAM_STR,
                        "Duplicate or concurrent use of -r or -w switch",
                        RTCPCLD_LOG_WHERE
                        );
        return(2);
      }
      mode = WRITE_DISABLE;
      modeSet++;
      break;
    case 'w':
      /*
       * Write mode switch. Must be specified if tape write access is required.
       */
      if ( modeSet != 0 ) {
        (void)dlf_write(
                        childUuid,
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_INTERNAL,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+1,
                        "ERROR_STRING",
                        DLF_MSG_PARAM_STR,
                        "Duplicate or concurrent use of -r or -w switch",
                        RTCPCLD_LOG_WHERE
                        );
        return(2);
      }
      mode = WRITE_ENABLE;
      modeSet++;
      break;
    default:
      sprintf(optstr,"-%c",c);
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_UNKNOPT,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+1,
                      "OPTION",
                      DLF_MSG_PARAM_STR,
                      optstr,
                      RTCPCLD_LOG_WHERE
                      );
      return(2);
      break;
    }
  }  

  /*
   * Fake a parent UUID in case we run stand-alone
   */
  if ( mainUuidStr == NULL ) Cuuid_create(
                                          &mainUuid
                                          );
  rc = checkArgs(
                 vid,
                 dgn,
                 lbltype,
                 density,
                 unit,
                 vdqmVolReqID
                 );
  if ( rc == -1 ) return(2);
  rc = initTapeReq(
                   vid,
                   side,
                   mode,
                   dgn,
                   density,
                   lbltype,
                   unit,
                   vdqmVolReqID
                   );
  if ( rc == -1 ) return(2);  

  rc = initLocks();
  if ( rc == -1 ) return(2);

  /*
   * Process the request
   */
  if ( tStartRequest <= 0 ) tStartRequest = (int)time(NULL);
  vidChildTape->tapereq.TStartRequest = tStartRequest;
  rc = vidWorker(
                 (vdqmVolReqID >= 0 ? &origSocket : NULL)
                 );
  save_serrno = serrno;
  retval = 0;
  if ( rc == -1 ) {
    setAborted(0);
    (void)rtcp_RetvalSHIFT(vidChildTape,NULL,&retval);
    if ( retval == 0 ) retval = UNERR;
  }
  switch (retval) {
  case 0:
    shiftMsg = "command successful";
    break;
  case RSLCT:
    shiftMsg = "Re-selecting another tape server";
    break;
  case BLKSKPD:
    shiftMsg = "command partially successful since blocks were skipped";
    break;
  case TPE_LSZ:
    shiftMsg = "command partially successful: blocks skipped and size limited by -s option";
    break;
  case MNYPARY:
    shiftMsg = "command partially successful: blocks skipped or too many errors on tape";
    break;
  case LIMBYSZ:
    shiftMsg = "command successful";
    break;
  default:
    shiftMsg = "command failed";
    break;
  }  
  (void)dlf_write(
                  mainUuid,
                  DLF_LVL_SYSTEM,
                  RTCPCLD_MSG_VIDWORKER_ENDED,
                  (struct Cns_fileid *)NULL,
                  6,
                  "",
                  DLF_MSG_PARAM_TPVID,
                  vidChildTape->tapereq.vid,
                  "MODE",
                  DLF_MSG_PARAM_INT,
                  vidChildTape->tapereq.mode,
                  "VDQMID",
                  DLF_MSG_PARAM_INT,
                  vidChildTape->tapereq.VolReqID,
                  "rtcpRC",
                  DLF_MSG_PARAM_INT,
                  rc,
                  "serrno",
                  DLF_MSG_PARAM_INT,
                  save_serrno,
                  "SHIFTMSG",
                  DLF_MSG_PARAM_STR,
                  shiftMsg
                  );
  if ( rc == -1 ) {
    setErrorInfo(vidChildTape,save_serrno,shiftMsg);
    (void)rtcpcld_setVIDFailedStatus(vidChildTape);
  } else {
    (void)rtcpcld_updateVIDStatus(
                                  vidChildTape,
                                  TAPE_PENDING,
                                  TAPE_FINISHED
                                  );
    (void)rtcpcld_updateVIDStatus(
                                  vidChildTape,
                                  TAPE_WAITVDQM,
                                  TAPE_FINISHED
                                  );
    (void)rtcpcld_updateVIDStatus(
                                  vidChildTape,
                                  TAPE_WAITMOUNT,
                                  TAPE_FINISHED
                                  );
    (void)rtcpcld_updateVIDStatus(
                                  vidChildTape,
                                  TAPE_MOUNTED,
                                  TAPE_FINISHED
                                  );
  }
  
  return(retval);
}
