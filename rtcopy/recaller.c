/******************************************************************************
 *                      recaller.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: recaller.c,v $ $Revision: 1.23 $ $Release$ $Date: 2005/07/06 09:56:14 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/


#ifndef lint
static char sccsid[] = "@(#)$RCSfile: recaller.c,v $ $Revision: 1.23 $ $Release$ $Date: 2005/07/06 09:56:14 $ Olof Barring";
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
#include <castor/BaseObject.h>
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

Cuuid_t childUuid, mainUuid;
static tape_list_t *tape = NULL;
static void *segmCountLock = NULL;
static int segmSubmitted = 0, segmCompleted = 0, segmFailed = 0;
static void *abortLock = NULL;
static int requestAborted = 0;
static int segmentFailed = 0;

int inChild = 1;
extern int checkFile;
static int filesCopied = 0;
static u_signed64 bytesCopied = 0;

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
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_SEGMCNTS),
                  (struct Cns_fileid *)NULL,
                  3,
                  "SUBM",
                  DLF_MSG_PARAM_INT,
                  segmSubmitted,
                  "COMPL",
                  DLF_MSG_PARAM_INT,
                  segmCompleted,
                  "FAILED",
                  DLF_MSG_PARAM_INT,
                  segmFailed
                  );
  rc = Cthread_mutex_unlock_ext(segmCountLock);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("Cthread_mutex_unlock_ext()");
    return(-1);
  }
  return(0);
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


int recallerCallbackFileCopied(
                               tapereq,
                               filereq
                               )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  file_list_t *file = NULL;
  int rc, save_serrno, finished = 0, failed = 0, ownerUid = -1, ownerGid = -1;
  struct Cns_fileid *castorFileId = NULL;

  if ( (tapereq == NULL) || (filereq == NULL) ) {
    (void)dlf_write(
                    childUuid,
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
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

  if ( rtcpcld_lockTape() == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_lockTape()");
    return(-1);
  }
  
  rc = rtcpcld_findFile(tape,filereq,&file);
  if ( rc == -1 ) {
    save_serrno = serrno;
    LOG_SYSCALL_ERR("rtcpcld_findFile()");
    (void)rtcpcld_unlockTape();
    serrno = save_serrno;
    return(-1);
  }
  file->filereq = *filereq;

  (void)rtcpcld_getFileId(file,&castorFileId);

  (void)rtcpcld_getOwner(castorFileId,&ownerUid,&ownerGid);

  if ( (filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED) ) {
    rc = rtcpcld_checkNsSegment(
                                tape,
                                file
                                );
    if ( rc == -1 ) {
      save_serrno = serrno;
      LOG_SYSCALL_ERR("rtcpcld_checkNsSegment()");
      (void)rtcpcld_updcRecallFailed(
                                     tape,
                                     file
                                     );
      (void)rtcpcld_unlockTape();
      (void)updateSegmCount(0,0,1);
      serrno = save_serrno;
      return(-1);
    }

    rc = rtcpcld_updcFileRecalled(
                                  tape,
                                  file
                                  );
    if ( rc == -1 ) {
      save_serrno = serrno;
      LOG_SYSCALL_ERR("rtcpcld_updcFileRecalled()");
      (void)rtcpcld_updcRecallFailed(
                                     tape,
                                     file
                                     );
      (void)rtcpcld_unlockTape();
      (void)updateSegmCount(0,0,1);
      serrno = save_serrno;
      return(-1);
    }
    filesCopied++;
    bytesCopied += filereq->bytes_out;
    (void)dlf_write(
                    childUuid,
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_STAGED),
                    castorFileId,
                    12,
                    "",
                    DLF_MSG_PARAM_TPVID,
                    tapereq->vid,
                    "TPSERV",
                    DLF_MSG_PARAM_STR,
                    tapereq->server,
                    "TPDRIVE",
                    DLF_MSG_PARAM_STR,
                    tapereq->unit,
                    "",
                    DLF_MSG_PARAM_UUID,
                    tapereq->rtcpReqId,
                    "FSEQ",
                    DLF_MSG_PARAM_INT,
                    filereq->tape_fseq,
                    "DISKPATH",
                    DLF_MSG_PARAM_STR,
                    filereq->file_path,
                    "OFFSET",
                    DLF_MSG_PARAM_INT64,
                    filereq->offset,
                    "FILESIZE",
                    DLF_MSG_PARAM_INT64,
                    filereq->bytes_out,
                    "OWNERUID",
                    DLF_MSG_PARAM_INT,
                    ownerUid,
                    "OWNERGID",
                    DLF_MSG_PARAM_INT,
                    ownerGid,
                    "TOTFILES",
                    DLF_MSG_PARAM_INT,
                    filesCopied,
                    "TOTBYTES",
                    DLF_MSG_PARAM_INT64,
                    bytesCopied
                    );
    finished = 1;
  } else {
    /*
     * Segment failed
     */
    rc = rtcpcld_updcRecallFailed(
                                  tape,
                                  file
                                  );
    if ( rc == -1 ) {
      save_serrno = serrno;
      LOG_SYSCALL_ERR("rtcpcld_updcRecallFailed()");
      (void)rtcpcld_unlockTape();
      (void)updateSegmCount(0,0,1);
      serrno = save_serrno;
      return(-1);
    }
    failed = 1;
  }

  if ( rtcpcld_unlockTape() == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_unlockTape()");
    return(-1);
  }
  (void)updateSegmCount(0,finished,failed);
  if ( failed != 0 ) setAborted(1);
  
  return(0);
}

/*
 * The way the rtcpc() handles morework requests is to loop on
 * this callback to get all files currently available for processing.
 * The loop is stopped by either another morework request or
 * a finished request. The former would cause a new morework loop
 * while the latter would rundown processing as soon as all unprocessed
 * file requests have finished.
 */
int recallerCallbackMoreWork(
                             tapereq,
                             filereq
                             )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  static int requestToProcess = 0;
  struct Cns_fileid *castorFileId = NULL;
  int rc, save_serrno;
  file_list_t *file = NULL;

  /*
   * Send requests one by one to make sure that the
   * path is always optimal at the time for position
   */
  if ( requestToProcess > 0 ) {
    requestToProcess = 0;
    return(0);
  }

  if ( rtcpcld_lockTape() == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_lockTape()");
    return(-1);
  }

  if ( checkAborted(NULL) != 0 ) {
    (void)rtcpcld_unlockTape();
    return(-1);
  }
  file = NULL;
  rc = rtcpcld_getSegmentToDo(tape,&file);
  if ( (rc == -1) || (file == NULL) ) {
    if ( serrno == EAGAIN ) {
      /*
       * There are semgents in SEGMENT_UNPROCESSED status
       */
      if ( requestToProcess > 0 ) {
        /*
         * We have already submitted some request to process in
         * this MOREWORK round. Don't wait for more.
         */
        if ( rtcpcld_unlockTape() == -1 ) {
          LOG_SYSCALL_ERR("rtcpcld_unlockTape()");
          return(-1);
        }
        requestToProcess = 0;
      } else {
        /*
         * Should not happen
         */
        (void)rtcpcld_unlockTape();
        serrno = SEINTERNAL;
        return(-1);
      }
    } else if ( serrno == ENOENT ) {
      (void)dlf_write(
                      childUuid,
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_NOMORESEGMS),
                      (struct Cns_fileid *)NULL,
                      0
                      );
      if ( rtcpcld_unlockTape() == -1 ) {
        LOG_SYSCALL_ERR("rtcpcld_unlockTape()");
        return(-1);
      }
      filereq->proc_status = RTCP_FINISHED;
      requestToProcess = 0;
    } else {
      /*
       * serrno != (EAGAIN || ENOENT)
       */
      save_serrno = serrno;
      LOG_SYSCALL_ERR("rtcpcld_getSegmentsToDo()");
      (void)rtcpcld_unlockTape();
      serrno = save_serrno;
      return(-1);
    }
  } else {
    strncpy(
            file->filereq.tape_path,
            filereq->tape_path,
            sizeof(file->filereq.tape_path)-1
            );
    *filereq = file->filereq;
    if ( rtcpcld_unlockTape() == -1 ) {
      LOG_SYSCALL_ERR("rtcpcld_unlockTape()");
      return(-1);
    }
    if ( filereq->position_method != TPPOSIT_BLKID ) {
      (void)rtcpcld_getFileId(file,&castorFileId);
      (void)dlf_write(
                      childUuid,
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_NOTBLKIDPOS),
                      (struct Cns_fileid *)castorFileId,
                      1,
                      "METHOD",
                      DLF_MSG_PARAM_INT,
                      filereq->position_method
                      );
    }
    requestToProcess = 1;
    (void)updateSegmCount(1,0,0);
  }
  /*
   * We are here because there is at least one segment to process
   * or there is nothing left to do and we should just let the currently
   * running file copies finish (if any).
   */
  
  return(0);
}

int recallerCallback(
                     tapereq,
                     filereq
                     )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  int rc = 0, save_serrno, msgNo;
  struct Cns_fileid *castorFileId = NULL;
  file_list_t *file = NULL;
  char *blkid = NULL, *func = NULL;
  char dlfErrBuf[CA_MAXLINELEN+1];  

  (void)dlf_seterrbuf(dlfErrBuf,CA_MAXLINELEN);
  if ( tapereq == NULL || filereq == NULL ) {
    (void)dlf_write(
                    childUuid,
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
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

  if ( rtcpcld_lockTape() == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_lockTape()");
    return(-1);
  }

  rc = rtcpcld_findFile(tape,filereq,&file);
  if ( (rc != -1) && (file != NULL) ) {
    (void)rtcpcld_getFileId(file,&castorFileId);
  }

  if ( rtcpcld_unlockTape() == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_unlockTape()");
    return(-1);
  }


  blkid = rtcp_voidToString(
                            (void *)filereq->blockid,
                            sizeof(filereq->blockid)
                            );
  if ( blkid == NULL ) blkid = strdup("unknown");

  switch (filereq->proc_status) {
  case RTCP_POSITIONED:
    msgNo = RTCPCLD_MSG_CALLBACK_POS;
    func = "processTapePositionCallback";
    if ( file != NULL ) file->filereq.proc_status = filereq->proc_status;
    if ( (tapereq->tprc != 0) ||
         (filereq->cprc != 0) ) {
      msgNo = RTCPCLD_MSG_COPYFAILED;
      func = "processFileCopyCallback";
      rc = recallerCallbackFileCopied(
                                      tapereq,
                                      filereq
                                      );
      if ( rc == -1 ) save_serrno = serrno;
    }
    break;
  case RTCP_FINISHED:
    msgNo = RTCPCLD_MSG_CALLBACK_CP;
    func = "processFileCopyCallback";
    if ( file != NULL ) file->filereq.proc_status = filereq->proc_status;
    rc = recallerCallbackFileCopied(
                                    tapereq,
                                    filereq
                                    );
    if ( rc == -1 ) save_serrno = serrno;
    if ( (tapereq->tprc != 0) ||
         (filereq->cprc != 0) ) {
      msgNo = RTCPCLD_MSG_COPYFAILED;
    }
    break;
  case RTCP_REQUEST_MORE_WORK:
    msgNo = RTCPCLD_MSG_CALLBACK_GETW;
    func = "processGetMoreWorkCallback";
    if ( filereq->cprc == 0 ) {
      rc = recallerCallbackMoreWork(
                                    tapereq,
                                    filereq
                                    );
      if ( rc == -1 ) save_serrno = serrno;
    } else {
      (void)updateSegmCount(0,0,1);
    }
    if ( filereq->proc_status == RTCP_REQUEST_MORE_WORK ) {
      msgNo = RTCPCLD_MSG_CALLBACK_ADDGETW;
    }
    break;
  default:
    msgNo = RTCPCLD_MSG_INTERNAL;
    func = "unprocessedCallback";
    if ( file != NULL ) file->filereq.proc_status = filereq->proc_status;
    if ( (tapereq->tprc != 0) ||
         (filereq->cprc != 0) ) {
      msgNo = RTCPCLD_MSG_COPYFAILED;
      func = "processFileCopyCallback";
      rc = recallerCallbackFileCopied(
                                      tapereq,
                                      filereq
                                      );
      if ( rc == -1 ) save_serrno = serrno;
    }
    break;
  }

  if ( msgNo > 0 ) {
    if ( (tapereq->tprc == 0) && (filereq->cprc == 0) ) {
      (void)dlf_write(
                      childUuid,
                      RTCPCLD_LOG_MSG(msgNo),
                      castorFileId,
                      8,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      tapereq->vid,
                      "TPSERV",
                      DLF_MSG_PARAM_STR,
                      tapereq->server,
                      "TPDRIVE",
                      DLF_MSG_PARAM_STR,
                      tapereq->unit,
                      "",
                      DLF_MSG_PARAM_UUID,
                      tapereq->rtcpReqId,
                      "FSEQ",
                      DLF_MSG_PARAM_INT,
                      filereq->tape_fseq,
                      "BLKID",
                      DLF_MSG_PARAM_STR,
                      (blkid != NULL ? blkid : "unknown"),
                      "DISKPATH",
                      DLF_MSG_PARAM_STR,
                      filereq->file_path,
                      "STATUS",
                      DLF_MSG_PARAM_INT,
                      filereq->proc_status
                      );
    } else {
      int tmpRC;
      rtcpErrMsg_t *err;
      if ( filereq->cprc != 0 ) {
        tmpRC = filereq->cprc;
        err = &(filereq->err);
      } else {
        tmpRC = tapereq->tprc;
        err = &(tapereq->err);
      }
      (void)dlf_write(
                      childUuid,
                      RTCPCLD_LOG_MSG(msgNo),
                      castorFileId,
                      12,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      tapereq->vid,
                     "TPSERV",
                      DLF_MSG_PARAM_STR,
                      tapereq->server,
                      "TPDRIVE",
                      DLF_MSG_PARAM_STR,
                      tapereq->unit,
                      "",
                      DLF_MSG_PARAM_UUID,
                      tapereq->rtcpReqId,
                      "FSEQ",
                      DLF_MSG_PARAM_INT,
                      filereq->tape_fseq,
                      "BLKID",
                      DLF_MSG_PARAM_STR,
                      (blkid != NULL ? blkid : "unknown"),
                      "DISKPATH",
                      DLF_MSG_PARAM_STR,
                      filereq->file_path,
                      "STATUS",
                      DLF_MSG_PARAM_INT,
                      filereq->proc_status,
                      (filereq->cprc != 0 ? "CPRC" : "TPRC"),
                      DLF_MSG_PARAM_INT,
                      tmpRC,
                      "ERROR_CODE",
                      DLF_MSG_PARAM_INT,
                      err->errorcode,
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      err->errmsgtxt,
                      "RTCP_SEVERITY",
                      DLF_MSG_PARAM_INT,
                      err->severity
                      );
    }
  }
  if ( blkid != NULL ) free(blkid);
  if ( castorFileId != NULL ) free(castorFileId);
  if ( rc == -1 ) serrno = save_serrno;
  return(rc);
}

int main(
         argc,
         argv
         )
     int argc;
     char **argv;
{  
  char *recallerFacility = RECALLER_FACILITY_NAME, cmdline[CA_MAXLINELEN+1];
  int rc, c, i, save_serrno = 0, failed = 0, runTime;
  time_t startTime, endTime;

  /*
   * If we are started by the rtcpclientd, the main accept socket has been
   * duplicated to file descriptor 0
   */
  SOCKET sock = 0;

  /* Initializing the C++ log */
  /* Necessary at start of program and after any fork */
  C_BaseObject_initLog("NewStagerLog", SVC_NOMSG);

  /*
   * Make sure that recalled disk files are properly protected
   */
  umask(077);

  startTime = time(NULL);
  Cuuid_create(
               &childUuid
               );
  
  /*
   * Initialise DLF for our facility
   */
  (void)rtcpcld_initLogging(
                            recallerFacility
                            );
  if ( getconfent("recaller","CHECKFILE",0) != NULL ) checkFile = 1;
  cmdline[0] = '\0';
  c=0;
  for (i=0; (i<CA_MAXLINELEN) && (c<argc);) {
    strcat(cmdline,argv[c++]);
    strcat(cmdline," ");
    i = strlen(cmdline)+1;
  }
  
  (void)dlf_write(
                  childUuid,
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_RECALLER_STARTED),
                  (struct Cns_fileid *)NULL,
                  1,
                  "COMMAND",
                  DLF_MSG_PARAM_STR,
                  cmdline
                  );

  /*
   * Create our tape list
   */
  rc = rtcp_NewTapeList(&tape,NULL,WRITE_DISABLE);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("rtcp_NewTapeList()");
    return(1);
  }

  rc = rtcpcld_initLocks(tape);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_initLocks()");
    return(1);
  }

  rc = initLocks();
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("initLocks()");
    return(1);
  }

  rc = rtcpcld_initNsInterface();
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_initNsInterface()");
    return(1);
  }

  rc = rtcpcld_parseWorkerCmd(
                              argc,
                              argv,
                              tape,
                              &sock
                              );
  if ( rc == 0 ) {
    rc = rtcpcld_initThreadPool(WRITE_ENABLE);
    if ( rc == -1 ) {
      LOG_SYSCALL_ERR("initThreadPool()");
    } else {
      rc = rtcpcld_runWorker(
                             tape,
                             (tape->tapereq.VolReqID <= 0 ? NULL : &sock),
                             rtcpcld_myDispatch,
                             recallerCallback
                             );
    }
  }
  if ( rc == -1 ) {
    save_serrno = serrno;
    if ( (checkAborted(&failed) == 0) && (failed == 0) ) {
      /*
       * This is an error not seen by callback. Depending on the
       * tape mover version this can happen if there is an error on the
       * first file. If so, force a callback.
       */
      if ( (tape->file != NULL) && 
           ((tape->file->filereq.cprc == -1) ||
            (tape->file->filereq.err.errorcode > 0) ||
            (*tape->file->filereq.err.errmsgtxt != '\0') ||
            ((tape->file->filereq.err.severity & RTCP_FAILED) != 0)) ) {
        (void)recallerCallbackFileCopied(
                                         &(tape->tapereq),
                                         &(tape->file->filereq)
                                         );
      }
    }
  }

  endTime = time(NULL);
  runTime = (int)endTime-startTime;
  rc = rtcpcld_workerFinished(
                              tape,
                              filesCopied,
                              bytesCopied,
                              runTime,
                              rc,
                              save_serrno
                              );
  return(rc);
}
