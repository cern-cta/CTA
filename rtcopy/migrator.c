/******************************************************************************
 *                      migrator.c
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
 * @(#)$RCSfile: migrator.c,v $ $Revision: 1.24 $ $Release$ $Date: 2004/12/08 14:40:57 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: migrator.c,v $ $Revision: 1.24 $ $Release$ $Date: 2004/12/08 14:40:57 $ Olof Barring";
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
#include <castor/Constants.h>
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
int inChild = 1;
static int diskFseq = 0;
static int filesCopied = 0;
static u_signed64 bytesCopied = 0;

static tape_list_t *tape = NULL;

int migratorCallbackFileCopied(
                               tapereq,
                               filereq
                               ) 
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  file_list_t *file = NULL;
  int rc, save_serrno, tapeCopyNb = 0;
  struct Cns_fileid *castorFileId = NULL;
  char *blkid;

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
    LOG_SYSCALL_ERR("rtcpcld_findFile()");
    return(-1);
  }
  file->filereq = *filereq;

  if ( rtcpcld_unlockTape() == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_unlockTape()");
    return(-1);
  }

  
  (void)rtcpcld_getFileId(file,&castorFileId);
  blkid = rtcp_voidToString(
                            (void *)filereq->blockid,
                            sizeof(filereq->blockid)
                            );
  if ( blkid == NULL ) blkid = strdup("unknown");

  if ( ((filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED)) ||
       ((filereq->cprc == -1) && (filereq->err.errorcode == ENOSPC)) ) {
    rc = rtcpcld_updateTape(
                            tape,
                            file,
                            0,
                            0
                            );
    if ( rc == -1 ) {
      save_serrno = serrno;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_FAILEDVMGRUPD),
                      (struct Cns_fileid *)castorFileId,
                      RTCPCLD_NB_PARAMS+6,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      tapereq->vid,
                      "SIDE",
                      DLF_MSG_PARAM_INT,
                      tapereq->side,
                      "MODE",
                      DLF_MSG_PARAM_INT,
                      tapereq->mode,
                      "FSEQ",
                      DLF_MSG_PARAM_INT,
                      filereq->tape_fseq,
                      "BLOCKID",
                      DLF_MSG_PARAM_STR,
                      blkid,
                      "ERROR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      RTCPCLD_LOG_WHERE
                      );
      (void)rtcpcld_updcMigrFailed(
                                   tape,
                                   file
                                   );
      serrno = save_serrno;
      return(-1);
    }

    if ( (filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED) ) {
      (void)rtcpcld_getTapeCopyNumber(file,&tapeCopyNb);
      rc = rtcpcld_updateNsSegmentAttributes(tape,file,tapeCopyNb);
      if ( rc == -1 ) {
        save_serrno = serrno;
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_FAILEDNSUPD),
                        (struct Cns_fileid *)castorFileId,
                        RTCPCLD_NB_PARAMS+7,
                        "",
                        DLF_MSG_PARAM_TPVID,
                        tapereq->vid,
                        "SIDE",
                        DLF_MSG_PARAM_INT,
                        tapereq->side,
                        "MODE",
                        DLF_MSG_PARAM_INT,
                        tapereq->mode,
                        "FSEQ",
                        DLF_MSG_PARAM_INT,
                        filereq->tape_fseq,
                        "BLOCKID",
                        DLF_MSG_PARAM_STR,
                        blkid,
                        "COPYNB",
                        DLF_MSG_PARAM_INT,
                        tapeCopyNb,
                        "ERROR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        RTCPCLD_LOG_WHERE
                        );
        (void)rtcpcld_updcMigrFailed(
                                     tape,
                                     file
                                   );
        serrno = save_serrno;
        return(-1);
      }
      rc = rtcpcld_updcFileMigrated(
                                    tape,
                                    file
                                    );
      if ( rc == -1 ) {
        LOG_SYSCALL_ERR("rtcpcld_updcFileMigrated()");
        return(-1);
      }
      /*
       * No lock needed since Migration file copy callbacks are
       * always serialized
       */
      filesCopied++;
      bytesCopied += filereq->bytes_in;
      (void)dlf_write(
                      childUuid,
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_STAGED),
                      castorFileId,
                      6,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      tapereq->vid,
                      "FSEQ",
                      DLF_MSG_PARAM_INT,
                      filereq->tape_fseq,
                      "DISKPATH",
                      DLF_MSG_PARAM_STR,
                      filereq->file_path,
                      "FILESIZE",
                      DLF_MSG_PARAM_INT64,
                      filereq->bytes_in,
                      "TOTFILES",
                      DLF_MSG_PARAM_INT,
                      filesCopied,
                      "TOTBYTES",
                      DLF_MSG_PARAM_INT64,
                      bytesCopied
                      );
    }
    if ( rtcpcld_lockTape() == -1 ) {
      LOG_SYSCALL_ERR("rtcpcld_lockTape()");
      return(-1);
    }
    rtcpcld_cleanupFile(file);
    if ( rtcpcld_unlockTape() == -1 ) {
      LOG_SYSCALL_ERR("rtcpcld_unlockTape()");
      return(-1);
    }
  } else {
    /*
     * Segment failed
     */
    rc = rtcpcld_updcMigrFailed(
                                tape,
                                file
                                );
    if ( rc == -1 ) {
      LOG_SYSCALL_ERR("rtcpcld_updcMigrFailed()");
      return(-1);
    }
  }
  
  return(0);
}

int migratorCallbackMoreWork(
                             tapereq,
                             filereq
                             )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  file_list_t *fl;
  static int moreWorkDone = 0; /* We're always called from a serialized context */
  int rc = 0, save_serrno;

  if ( moreWorkDone != 0 ) {
    /*
     * There was already something to do. Let tape mover go ahead
     * with the processing of the new segment. Since the filereq
     * already has proc_status == RTCP_REQUEST_MORE_WORK we just leave
     * it as it is and we will be called back again when tape mover
     * is ready to receive next file.
     */
    moreWorkDone = 0;
    return(0);
  }

  if ( rtcpcld_lockTape() == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_lockTape()");
    return(-1);
  }

  for (;;) {
    rc = rtcpcld_getSegmentToDo(tape,&fl);
    if ( (rc == -1) || (fl == NULL) ) {
      save_serrno = serrno;
      if ( serrno == ENOENT ) {
        /*
         * Nothing more to do. Flag the file request as finished in order to
         * stop the processing (any value different from RTCP_REQUEST_MORE_WORK
         * or RTCP_WAITING would do the job).
         */
        (void)dlf_write(
                        childUuid,
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_NOMORESEGMS),
                        (struct Cns_fileid *)NULL,
                        0
                        );
        filereq->proc_status = RTCP_FINISHED;
        (void)rtcpcld_unlockTape();
        return(0);
      } else if ( save_serrno == EAGAIN ) {
        sleep(1);
        continue;
      } else {
        (void)dlf_write(
                        childUuid,
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+2,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "rtcpcld_getSegmentsToDo()",
                        "ERROR_STRING",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        RTCPCLD_LOG_WHERE
                        );
        serrno = save_serrno;
        (void)rtcpcld_unlockTape();
        return(-1);
      }
    }
    break;
  }
  /*
   * We got a new file to migrate. Assign the next
   * tape fseq.
   */
  if ( fl->filereq.proc_status != RTCP_WAITING ) {
    /*
     * Shouldn't happen
     */
    (void)rtcpcld_unlockTape();
    serrno = SEINTERNAL;
    return(-1);
  }
  fl->filereq.tape_fseq = rtcpcld_getAndIncrementTapeFseq();
  diskFseq++;
  fl->filereq.disk_fseq = diskFseq;
  *filereq = fl->filereq;
  moreWorkDone = 1;
  if ( rtcpcld_unlockTape() == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_unlockTape()");
    return(-1);
  }
  
  return(rc);
}

int migratorCallback(
                     tapereq,
                     filereq
                     )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  int rc = 0, msgNo;
  struct Cns_fileid *castorFileId = NULL;
  file_list_t *file = NULL;
  char *blkid = NULL, *func = NULL;

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
    if ( (tapereq->tprc != 0) ||
         (filereq->cprc != 0) ) {
      msgNo = RTCPCLD_MSG_CALLBACK_CP;
      func = "processFileCopyCallback";
      rc = migratorCallbackFileCopied(
                                      tapereq,
                                      filereq
                                      );
    }
    break;
  case RTCP_FINISHED:
    msgNo = RTCPCLD_MSG_CALLBACK_CP;
    func = "processFileCopyCallback";
    rc = migratorCallbackFileCopied(
                                    tapereq,
                                    filereq
                                    );
    break;
  case RTCP_REQUEST_MORE_WORK:
    msgNo = RTCPCLD_MSG_CALLBACK_GETW;
    func = "processGetMoreWorkCallback";
    if ( filereq->cprc == 0 ) {
      rc = migratorCallbackMoreWork(
                                    tapereq,
                                    filereq
                                    );
    }
    break;
  default:
    msgNo = RTCPCLD_MSG_INTERNAL;
    func = "unprocessedCallback";
    if ( (tapereq->tprc != 0) ||
         (filereq->cprc != 0) ) {
      msgNo = RTCPCLD_MSG_CALLBACK_CP;
      func = "processFileCopyCallback";
      rc = migratorCallbackFileCopied(
                                      tapereq,
                                      filereq
                                      );
    }
    break;
  }

  if ( msgNo > 0 ) {
    if ( (tapereq->tprc == 0) && (filereq->cprc == 0) ) {
      (void)dlf_write(
                      childUuid,
                      RTCPCLD_LOG_MSG(msgNo),
                      castorFileId,
                      9,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      tapereq->vid,
                      "TPSERV",
                      DLF_MSG_PARAM_STR,
                      tapereq->server,
                      "TPDRIVE",
                      DLF_MSG_PARAM_STR,
                      tapereq->unit,
                      "RTCPD",
                      DLF_MSG_PARAM_UUID,
                      tapereq->rtcpReqId,
                      "DISKCP",
                      DLF_MSG_PARAM_UUID,
                      filereq->stgReqId,
                      "FSEQ",
                      DLF_MSG_PARAM_INT,
                      filereq->tape_fseq,
                      "BLKID",
                      DLF_MSG_PARAM_STR,
                      (blkid != NULL ? blkid : "unknown"),
                      "PATH",
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
                      "RTCPD",
                      DLF_MSG_PARAM_UUID,
                      tapereq->rtcpReqId,
                      "DISKCP",
                      DLF_MSG_PARAM_UUID,
                      filereq->stgReqId,
                      "FSEQ",
                      DLF_MSG_PARAM_INT,
                      filereq->tape_fseq,
                      "BLKID",
                      DLF_MSG_PARAM_STR,
                      (blkid != NULL ? blkid : "unknown"),
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
  return(rc);
}

int main(
         argc,
         argv
         )
     int argc;
     char **argv;
{
  char *migratorFacility = MIGRATOR_FACILITY_NAME, cmdline[CA_MAXLINELEN+1];
  int rc, c, i, save_serrno = 0;

  /*
   * If we are started by the rtcpclientd, the main accept socket has been
   * duplicated to file descriptor 0
   */
  SOCKET sock = 0;

  /* Initializing the C++ log */
  /* Necessary at start of program and after any fork */
  C_BaseObject_initLog("NewStagerLog", SVC_NOMSG);

  Cuuid_create(
               &childUuid
               );
  
  /*
   * Initialise DLF for our facility
   */
  (void)rtcpcld_initLogging(
                            migratorFacility
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
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_MIGRATOR_STARTED),
                  (struct Cns_fileid *)NULL,
                  1,
                  "COMMAND",
                  DLF_MSG_PARAM_STR,
                  cmdline
                  );

  /*
   * Create our tape list
   */
  rc = rtcp_NewTapeList(&tape,NULL,WRITE_ENABLE);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("rtcp_NewTapeList()");
    return(1);
  }

  rc = rtcpcld_initLocks(tape);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_initLocks()");
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
      if ( (tape->file == NULL) ||
           (tape->file->filereq.tape_fseq <= 0) ) {
        rc = -1;
        serrno = SEINTERNAL;
      } else {
        rc = rtcpcld_setTapeFseq(tape->file->filereq.tape_fseq);
      }
      if ( rc == -1 ) {
        LOG_SYSCALL_ERR("rtcpcld_setTapeFseq()");
      } else {
        rc = rtcpcld_runWorker(
                               tape,
                               (tape->tapereq.VolReqID <= 0 ? NULL : &sock),
                               rtcpcld_myDispatch,
                               migratorCallback
                               );
      }
    }
  }
  if ( rc == -1 ) save_serrno = serrno;

  if ( rtcpcld_restoreSelectedTapeCopies(tape) == -1 ) {
    if ( rc == 0 ) {
      save_serrno = serrno;
      rc = -1;
    }
    LOG_SYSCALL_ERR("rtcpcld_restoreSelectedTapeCopies()");
  }

  if ( rtcpcld_returnStream(tape) == -1 ) {
    if ( rc == 0 ) {
      save_serrno = serrno;
      rc = -1;
    }
    LOG_SYSCALL_ERR("rtcpcld_returnStream()");
  }

  rc = rtcpcld_workerFinished(
                              tape,
                              rc,
                              save_serrno
                              );
  return(rc);
}
