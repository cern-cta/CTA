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
 * @(#)$RCSfile: migrator.c,v $ $Revision: 1.6 $ $Release$ $Date: 2004/10/28 08:10:08 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: migrator.c,v $ $Revision: 1.6 $ $Release$ $Date: 2004/10/28 08:10:08 $ Olof Barring";
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
int diskFseq = 0;

static tape_list_t *tape = NULL;

int migratorCallbackFileCopied(
                               tapereq,
                               filereq
                               ) 
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  file_list_t *file;
  int rc, save_serrno;
  struct Cns_fileid *castorFileId = NULL;
  char *blkid;

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

  (void)rtcpcld_getFileId(filereq,&castorFileId);
  blkid = rtcp_voidToString(
                            (void *)filereq->blockid,
                            sizeof(filereq->blockid)
                            );
  if ( blkid == NULL ) blkid = strdup("unknown");

  if ( ((filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED)) ) {
    rc = rtcpcld_updateTape(
                            tape,
                            filereq,
                            0,
                            0
                            );
    if ( rc == -1 ) {
      save_serrno = serrno;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_FAILEDVMGRUPD,
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
                                   filereq
                                   );
      serrno = save_serrno;
      return(-1);
    }

    rc = rtcpcld_updateNsSegmentAttributes(tape,filereq);
    if ( rc == -1 ) {
      save_serrno = serrno;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_FAILEDNSUPD,
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
                                   filereq
                                   );
      serrno = save_serrno;
      return(-1);
    }
    rc = rtcpcld_updcFileMigrated(
                                  tape,
                                  filereq
                                  );
    if ( rc == -1 ) {
      LOG_SYSCALL_ERR("rtcpcld_updcFileMigrated()");
      return(-1);
    }
    rc = rtcpcld_findFile(tape,filereq,&file);
    if ( rc == -1 ) {
      LOG_SYSCALL_ERR("rtcpcld_findFile()");
      return(-1);
    }
    rtcpcld_cleanupFile(file);
  } else {
    /*
     * Segment failed
     */
    rc = rtcpcld_updcMigrFailed(
                                tape,
                                filereq
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

  rc = rtcpcld_getSegmentsToDo(tape);  
  if ( rc == -1 ) {
    save_serrno = serrno;
    if ( serrno == ENOENT ) {
      /*
       * Nothing more to do. Flag the file request as finished in order to
       * stop the processing (any value different from RTCP_REQUEST_MORE_WORK
       * or RTCP_WAITING would do the job).
       */
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_SYSTEM,
                      RTCPCLD_MSG_NOMORESEGMS,
                      (struct Cns_fileid *)NULL,
                      0
                      );
      filereq->proc_status = RTCP_FINISHED;
      return(0);
    } else {
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
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
      return(-1);
    }
  }
  /*
   * We got a new file to migrate. Assign the next
   * tape fseq.
   */
  fl = tape->file->prev;
  if ( fl->filereq.proc_status != RTCP_WAITING ) {
    /*
     * Shouldn't happen
     */
    serrno = SEINTERNAL;
    return(-1);
  }
  *filereq = fl->filereq;
  filereq->tape_fseq = rtcpcld_getAndIncrementTapeFseq();
  diskFseq++;
  filereq->disk_fseq = diskFseq;
  moreWorkDone = 1;
  
  return(rc);
}

int migratorCallback(
                     tapereq,
                     filereq
                     )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  int rc = 0, save_serrno, msgNo, level = DLF_LVL_SYSTEM;
  struct Cns_fileid *castorFileId = NULL;
  char *blkid, *func;

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

  (void)rtcpcld_getFileId(filereq,&castorFileId);
  blkid = rtcp_voidToString(
                            (void *)filereq->blockid,
                            sizeof(filereq->blockid)
                            );
  if ( blkid == NULL ) blkid = strdup("unknown");

  switch (filereq->proc_status) {
  case RTCP_POSITIONED:
      msgNo = RTCPCLD_MSG_CALLBACK_POS;
      func = "processTapePositionCallback";
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
    level = DLF_LVL_ERROR;
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
                      level,
                      msgNo,
                      castorFileId,
                      6,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      tapereq->vid,
                      "",
                      DLF_MSG_PARAM_UUID,
                      tapereq->rtcpReqId,
                      "",
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
                      DLF_LVL_ERROR,
                      msgNo,
                      castorFileId,
                      10,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      tapereq->vid,
                      "",
                      DLF_MSG_PARAM_UUID,
                      tapereq->rtcpReqId,
                      "",
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
  SOCKET sock = 0;
  int rc, c, i, save_serrno;

  /*
   * If we are started by the rtcpclientd, the main accept socket has been
   * duplicated to file descriptor 0
   */
  SOCKET origSocket = 0;

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
                  DLF_LVL_SYSTEM,
                  RTCPCLD_MSG_MIGRATOR_STARTED,
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
                               &sock,
                               rtcpcld_myDispatch,
                               migratorCallback
                               );
      }
    }
  }
  if ( rc == -1 ) save_serrno = serrno;

  rc = rtcpcld_workerFinished(
                              tape,
                              rc,
                              save_serrno
                              );
  return(rc);
}
