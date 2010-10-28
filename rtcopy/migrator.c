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
 * @(#)$RCSfile: migrator.c,v $ $Revision: 1.59 $ $Release$ $Date: 2009/08/18 09:43:01 $ $Author: waldron $
 *
 *
 *
 * @author Olof Barring
 *****************************************************************************/

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <sys/time.h>
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
#include <Cns_api.h>
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
extern int rtcpc_runReq_ext (
			     rtcpHdr_t *,
			     rtcpc_sockets_t **,
			     tape_list_t *,
			     int (*)(void *(*)(void *), void *)
			     );
extern int (*rtcpc_ClientCallback) (
				    rtcpTapeRequest_t *,
				    rtcpFileRequest_t *
				    );
Cuuid_t childUuid, mainUuid;
int inChild = 1;
extern int checkFile;
static char *tapePoolName = NULL;
static int diskFseq = 0;
static int filesCopied = 0;
static u_signed64 bytesCopied = 0;
static tape_list_t *tape = NULL;
typedef struct SvcClassList {
  char *svcClassName;
  u_signed64 totBytes;
  int totFiles;
  struct SvcClassList *next;
  struct SvcClassList *prev;
} SvcClassList_t;
static SvcClassList_t *svcClassList = NULL;

void updateSvcClassList(
                        svcClassName,
                        nbBytes
                        )
     char *svcClassName;
     u_signed64 nbBytes;
{
  SvcClassList_t *iterator;
  int found = 0;

  if ( svcClassName == NULL ) return;
  CLIST_ITERATE_BEGIN(svcClassList,iterator)
    {
      if ( strcmp(iterator->svcClassName,svcClassName) == 0 ) {
        iterator->totFiles++;
        iterator->totBytes += nbBytes;
        found = 1;
      }
    }
  CLIST_ITERATE_END(svcClassList,iterator);

  if ( found == 0 ) {
    iterator = (SvcClassList_t *)calloc(1,sizeof(SvcClassList_t));
    if ( iterator == NULL ) return;
    iterator->svcClassName = strdup(svcClassName);
    iterator->totFiles = 1;
    iterator->totBytes = nbBytes;
    CLIST_INSERT(svcClassList,iterator);
  }
  return;
}

void logSvcClassTotals()
{
  SvcClassList_t *iterator;

  CLIST_ITERATE_BEGIN(svcClassList,iterator)
    {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_STATS),
                      (struct Cns_fileid *)NULL,
                      7,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      (tape != NULL ? tape->tapereq.vid : "UNKN"),
                      "TPSERV",
                      DLF_MSG_PARAM_STR,
                      (tape != NULL ? tape->tapereq.server : "UNKN"),
                      "TPDRIVE",
                      DLF_MSG_PARAM_STR,
                      (tape != NULL ? tape->tapereq.unit : "UNKN"),
                      "TAPEPOOL",
                      DLF_MSG_PARAM_STR,
                      (tapePoolName != NULL ? tapePoolName : "unknown"),
                      "SVCCLASS",
                      DLF_MSG_PARAM_STR,
                      (iterator->svcClassName != NULL ? iterator->svcClassName : "unknown"),
                      "TOTFILES",
                      DLF_MSG_PARAM_INT,
                      iterator->totFiles,
                      "TOTBYTES",
                      DLF_MSG_PARAM_INT64,
                      iterator->totBytes
                      );
    }
  CLIST_ITERATE_END(svcClassList,iterator);
  return;
}

int migratorCallbackFileCopied(
                               tapereq,
                               filereq
                               )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
{
  file_list_t *file = NULL;
  int rc, save_serrno, tapeCopyNb = 0, ownerUid = -1, ownerGid = -1;
  int filesWritten = 0;
  struct Cns_fileid *castorFileId = NULL;
  char *blkid, *svcClassName = NULL;
  time_t last_mod_time;

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
  (void)rtcpcld_getOwner(castorFileId,&ownerUid,&ownerGid);
  (void)rtcpcld_getMigrSvcClassName(file,&svcClassName);
  (void)rtcpcld_getLastModificationTime(file,&last_mod_time);

  if ( ((filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED)) ||
       ((filereq->cprc == -1) && ((filereq->err.errorcode == ENOSPC) ||
                                  (rtcpcld_handleTapeError(tape,file) == 1))) ) {
    rc = rtcpcld_updateTape(
                            tape,
                            file,
                            0,
                            0,
                            &filesWritten
                            );
    if ( (rc == -1) ||
         ((filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED) && (filesWritten == 0)) ) {
      save_serrno = serrno;
      if ( (filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED) && (filesWritten == 0) ) save_serrno = SEINTERNAL;
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_FAILEDVMGRUPD),
                      (struct Cns_fileid *)castorFileId,
                      RTCPCLD_NB_PARAMS+9,
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
                      "OWNERUID",
                      DLF_MSG_PARAM_INT,
                      ownerUid,
                      "OWNERGID",
                      DLF_MSG_PARAM_INT,
                      ownerGid,
                      "FILES",
                      DLF_MSG_PARAM_INT,
                      filesWritten,
                      "ERROR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      RTCPCLD_LOG_WHERE
                      );
      (void)rtcpcld_updcMigrFailed(
                                   tape,
                                   file
                                   );
      if ( blkid != NULL ) free(blkid);
      if ( castorFileId != NULL ) free(castorFileId);
      serrno = save_serrno;
      return(-1);
    }

    if ( (filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED) &&
         (filesWritten == 1) ) {
      (void)rtcpcld_getTapeCopyNumber(file,&tapeCopyNb);
      rc = rtcpcld_updateNsSegmentAttributes(tape,file,tapeCopyNb,castorFileId,last_mod_time);
      if ( rc == -1 ) {
        save_serrno = serrno;
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_FAILEDNSUPD),
                        (struct Cns_fileid *)castorFileId,
                        RTCPCLD_NB_PARAMS+10,
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
                        "DISKPATH",
                        DLF_MSG_PARAM_STR,
                        filereq->file_path,
                        "COPYNB",
                        DLF_MSG_PARAM_INT,
                        tapeCopyNb,
                        "OWNERUID",
                        DLF_MSG_PARAM_INT,
                        ownerUid,
                        "OWNERGID",
                        DLF_MSG_PARAM_INT,
                        ownerGid,
                        "ERROR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        RTCPCLD_LOG_WHERE
                        );
        (void)rtcpcld_updcMigrFailed(
                                     tape,
                                     file
                                   );
        if ( blkid != NULL ) free(blkid);
        if ( castorFileId != NULL ) free(castorFileId);
        serrno = save_serrno;
        return(-1);
      }
      rc = rtcpcld_updcFileMigrated(
                                    tape,
                                    file
                                    );
      if ( rc == -1 ) {
        LOG_SYSCALL_ERR("rtcpcld_updcFileMigrated()");
        if ( blkid != NULL ) free(blkid);
        if ( castorFileId != NULL ) free(castorFileId);
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
                      13,
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
                      "FILESIZE",
                      DLF_MSG_PARAM_INT64,
                      filereq->bytes_in,
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
                      bytesCopied,
                      "SVCCLASS",
                      DLF_MSG_PARAM_STR,
                      (svcClassName != NULL ? svcClassName : "unknown"),
                      "TAPEPOOL",
                      DLF_MSG_PARAM_STR,
                      (tapePoolName != NULL ? tapePoolName : "unknown")
                      );
      updateSvcClassList(svcClassName,filereq->bytes_in);
    }
    if ( rtcpcld_lockTape() == -1 ) {
      LOG_SYSCALL_ERR("rtcpcld_lockTape()");
      if ( blkid != NULL ) free(blkid);
      if ( castorFileId != NULL ) free(castorFileId);
      return(-1);
    }

    /*
     * Remove the associated data structures from memory if
     * the migration was successful. Otherwise (e.g. VOLUME OVERFLOW)
     * we have to keep the file to permit resetting the status of
     * the tape copy before this migration stream exits.
     */
    if ( filereq->cprc == 0 ) {
      rtcpcld_cleanupFile(file);
    }
    if ( rtcpcld_unlockTape() == -1 ) {
      LOG_SYSCALL_ERR("rtcpcld_unlockTape()");
      if ( blkid != NULL ) free(blkid);
      if ( castorFileId != NULL ) free(castorFileId);
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
      if ( blkid != NULL ) free(blkid);
      if ( castorFileId != NULL ) free(castorFileId);
      return(-1);
    }
  }

  if ( blkid != NULL ) free(blkid);
  if ( castorFileId != NULL ) free(castorFileId);
  return(0);
}

int migratorCallbackMoreWork(rtcpFileRequest_t *filereq)
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
        filereq->proc_status = RTCP_FINISHED;
        (void)rtcpcld_unlockTape();
        return(0);
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
      msgNo = RTCPCLD_MSG_COPYFAILED;
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
    if ( (tapereq->tprc != 0) ||
         (filereq->cprc != 0) ) {
      msgNo = RTCPCLD_MSG_COPYFAILED;
    }
    break;
  case RTCP_REQUEST_MORE_WORK:
    msgNo = RTCPCLD_MSG_CALLBACK_GETW;
    func = "processGetMoreWorkCallback";
    if ( filereq->cprc == 0 ) {
      rc = migratorCallbackMoreWork(filereq);
    }
    if ( filereq->proc_status == RTCP_REQUEST_MORE_WORK ) {
      msgNo = RTCPCLD_MSG_CALLBACK_ADDGETW;
    }
    castorFileId = (struct Cns_fileid *)calloc(1,sizeof(struct Cns_fileid));
    if ( castorFileId != NULL ) {
      castorFileId->fileid = filereq->castorSegAttr.castorFileId;
      strncpy(
              castorFileId->server,
              filereq->castorSegAttr.nameServerHostName,
              sizeof(castorFileId->server)-1
              );
    }
    break;
  default:
    msgNo = RTCPCLD_MSG_INTERNAL;
    func = "unprocessedCallback";
    if ( (tapereq->tprc != 0) ||
         (filereq->cprc != 0) ) {
      msgNo = RTCPCLD_MSG_COPYFAILED;
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
                      "",
                      DLF_MSG_PARAM_UUID,
                      tapereq->rtcpReqId,
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
  (void)rtcpcld_doCommit();
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
  char *p;
  struct Cns_segattrs startSegment;
  int rc, c, i, save_serrno = 0, runTime;
  time_t startTime, endTime;

  startTime = time(NULL);
  /*
   * If we are started by the rtcpclientd, the main accept socket has been
   * duplicated to file descriptor 0
   */
  SOCKET sock = 0;

  Cuuid_create(
               &childUuid
               );

  /*
   * Initialise DLF for our facility
   */
  (void)rtcpcld_initLogging(
                            migratorFacility
                            );

  if ( getconfent("migrator","CHECKFILE",0) != NULL ) checkFile = 1;
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
    dlf_shutdown();
    return(1);
  }

  rc = rtcpcld_initLocks(tape);
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_initLocks()");
    dlf_shutdown();
    return(1);
  }

  rc = rtcpcld_initNsInterface();
  if ( rc == -1 ) {
    LOG_SYSCALL_ERR("rtcpcld_initNsInterface()");
    dlf_shutdown();
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
        rc = 0;
        if ( (((p = getconfent("migrator","CHECKFSEQ",0)) != NULL) ||
              ((p = getenv("migrator_CHECKFSEQ")) != NULL)) &&
             (strcmp(p,"YES") == 0) ) {
          memset(&startSegment,'\0',sizeof(startSegment));
          rc = Cns_lastfseq(tape->tapereq.vid,tape->tapereq.side,&startSegment);
          if ( rc == -1 ) {
            save_serrno = serrno;
            if ( save_serrno == ENOENT ) {
              /* new tape, not yes known to the name server */
              rc = 0;
            } else {
              LOG_SYSCALL_ERR("Cns_lastfseq()");
            }
          }
          if ( rc == 0 ) {
            if ( tape->file->filereq.tape_fseq > startSegment.fseq ) {
              (void)dlf_write(
                              (inChild == 0 ? mainUuid : childUuid),
                              RTCPCLD_LOG_MSG(RTCPCLD_MSG_CHECKSTARTFSEQ),
                              (struct Cns_fileid *)NULL,
                              4,
                              "",
                              DLF_MSG_PARAM_TPVID,
                              tape->tapereq.vid,
                              "SIDE",
                              DLF_MSG_PARAM_INT,
                              tape->tapereq.side,
                              "VMGRFSEQ",
                              DLF_MSG_PARAM_INT,
                              tape->file->filereq.tape_fseq,
                              "CNSFSEQ",
                              DLF_MSG_PARAM_INT,
                              startSegment.fseq
                              );
            } else {
              rc = -1;
              save_serrno = serrno = ERTWRONGFSEQ;
              (void)dlf_write(
                              (inChild == 0 ? mainUuid : childUuid),
                              RTCPCLD_LOG_MSG(RTCPCLD_MSG_WRONGSTARTFSEQ),
                              (struct Cns_fileid *)NULL,
                              RTCPCLD_NB_PARAMS+4,
                              "",
                              DLF_MSG_PARAM_TPVID,
                              tape->tapereq.vid,
                              "SIDE",
                              DLF_MSG_PARAM_INT,
                              tape->tapereq.side,
                              "VMGRFSEQ",
                              DLF_MSG_PARAM_INT,
                              tape->file->filereq.tape_fseq,
                              "CNSFSEQ",
                              DLF_MSG_PARAM_INT,
                              startSegment.fseq,
                              RTCPCLD_LOG_WHERE
                              );
              serrno = save_serrno;
            }
          }
        }
        if ( rc == 0 ) {
          rc = rtcpcld_setTapeFseq(tape->file->filereq.tape_fseq);
          if ( rc == -1 ) {
            LOG_SYSCALL_ERR("rtcpcld_setTapeFseq()");
          }
        }
      }
      if ( rc == 0 ) {
        (void)rtcpcld_getTapePoolName(tape,&tapePoolName);
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

  logSvcClassTotals();
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
  dlf_shutdown();
  return(rc);
}
