/******************************************************************************
 *                      rtcpcldVmgrInterface.c
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
 * @(#)$RCSfile: rtcpcldVmgrInterface.c,v $ $Revision: 1.9 $ $Release$ $Date: 2004/11/03 11:47:35 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldVmgrInterface.c,v $ $Revision: 1.9 $ $Release$ $Date: 2004/11/03 11:47:35 $ Olof Barring";
#endif /* not lint */

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>                  /* Standard data types          */
#include <sys/stat.h>
#include <errno.h>
#if defined(_WIN32)
#include <io.h>
#include <winsock2.h>
extern char *geterr();
WSADATA wsadata;
#else /* _WIN32 */
#include <unistd.h>
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <sys/time.h>
#endif /* _WIN32 */
#include <patchlevel.h>
#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <osdep.h>
#include <trace.h>
#include <serrno.h>
#include <Cgetopt.h>
#include <Cpwd.h>
#include <dlf_api.h>
#include <Cnetdb.h>
#include <Cuuid.h>
#include <Cmutex.h>
#include <u64subr.h>
#include <Cglobals.h>
#include <serrno.h>
#include <Cns_api.h>
#include <vmgr_api.h>
/*
#include <Ctape_constants.h>
#include <castor/stager/Tape.h>
#include <castor/stager/Segment.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <castor/stager/IStagerSvc.h>
#include <castor/Services.h>
#include <castor/BaseAddress.h>
#include <castor/db/DbAddress.h>
#include <castor/IAddress.h>
#include <castor/IObject.h>
#include <castor/IClient.h>
*/
#include <castor/Constants.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld_messages.h>
#include <rtcpcld.h>

/** Global needed for determine which uuid to log
 */
int inChild;
/** uuid's set by calling process (rtcpclientd:mainUuid, VidWorker:childUuid)
 */
Cuuid_t childUuid, mainUuid;

static unsigned char nullblkid[4] = {'\0', '\0', '\0', '\0'};

/** Imported from old stage_util.c . Gives the max fseq that fits in label */
static int maxTapeFseq(
                       labeltype
                       )
     char *labeltype;
{
	if (labeltype == NULL) return(-1);
  
	if ((strcmp(labeltype,"al") == 0) ||  /* Ansi Label */
		(strcmp(labeltype,"sl") == 0))    /* Standard Label */
		return(9999);
	if (strcmp(labeltype,"aul") == 0)     /* Ansi (extended) User Label */
		return(INT_MAX / 3);
	if ((strcmp(labeltype,"nl") == 0) ||  /* No Label */
		(strcmp(labeltype,"blp") == 0))   /* Bypass Label Type */
		return(INT_MAX);
  
	return(-1);                           /* Unknown type : not limited */
}

static int validTape(
                     vid,
                     side
                     )
     char *vid;
     int side;
{
  if ( (vid == NULL) || (*vid == '\0') || 
       (strlen(vid) > CA_MAXVIDLEN) || (side < 0) ) return(0);
  return(1);
}

char *rtcpcld_tapeStatusStr(
                            status
                            )
     int status;
{
  static int statusStrKey = -1;
  static char *unknown = "(unknown)";
  char *buf = NULL, *p;
  int rc, i;
  int statusCodes[] = {
    DISABLED,
    EXPORTED,
    TAPE_BUSY,
    TAPE_FULL,
    TAPE_RDONLY,
    ARCHIVED,
    0
  };
  char statusStrs[][16] = {
    "DISABLED|",
    "EXPORTED|",
    "BUSY|",
    "FULL|",
    "RDONLY|",
    "ARCHIVED|",
    ""
  };

  rc = Cglobals_get(
                    &statusStrKey,
                    (void **)&buf,
                    strlen("ARCHIVED|DISABLED|EXPORTED|BUSY|FULL|RDONLY")+1
                    );
  if ( (rc == -1) || (buf == NULL) ) return(unknown);

  *buf = '\0';
  for ( i=0; *statusStrs[i] != '\0'; i++ ) {
    if ( (status & statusCodes[i]) == statusCodes[i] ) 
      strcat(buf,statusStrs[i]);
  }

  /* cut off the last "|" */
  i = strlen(buf);
  if ( i>0 ) buf[i-1] = '\0';

  return(buf);
}

int getVmgrErrBuf(
                  vmgrErrBuf
                  )
     char **vmgrErrBuf;
{
  static int nsErrBufKey = -1;
  static int vmgrErrBufKey = -1;
  void *tmpBuf = NULL;
  int rc;
  
  rc = Cglobals_get(
                    &vmgrErrBufKey,
                    &tmpBuf,
                    512
                    );
  if ( rc == -1 ) return(-1);
  rc = vmgr_seterrbuf(tmpBuf,512);
  if ( rc == -1 ) return(-1);
  if ( vmgrErrBuf != NULL ) *vmgrErrBuf = (char *)tmpBuf;
  return(0);
}

/*
 * VMGR interface
 */
int rtcpcld_gettape(
                    tapePool,
                    initialSizeToTransfer,
                    tape
                    )
     char *tapePool;
     tape_list_t **tape;
     u_signed64 initialSizeToTransfer;
{
  int rc, rcGetTape, i, nbRetriesMaxFseq = 1;  
  char model[CA_MAXMODELLEN+1];
  int maxFseq, startFseq, save_serrno;
  char *vmgrErrMsg = NULL;
  u_signed64 estimatedFreeSpace;
  rtcpTapeRequest_t *tapereq;
  file_list_t *fl;

  if ( (tapePool == NULL) || (tape == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }

  rc = rtcp_NewTapeList(tape,NULL,WRITE_ENABLE);
  if ( rc == -1 ) {
    save_serrno = serrno;
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
    serrno = save_serrno;
    return(-1);
  }
  /*
   * Create a file request to hold the start fseq
   */
  fl = NULL;
  rc = rtcp_NewFileList(tape,&fl,WRITE_ENABLE);
  if ( rc == -1 ) {
    save_serrno = serrno;
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "rtcp_NewFileList()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }
  tapereq = &((*tape)->tapereq);
  (void)getVmgrErrBuf(&vmgrErrMsg);
  for ( i=0; i<nbRetriesMaxFseq; i++ ) {
    serrno = 0;
    if ( vmgrErrMsg != NULL ) *vmgrErrMsg = '\0';
    *tapereq->vid = '\0';
    *tapereq->dgn = '\0';
    *tapereq->density = '\0';
    *model = '\0';
    *tapereq->label = '\0';
    *tapereq->vsn = '\0';
    tapereq->side = -1;
    rcGetTape = vmgr_gettape(
                             tapePool,
                             initialSizeToTransfer,
                             NULL,
                             tapereq->vid,
                             tapereq->vsn,
                             tapereq->dgn,
                             tapereq->density,
                             tapereq->label,
                             model,
                             &(tapereq->side),
                             &startFseq,
                             &estimatedFreeSpace
                             );
    if ( rcGetTape != 0 ) {
      save_serrno = serrno;
      switch (save_serrno) {
      case ENOSPC:
        /*
         * No space left in tape pool. Cannot retry here because we
         * are not in a multi-thread/process context. The rtcpclientd,
         * which is our caller, is single threaded while selecting
         * tapes so we cannot allow to block
         */
      case ENOENT:
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_VMGRFATAL),
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+1,
                        "POOLNAME",
                        DLF_MSG_PARAM_STR,
                        tapePool,
                        RTCPCLD_LOG_WHERE
                        );
        break;
      case SENOSHOST:
      case SENOSSERV:
      case SECOMERR:
      case SESYSERR:
      case EVMGRNACT:
        LOG_SYSCALL_ERR("vmgr_gettape()");
        break;
      case ESEC_NO_CONTEXT:
      case ESEC_CTX_NOT_INITIALIZED:
        LOG_SECURITY_ERR("vmgr_gettape()");
      break;
      case EFAULT:
      case EACCES:
      case EINVAL:
      default:
        /*
         * Should never happen. We log VID as a string here since EFAULT may
         * imply that it's a NULL
         */
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+2,
                        "POOLNAME",
                        DLF_MSG_PARAM_STR,
                        tapePool,
                        "VIDPARAM",
                        DLF_MSG_PARAM_STR,
                        (tapereq->vid == NULL ? "(null)" : tapereq->vid),
                        RTCPCLD_LOG_WHERE
                        );
        break;
      }
      /*
       * Never retry VMGR errors here. Instead we should put the Stream
       * in PENDING or WAITSPACE (if no more tapes) and wait for it
       * to be selected again with streamsToDo()
       */
      break;
    } else {
      /*
       * vmgr_gettape() was successful. Check that the returned start fseq is OK.
       */
      maxFseq = maxTapeFseq(tapereq->label);
      if ( (maxFseq > 0) && (startFseq > maxFseq) ) {
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_MAXFSEQ),
                        (struct Cns_fileid *)NULL,
                        6,
                        "TPPOOL",
                        DLF_MSG_PARAM_STR,
                        tapePool,
                        "",
                        DLF_MSG_PARAM_TPVID,
                        tapereq->vid,
                        "SIDE",
                        DLF_MSG_PARAM_INT,
                        tapereq->side,
                        "LBLTYPE",
                        DLF_MSG_PARAM_STR,
                        tapereq->label,
                        "",
                        DLF_MSG_PARAM_INT,
                        startFseq,
                        "",
                        DLF_MSG_PARAM_INT,
                        maxFseq
                        );
        rc = vmgr_updatetape(
                             tapereq->vid,
                             tapereq->side,
                             (u_signed64) 0,
                             0,
                             0,
                             TAPE_RDONLY
                             );
        if ( rc == -1 ) {
          (void)dlf_write(
                          (inChild == 0 ? mainUuid : childUuid),
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                          (struct Cns_fileid *)NULL,
                          RTCPCLD_NB_PARAMS+3,
                          "SYSCALL",
                          DLF_MSG_PARAM_STR,
                          "vmgr_updatetape()",
                          "ERROR_STR",
                          DLF_MSG_PARAM_STR,
                          sstrerror(serrno),
                          "VMGRERR",
                          DLF_MSG_PARAM_STR,
                          (vmgrErrMsg != NULL ? vmgrErrMsg : "(null)"),
                          RTCPCLD_LOG_WHERE
                          );
        }
        /*
         * OK, allow for one retry
         */
        rcGetTape = -1;
        save_serrno = SEWOULDBLOCK;
        continue;
      }
    }
    break;
  }
  
  if ( rcGetTape == -1 ) {
    serrno = save_serrno;
    return(-1);
  }
  (void)dlf_write(
                  (inChild == 0 ? mainUuid : childUuid),
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_GOTTAPE),
                  (struct Cns_fileid *)NULL,
                  RTCPCLD_NB_PARAMS+4,
                  "",
                  DLF_MSG_PARAM_TPVID,
                  tapereq->vid,
                  "DGN",
                  DLF_MSG_PARAM_STR,
                  tapereq->dgn,
                  "SIDE",
                  DLF_MSG_PARAM_INT,
                  tapereq->side,
                  "STARTFSEQ",
                  DLF_MSG_PARAM_INT,
                  startFseq
                  );

  fl->filereq.tape_fseq = startFseq;
  return(0);
}

int tapeStatus(
               tape,
               freeSpace,
               status
               )
     tape_list_t *tape;
     u_signed64 *freeSpace;
     int *status;
{
  int rc, save_serrno, nbRetries = 0;
  int retryTime = RTCPCLD_GETTAPE_RETRY_TIME;
  struct vmgr_tape_info vmgrTapeInfo;
  char *vmgrErrMsg = NULL, *statusStr = NULL;
  rtcpTapeRequest_t *tapereq;

  if ( tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  tapereq = &(tape->tapereq);

  (void)getVmgrErrBuf(&vmgrErrMsg);
  for (;;) {
    if ( vmgrErrMsg != NULL ) *vmgrErrMsg = '\0';
    serrno = 0;
    rc = vmgr_querytape(tapereq->vid,tapereq->side,&vmgrTapeInfo,tapereq->dgn);
    save_serrno = serrno;
    statusStr = rtcpcld_tapeStatusStr(vmgrTapeInfo.status);
    if ( status != NULL ) *status = vmgrTapeInfo.status;
    if ( freeSpace != NULL ) {
      *freeSpace = 1024*((u_signed64)vmgrTapeInfo.estimated_free_space);
    }
    if ( rc == 0 ) {
      if ( ((vmgrTapeInfo.status & DISABLED) == DISABLED) ||
           ((vmgrTapeInfo.status & EXPORTED) == EXPORTED) ||
           ((vmgrTapeInfo.status & ARCHIVED) == ARCHIVED) ) {
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_TAPEUNAVAILABLE),
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+3,
                        "",
                        DLF_MSG_PARAM_TPVID,
                        tapereq->vid,
                        "DGN",
                        DLF_MSG_PARAM_STR,
                        tapereq->dgn,
                        "STATUS",
                        DLF_MSG_PARAM_STR,
                        statusStr,
                        RTCPCLD_LOG_WHERE
                        );
        if ( (vmgrTapeInfo.status & DISABLED) == DISABLED ) {
          serrno = ETHELD;
        } else if ( (vmgrTapeInfo.status & EXPORTED) == EXPORTED ) {
          serrno = ETABSENT;
        } else if ( (vmgrTapeInfo.status & ARCHIVED) == ARCHIVED ) {
          serrno = ETARCH;
        }
        return(-1);
      }
      strcpy(tapereq->vsn,vmgrTapeInfo.vsn);
      strcpy(tapereq->label,vmgrTapeInfo.lbltype);
      strcpy(tapereq->density,vmgrTapeInfo.density);
      break;
    }
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_MAXRETRY),
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+5,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "vmgr_querytape()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    "VMGRERR",
                    DLF_MSG_PARAM_STR,
                    (vmgrErrMsg != NULL ? vmgrErrMsg : "(null)"),
                    "NBRETRIES",
                    DLF_MSG_PARAM_INT,
                    nbRetries++,
                    "RETRYTIME",
                    DLF_MSG_PARAM_INT,
                    retryTime,
                    RTCPCLD_LOG_WHERE
                    );
    sleep(retryTime);
  }
  return(0);
}

int rtcpcld_tapeOK(
                   tape
                   )
     tape_list_t *tape;
{
  return(tapeStatus(tape,NULL,NULL));
}

int rtcpcld_updateTape(
                       tape,
                       file,
                       endOfRequest,
                       rtcpc_serrno
                       )
     tape_list_t *tape;
     file_list_t *file;
     int endOfRequest, rtcpc_serrno;
{
  rtcpTapeRequest_t *tapereq;
  rtcpFileRequest_t *filereq = NULL;
  int rc, save_serrno, maxFseq;
  u_signed64 bytesWritten = 0, freeSpace = 0;
  int compressionFactor = 0, filesWritten = 0, flags = 0;
  char *vmgrErrMsg = NULL, *statusStr = NULL;
  struct Cns_fileid *fileId = NULL;

  if ( (tape == NULL) || 
       (validTape(tape->tapereq.vid, tape->tapereq.side) == 0) ) {
    serrno = EINVAL;
    return(-1);
  }
  if ( file != NULL ) filereq = &(file->filereq);
  
  tapereq = &(tape->tapereq);
  /**
   * \internal
   * No vmgr update needed when reading from tape
   */
  if ( tapereq->mode == WRITE_DISABLE ) return(0);

  /**
   * Make sure to maintain BUSY state if this is not end of request
   */
  if ( endOfRequest == 0 ) {
    flags = TAPE_BUSY;
  } else {
    /*
     * Make sure we don't override some important status already set
     */
    rc = tapeStatus(tape,&freeSpace,&flags);
    if ( rc == -1 ) {
      LOG_SYSCALL_ERR("rtcpcld_tapeStatus()");
      return(-1);
    }
    /*
     * If tape not busy, do nothing
     */
    if ( (flags & TAPE_BUSY) == 0 ) return(0);
    /*
     * We're finished with this tape (since endOfRequest != 0).
     * Always reset the BUSY flag.
     */
    flags = flags & ~TAPE_BUSY;
    if ( rtcpc_serrno > 0 ) {
      /*
       * Request finished. rtcpc_serrno should only be set if there was an error
       */
      switch (rtcpc_serrno) {
      case ETWLBL:                          /* Wrong label type */
      case ETWVSN:                          /* Wrong vsn */
      case ETHELD:                          /* Volume held (TMS) */
      case ETVUNKN:                         /* Volume unknown or absent (TMS) */
      case ETOPAB:                          /* Operator cancel */
      case ETNOSNS:                         /* No sense */
        flags = DISABLED;
        break;
      case ETABSENT:                        /* Volume absent (TMS) */
        flags = EXPORTED;
        break;
      case ETNXPD:                          /* File not expired */
      case ETWPROT:                         /* Cartridge physically write protected or tape read-only (TMS) */
        flags = TAPE_RDONLY;
        break;
      case ETARCH:                          /* Volume in inactive library */
        flags = ARCHIVED;
        break;
      default:
        break;
      }
    }
  }

  if ( filereq != NULL ) {
    (void)rtcpcld_getFileId(file,&fileId); /* Only for logging */
    /*
     * Exceeded number of tape fseq allowed by the label ?
     */
    maxFseq = maxTapeFseq(tapereq->label);
    if ( (maxFseq > 0) && (filereq->tape_fseq >= maxFseq) ) {
      flags = TAPE_RDONLY;
    } else {
      /*
       * Update the VMGR for this file if we are sure we're going to
       * add the segment in the name server or if the tape is full. If
       * the tape is full we are not going to write the segment 
       * (feature request 5179) to the name server but we should still
       * update VMGR with the correct space left on tape and flag it FULL.
       */
      if ( (tapereq->tprc == 0) &&
           (((filereq->cprc == 0) && (filereq->proc_status == RTCP_FINISHED)) ||
            ((filereq->cprc < 0) && 
             ((filereq->err.severity & RTCP_FAILED) == RTCP_FAILED) &&
             (filereq->err.errorcode == ENOSPC))) ) {
        /*
         * Tape full. Update VMGR with the real space left and flag the tape FULL
         */
        if ( (filereq->cprc != 0) &&
             ((filereq->err.severity & RTCP_FAILED) == RTCP_FAILED) &&
             (filereq->err.errorcode == ENOSPC) ) {
          flags = TAPE_FULL;
          if ( freeSpace == 0 ) {
            rc = tapeStatus(tape,&freeSpace,&flags);
            if ( rc == -1 ) {
              LOG_SYSCALL_ERR("rtcpcld_tapeStatus()");
              return(-1);
            }
          }
          /*
           * Try to set the real free space
           * Note that vmgr_updatetape() gives the increment, not the
           * absolute value.
           */
          if ( freeSpace > filereq->bytes_in) {
            bytesWritten = freeSpace - filereq->bytes_in;
          }
          filesWritten = 0;
          if ( (filereq->bytes_out > 0) && (filereq->host_bytes>0) ) {
            compressionFactor = (filereq->host_bytes * 100) / filereq->bytes_out;
          } else {
            compressionFactor = 100;
          }
          {
            char u64buf[32];
            rtcp_log(LOG_DEBUG,
                     "updateTape() on ENOSPC: bytes_in=%s, bytes_out=%s, host_bytes=%s, freeSpace=%s, bytesWritten=%s\n",
                     u64tostr(filereq->bytes_in,u64buf,-sizeof(u64buf)),
                     u64tostr(filereq->bytes_out,u64buf,-sizeof(u64buf)),
                     u64tostr(filereq->host_bytes,u64buf,-sizeof(u64buf)),
                     u64tostr(freeSpace,u64buf,-sizeof(u64buf)),
                     u64tostr(bytesWritten,u64buf,-sizeof(u64buf))
                     );
          }
        } else {
          if ( (filereq->cprc == 0) || (filereq->host_bytes > 0) ) {
            bytesWritten = filereq->bytes_in;
            if ( (filereq->bytes_out > 0) ) {
              compressionFactor = (filereq->host_bytes * 100) / filereq->bytes_out;
            } else {
              compressionFactor = 100;
            }
            filesWritten = 1;
          }
        }
      } else if ( (filereq->cprc != 0) || (tapereq->tprc != 0) ) {
        /**
         * \internal
         * Parity (recoverable and unrecoverable) and label errors may appear in
         * - tapereq if error happened while mounting the tape
         * - filereq if error happened while positioning to the file or
         *   reading/writing the file.
         *
         * In this case we set the volume readonly to avoid further problems.
         */
        if ( (filereq->err.errorcode == ETPARIT) || 
             (filereq->err.errorcode == ETUNREC) || 
             (filereq->err.errorcode == ETLBL) || 
             (tapereq->err.errorcode == ETPARIT) || 
             (tapereq->err.errorcode == ETUNREC) || 
             (tapereq->err.errorcode == ETLBL) ) {
          flags = TAPE_RDONLY; 
        }
      }
    }
  }

  statusStr = rtcpcld_tapeStatusStr(flags);

  (void)getVmgrErrBuf(&vmgrErrMsg);
  rc = vmgr_updatetape(
                       tapereq->vid,
                       tapereq->side,
                       bytesWritten,
                       compressionFactor,
                       filesWritten,
                       flags
                       );
  if ( rc == -1 ) {
    save_serrno = serrno;
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                    (struct Cns_fileid *)fileId,
                    RTCPCLD_NB_PARAMS+4,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "vmgr_updatetape()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    "VMGRERR",
                    DLF_MSG_PARAM_STR,
                    (vmgrErrMsg != NULL ? vmgrErrMsg : "(null)"),
                    "STATUS",
                    DLF_MSG_PARAM_STR,
                    statusStr,
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  } else {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_UPDATETAPE),
                    (struct Cns_fileid *)fileId,
                    5,
                    "",
                    DLF_MSG_PARAM_TPVID,
                    tapereq->vid,
                    "BYTESWRT",
                    DLF_MSG_PARAM_INT64,
                    bytesWritten,
                    "COMPRFAC",
                    DLF_MSG_PARAM_INT,
                    compressionFactor,
                    "NBFILES",
                    DLF_MSG_PARAM_INT,
                    filesWritten,
                    "STATUS",
                    DLF_MSG_PARAM_STR,
                    statusStr
                    );
  }
  
  return(0);
}

int rtcpcld_segmentOK(
                      segment
                      ) 
     struct Cns_segattrs *segment;
{
  int rc;
  tape_list_t tape;

  if ( (segment == NULL) || (segment->vid[0] == '\0') ) {
    serrno = EINVAL;
    return(-1);
  }

  if ( segment->s_status != '-' ) {
    /* 
     * only 'deleted' status possible
     */
    serrno = ENOENT;
    return(-1);
  }

  memset(&tape,'\0',sizeof(tape));
  strcpy(tape.tapereq.vid,segment->vid);
  tape.tapereq.side = segment->side;
  rc = rtcpcld_tapeOK(&tape);
  if ( rc == -1 ) return(-1);

  return(0);
}
