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
 * @(#)$RCSfile: rtcpcldVmgrInterface.c,v $ $Revision: 1.2 $ $Release$ $Date: 2004/10/11 15:53:12 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldVmgrInterface.c,v $ $Revision: 1.2 $ $Release$ $Date: 2004/10/11 15:53:12 $ Olof Barring";
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

/** Global for switching off logging when called from rtcpcldapi.c
 */
int dontLog = 0;
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
  int rc, rcGetTape, i, nbRetries, maxNbRetries = RTCPCLD_GETTAPE_RETRIES;
  int retryTime, retryTimeENOSPC;
  int maxRetryTime = RTCPCLD_GETTAPE_RETRY_TIME;
  int maxRetryTimeENOSPC = RTCPCLD_GETTAPE_ENOSPC_RETRY_TIME;
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
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
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
    }
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
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
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
    }
    serrno = save_serrno;
    return(-1);
  }
  tapereq = &((*tape)->tapereq);
  (void)getVmgrErrBuf(&vmgrErrMsg);
  for ( i=0; i<maxNbRetries; i++ ) {
    serrno = 0;
    rtcp_log(LOG_DEBUG,"rtcpcld_gettape() calling vmgr_gettape(%s,%d,...)\n",
             tapePool,initialSizeToTransfer);
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
      if ( save_serrno != ENOSPC ) {
        retryTimeENOSPC = 0;
        retryTime = maxRetryTime;
      } else {
        /*
         * No space left in tape pool. Loop forever until somebody adds
         * some more tapes.
         */
        if ( i+1 >= maxNbRetries ) maxNbRetries++;
        /* Progressive retry time, start with 5 minutes */
        if ( retryTimeENOSPC <= 0 ) {
          retryTimeENOSPC = 5*60;
        } else if ( retryTimeENOSPC < maxRetryTimeENOSPC ) {
          retryTimeENOSPC *= 2;
        }
        /* Up to a maximum set by maxRetryTimeENOSPC */
        if ( retryTimeENOSPC > maxRetryTimeENOSPC ) {
          retryTimeENOSPC = maxRetryTimeENOSPC;
        }
        retryTime = retryTimeENOSPC;
      }
      
      if ( dontLog == 0 ) {
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_SYSCALL,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+6,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "vmgr_gettape()",
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(save_serrno),
                        "VMGRERR",
                        DLF_MSG_PARAM_STR,
                        (vmgrErrMsg != NULL ? vmgrErrMsg : "(null)"),
                        "NBRETRIES",
                        DLF_MSG_PARAM_INT,
                        nbRetries,
                        "MAXRETRIES",
                        DLF_MSG_PARAM_INT,
                        maxNbRetries,
                        "RETRYTIME",
                        DLF_MSG_PARAM_INT,
                        retryTime,
                        RTCPCLD_LOG_WHERE
                        );
      }
      sleep(retryTime);
    } else {
      /*
       * vmgr_gettape() was successful. Check that the returned start fseq is OK.
       */
      maxFseq = maxTapeFseq(tapereq->label);
      if ( (maxFseq > 0) && (startFseq > maxFseq) ) {
        if ( dontLog == 0 ) {
          (void)dlf_write(
                          (inChild == 0 ? mainUuid : childUuid),
                          DLF_LVL_ERROR,
                          RTCPCLD_MSG_MAXFSEQ,
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
        }
        rc = vmgr_updatetape(
                             tapereq->vid,
                             tapereq->side,
                             (u_signed64) 0,
                             0,
                             0,
                             TAPE_RDONLY
                             );
        if ( rc == -1 ) {
          if ( dontLog == 0 ) {
            (void)dlf_write(
                            (inChild == 0 ? mainUuid : childUuid),
                            DLF_LVL_ERROR,
                            RTCPCLD_MSG_SYSCALL,
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
        }
        sleep(2);
      } else {
        /*
         * Everything is fine. We got a VID to work with
         */
        break;
      }
    }
  }
  if ( rcGetTape == -1 ) {
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_MAXRETRY,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+3,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "vmgr_getape()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      "VMGRERR",
                      DLF_MSG_PARAM_STR,
                      (vmgrErrMsg != NULL ? vmgrErrMsg : "(null)"),
                      RTCPCLD_LOG_WHERE
                      );
    }
    serrno = save_serrno;
    return(-1);
  }
  if ( dontLog == 0 ) {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    DLF_LVL_SYSTEM,
                    RTCPCLD_MSG_GOTTAPE,
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
  }

  fl->filereq.tape_fseq = startFseq;
  return(0);
}

int rtcpcld_tapeOK(
                   tape
                   )
     tape_list_t *tape;
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
  (void)rtcpcld_getVmgrErrBuf(&vmgrErrMsg);
  for (;;) {
    if ( vmgrErrMsg != NULL ) *vmgrErrMsg = '\0';
    serrno = 0;
    rc = vmgr_querytape(tapereq->vid,tapereq->side,&vmgrTapeInfo,tapereq->dgn);
    save_serrno = serrno;
    statusStr = rtcpcld_tapeStatusStr(vmgrTapeInfo.status);
    if ( rc == 0 ) {
      if ( ((vmgrTapeInfo.status & DISABLED) == DISABLED) ||
           ((vmgrTapeInfo.status & EXPORTED) == EXPORTED) ||
           ((vmgrTapeInfo.status & ARCHIVED) == ARCHIVED) ) {
        (void)dlf_write(
                        (inChild == 0 ? mainUuid : childUuid),
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_TAPEUNAVAILABLE,
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
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_MAXRETRY,
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
    }
    sleep(retryTime);
  }
  return(0);
}

int rtcpcld_updateTape(
                       tapereq,
                       filereq,
                       endOfRequest,
                       rtcpc_serrno
                       )
     rtcpTapeRequest_t *tapereq;
     rtcpFileRequest_t *filereq;
     int endOfRequest, rtcpc_serrno;
{
  int rc, save_serrno, maxFseq;
  u_signed64 bytesWritten = 0;
  int compressionFactor = 0, filesWritten = 0, flags = 0;
  char *vmgrErrMsg = NULL, *statusStr = NULL;
  struct Cns_fileid *fileId = NULL;

  if ( (tapereq == NULL) || (validTape(tapereq->vid, tapereq->side) == 0) ) {
    serrno = EINVAL;
    return(-1);
  }
  
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
  } else if ( rtcpc_serrno > 0 ) {
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

  if ( filereq != NULL ) {
    (void)rtcpcld_getFileId(filereq,&fileId); /* Only for logging */
    /*
     * Tape full?
     */
    if ( (filereq->cprc != 0) &&
         ((filereq->err.severity & RTCP_FAILED) == RTCP_FAILED) &&
         (filereq->err.errorcode == ENOSPC) ) {
      flags = TAPE_FULL;
    }

    /*
     * Exceeded number of tape fseq allowed by the label ?
     */
    maxFseq = maxTapeFseq(tapereq->label);
    if ( (maxFseq > 0) && (filereq->tape_fseq >= maxFseq) ) {
      flags = TAPE_RDONLY;
    } else {
      /**
       * \internal
       * Here follows some comments explaining from the old stager_castor.c:
       *
       * Note: by definition, here, bytes_in is > 0
       * Please see [Bug #1723] of CASTOR. Here is a resume:
       *
       *  After checking with Olof;
       *
       *  It is confirmed that if
       *  - no header label is writen, bytes_in > 0 (original size of disk file)
       *    and host_bytes == 0
       *  - header label ok but no data writen, bytes_in == 0 and host_bytes > 0
       *  - header label ok and xxx (uncompressed way of speaking) bytes of data,
       *    bytes_in is xxx, host_bytes is > 0 (should be > xxx btw)
       *
       *  A priori in the first case, bytes_in should be zero (then stager would
       *  have never writen a fake segment in the name server tables), and this will
       *  be probably fixed in a next version of CASTOR (major or minor, don't know).
       *
     *  Conclusion: we will write a segment only if both bytes_in and host_bytes
     *  are > 0, segment (uncompressed) size if then bytes_in.
     */
      if ( (filereq->cprc == 0) || (filereq->host_bytes > 0) ) {
        bytesWritten = filereq->bytes_in;
        if ( (filereq->bytes_out > 0) ) {
          compressionFactor = (filereq->host_bytes * 100) / filereq->bytes_out;
        }
        filesWritten = 1;
      }
    }
    
    if ( (endOfRequest == 1) && 
         ((filereq->cprc != 0) || (tapereq->tprc != 0)) ) {
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

  statusStr = rtcpcld_tapeStatusStr(flags);

  (void)rtcpcld_getVmgrErrBuf(&vmgrErrMsg);
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
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
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
    }
    serrno = save_serrno;
    return(-1);
  } else {
    if ( dontLog == 0 ) {
      (void)dlf_write(
                      (inChild == 0 ? mainUuid : childUuid),
                      DLF_LVL_SYSTEM,
                      RTCPCLD_MSG_UPDATETAPE,
                      (struct Cns_fileid *)fileId,
                      4,
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
