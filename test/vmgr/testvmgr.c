/******************************************************************************
 *                      testvmgr.c
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
 * @(#)$RCSfile: testvmgr.c,v $ $Revision: 1.1 $ $Release$ $Date: 2007/08/24 11:43:35 $ $Author: obarring $
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
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
WSADATA wsadata;
#else /* _WIN32 */
#include <unistd.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <wait.h>
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
#include <Cgetopt.h>
#include <Cinit.h>
#include <Cthread_api.h>
#include <Cuuid.h>
#include <Cgrp.h>
#include <Cpwd.h>
#include <Cnetdb.h>
#include <u64subr.h>
#include <dlf_api.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <castor/Constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <Cglobals.h>
#include <serrno.h>
#include <Cns_api.h>
#include <vmgr_api.h>
#include <castor/Constants.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>

int tapeStatus(tape_list_t *, u_signed64 *, int *, int *);

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
  char *buf = NULL;
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
    return(-1);
  }
  /*
   * Create a file request to hold the start fseq
   */
  fl = NULL;
  rc = rtcp_NewFileList(tape,&fl,WRITE_ENABLE);
  if ( rc == -1 ) {
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
      case EFAULT:
      case EACCES:
      case EINVAL:
      case ENOENT:
        fprintf(stderr,"gettape() fatal error: %d\n",save_serrno);
        break;
      case SENOSHOST:
      case SENOSSERV:
      case SECOMERR:
      case SESYSERR:
      case EVMGRNACT:
        fprintf(stderr,"vmgr_gettape() error %d",save_serrno);
        break;
      case ESEC_NO_CONTEXT:
      case ESEC_CTX_NOT_INITIALIZED:
        break;
      default:
        /*
         * Should never happen. We log VID as a string here since EFAULT may
         * imply that it's a NULL
         */
        fprintf(stderr,"gettape() 'should never happen' error: %d\n",save_serrno);
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
        fprintf(stderr,"gettape() max fseq exceeded: %d %d\n",startFseq,maxFseq);
        rc = vmgr_updatetape(
                             tapereq->vid,
                             tapereq->side,
                             (u_signed64) 0,
                             0,
                             0,
                             TAPE_RDONLY
                             );
        if ( rc == -1 ) {
          fprintf(stderr,"vmgr_upatetape(%s,TAPE_RDONLY): %d\n",
                  tapereq->vid,serrno);
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

  fl->filereq.tape_fseq = startFseq;
  return(0);
}

int rtcpcld_updateTape(
                       tape,
                       file,
                       endOfRequest,
                       rtcpc_serrno,
                       _filesWritten
                       )
     tape_list_t *tape;
     file_list_t *file;
     int endOfRequest, rtcpc_serrno;
     int *_filesWritten;
{
  rtcpTapeRequest_t *tapereq;
  rtcpFileRequest_t *filereq = NULL;
  int rc,  maxFseq;
  u_signed64 bytesWritten = 0, freeSpace = 0;
  int compressionFactor = 0, filesWritten = 0, flags = 0, nbFiles;
  char *vmgrErrMsg = NULL, *statusStr = NULL;

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

  /*
   * Make sure we don't override some important status already set
   */
  rc = tapeStatus(tape,&freeSpace,&flags,&nbFiles);
  if ( rc == -1 ) {
    return(-1);
  }

  if ( (_filesWritten != NULL) && (*_filesWritten != nbFiles+1) ) {
    fprintf(stderr,"wrong file count %s: migrator count %d, VMGR count %d\n",
            tape->tapereq.vid,*_filesWritten,nbFiles);
  }

  /**
   * Make sure to maintain BUSY state if this is not end of request
   *and the tape is not full
   */
  if ( endOfRequest == 0 ) {
    if ( flags != TAPE_FULL) flags = TAPE_BUSY;
  } else {
    /*
     * End of request. This is rtcpclientd calling us after a migrator exit.
     * If tape not busy, do nothing
     */
    if ( (flags & TAPE_BUSY) == 0 ) return(0);
    /*
     * We're finished with this tape (since endOfRequest != 0).
     * Always reset the BUSY flag.
     */
    flags = flags & ~TAPE_BUSY;
  }
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
    case ENOSPC:
      if ( flags == TAPE_FULL ) return(0); /* Tape already flagged full */
      break;
    default:
      break;
    }
  }

  if ( filereq != NULL ) {
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
          if ( freeSpace == 0 ) {
            rc = tapeStatus(tape,&freeSpace,&flags,NULL);
            if ( rc == -1 ) {
              return(-1);
            }
          }
          if ( (flags & (TAPE_FULL|DISABLED|EXPORTED|TAPE_RDONLY|ARCHIVED)) == 0 ) {
            flags = TAPE_FULL;
          }
          /**
           * Set the real free space in bytesWritten when a tape is FULL.
           */
          bytesWritten = filereq->bytes_out;
          filesWritten = 0;
          compressionFactor = 100;
        } else {
          if ( (filereq->cprc == 0) || (filereq->host_bytes > 0) ) {
            bytesWritten = filereq->bytes_in;
            if ( (filereq->bytes_out > 0) ) {
              compressionFactor = (filereq->host_bytes * 100) / filereq->bytes_out;
              if ( (compressionFactor < 95) && (strcmp(tapereq->devtype,"3592") == 0) )
                compressionFactor = 100;
            } else if ( (filereq->bytes_out == 0) && (strcmp(tapereq->devtype,"3592") == 0) ) {
              compressionFactor = 100;
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
    return(-1);
  } else {
    if ( _filesWritten != NULL ) {
      *_filesWritten = nbFiles+1;
    }
  }
  return(0);
}

int tapeStatus(
               tape,
               freeSpace,
               status,
               nbVmgrFiles
               )
     tape_list_t *tape;
     u_signed64 *freeSpace;
     int *status;
     int *nbVmgrFiles;
{
  int rc, save_serrno;
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
        fprintf(stderr,"Tape %s unavailable, status=%d (%s)\n",tapereq->vid,
                vmgrTapeInfo.status,statusStr);
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
    /*
     * We are here because of an error
     */
    fprintf(stderr,"vmgr_querytape() error %d\n",save_serrno);
    if ( (save_serrno == SECOMERR) || (save_serrno == EVMGRNACT) ) {
      sleep(5);
    } else {
      serrno = save_serrno;
      return(-1);
    }
  }
  if ( nbVmgrFiles != NULL ) *nbVmgrFiles = vmgrTapeInfo.nbfiles;
  return(0);
}

int rtcpcld_tapeOK(
                   tape
                   )
     tape_list_t *tape;
{
  return(tapeStatus(tape,NULL,NULL,NULL));
}


int main(int argc, char *argv[]) 
{
  char *tapePoolName;
  u_signed64 sizeToTransfer;
  tape_list_t *tl = NULL;
  int nbLoops, nbFilesPerLoop, i, j, rc, filesWritten;

  if ( argc < 4 ) {
    fprintf(stderr,"usage: %s tapePoolName nbLoops nbFilesPerLoop\n",argv[0]);
    return(1);
  }
  
  tapePoolName = strdup(argv[1]);
  nbLoops = atoi(argv[2]);
  nbFilesPerLoop = atoi(argv[3]);  
  
  for (i=0; i<nbLoops; i++){
    if ( (tl != NULL) && (tl->file != NULL) ) free(tl->file);
    if ( tl != NULL ) free(tl);
    sizeToTransfer = 10*((u_signed64)1024*1024*1024);
    rc = rtcpcld_gettape(tapePoolName,sizeToTransfer,&tl);
    if ( rc == -1 ) {
      fprintf(stderr,"gettape(): loop %d, rc = %d, errno=%d, serrno=%d\n",
              i,rc,errno,serrno);
      continue;
    }

    printf("loop %d, got vid %s from %s, start_fseq=%d\n",i,tl->tapereq.vid,
           tapePoolName,tl->file->filereq.tape_fseq);
    for ( j=0; j<nbFilesPerLoop; j++ ) {
      filesWritten = tl->file->filereq.tape_fseq;
      tl->file->filereq.cprc = 0;
      tl->file->filereq.proc_status = RTCP_FINISHED;
      tl->file->filereq.bytes_out = (u_signed64)10*1024*1024;
      tl->file->filereq.bytes_in = (u_signed64)10*1024*1024;
      tl->file->filereq.host_bytes = (u_signed64)10*1024*1024;
      rc = rtcpcld_updateTape(
                              tl,
                              tl->file,
                              0,
                              0,
                              &filesWritten
                              );
      if ( rc == -1 ) {
        fprintf(stderr,"updatetape(%s): loop %d file %d rc=%d, filesWritte=%d, errno=%d, serrno=%d\n",
                tl->tapereq.vid,i,j,rc,filesWritten,errno,serrno);
        continue;
      }
      if ( tl->file->filereq.tape_fseq != filesWritten ) {
        fprintf(stderr,"Loop %d, %s file %d wrong file count: migrator %d, VMGR %d\n",
                i,tl->tapereq.vid,j,tl->file->filereq.tape_fseq,filesWritten);
        exit(2);
      }
      tl->file->filereq.tape_fseq++;
    }
    filesWritten = tl->file->filereq.tape_fseq;
    rc = rtcpcld_updateTape(
                            tl,
                            NULL,
                            0,
                            0,
                            &filesWritten
                            );
    if ( rc == -1 ) {
      fprintf(stderr,"end of loop %d, updatetape(%s): rc=%d, errno=%d, serrno=%d\n",
              i,tl->tapereq.vid,rc,errno,serrno);
    }
    if ( tl->file->filereq.tape_fseq != filesWritten ) {
      fprintf(stderr,"end loop %d, %s wrong file count: migrator %d, VMGR %d\n",
              i,tl->tapereq.vid,tl->file->filereq.tape_fseq,filesWritten);
      exit(2);
    }
    printf("end of loop %d: vid %s, migrator file count %d, VMGR file count %d\n",
           i,tl->tapereq.vid,tl->file->filereq.tape_fseq,filesWritten);
    
    rc = rtcpcld_updateTape(
                            tl,
                            NULL,
                            1,
                            0,
                            NULL
                            );
    if ( rc == -1 ) {
      fprintf(stderr,"end of loop %d, updatetape(%s): rc=%d, errno=%d, serrno=%d\n",
              i,tl->tapereq.vid,rc,errno,serrno);
    } else {
      printf("end of loop %d free %s\n",i,tl->tapereq.vid);
    }
  }
  printf("end of %d loops program exit\n",i);
  return(0);
}
