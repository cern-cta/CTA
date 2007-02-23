/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpapi.c - RTCOPY client API library
 */

#include <stdlib.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <direct.h>
#include <io.h>
#include <fcntl.h>
#define pipe(x) _pipe(x, 512, O_BINARY)
#else  /* _WIN32 */
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#endif /* _WIN32 */
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <Cmutex.h>
#include <Cnetdb.h>
#include <Ctape_api.h>
#define RFIO_KERNEL 1
#include <rfio_api.h>
#include <log.h>
#include <osdep.h>
#if !defined(VDQM)
#include <marshall.h>
#endif /* VDQM */
#include <net.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <rtcp_server.h>
#include <vdqm_api.h>
#include <common.h>
#include <ctype.h>

typedef struct rtcpcThrData 
{
  rtcpHdr_t hdr;
  tape_list_t *tape;
  rtcpTapeRequest_t tapereq;
  rtcpFileRequest_t filereq;
  SOCKET s;
  SOCKET pipe;
  int index;
  int reqID;
  int dumptape;
  int finished;
  int status;
  int th_serrno;
  struct rtcpcThrData *next;
  struct rtcpcThrData *prev;
} rtcpcThrData_t;
static rtcpcThrData_t *activeThreads = NULL;

#if defined(_WIN32)
extern uid_t getuid();
extern gid_t getgid();
#endif /* _WIN32 */

#include <serrno.h>

#define RFIO_NETOPT 	2
#define RFIO_NONET	1

extern int rtcpc_InitNW _PROTO((SOCKET **, int *));
extern int rtcp_CleanUp _PROTO((SOCKET **, int));
extern int rtcp_RecvReq _PROTO((SOCKET *, rtcpHdr_t *, rtcpClientInfo_t *, 
                                rtcpTapeRequest_t *, rtcpFileRequest_t *));
extern int rtcp_SendReq _PROTO((SOCKET *, rtcpHdr_t *, rtcpClientInfo_t *, 
                                rtcpTapeRequest_t *, rtcpFileRequest_t *));
extern int rtcp_RecvAckn _PROTO((SOCKET *, int));
extern int rtcp_SendAckn _PROTO((SOCKET *, int));
extern int rtcp_CloseConnection _PROTO((SOCKET *));

extern int rtcp_Listen _PROTO((SOCKET, SOCKET *, int, int));
extern int rtcp_CheckConnect _PROTO((SOCKET *, tape_list_t *));
#if TMS
extern int rtcp_CallTMS _PROTO((tape_list_t *, char *));
#endif
#if VMGR
extern int rtcp_CallVMGR _PROTO((tape_list_t *, char *));
#endif
extern int rtcpc_InitReqStruct _PROTO((
                                       rtcpTapeRequest_t *, 
                                       rtcpFileRequest_t *
                                       ));
extern int rtcp_CheckReqStructures _PROTO((
                                           SOCKET *, 
                                           rtcpClientInfo_t *, 
                                           tape_list_t *
                                           ));
int (*rtcpc_ClientCallback) _PROTO((
                                    rtcpTapeRequest_t *, 
                                    rtcpFileRequest_t *
                                    )) = NULL;
int rtcpc_finished _PROTO((
                           rtcpc_sockets_t **,
                           rtcpHdr_t *,
                           tape_list_t *));

/*
 * Kill mechanism is not thread-safe if several threads run rtcpc() in
 * parallel. It is used by rtcopy server when running old SHIFT clients.
 */
static rtcpc_sockets_t *last_socks = NULL;
static tape_list_t *last_tape = NULL;
static int rtcpc_killed = 0;
static void *lastFileLock = NULL;
static int success = 0;
static int failed = -1;

static int rtcpc_SetKillInfo(rtcpc_sockets_t *socks, tape_list_t *tape) {

  if ( socks == NULL || tape == NULL ) return(-1);
  last_socks = socks;
  last_tape = tape;
  return(0);
}

static int rtcpc_ResetKillInfo() {
  last_socks = NULL;
  last_tape = NULL;
  return(0);
}

static void local_log(int level, char *format, ...) {
  return;
}

static int rtcpc_FixPath(file_list_t *file, int fn_size, int mode) {
  rtcpFileRequest_t *filereq;
  char tmp_path[CA_MAXPATHLEN+1];
  char local_host[CA_MAXHOSTNAMELEN+1];
  char *filename, *dskhost, *path;
#if defined(_WIN32)
  char dir_delim[] = "\\";
#else /* _WIN32 */
  char dir_delim[] = "/";
#endif /* _WIN32 */
#if defined(USE_RFIO)
  int c;
#endif /* USE_RFIO */

  if ( file == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  filereq = &file->filereq;
  filename = filereq->file_path;
  if ( *filename == '\0' || fn_size <= 0 ) {
    serrno = EINVAL;
    return(-1);
  }

  if ( fn_size > CA_MAXPATHLEN+1 ) {
    serrno = SENAMETOOLONG;
    return(-1);
  }

  /* 
   * Filename is a dot, in this case this means that the filename 
   * is not given and has to be allocated by the stager itself 
   * (deferred space allocation). Otherwise we should check that the
   * path is valid.
   * If filename begins with 'localhost' it is assumed to be
   * on a shared filesystem. 
   */
  if ( (strcmp(filename,".") != 0) && 
       (strstr(filename,"localhost") != filename) ) {
    /*
     * File name is local ?
     */
    if ( rfio_parse(filename, &dskhost, &path) == 0 ) {
      gethostname(local_host,CA_MAXHOSTNAMELEN);
      /*
       * Relative path ?
       */
      if ( path[0] != *dir_delim ) {
        getcwd(tmp_path,CA_MAXPATHLEN) ;
        strcat(tmp_path,dir_delim) ;
        if ( (int)(strlen(tmp_path) + strlen(path)) > fn_size ) {
          serrno = SENAMETOOLONG;
          return(-1);
        }
        strcat (tmp_path, path ) ;
      } else
        strcpy( tmp_path, path);

      rtcp_log(LOG_DEBUG, "File name %s changed into: %s:%s\n", 
               filename, local_host, tmp_path ) ;
      strcpy( filename , local_host);
      strcat( filename , ":");
      strcat( filename , tmp_path) ;
    }
  }
  return(0);
}

/*
 * Print the standard tpread/tpwrite messages
 */
int rtcpc_GiveInfo(tape_list_t *tl,
                   file_list_t *fl,
                   int dumptape) {
  int TransferTime,nbKBin,nbKBout,nbKbytes, cond, rc, save_serrno;
  file_list_t *fltmp;
  tape_list_t *tltmp;
  rtcpTapeRequest_t *tapereq = NULL;
  rtcpFileRequest_t *filereq = NULL;
  char *cmd,cmds[][8] = {"CPTPDSK","CPDSKTP","DUMP"};
  u_signed64 nbbytes, nbhost_bytes;
  u_signed64 nbrecs;
  float compress;

  if ( tl == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  tapereq = &tl->tapereq;
  if ( fl != NULL ) filereq = &fl->filereq;

  cmd = cmds[0];
  if ( tapereq->mode == WRITE_ENABLE ) cmd = cmds[1];
  if ( dumptape == TRUE ) cmd = cmds[2];

  /*
   * First check if there was an error
   */
  if ( tapereq->tprc != 0 || *tapereq->err.errmsgtxt != '\0' ||
       (tapereq->err.severity & RTCP_FAILED) != 0 ) {
    if ( *tapereq->err.errmsgtxt != '\0' ) {
      if ( strstr(tapereq->err.errmsgtxt,cmd) != NULL )
        rtcp_log(LOG_ERR,"%s\n",tapereq->err.errmsgtxt);
      else
        rtcp_log(LOG_ERR," %s ! %s\n",cmd,tapereq->err.errmsgtxt);
    } else {
      if ( tapereq->err.errorcode != 0 ) 
        rtcp_log(LOG_ERR," %s ! %s\n",cmd,
                 sstrerror(tapereq->err.errorcode));

    }
  }
  if ( fl == NULL ) return(0);
  if ( filereq->cprc != 0 || *filereq->err.errmsgtxt != '\0' ||
       (filereq->err.severity & RTCP_FAILED) != 0 ) {
    if ( *filereq->err.errmsgtxt != '\0' ) {
      if ( strstr(filereq->err.errmsgtxt,cmd) != NULL )
        rtcp_log(LOG_ERR,"%s\n",filereq->err.errmsgtxt);
      else
        rtcp_log(LOG_ERR," %s ! %s\n",cmd,filereq->err.errmsgtxt);
    } else {
      if ( filereq->err.errorcode != 0 ) 
        rtcp_log(LOG_ERR," %s ! %s\n",cmd,
                 sstrerror(filereq->err.errorcode));

    }
  }
  if ( tapereq->tprc != 0 || filereq->cprc != 0 ) return(0);
  /*
   * Don't print anything for RTCP_REQUEST_MORE_WORK status requests
   * since they are only placeholders
   */
  if ( filereq->proc_status == RTCP_REQUEST_MORE_WORK ) return(0);
    

  /*
   * Messages after positioned and copy. Print them all at the
   * same time to avoid unsync. messages between different files.
   */
  if ( filereq->proc_status >= RTCP_PARTIALLY_FINISHED ) {
    /*
     * Print position messages. If concatenate to tape we
     * should only print the first element.
     */
    if ( !( (tapereq->mode == WRITE_ENABLE) &&
            (filereq->concat & CONCAT) != 0 ) ) {
      /*
       * If concatenating on disk we suppress the last message for
       * the last (EOD) structure since no data transfer is associated
       * with it.
       */
      if ( tapereq->mode == WRITE_DISABLE &&
           (filereq->concat & CONCAT_TO_EOD) != 0 &&
           (filereq->err.severity & RTCP_EOD) != 0 ) return(0);
      /*
       * In case of a read of an unknown number files to EOD,
       * we also suppress the message for the last (EOD)
       * structure since no data transfer is associated with it.
       */
      if ( tapereq->mode == WRITE_DISABLE && 
           (filereq->concat & NOCONCAT_TO_EOD) != 0 &&
           (filereq->err.severity & RTCP_EOD) != 0 ) return(0);
      rtcp_log(LOG_DEBUG,"mode=%d, concat=0x%x, severity=0x%x\n",
               tapereq->mode,filereq->concat,filereq->err.severity);

      rtcp_log(LOG_INFO," %s - TAPE MOUNTED ON UNIT %s\n",
               cmd,tapereq->unit);
      if ( *filereq->recfm != '\0') rtcp_log(LOG_INFO,RT209,
                                             cmd,*filereq->recfm);
      /*
       * Append mode message ?
       */
      if ( (tapereq->mode == WRITE_DISABLE) &&
           ((filereq->concat & (CONCAT | CONCAT_TO_EOD)) != 0) ) {
        rtcp_log(LOG_INFO,RT203,cmd);
      }

      if ( filereq->blocksize > 0 ) 
        rtcp_log(LOG_INFO,RT204,cmd,filereq->blocksize);
      if ( filereq->recordlength > 0 ) 
        rtcp_log(LOG_INFO,RT210,cmd,filereq->recordlength);
      if ( filereq->maxnbrec > 0 ) 
        rtcp_log(LOG_INFO,RT207,cmd,(u_signed64)filereq->maxnbrec);
      if ( filereq->maxsize > 0 ) 
        rtcp_log(LOG_INFO,RT208,cmd,
                 (int)(filereq->maxsize/(u_signed64)(1024*1024)));
      if ( (filereq->convert & EBCCONV) != 0 ) {
        if ( tapereq->mode == WRITE_ENABLE ) 
          rtcp_log(LOG_INFO,RT213,cmd);
        else rtcp_log(LOG_INFO,RT214,cmd);
      }
    }
    /*
     * Print transfer statistics messages.
     * Only print the last element if concatenating on tape.
     */
    nbbytes = nbhost_bytes = 0;
    if ( tapereq->mode == WRITE_ENABLE ) {
      rc = Cmutex_lock(&lastFileLock,-1);
      if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"rtcpc_GiveInfo() Cmutex_lock(lastFileLock(%p),-1): %s\n",
                 &lastFileLock,sstrerror(save_serrno));
        serrno =save_serrno;
        return(-1);
      }
      cond = (fl->next->filereq.concat & CONCAT);
      (void)Cmutex_unlock(&lastFileLock);
      if ( cond != 0 ) return(0);
      else {
        fltmp = fl;
        while ( (fltmp->prev != fl) &&
                (fltmp->filereq.concat & CONCAT) != 0 ) {
          fltmp = fltmp->prev;
          nbbytes += fltmp->filereq.bytes_in;
        }
      }
    }

    /*
     * To be backward compatible:
     *    print EOF message (RT205) if all data was successfully read
     *              from tape.
     *    print End of Transfer message (RT206) if all data was successfully
     *              written to tape or if data was truncated due to max 
     *              nb records when reading from tape.
     */
    nbrecs = filereq->nbrecs;
    if ( (tapereq->mode == WRITE_DISABLE) && 
         (filereq->err.severity == RTCP_OK) ) rtcp_log(LOG_INFO,RT205,cmd); 

    /*
     * END OF TRANSFER message
     */
    if ( tapereq->mode == WRITE_ENABLE ) {
      rtcp_log(LOG_INFO,RT206,cmd);
    } 
    /*
     * Truncated due to max nb recs. 
     */
    if ( (filereq->err.severity & (RTCP_OK | RTCP_LIMBYSZ)) ==
         (RTCP_OK | RTCP_LIMBYSZ) || 
         (filereq->err.severity & (RTCP_OK | RTCP_TPE_LSZ)) ==
         (RTCP_OK | RTCP_TPE_LSZ) ) {
      if ( (tapereq->mode == WRITE_DISABLE) &&
           (filereq->maxnbrec <= filereq->nbrecs) ) 
        rtcp_log(LOG_INFO,RT206,cmd);
      rtcp_log(LOG_INFO,RT212,cmd);
    }
    /*
     * Give transfer statistics.
     */
    if ( tapereq->mode == WRITE_ENABLE )
      nbbytes += filereq->bytes_in;
    else
      nbbytes += filereq->bytes_out;

    /*
     * Print compression statistics if available
     */
    compress = 0;
    if (  filereq->host_bytes > 0 ) {
      if ( tapereq->mode == WRITE_ENABLE && filereq->bytes_out > 0 ) {
        /*
         * On write we must take into account the label to
         * get precis info. The total size (data + label)
         * is contained in host_bytes
         */
        nbKBin = (int)(filereq->host_bytes / 1024);
        nbKBout = (int)(filereq->bytes_out / 1024);
        rtcp_log(LOG_INFO,RT215,cmd,nbKBin);
        rtcp_log(LOG_INFO,RT216,cmd,nbKBout);
        if ( nbKBout > 0 ) compress = (float)nbKBin/(float)nbKBout;
        rtcp_log(LOG_INFO,RT217,cmd,compress);
      } else if ( tapereq->mode == WRITE_DISABLE && 
                  filereq->bytes_in > 0 ) {
        nbKBin = (int)(filereq->bytes_in / 1024);
        nbKBout = (int)(filereq->host_bytes / 1024);
        rtcp_log(LOG_INFO,RT218,cmd,nbKBout);
        rtcp_log(LOG_INFO,RT219,cmd,nbKBin);
        if ( nbKBin > 0 ) compress = (float)nbKBout/(float)nbKBin;
        rtcp_log(LOG_INFO,RT217,cmd,compress);
      }
    }

    /*
     * Add message for volume spanning if this is the case.
     * Don't print final statistics until the end.
     */
    rc = Cmutex_lock(&lastFileLock,-1);
    if ( rc == -1 ) {
      save_serrno = serrno;
      rtcp_log(LOG_ERR,"rtcpc_GiveInfo() Cmutex_lock(lastFileLock(%p),-1): %s\n",
               &lastFileLock,sstrerror(save_serrno));
      serrno =save_serrno;
      return(-1);
    }
    if ( fl->next == tl->file && tl->next != tl ) {
      (void)Cmutex_unlock(&lastFileLock);
      rtcp_log(LOG_INFO,RT223,cmd); 
      if ( filereq->proc_status != RTCP_FINISHED ) return(0);
    } else {
      (void)Cmutex_unlock(&lastFileLock);
    }

    /*
     * Finally the bandwidth message
     */
    TransferTime = 0;
    if ( (filereq->concat & VOLUME_SPANNING) != 0 ) {
      /*
       * For volume spanning we need to get the transfer time correct:
       * accumulate the individual transfer times for each file section.
       */
      nbbytes = 0;
      nbrecs = 0;
      CLIST_ITERATE_BEGIN(tl,tltmp) {
        CLIST_ITERATE_BEGIN(tltmp->file,fltmp) {
          if ( fltmp->filereq.proc_status == RTCP_FINISHED || 
               fltmp->filereq.proc_status == RTCP_EOV_HIT ) {
            nbrecs += fltmp->filereq.nbrecs;
            if ( tapereq->mode == WRITE_ENABLE &&
                 fltmp->filereq.proc_status == RTCP_FINISHED )
              nbbytes += fltmp->filereq.bytes_in;
            else if ( tapereq->mode == WRITE_DISABLE )
              nbbytes += fltmp->filereq.bytes_out;

            /*
             * Transfer time of tape file.
             */
            if ( tapereq->mode == WRITE_DISABLE ||
                 (fltmp->filereq.concat & CONCAT) == 0 ) {
              TransferTime += max(
                                  (time_t)fltmp->filereq.TEndTransferDisk,
                                  (time_t)fltmp->filereq.TEndTransferTape) -
                (time_t)max(
                            (time_t)fltmp->filereq.TStartTransferDisk,
                            (time_t)fltmp->filereq.TStartTransferTape);
            }
          } 
        } CLIST_ITERATE_END(tltmp->file,fltmp);
      } CLIST_ITERATE_END(tl,tltmp);
    } else {
      TransferTime = (time_t)max((time_t)filereq->TEndTransferDisk,
                                 (time_t)filereq->TEndTransferTape) - 
        (time_t)max((time_t)filereq->TStartTransferDisk,
                    (time_t)filereq->TStartTransferTape);
    }

    rtcp_log(LOG_INFO,RT201,cmd,(u_signed64)nbbytes);
    rtcp_log(LOG_INFO,RT202,cmd,(u_signed64)nbrecs);

    /*
     * Truncated due to max size on read
     */
    if ( (tapereq->mode == WRITE_DISABLE) &&
         ((filereq->err.severity & (RTCP_OK | RTCP_LIMBYSZ)) ==
          (RTCP_OK | RTCP_LIMBYSZ) ||
          (filereq->err.severity & (RTCP_OK | RTCP_TPE_LSZ)) ==
          (RTCP_OK | RTCP_TPE_LSZ)) &&
         ( filereq->maxsize <= filereq->bytes_out ) ) {
      rtcp_log(LOG_INFO,RT212,cmd);
    }

    nbKbytes = (int)(nbbytes / (u_signed64)1024);
    if ( TransferTime == 0 ) TransferTime = 1;
    if ( TransferTime > 0 )
      rtcp_log(LOG_INFO,RT221,cmd,(unsigned int)nbKbytes,
               (*filereq->ifce=='\0' ? "unknown" : filereq->ifce) ,
               (unsigned int)nbKbytes/TransferTime);
  }
  return(0);
}

int rtcpc_CmpFileRequests(rtcpFileRequest_t *filereq1,
                          rtcpFileRequest_t *filereq2) {
  /*
   * Check for special proc_status for adding more filereq to
   * a running request
   */
  if ( (filereq1->proc_status == RTCP_REQUEST_MORE_WORK) &&
       (filereq2->proc_status == RTCP_REQUEST_MORE_WORK) ) return(1);
  /*
   * Position method must match...
   */
  rtcp_log(LOG_DEBUG,"rtcpc_CmpFileRequests() position_method: %d, %d\n",
           filereq1->position_method,filereq2->position_method);
  if ( filereq1->position_method != filereq2->position_method ) return(0);

  rtcp_log(LOG_DEBUG,"rtcpc_CmpFileRequests() tape_fseq: %d, %d\n",
           filereq1->tape_fseq,filereq2->tape_fseq);


  switch(filereq1->position_method) {
  case TPPOSIT_EOI:
    /*
     * Position to End Of Information. Either both request have same
     * tape_fseq (which means that there has already been one update) or
     * one of the requests must have set tape_fseq (the one which has 
     * been updated) and the other must not have set tape_fseq or
     * neither of them have set tape_fseq (this is the case for an
     * error before Ctape_position() has been called).
     */
    if ( filereq1->tape_fseq > 0 &&
         filereq1->tape_fseq == filereq2->tape_fseq ) break;
    if ( filereq1->tape_fseq > 0 && filereq2->tape_fseq > 0 ) return(0);
    break;
  case TPPOSIT_FID:
    /*
     * Position by file ID. The fileid's must be equal.
     */
    rtcp_log(LOG_DEBUG,"rtcpc_CmpFileRequests(): fid: %s, %s\n",
             filereq1->fid,filereq2->fid);
    if ( strcmp(filereq1->fid,filereq2->fid) != 0 ) return(0);
    break;
  case TPPOSIT_BLKID:
    /*
     * Position by blockid (locate). The blockid's must be equal.
     */
    rtcp_log(LOG_DEBUG,"rtcpc_CmpFileRequests(): blkid: %d:%d:%d:%d, %d:%d:%d:%d\n",
             (int)filereq1->blockid[0],(int)filereq1->blockid[1],
             (int)filereq1->blockid[2],(int)filereq1->blockid[3],
             (int)filereq2->blockid[0],(int)filereq2->blockid[1],
             (int)filereq2->blockid[2],(int)filereq2->blockid[3]);
    if ( filereq1->blockid[0] != filereq2->blockid[0] ||
         filereq1->blockid[1] != filereq2->blockid[1] ||
         filereq1->blockid[2] != filereq2->blockid[2] ||
         filereq1->blockid[3] != filereq2->blockid[3] ) return(0);
    break;
  case TPPOSIT_FSEQ:
  default:
    /*
     * Position by file sequence number: the fseqs must be valid
     * and equal. If we concatenate all remaining tape files into a
     * single disk file, there is normally not enough file structures
     * (since client didn't known the number of files on tape), which
     * means we should accept. Note that this can only happen for
     * the last file structure in the request.
     * 15/9/2000: we also process NOCONCAT_TO_EOD similarly if it
     *            is a deferred stagein.
     */
    if ( filereq1->position_method != TPPOSIT_FSEQ )
      rtcp_log(LOG_DEBUG,"rtcpc_CmpFileRequests() unknown position method\n");
    rtcp_log(LOG_DEBUG,"rtcpc_CmpFileRequests() concat: 0x%x, 0x%x\n",
             filereq1->concat,filereq2->concat);
    if ( ((filereq1->concat & CONCAT_TO_EOD) != 0 &&
          (filereq2->concat & CONCAT_TO_EOD) != 0) ||
         (filereq1->def_alloc > 0 && filereq2->def_alloc > 0 &&
          (filereq1->concat & NOCONCAT_TO_EOD) != 0 &&
          (filereq2->concat & NOCONCAT_TO_EOD) != 0) ) break;
    if ( filereq1->tape_fseq != filereq2->tape_fseq ) return(0);
    if ( filereq1->tape_fseq <= 0 ) return(0);
    break;
  }
  /*
   * Match disk files
   */
  rtcp_log(LOG_DEBUG,"rtcpc_CmpFileRequests() disk_fseq: %d, %d\n",
           filereq1->disk_fseq,filereq2->disk_fseq);
  if ( filereq1->disk_fseq > 0 && filereq2->disk_fseq > 0 &&
       filereq1->disk_fseq != filereq2->disk_fseq ) return(0);

  rtcp_log(LOG_DEBUG,"rtcpc_CmpFileRequests() file_path: %s, %s\n",
           filereq1->file_path,filereq2->file_path);
  /*
   * Compare paths. Note that the stager may change the path
   * (e.g. in case of ENOSPC on stagein). 
   */
  if ( *filereq1->stageID == '\0' && *filereq2->stageID == '\0' ) {
    if ( strcmp(filereq1->file_path,filereq2->file_path) != 0 && 
         strcmp(filereq1->file_path,".") != 0 && 
         strcmp(filereq2->file_path,".") != 0 ) return(0);
  }
  /*
   * All tests passed!
   */
  return(1);
}
 

int rtcpc_UpdateProcStatus(tape_list_t *tape,
                           rtcpTapeRequest_t *tapereq,
                           rtcpFileRequest_t *filereq,
                           int dumptape) {
  int rc, listLocked, match_found, save_proc_status, save_serrno;
  tape_list_t *tl;
  file_list_t *fl, *fltmp;

  if ( tape == NULL || (tapereq == NULL && filereq == NULL ) ||
       (tapereq != NULL && filereq != NULL ) ) {
    serrno = EINVAL;
    return(-1);
  }

  rc = 0;
  match_found = 0;
  listLocked = 0;
  CLIST_ITERATE_BEGIN(tape,tl) {
    if ( tapereq != NULL ) {
      if ( strcmp(tl->tapereq.vid,tapereq->vid) == 0 ) {
        /*
         * DGN and server is not part of the RTCOPY protocol since it
         * is known by both client and server before the
         * request is sent.
         */
        strcpy(tapereq->dgn,tl->tapereq.dgn);
        if ( *tl->tapereq.server != '\0' && *tapereq->server == '\0' ) 
          strcpy(tapereq->server,tl->tapereq.server);
        rtcp_log(LOG_DEBUG,"received unit %s, stored %s\n",
                 tapereq->unit,tl->tapereq.unit);
        tl->tapereq = *tapereq;
        (void)rtcpc_GiveInfo(tl,NULL,dumptape);
        match_found = 1;
        break;
      }
    } else {
      rc = Cmutex_lock(&lastFileLock,-1);
      if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"rtcpc_UpdateProcStatus() Cmutex_lock(lastFileLock(%p),-1): %s\n",
                 &lastFileLock,sstrerror(save_serrno));
        serrno =save_serrno;
        return(-1);
      }
      listLocked = 1;
      CLIST_ITERATE_BEGIN(tl->file,fl) {
        if ( listLocked == 1 ) (void)Cmutex_unlock(&lastFileLock);
        listLocked = 0;
        if ( rtcpc_CmpFileRequests(&fl->filereq,filereq) ) {
          rtcp_log(LOG_DEBUG,"fseq=(%d,%d), received proc status %d, stored %d\n",
                   filereq->tape_fseq, fl->filereq.tape_fseq,
                   filereq->proc_status,fl->filereq.proc_status);

          save_proc_status = fl->filereq.proc_status;
          /*
           * Update user structure (if appropriate)
           */
          if ( filereq->cprc != 0 ) {
            /*
             * Always copy if there was an error and no local error has been set
             */
            if ( fl->filereq.cprc == 0 ) fl->filereq = *filereq;
            else {
              rtcp_log(LOG_ERR,"Received error %d (%s) will not overwrite local error %d (%s)\n",
                       filereq->err.errorcode,filereq->err.errmsgtxt,
                       fl->filereq.err.errorcode,fl->filereq.err.errmsgtxt);
            }
          } else if ( (tl->tapereq.mode == WRITE_DISABLE) &&
                      (filereq->concat & 
                       (NOCONCAT_TO_EOD|CONCAT_TO_EOD)) != 0 &&
                      (filereq->err.severity & RTCP_EOD) != 0 ) {
            /*
             * Don't copy last (empty!) element if we are
             * concatenating tape files to EOD and last tape
             * has been reached. Copy the EOD message.
             */
            fl->filereq.proc_status = RTCP_FINISHED;
            fl->filereq.err.severity |= RTCP_EOD;
            strcat(fl->filereq.err.errmsgtxt,
                   filereq->err.errmsgtxt);
          } else if ( (fl->filereq.concat & VOLUME_SPANNING) != 0 ) {
            /*
             * Multi-volume spanning:
             * Unfortunately the file section number is
             * internal information and therefore not returned
             * with the file request. Thus, there is no easy way
             * to match the file requests for the individual
             * file sections since they are identical in all
             * other respects. We have to rely on that
             * they drop in sequentially so that a new status is
             * always higher than the preceeding, up to
             * EOV hit which is the final state of an intermediate 
             * file section.
             */
            if ( fl->filereq.proc_status<filereq->proc_status &&
                 fl->filereq.proc_status<RTCP_FINISHED ) {
              fl->filereq = *filereq;
            } else {
              break; /* No match: iterate to next volume */
            }
          } else fl->filereq = *filereq; 

          if ( rtcpc_ClientCallback != 
               (int (*) _PROTO((rtcpTapeRequest_t *,
                                rtcpFileRequest_t *)))NULL ) {
            rc = rtcpc_ClientCallback(&tl->tapereq,&fl->filereq);
            if ( rc == -1 ) {
              save_serrno = serrno;
              rtcp_log(LOG_ERR,
                       "rtcpc_ClientCallback() error: %s\n",
                       sstrerror(serrno));
              if ( fl->filereq.cprc == 0 ) {
                fl->filereq.cprc = -1;
                fl->filereq.err.errorcode = save_serrno;
                fl->filereq.err.severity = RTCP_FAILED;
                strcpy(fl->filereq.err.errmsgtxt,sstrerror(save_serrno));
              }
              serrno = save_serrno;
              return(-1);
            }
            /*
             * Check if client had requested more work
             */
            if ( filereq->proc_status == RTCP_REQUEST_MORE_WORK ) {
              /*
               * Insert a new dummy element if necessary
               */
              if ( fl->filereq.proc_status == RTCP_WAITING ) {
                rc = Cmutex_lock(&lastFileLock,-1);
                if ( rc == -1 ) {
                  save_serrno = serrno;
                  rtcp_log(LOG_ERR,"rtcpc_UpdateProcStatus() Cmutex_lock(lastFileLock(%p),-1): %s\n",
                           &lastFileLock,sstrerror(save_serrno));
                  serrno =save_serrno;
                  return(-1);
                }
                rc = rtcp_NewFileList(
                                      &tl,
                                      &fltmp,
                                      tl->tapereq.mode
                                      );
                if ( rc == -1 ) {
                  save_serrno = serrno;
                  rtcp_log(LOG_ERR,"rtcp_NewFileList() error: %s\n",
                           sstrerror(serrno));
                  (void)Cmutex_unlock(&lastFileLock);
                  serrno = save_serrno;
                  return(-1);
                }
                fltmp->filereq = *filereq;
                (void)Cmutex_unlock(&lastFileLock);
              }
              *filereq = fl->filereq;
            }
          }

          (void)rtcpc_GiveInfo(tl,fl,dumptape);
          /*
           * It may happen that messages arrive out of sync.. Make
           * sure that important status values are not overridden.
           */
          if ( save_proc_status == RTCP_FINISHED &&
               fl->filereq.proc_status == RTCP_PARTIALLY_FINISHED ) 
            fl->filereq.proc_status = save_proc_status;
          match_found = 1;
          break;
        }
        rc = Cmutex_lock(&lastFileLock,-1);
        if ( rc == -1 ) {
          save_serrno = serrno;
          rtcp_log(LOG_ERR,"rtcpc_UpdateProcStatus() Cmutex_lock(lastFileLock(%p),-1): %s\n",
                   &lastFileLock,sstrerror(save_serrno));
          serrno =save_serrno;
          return(-1);
        }
        listLocked = 1;
      } CLIST_ITERATE_END(tl->file,fl);
      if ( listLocked == 1 ) (void)Cmutex_unlock(&lastFileLock);
      listLocked = 0;
    }
    if ( match_found == 1 ) break;
  } CLIST_ITERATE_END(tape,tl);

  if ( match_found == 0 ) {
    rtcp_log(LOG_DEBUG,"rtcpc_UpdateProcStatus(): Could not find a matching entry!\n");
  }
  return(0);
}

void rtcpc_DumpRequest(SOCKET *s, tape_list_t *tape) {
  tape_list_t *tl;
  file_list_t *fl;
  rtcpHdr_t hdr;

  hdr.magic = RTCOPY_MAGIC;
  hdr.len = -1;
  CLIST_ITERATE_BEGIN(tape,tl) {
    hdr.reqtype = RTCP_TAPEERR_REQ;
    (void)rtcp_SendReq(s,&hdr,NULL,&tl->tapereq,NULL);
    (void)Cmutex_lock(&lastFileLock,-1);
    CLIST_ITERATE_BEGIN(tl->file,fl) {
      (void)Cmutex_unlock(&lastFileLock);
      hdr.reqtype = RTCP_FILEERR_REQ;
      (void)rtcp_SendReq(s,&hdr,NULL,NULL,&fl->filereq);
      (void)Cmutex_lock(&lastFileLock,-1);
    } CLIST_ITERATE_END(tl->file,fl);
    (void)Cmutex_unlock(&lastFileLock);
  } CLIST_ITERATE_END(tape,tl);
  hdr.reqtype = RTCP_ENDOF_REQ;
  (void)rtcp_SendReq(s,&hdr,NULL,NULL,NULL);
  return;
}

int rtcpc_SetLocalErrorStatus(tape_list_t *tape,
                              int errorcode,
                              char *errmsg,
                              int severity,
                              int rc) {
  if ( tape == NULL ) return(rc);
  if ( errmsg != NULL && *tape->tapereq.err.errmsgtxt == '\0' )
    strcpy(tape->tapereq.err.errmsgtxt,errmsg);
  if ( errorcode > 0 && tape->tapereq.err.errorcode <= 0)
    tape->tapereq.err.errorcode = errorcode;
  if ( tape->tapereq.tprc == 0 ) tape->tapereq.tprc = rc;
  if ( tape->tapereq.err.severity == RTCP_OK )
    tape->tapereq.err.severity = severity;
  return(rc);
}

int rtcpc_GetDeviceQueues(char *dgn, char *server, int *_nb_queued,
                          int *_nb_units, int *_nb_used) {
  vdqmVolReq_t VolReq;
  vdqmDrvReq_t DrvReq;
  vdqmnw_t *nw = NULL;
  int rc, nb_queued, nb_units, nb_used;

  nb_queued = nb_units = nb_used = 0;
  memset(&VolReq,'\0',sizeof(vdqmVolReq_t));
  memset(&DrvReq,'\0',sizeof(vdqmDrvReq_t));

  if ( server != NULL ) {
    strcpy(VolReq.server,server);
    strcpy(DrvReq.server,server);
  }
  if ( dgn != NULL ) {
    strcpy(VolReq.dgn,dgn);
    strcpy(DrvReq.dgn,dgn);
  }

  while ( (rc = vdqm_NextVol(&nw,&VolReq)) != -1 ) {
    if ( *VolReq.volid == '\0' ) continue;
    nb_queued++;
  }

  nw = NULL;
  while ( (rc = vdqm_NextDrive(&nw,&DrvReq)) != -1 ) {
    if ( *DrvReq.drive == '\0' ) continue;
    if ( DrvReq.VolReqID > 0 ) nb_used++;
    if ( (DrvReq.status & VDQM_UNIT_UP) != 0 &&
         *DrvReq.dedicate == '\0' ) nb_units++;
  }

  if ( _nb_queued != NULL ) *_nb_queued = nb_queued;
  if ( _nb_units != NULL ) *_nb_units = nb_units;
  if ( _nb_used != NULL ) *_nb_used = nb_used;

  return(0);
}

int rtcpc_SelectServer(rtcpc_sockets_t **socks,
                       tape_list_t *tape,
                       char *realVID,
                       int port,
                       int *ReqID) {
  int rc, i, local_severity, reqID, save_serrno;
  char tpserver[CA_MAXHOSTNAMELEN+1];
  rtcpTapeRequest_t *tapereq = NULL;

  if ( socks == NULL || *socks == NULL || (*socks)->listen_socket == NULL ||
       *(*socks)->listen_socket == INVALID_SOCKET || tape == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  tapereq = &tape->tapereq;

  rtcp_log(LOG_INFO,"selecting tape server ...\n");
  if ( ReqID != NULL ) reqID = *ReqID;
  else reqID = -1;

  while ( (*socks)->accept_socket == INVALID_SOCKET && rtcpc_killed == 0 ) {
    if ( reqID == -1 ) {
      rtcp_log(LOG_DEBUG,"rtcpc_SelectServer() call vdqm_SendVolReq()\n");
      rc = vdqm_SendVolReq(NULL,&reqID,realVID,tapereq->dgn,
                           tapereq->server,tapereq->unit,tapereq->mode,port);
      if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"rtcpc_SelectServer() vdqm_SendVolReq(): %s\n",
                 sstrerror(serrno));
        (void)rtcpc_finished(socks,NULL,NULL);
        serrno = save_serrno;
        return(rtcpc_SetLocalErrorStatus(tape,serrno,
                                         "vdqm_SendVolReq() error",
                                         RTCP_FAILED|RTCP_SYERR,
                                         rc));
      } else {
        rtcp_log(LOG_DEBUG,"rtcpc_SelectServer() VDQM request id=%d\n",reqID);
      }
      i = 0;
    }
    tapereq->VolReqID = reqID;

    rc = rtcp_Listen(*((*socks)->listen_socket),&((*socks)->accept_socket),
                     RTCPC_PING_INTERVAL, RTCP_ACCEPT_FROM_SERVER);
    /*
     * Returned from a blocking call. Check if killed in meanwhile
     */
    if ( rc == -1 || rtcpc_killed != 0 ) {
      save_serrno = serrno;
      (void)rtcpc_finished(socks,NULL,tape);
      if ( rtcpc_killed != 0 ) {
        rc = -1;
        save_serrno = ERTUSINTR;
        local_severity = RTCP_FAILED|RTCP_USERR;
      } else {
        local_severity = RTCP_FAILED|RTCP_SYERR;
      }
      rtcp_log(LOG_ERR,"rtcpc_SelectServer() rtcp_Listen(): %s\n",
               sstrerror(save_serrno));
      serrno = save_serrno;
      return(rtcpc_SetLocalErrorStatus(tape,serrno,"rtcp_Listen() error",
                                       local_severity,rc));
    }

    if ( (*socks)->accept_socket == INVALID_SOCKET ) {
      rc = vdqm_PingServer(NULL,tapereq->dgn,reqID);
      if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_DEBUG,
                 "rtcpc_SelectServer() vdqm_PingServer(): %s\n",
                 sstrerror(serrno)); 
        /*
         * Retry forever on server hold status
         */
        if ( save_serrno != EVQHOLD ) {
          i++;
          /*
           * Retry a few times on communication error.
           */
          if ( i > 5 || save_serrno != SECOMERR ) {
            rtcp_log(LOG_DEBUG,
                     "rtcpc_SelectServer() queue position lost.\n");
            reqID = -1;
          }
        }
      } else {
        rtcp_log(LOG_DEBUG,
                 "rtcpc_SelectServer() VolReqID %d, queue position: %d\n",
                 reqID,rc);
      }
    } 
  }

  if ( rtcpc_killed != 0 ) {
    rtcp_log(LOG_ERR,"rtcpc_SelectServer() request aborting\n");
    (void)rtcpc_finished(&socks,NULL,tape);
    serrno = ERTUSINTR;
    local_severity = RTCP_FAILED|RTCP_USERR;
    return(rtcpc_SetLocalErrorStatus(tape,serrno,"request aborting",
                                     local_severity,-1));
  }

  (*socks)->abort_socket = (*socks)->accept_socket;
  rtcp_log(LOG_DEBUG,
           "rtcpc_SelectServer() server main connection socket is %d\n",
           (*socks)->accept_socket);
  *tpserver = '\0';
  rc = CDoubleDnsLookup(((*socks)->accept_socket),tpserver);
  if ( rc == -1 ) {
    save_serrno = serrno;
    if ( serrno == SENOSHOST ) {
      rtcp_log(LOG_ERR,"rtcpc_SelectServer() double DNS lookup rejected pretended host %s\n",
               (*tpserver != '\0' ? tpserver : "(unknown)"));
      (void)rtcp_CloseConnection(&((*socks)->accept_socket));
      serrno = save_serrno;
      return(-1);
    }
    rtcp_log(LOG_INFO,"! selected tape server is <unknown>.\n\n\n");
  } else {
    rtcp_log(LOG_INFO,"* %s is a possible tape server.\n",tpserver);
    rtcp_log(LOG_INFO,"! selected tape server is %s.\n\n\n",tpserver);
    if ( *tape->tapereq.server == '\0' ) 
      strcpy(tape->tapereq.server,tpserver);
  }
  if ( ReqID != NULL ) *ReqID = reqID;
  return(reqID);
}

int rtcpc_EndOfRequest(tape_list_t *tape) {
  int rc, nbLeft, EODreached, save_serrno;
  tape_list_t *tl;
  file_list_t *fl;

  rc = nbLeft = 0;
  EODreached = FALSE;

  CLIST_ITERATE_BEGIN(tape,tl) {
    if ( rc == -1 || tl->tapereq.tprc == -1 ) {
      rc = -1;
      break;
    }
    rc = Cmutex_lock(&lastFileLock,-1);
    if ( rc == -1 ) {
      save_serrno = serrno;
      rtcp_log(LOG_ERR,"rtcpc_EndOfRequest() Cmutex_lock(lastFileLock(%p),-1): %s\n",
               &lastFileLock,sstrerror(save_serrno));
      serrno =save_serrno;
      return(-1);
    }
    CLIST_ITERATE_BEGIN(tl->file,fl) {
      (void)Cmutex_unlock(&lastFileLock);
      if ( fl->filereq.cprc == -1 ) {
        rc = -1;
        break;
      }
      if ( (fl->filereq.err.severity & RTCP_EOD) != 0 ) EODreached = TRUE;
      if ( EODreached == TRUE ) fl->filereq.proc_status = RTCP_FINISHED;
      if ( fl->filereq.proc_status < RTCP_FINISHED ) nbLeft++;
      rc = Cmutex_lock(&lastFileLock,-1);
      if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"rtcpc_EndOfRequest() Cmutex_lock(lastFileLock(%p),-1): %s\n",
                 &lastFileLock,sstrerror(save_serrno));
        serrno =save_serrno;
        return(-1);
      }
    } CLIST_ITERATE_END(tl->file,fl);
    (void)Cmutex_unlock(&lastFileLock);
    EODreached = FALSE;
    if ( nbLeft == 0 ) tl->tapereq.TEndRequest = (int)time(NULL);
  } CLIST_ITERATE_END(tape,tl);
  if ( rc == -1 || nbLeft == 0 ) return(1);
  else return(0);
}

int rtcpc_finished(rtcpc_sockets_t **socks, rtcpHdr_t *hdr, tape_list_t *tape) {
  int i, rc, retval, save_serrno;

  if ( rtcpc_killed == 0 ) (void)rtcpc_ResetKillInfo();
  rc = retval = save_serrno = 0;
  if ( socks != NULL && *socks != NULL ) {
    if ( (*socks)->abort_socket != INVALID_SOCKET && hdr != NULL ) {
      hdr->magic = RTCOPY_MAGIC;
      hdr->len = -1;
      rc = rtcp_SendReq(&((*socks)->abort_socket),hdr,NULL,NULL,NULL);
      if ( hdr->reqtype == RTCP_ENDOF_REQ ) {
        if ( rc == -1 ) {
          save_serrno = serrno;
          rtcp_log(LOG_ERR,"rtcpc_finished() rtcp_SendReq(ENDOF_REQ): %s\n",
                   sstrerror(serrno));
        } else {
          rtcp_log(LOG_DEBUG,
                   "rtcpc_finished(): Receive acknowledge\n");
          rc = rtcp_RecvAckn(&((*socks)->abort_socket),hdr->reqtype);
          if ( rc == -1 ) {
            save_serrno = serrno;
            rtcp_log(LOG_ERR,"rtcpc_finished() rtcp_RecvAckn(TAPE_REQ): %s\n",
                     sstrerror(serrno));
          }
        }
      }
      (void)rtcp_CloseConnection(&((*socks)->abort_socket));
      if ( rc == -1 ) retval = -1;
    }
    for (i=0; i<100; i++) {
      if ( (*socks)->proc_socket[i] != NULL &&
           *((*socks)->proc_socket[i]) != INVALID_SOCKET ) {
        (void)rtcp_CloseConnection((*socks)->proc_socket[i]);
        free((*socks)->proc_socket[i]);
        (*socks)->proc_socket[i] = NULL;
      }
    }
    (void)rtcp_CleanUp(&((*socks)->listen_socket),0);
    free(*socks);
    *socks = NULL;
  } 

  /*
   * We only cancel VDQM job if it hasn't been assigned. Otherwise
   * it is automatically cancelled when the tape server releases the drive.
   */
  if ( tape != NULL && tape->tapereq.VolReqID > 0 &&
       tape->tapereq.TStartRtcpd <= 0 ) {
    rtcp_log(LOG_DEBUG,"rtcpc_finished() cancelling VolReqID %d, VID: %s dgn: %s\n",
             tape->tapereq.VolReqID,tape->tapereq.vid,tape->tapereq.dgn);
    rc = vdqm_DelVolumeReq(NULL,tape->tapereq.VolReqID,tape->tapereq.vid,
                           tape->tapereq.dgn,NULL,NULL,0);
    rtcp_log(LOG_DEBUG,"rtcpc_finished() vdqm_DelVolumeReq() returned rc=%d %s\n",
             rc,(rc != 0 ? sstrerror(serrno) : ""));
    serrno = save_serrno;
  }
  (void)rtcpc_ResetKillInfo();
  rc = retval;
  if ( save_serrno != 0 ) serrno = save_serrno;
  return(rc);
}

int rtcpc_kill() {
  rtcpHdr_t hdr;
  hdr.reqtype = RTCP_ABORT_REQ;
  rtcpc_killed = 1;
  return(rtcpc_finished(&last_socks,&hdr,last_tape));
}

int rtcpc_InitReq(rtcpc_sockets_t **socks, int *port, tape_list_t *tape) {
  int save_serrno, local_severity, rc;

  if ( rtcp_log == (void (*) _PROTO((int, const char *, ...)))NULL )
    rtcp_log = (void (*) _PROTO((int, const char *, ...)))local_log;

  if ( socks == NULL || tape == NULL ) {
    rtcp_log(LOG_ERR,
             "rtcpc_InitReq() called with NULL argument(s): socks=%p, tape=%p\n",
             socks,tape);
    serrno = EINVAL;
    local_severity = RTCP_FAILED|RTCP_USERR; 
    if ( tape != NULL ) return(rtcpc_SetLocalErrorStatus(
                                                         tape,
                                                         serrno,
                                                         "NULL argument(s)",
                                                         local_severity,
                                                         -1
                                                         ));
    else return(-1);
  }

  if ( rtcp_CheckReqStructures(NULL,NULL,tape) == -1 ) return(-1);

  if ( *(tape->tapereq.vid) == '\0' && *(tape->tapereq.vsn) == '\0' ) {
    rtcp_log(LOG_INFO,"! vid or vsn must be specified\n");
    serrno = EINVAL;
    local_severity = RTCP_FAILED|RTCP_USERR;
    return(rtcpc_SetLocalErrorStatus(
                                     tape,
                                     serrno,
                                     "vid or vsn must be specified",
                                     local_severity,
                                     -1
                                     ));
  }

  *socks = (rtcpc_sockets_t *)calloc(1,sizeof(rtcpc_sockets_t));
  if ( *socks == NULL ) {
    save_serrno = errno;
    rtcp_log(LOG_ERR,"rtcpc_InitReq() malloc(): %s\n",sstrerror(errno));
    serrno = save_serrno;
    local_severity = RTCP_FAILED|RTCP_SYERR;
    return(rtcpc_SetLocalErrorStatus(tape,serrno,"malloc() error",
                                     local_severity,-1));
  }
  (void)rtcpc_SetKillInfo(*socks,tape);
  (*socks)->listen_socket = NULL;
  (*socks)->accept_socket = (*socks)->abort_socket = INVALID_SOCKET;
  (*socks)->nb_proc_sockets = 0;

  rc = rtcpc_InitNW(&((*socks)->listen_socket),port);
  if ( rc == -1 || (*socks)->listen_socket == NULL ) {
    save_serrno = serrno;
    rtcp_log(LOG_ERR,"rtcpc_InitReq() rtcpc_InitNW(): %s\n",sstrerror(serrno));
    serrno = save_serrno;
    local_severity = RTCP_FAILED|RTCP_SYERR;
    return(rtcpc_SetLocalErrorStatus(tape,serrno,"rtcpc_InitNW() error",
                                     local_severity,rc));
  }
  rtcp_log(LOG_DEBUG,"rtcpc_InitReq() opened listen socket %d\n",
           *((*socks)->listen_socket));
  return(0);
}

/** rtcpc() - main entry point for running RTCOPY requests
 *
 * @param tape_list_t *tape - pointer to head tape element of an RTCOPY request list
 *
 */
int rtcpc(tape_list_t *tape) {
  int rc, port = 0, reqID, local_severity;
  int nb_queued, nb_units, nb_used;
  rtcpc_sockets_t *socks = NULL;
  rtcpTapeRequest_t *tapereq;
  rtcpHdr_t hdr;
  char realVID[CA_MAXVIDLEN+1];
  int save_serrno = 0, tStartRequest;

  if ( rtcp_log == (void (*) _PROTO((int, const char *, ...)))NULL )
    rtcp_log = (void (*) _PROTO((int, const char *, ...)))local_log;

  tStartRequest = (int)time(NULL);
  tapereq = &tape->tapereq;
  /*
   * Is it an info request?
   */
  if ( tapereq->mode == RTCP_INFO_REQ ) {
    rc = rtcpc_GetDeviceQueues(tapereq->dgn,tapereq->server,
                               &nb_queued,&nb_units,&nb_used);
    rtcp_log(LOG_ERR,"\tDevice group: %s\tServer: %s\tCapacity: %d\tQueue: %d\n",
             (*tapereq->dgn!='\0' ? tapereq->dgn : "(all)"),
             (*tapereq->server!='\0' ? tapereq->server : "(all)"),
             nb_units,nb_used-nb_units+nb_queued);
    return(rc);
  }

  rc = rtcpc_InitReq(&socks,&port,tape);
  if ( rc == -1 ) return(-1);

#if VMGR
  /* Call VMGR */
  if ((rc = rtcp_CallVMGR(tape,realVID)) != 0) {
#endif
#if TMS
#if VMGR
    /* VMGR fails with acceptable serrno and TMS is available */
    if (serrno == ETVUNKN) {
#endif
      /* Call TMS */
      rc = rtcp_CallTMS(tape,realVID);
#if VMGR
    }
#endif
#elif (! defined(VMGR))
    /* No TMS nor VMGR : take argument as it is */
    if ( realVID != NULL ) {
      strcpy(realVID,tape->tapereq.vid);
      UPPERCASE(realVID);
    }
    rc = 0;
#endif
#if VMGR
  }
#endif
  if ( rc == -1 || rtcpc_killed != 0 ) {
    save_serrno = serrno;
    (void)rtcpc_finished(&socks,NULL,NULL);
    if ( rtcpc_killed != 0 ) {
      rc = -1;
      save_serrno = ERTUSINTR;
      local_severity = RTCP_FAILED|RTCP_USERR;
    } else {
      local_severity = RTCP_FAILED|RTCP_SYERR;
    }
#if TMS
    rtcp_log(LOG_ERR,"rtcpc() rtcp_CallTMS(): %s\n",sstrerror(save_serrno));
#else
    rtcp_log(LOG_ERR,"rtcpc() rtcp_CallVMGR(): %s\n",sstrerror(save_serrno));
#endif
    serrno = save_serrno;
    return(rtcpc_SetLocalErrorStatus(tape,serrno,"rtcp_CallTMS() error",
                                     local_severity,rc));
  }

  reqID = -1;
  for (;;) {
    rc = rtcpc_SelectServer(&socks,tape,realVID,port,&reqID);
    if ( rc == -1 && serrno == SENOSHOST ) {
      sleep(1);
      serrno = 0;
    } else break;
  }
  if ( rc == -1 ) return(rc);
  tape->tapereq.VolReqID = reqID;
  tape->tapereq.TStartRequest = tStartRequest;
  hdr.magic = RTCOPY_MAGIC;

  rc = rtcpc_sendReqList(&hdr,&socks, tape);
  if ( rc == -1 ) return(rc);  

  if ( realVID != NULL && (*realVID != '\0' &&
                           strcmp(realVID,tape->tapereq.vid) != 0) ) {
    strcpy(tape->tapereq.vid,realVID);
  }

  rc = rtcpc_sendEndOfReq(&hdr,&socks,tape);
  if ( rc == -1 ) return(-1);

  return(rtcpc_runReq(&hdr,&socks,tape));
}

int rtcpc_sendEndOfReq(
                       rtcpHdr_t *hdr,
                       rtcpc_sockets_t **socks,
                       tape_list_t *tape
                       ) {
  int rc, local_severity, save_serrno = 0;
  rtcpTapeRequest_t *tapereq;
  rtcpFileRequest_t *filereq;

  hdr->reqtype = RTCP_NOMORE_REQ;
  hdr->len = -1;
  filereq = NULL;
  tapereq = NULL;
  rtcp_log(
           LOG_DEBUG,
           "rtcpc_sendEndOfReq(): send end of request\n"
           );
  rc = rtcp_SendReq(
                    &((*socks)->accept_socket),
                    hdr,
                    NULL,
                    tapereq,
                    filereq
                    );
  if ( rc == -1 || rtcpc_killed != 0 ) {
    save_serrno = serrno;
    if ( rtcpc_killed != 0 ) {
      rc = -1;
      save_serrno = ERTUSINTR;
      local_severity = RTCP_FAILED|RTCP_USERR;
    } else {
      local_severity = RTCP_FAILED|RTCP_SYERR;
    }
    rtcp_log(
             LOG_ERR,
             "rtcpc_sendEndOfReq() rtcp_SendReq(NOMORE_REQ): %s\n",
             sstrerror(save_serrno)
             );
    hdr->reqtype = RTCP_ABORT_REQ;
    (void)rtcpc_finished(
                         socks,
                         hdr,
                         tape
                         );
    serrno = save_serrno;
    return(rtcpc_SetLocalErrorStatus(
                                     tape,
                                     save_serrno,
                                     "rtcp_SendReq(NOMORE_REQ) error",
                                     local_severity,
                                     rc
                                     ));
  }
  rtcp_log(LOG_DEBUG,"rtcpc_sendEndOfReq(): receive EOR acknowledge\n");
  rc = rtcp_RecvAckn(
                     &((*socks)->accept_socket),
                     hdr->reqtype
                     );
  if ( rc == -1 || rtcpc_killed != 0 ) {
    save_serrno = serrno;
    if ( rtcpc_killed != 0 ) {
      rc = -1;
      save_serrno = ERTUSINTR;
      local_severity = RTCP_FAILED|RTCP_USERR;
    } else {
      local_severity = RTCP_FAILED|RTCP_SYERR;
    }
    rtcp_log(
             LOG_ERR,
             "rtcpc_sendEndOfReq() rtcp_RecvAckn(NOMORE_REQ): %s\n",
             sstrerror(save_serrno)
             );
    hdr->reqtype = RTCP_ABORT_REQ;
    (void)rtcpc_finished(
                         socks,
                         hdr,
                         tape
                         );
    serrno = save_serrno;
    return(rtcpc_SetLocalErrorStatus(
                                     tape,
                                     save_serrno,
                                     "rtcp_RecvAckn(NOMORE_REQ) error",
                                     local_severity,
                                     rc
                                     ));
  }

  return(0);
}


/** rtcpc_SendReqList() - send RTCOPY request list to server
 *
 */
int rtcpc_sendReqList(
                      rtcpHdr_t *hdr,
                      rtcpc_sockets_t **socks,
                      tape_list_t *tape
                      ) {
  int reqID, rc, tStartRequest, mode, local_severity, save_serrno = 0;
  int dumptape = FALSE;
  tape_list_t *tl;
  file_list_t *fl;
  rtcpTapeRequest_t *tapereq;
  rtcpFileRequest_t *filereq;

  reqID = tape->tapereq.VolReqID;
  tStartRequest = tape->tapereq.TStartRequest;
  CLIST_ITERATE_BEGIN(tape,tl) {
    hdr->reqtype = RTCP_TAPEERR_REQ;
    hdr->len = -1;
    filereq = NULL;
    tapereq = &tl->tapereq;
    tapereq->VolReqID = reqID;
    tapereq->TStartRequest = tStartRequest;
    tapereq->err.severity = tapereq->err.severity & ~RTCP_RESELECT_SERV;
    tapereq->tprc = 0;
    mode = tapereq->mode;
    rtcp_log(
             LOG_DEBUG,
             "rtcpc_sendReqList(): send tape request, unit=%s, reqID %d\n",
             tapereq->unit,
             reqID
             );
    rc = rtcp_SendReq(
                      &((*socks)->accept_socket),
                      hdr,
                      NULL,
                      tapereq,
                      filereq
                      );
    if ( rc == -1 || rtcpc_killed != 0 ) {
      save_serrno = serrno;
      if ( rtcpc_killed != 0 ) {
        rc = -1;
        save_serrno = ERTUSINTR;
        local_severity = RTCP_FAILED|RTCP_USERR;
      } else {
        local_severity = RTCP_FAILED|RTCP_SYERR;
      }
      rtcp_log(
               LOG_ERR,
               "rtcpc_sendReqList() rtcp_SendReq(TAPE_REQ): %s\n",
               sstrerror(save_serrno)
               );
      (void)rtcpc_SetLocalErrorStatus(
                                      tape,
                                      save_serrno,
                                      "rtcp_SendReq(TAPE_REQ) error",
                                      local_severity,
                                      rc
                                      );
      break;
    }
    rtcp_log(
             LOG_DEBUG,
             "rtcpc_sendReqList(): receive acknowledge\n"
             );
    rc = rtcp_RecvAckn(
                       &((*socks)->accept_socket),
                       hdr->reqtype
                       );
    if ( rc == -1 || rtcpc_killed != 0 ) {
      save_serrno = serrno;
      if ( rtcpc_killed != 0 ) {
        rc = -1;
        save_serrno = ERTUSINTR;
        local_severity = RTCP_FAILED|RTCP_USERR;
      } else {
        local_severity = RTCP_FAILED|RTCP_SYERR;
      }
      rtcp_log(
               LOG_ERR,
               "rtcpc_sendReqList() rtcp_RecvAckn(TAPE_REQ): %s\n",
               sstrerror(save_serrno)
               );
      (void)rtcpc_SetLocalErrorStatus(
                                      tape,
                                      save_serrno,
                                      "rtcp_RecvAckn(TAPE_REQ) error",
                                      local_severity,
                                      rc
                                      );
      break;
    }
    if ( tl->file == NULL ) {
      /*
       * This is the signature of a dumptape request. There
       * is only the tape request and an attached dumptape request.
       */
      dumptape = TRUE;
      rtcp_log(LOG_DEBUG,"rtcpc_sendReqList(): send dumptape request\n");
      hdr->reqtype = RTCP_DUMPTAPE_REQ;
      rc = rtcp_SendReq(
                        &((*socks)->accept_socket),
                        hdr,
                        NULL,
                        NULL,
                        NULL
                        );
      if ( rc == -1 || rtcpc_killed != 0 ) {
        save_serrno = serrno;
        if ( rtcpc_killed != 0 ) {
          rc = -1;
          save_serrno = ERTUSINTR;
          local_severity = RTCP_FAILED|RTCP_USERR;
        } else {
          local_severity = RTCP_FAILED|RTCP_SYERR;
        }
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_sendReqList() rtcp_SendReq(DUMPTAPE_REQ): %s\n",
                 sstrerror(save_serrno)
                 );
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "rtcp_SendReq(DUMPTAPE_REQ) error",
                                        local_severity,
                                        rc
                                        );
        break;
      }
      rc = rtcp_SendTpDump(
                           &((*socks)->accept_socket),
                           &(tl->dumpreq)
                           );
      if ( rc == -1 || rtcpc_killed != 0 ) {
        save_serrno = serrno;
        if ( rtcpc_killed != 0 ) {
          rc = -1;
          save_serrno = ERTUSINTR;
          local_severity = RTCP_FAILED|RTCP_USERR;
        } else {
          local_severity = RTCP_FAILED|RTCP_SYERR;
        }
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_sendReqList() rtcp_SendTpDump(DUMPTAPE_REQ): %s\n",
                 sstrerror(save_serrno)
                 );
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "rtcp_SendTpDump(DUMPTAPE_REQ) error",
                                        local_severity,
                                        rc
                                        );
        break;
      }
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_sendReqList(): receive acknowledge\n"
               );
      rc = rtcp_RecvAckn(&((*socks)->accept_socket),hdr->reqtype);
      if ( rc == -1 || rtcpc_killed != 0) {
        save_serrno = serrno;
        if ( rtcpc_killed != 0 ) {
          rc = -1;
          save_serrno = ERTUSINTR;
          local_severity = RTCP_FAILED|RTCP_USERR;
        } else {
          local_severity = RTCP_FAILED|RTCP_SYERR;
        }
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_sendReqList() rtcp_RecvAckn(TAPE_REQ): %s\n",
                 sstrerror(save_serrno)
                 );
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "rtcp_RecvAckn(TAPE_REQ) error",
                                        local_severity,
                                        rc
                                        );
        break;
      }
      /*
       * Multi volume dumptape is not possible
       */
      break;
    }
    CLIST_ITERATE_BEGIN(tl->file,fl) {
      filereq = &fl->filereq;
      rc = rtcpc_FixPath(
                         fl,
                         sizeof(filereq->file_path),
                         mode
                         );
      UPPERCASE(filereq->fid);
      hdr->reqtype = RTCP_FILEERR_REQ;
      filereq->err.severity = filereq->err.severity & ~RTCP_RESELECT_SERV;
      filereq->cprc = 0;
      hdr->len = -1;
      filereq->VolReqID = reqID;
      if ( filereq->umask <= 0 )
        (void) umask(filereq->umask = (int) umask(0));
      if ( filereq->proc_status <= RTCP_WAITING ) 
        filereq->proc_status = RTCP_WAITING;
      rtcp_log(LOG_DEBUG,"rtcpc_sendReqList(): send file request\n");
      rc = rtcp_SendReq(
                        &((*socks)->accept_socket),
                        hdr,
                        NULL,
                        tapereq,
                        filereq
                        );
      if ( rc == -1 || rtcpc_killed != 0 ) {
        save_serrno = serrno;
        if ( rtcpc_killed != 0 ) {
          rc = -1;
          save_serrno = ERTUSINTR;
          local_severity = RTCP_FAILED|RTCP_USERR;
        } else {
          local_severity = RTCP_FAILED|RTCP_SYERR;
        }
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_sendReqList() rtcp_SendReq(FILE_REQ): %s\n",
                 sstrerror(save_serrno)
                 );
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "rtcp_SendReq(FILE_REQ) error",
                                        local_severity,
                                        rc
                                        );
        break;
      }
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_sendReqList(): receive acknowledge\n"
               );
      rc = rtcp_RecvAckn(
                         &((*socks)->accept_socket),
                         hdr->reqtype
                         );
      if ( rc == -1 || rtcpc_killed != 0 ) {
        save_serrno = serrno;
        if ( rtcpc_killed != 0 ) {
          rc = -1;
          save_serrno = ERTUSINTR;
          local_severity = RTCP_FAILED|RTCP_USERR;
        } else {
          local_severity = RTCP_FAILED|RTCP_SYERR;
        }
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_sendReqList() rtcp_RecvAckn(FILE_REQ): %s\n",
                 sstrerror(save_serrno)
                 );
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "rtcp_RecvAckn(FILE_REQ) error",
                                        local_severity,
                                        rc
                                        );
        break;
      }
    } CLIST_ITERATE_END(tl->file,fl);
    if ( rc == -1 || rtcpc_killed != 0 ) break;
  } CLIST_ITERATE_END(tape,tl);

  if ( rc == -1 || rtcpc_killed != 0 ) {
    hdr->reqtype = RTCP_ABORT_REQ;
    (void)rtcpc_finished(
                         socks,
                         hdr,
                         tape
                         );
    serrno = save_serrno;
    return(rc);
  }

  return(0);
}

void *rtcpc_processReqUpdate(void *arg) 
{
  rtcpcThrData_t *thr;
  tape_list_t *tape, *tl;
  file_list_t *fl;
  rtcpHdr_t tmpHdr, *hdr;
  rtcpTapeRequest_t *tapereq;
  rtcpFileRequest_t *filereq, fltmp;
  SOCKET s;
  int check_more_work, rc = 0, retval = 0, save_serrno= 0, reqID, dumptape;
  char wakeUpMsg[1] = "";

  if ( arg == NULL ) {
    rtcp_log(LOG_ERR,"rtcpc_processReqUpdate() called with NULL argument\n");
    return((void *)&failed);
  }
  
  thr = (rtcpcThrData_t *)arg;
  hdr = &thr->hdr;
  tape = thr->tape;
  tapereq = &thr->tapereq;
  filereq = &thr->filereq;
  s = thr->s;
  reqID = thr->reqID;
  dumptape = thr->dumptape;
  rtcp_log(LOG_DEBUG,"rtcpc_processReqUpdate() called with socket %d, request 0x%x\n",
           s,hdr->reqtype);

  /* Dummy loop to break out from */
  for (;;) {
    if ( dumptape == TRUE ) {
      tl = tape;
      if ( rtcp_NewFileList(&tl,&fl,WRITE_DISABLE) == -1 ) {
        if ( save_serrno <= 0 ) save_serrno = serrno;
        retval = -1;
        break;
      }
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): file list updated by DUMPTAPE\n"
               );
      fl->filereq = *filereq;
      break;
    }
    check_more_work = 0;
    if ( filereq->proc_status == RTCP_REQUEST_MORE_WORK ) {
      check_more_work = 1;
      fltmp = *filereq;
    }            
    tmpHdr = *hdr;
    do {
      if ( check_more_work == 1 )  *filereq = fltmp;
      if ( rtcpc_UpdateProcStatus(
                                  tape,
                                  NULL,
                                  filereq,
                                  dumptape
                                  ) == -1 ) {
        hdr->reqtype = RTCP_ABORT_REQ;
        if ( save_serrno <= 0 ) save_serrno = serrno;
        retval = -1;
        break;
      }
      if ( filereq->cprc != 0 ) {
        rtcp_log(
                 LOG_DEBUG,
                 "rtcpc_processReqUpdate(): FILE request RC=%d, severity=0x%x, serrno=%d, max_tpretry=%d, max_cpretry=%d\n",
                 filereq->cprc,
                 filereq->err.severity,
                 filereq->err.errorcode,
                 filereq->err.max_tpretry,
                 filereq->err.max_cpretry
                 );
        if ( save_serrno <= 0 ) save_serrno = filereq->err.errorcode;
        retval = -1;
      }
      if ( (check_more_work == 1) && 
           ((filereq->proc_status == RTCP_WAITING) ||
            (filereq->proc_status == RTCP_REQUEST_MORE_WORK)) ) {
        if ( filereq->proc_status == RTCP_WAITING ) {
          UPPERCASE(filereq->fid);
          filereq->err.severity = filereq->err.severity & 
            ~RTCP_RESELECT_SERV;
          filereq->cprc = 0;
          filereq->err.severity = RTCP_OK;
          *filereq->err.errmsgtxt = '\0';
          if ( filereq->proc_status == RTCP_REQUEST_MORE_WORK )
            filereq->concat = NOCONCAT;
          filereq->VolReqID = reqID;
          if ( filereq->umask <= 0 )
            (void) umask(filereq->umask = (int) umask(0));
          rtcp_log(
                   LOG_DEBUG,
                   "rtcpc_processReqUpdate(): add new file request (%s)\n",
                   filereq->file_path
                   );
        } else {
          rtcp_log(
                   LOG_DEBUG,
                   "rtcpc_processReqUpdate(): add new file request placeholder for more work\n"
                   );
        }
        
        tmpHdr.reqtype = RTCP_FILEERR_REQ;
        tmpHdr.len = -1;
        
        rc = rtcp_SendReq(
                          &s,
                          &tmpHdr,
                          NULL,
                          NULL,
                          filereq
                          );
        if ( rc == -1 ) {
          retval = -1;
          if ( save_serrno <= 0 ) save_serrno = serrno;
          rtcp_log(
                   LOG_ERR,
                   "rtcpc_runReq() rtcp_SendReq(): %s\n",
                   sstrerror(serrno)
                   );
          break;
        }
        rtcp_log(
                 LOG_DEBUG,
                 "rtcpc_processReqUpdate(): receive acknowledge\n"
                 );
        rc = rtcp_RecvAckn(
                           &s,
                           tmpHdr.reqtype
                           );
        if ( rc == -1 ) {
          retval = -1;
          if ( save_serrno <= 0 ) save_serrno = serrno;
          rtcp_log(
                   LOG_ERR,
                   "rtcpc_runReq() rtcp_RecvReq(FILE_REQ): %s\n",
                   sstrerror(serrno)
                   );
          break;
        }
      }
    } while ( (retval != -1 ) && (check_more_work == 1) && 
              (filereq->proc_status == RTCP_WAITING) );
    if ( check_more_work == 1 ) {
      tmpHdr.magic = RTCOPY_MAGIC;
      tmpHdr.reqtype = RTCP_NOMORE_REQ;
      tmpHdr.len = -1;
      rtcp_log(LOG_DEBUG,"rtcpc_processReqUpdate(): send end of request\n");
      if ( rtcp_SendReq(
                        &s,
                        &tmpHdr,
                        NULL,
                        NULL,
                        NULL
                        ) == -1 ) {
        retval = -1;
        if ( save_serrno <= 0 ) save_serrno = serrno;
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_processReqUpdate() rtcp_SendReq(RTCP_NOMORE_REQ): %s\n",
                 sstrerror(serrno)
                 );
      } else {
        rtcp_log(
                 LOG_DEBUG,
                 "rtcpc_processReqUpdate(): receive EOR acknowledge\n"
                 );
        if ( rtcp_RecvAckn(
                           &s,
                           tmpHdr.reqtype
                           ) == -1 ) {
          retval = -1;
          if ( save_serrno <= 0 ) save_serrno = serrno;
          rtcp_log(
                   LOG_ERR,
                   "rtcpc_processReqUpdate() rtcp_Recvackn(RTCP_NOMORE_REQ): %s\n",
                   sstrerror(serrno)
                   );
        }
      }
    }
    break;
  } /* for (;;) */
  
  rtcp_log(
           LOG_DEBUG,
           "rtcpc_processReqUpdate(): send 0x%x request acknowledge\n",
           hdr->reqtype
           );
  if ( rtcp_SendAckn(
                     &s,
                     hdr->reqtype
                     ) == -1 ) {
    retval = -1;
    if ( save_serrno <= 0 ) save_serrno = serrno;
    rtcp_log(
             LOG_ERR,
             "rtcpc_processReqUpdate() rtcp_SendAckn(): %s\n",
             sstrerror(serrno)
             );
  }
  
  (void)Cmutex_lock(activeThreads,-1);
  if ( retval == -1 ) {
    thr->th_serrno = save_serrno;
    thr->status = -1;
  }
  thr->finished = 1;
  (void)Cmutex_unlock(activeThreads);
  if ( thr->pipe != INVALID_SOCKET ) {
    rtcp_log(LOG_DEBUG,"rtcpc_processReqUpdate(): write to internal pipe %d\n",
             thr->pipe);
    (void)write(thr->pipe,wakeUpMsg,sizeof(wakeUpMsg));
  }
  rtcp_log(LOG_DEBUG,"rtcpc_processReqUpdate(): processing finished\n");
  return((void *)&success);
}

int rtcpc_internalDispatchRoutine(void *(routine)(void *), void *arg) {
  void *ret;
  int rc = 0;
  
  ret = routine(arg);
  if ( ret != NULL ) rc = *(int *)ret;
  return(rc);
}



int rtcpc_runReq_ext(
                     rtcpHdr_t *hdr,
                     rtcpc_sockets_t **socks,
                     tape_list_t *tape,
                     int (*dispatchRoutine)(void *(*)(void *), void *)
                     ) {
  int rc, i, found, socketIndex, reqID;
  char wakeUpMsg[1];
  SOCKET internalPipe[2] = {INVALID_SOCKET, INVALID_SOCKET};
  rtcpcThrData_t *thr = NULL;
  rtcpTapeRequest_t *tapereq, tpreq;
  rtcpFileRequest_t *filereq, flreq;
  char tpdumpbuf[CA_MAXLINELEN+1];
  int retval = 0;
  int save_serrno = 0;
  int dumptape = FALSE;
  fd_set rd_set, wr_set, exc_set, rd_set_save, wr_set_save, exc_set_save;
  int maxfd;
  struct timeval t_out;
  int (*internalDispatchRoutine)(void *(*)(void *), void *) = 
    rtcpc_internalDispatchRoutine;

  reqID = tape->tapereq.VolReqID;
  if ( dispatchRoutine != NULL ) internalDispatchRoutine = dispatchRoutine;
  /*
   * Keep the accept socket for sending abort if necessary
   */
  for (i=0; i<100; i++) (*socks)->proc_socket[i] = NULL;
  if ( dispatchRoutine != NULL ) {
    rc = pipe(internalPipe);
    if ( rc == -1 ) {
      save_serrno = errno;
      rtcp_log(LOG_ERR,"rtcpc_runReq() pipe(): %s\n",strerror(save_serrno));
      (void)rtcpc_SetLocalErrorStatus(
                                      tape,
                                      save_serrno,
                                      "pipe() error",
                                      RTCP_FAILED|RTCP_SYERR,
                                      retval
                                      );
      serrno = save_serrno;
      return(-1);
    }
  }
  socketIndex = -1;
  for (;;) {
    maxfd = 0;
    FD_ZERO(&rd_set);
    FD_ZERO(&wr_set);
    FD_ZERO(&exc_set);
    FD_SET(*((*socks)->listen_socket),&rd_set);
    if ( (*socks)->abort_socket != INVALID_SOCKET ) 
      FD_SET((*socks)->abort_socket,&rd_set);
    if ( internalPipe[0] != INVALID_SOCKET )
      FD_SET(internalPipe[0],&rd_set);
#if !defined(_WIN32)
    maxfd = (*socks)->abort_socket;
    if ( (*socks)->abort_socket < *((*socks)->listen_socket) ) 
      maxfd = *((*socks)->listen_socket);
    if ( maxfd < internalPipe[0] ) maxfd = internalPipe[0];
#endif /* !_WIN32 */
    for (i=0; i<100; i++) {
      if ( (*socks)->proc_socket[i] != NULL &&
           *(*socks)->proc_socket[i] != INVALID_SOCKET ) {
        FD_SET(*((*socks)->proc_socket[i]),&rd_set);
#if !defined(_WIN32)
        if (*((*socks)->proc_socket[i]) > maxfd) 
          maxfd = *((*socks)->proc_socket[i]);
#endif /* !_WIN32 */
      }
    }
    exc_set = rd_set;
    maxfd++;
    rtcp_log(LOG_DEBUG,"rtcpc_runReq(): wait for connection\n");
    rd_set_save = rd_set;
    wr_set_save = wr_set;
    exc_set_save = exc_set;
    rc = 0;
    while ( rc == 0 && rtcpc_killed == 0 ) {
      t_out.tv_sec = (time_t)30;
      t_out.tv_usec = 0;
      rd_set = rd_set_save;
      wr_set = wr_set_save;
      exc_set = exc_set_save;
      rc = select(maxfd,&rd_set,&wr_set,&exc_set,&t_out);
      if ( rc == 0 ) {
        hdr->magic = RTCOPY_MAGIC;
        hdr->reqtype = RTCP_PING_REQ;
        hdr->len = 0;
        rtcp_log(
                 LOG_DEBUG,
                 "rtcpc_runReq(): ping server on socket %d\n",
                 (*socks)->abort_socket
                 );
        rc = rtcp_SendReq(
                          &((*socks)->abort_socket),
                          hdr,
                          NULL,
                          NULL,
                          NULL
                          );
        rtcp_log(LOG_DEBUG,"rtcpc_runReq(): ping returned %d\n",rc);
        if ( rc == -1 ) break;
        rc = 0;
      } 
    }

    if ( rtcpc_killed != 0 ) {
      retval = -1;
      save_serrno = ERTUSINTR;
      break;
    }
    if ( rc < 0 ) {
      if ( save_serrno <= 0 ) save_serrno = serrno;
      rtcp_log(
               LOG_ERR,
               "rtcpc_runReq() select(), rc=%d: %s\n",
               rc,
               sstrerror(errno)
               );
      if ( retval != -1 ) {
        retval = -1;
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "select() error",
                                        RTCP_FAILED|RTCP_SYERR,
                                        retval
                                        );
      }
      break;
    }
    if ( FD_ISSET(*((*socks)->listen_socket),&rd_set) ) {
      /*
       * New connection. We don't assume there is a message
       * on the new socket.
       */
      rc = rtcp_Listen(
                       *((*socks)->listen_socket),
                       &((*socks)->accept_socket),
                       -1,
		       RTCP_ACCEPT_FROM_SERVER
                       );
      if ( rc == -1 || (*socks)->accept_socket == INVALID_SOCKET ) {
        if ( save_serrno <=0 ) save_serrno = serrno;
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_runReq() rtcp_Listen(): %s\n",
                 sstrerror(serrno)
                 );
        if ( retval != -1 ) {
          retval = -1;
          (void)rtcpc_SetLocalErrorStatus(
                                          tape,
                                          save_serrno,
                                          "rtcp_Listen() error",
                                          RTCP_FAILED|RTCP_SYERR,
                                          retval
                                          );
        }
        break;
      }
      /*
       * At this stage we only authorise connections from the
       * selected tape server and authorised hosts.
       */
      rc = rtcp_CheckConnect(
                             &((*socks)->accept_socket),
                             tape
                             );
      if ( rc != 1 ) {
        rtcp_log(LOG_ERR,"rtcpc_runReq() dropping unauthorised connection\n");
        (void)rtcp_CloseConnection(
                                   &((*socks)->accept_socket)
                                   );
        continue;
      }

      for (i=0; i<100; i++) if ( (*socks)->proc_socket[i] == NULL ) break;
      (*socks)->proc_socket[i] = (SOCKET *)malloc(sizeof(SOCKET));
      if ( (*socks)->proc_socket[i] == NULL ) {
        if ( save_serrno <= 0 ) save_serrno = errno;
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_runReq() malloc(): %s\n",
                 sstrerror(errno)
                 );
        retval = -1;
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "malloc() error",
                                        RTCP_FAILED|RTCP_SYERR,
                                        retval
                                        );
        break;
      }
      *((*socks)->proc_socket[i]) = (*socks)->accept_socket;
      socketIndex = i;
      (*socks)->nb_proc_sockets++;
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): new connection, socket = %d, nb sockets=%d\n",
               (*socks)->accept_socket,
               (*socks)->nb_proc_sockets
               );
      continue;
    } else if ( (*socks)->abort_socket != INVALID_SOCKET &&
                FD_ISSET((*socks)->abort_socket,&rd_set) ) {
      (*socks)->accept_socket = (*socks)->abort_socket;
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): data on abort socket %d\n",
               (*socks)->accept_socket
               );
    } else if ( internalPipe[0] != INVALID_SOCKET &&
                FD_ISSET(internalPipe[0],&rd_set) ) {
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): event on internal pipe %d\n",
               internalPipe[0]
               );
      /*
       * Internal pipe is only used for synchronisation. No relevant data is passed
       */
      (void)read(internalPipe[0],wakeUpMsg,sizeof(wakeUpMsg));
      (void)Cmutex_lock(activeThreads,-1);
      for (;;) {
        found = 0;
        CLIST_ITERATE_BEGIN(activeThreads,thr) {
          if ( thr->finished == 1 ) {
            found = 1;
            if ( thr->status == -1 ) {
              retval = -1;
              if ( save_serrno <= 0 ) save_serrno = thr->th_serrno;
            }
            i = thr->index;
            if ( i>=0 ) *((*socks)->proc_socket[i]) = thr->s;
            CLIST_DELETE(activeThreads,thr);
            free(thr);
            break;
          }
        } CLIST_ITERATE_END(activeThreads,thr);
        if ( found == 0 ) break;
      }
      (void)Cmutex_unlock(activeThreads);
      if ( retval == -1 ) break;
      continue;
    } else {
      for (i=0; i<100; i++) {
        if ( (*socks)->proc_socket[i] != NULL && 
             *((*socks)->proc_socket[i]) != INVALID_SOCKET && 
             FD_ISSET(*((*socks)->proc_socket[i]),&rd_set) ) {
          (*socks)->accept_socket = *((*socks)->proc_socket[i]);
          socketIndex = i;
          break;
        }
      }
    }
    (void)rtcpc_InitReqStruct(
                              &tpreq,
                              &flreq
                              );
    rtcp_log(
             LOG_DEBUG,
             "rtcpc_runReq(): receive data on socket %d\n",
             (*socks)->accept_socket
             );
    rc = rtcp_RecvReq(
                      &((*socks)->accept_socket),
                      hdr,
                      NULL,
                      &tpreq,
                      &flreq
                      );
    if ( rc == -1 ) {
      if ( save_serrno <= 0 ) save_serrno = serrno;
      rtcp_log(
               LOG_ERR,
               "rtcpc_runReq() rtcp_RecvReq(): %s\n",
               sstrerror(serrno)
               );
      if ( retval != -1 ) {
        retval = -1;
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "rtcp_RecvReq() error",
                                        RTCP_FAILED|RTCP_SYERR,
                                        retval
                                        );
      }
      break;
    }

    tapereq = &tpreq;
    filereq = &flreq;
    switch (hdr->reqtype) {
    case RTCP_DUMP_REQ: 
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): monitoring request on socket %d\n",
               (*socks)->accept_socket
               );
      rtcpc_DumpRequest(
                        &((*socks)->accept_socket),
                        tape
                        );
      (void)rtcp_CloseConnection(
                                 &((*socks)->accept_socket)
                                 );
      free((*socks)->proc_socket[i]);
      (*socks)->proc_socket[i] = NULL;
      (*socks)->nb_proc_sockets--;
      break;
    case RTCP_KILLJID_REQ :
    case RTCP_RSLCT_REQ:
      rtcp_log(LOG_ERR,"rtcpc_runReq(): %s request by operator\n",
               (hdr->reqtype==RTCP_KILLJID_REQ ? "KILLJID":"RSLCT"));
      retval = -1;
      save_serrno = ERTOPINTR;
      if ( hdr->reqtype == RTCP_KILLJID_REQ ) {
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "request killed by operator",
                                        RTCP_FAILED|RTCP_SYERR,
                                        retval
                                        );
      } else {
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "re-select server request by operator",
                                        RTCP_RESELECT_SERV,
                                        retval
                                        );
      }
      (void)rtcp_SendAckn(
                          &((*socks)->accept_socket),
                          hdr->reqtype
                          );
      hdr->magic = RTCOPY_MAGIC;
      (void)rtcp_SendReq(
                         &((*socks)->abort_socket),
                         hdr,
                         NULL,
                         NULL,
                         NULL
                         );
      (void)rtcp_CloseConnection(
                                 &((*socks)->accept_socket)
                                 );
      (void)rtcp_CloseConnection(
                                 &((*socks)->abort_socket)
                                 );
      free((*socks)->proc_socket[i]);
      (*socks)->proc_socket[i] = NULL;
      (*socks)->nb_proc_sockets--;
      (*socks)->abort_socket = INVALID_SOCKET;
      break;
    case RTCP_ENDOF_REQ :
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): end of request received on socket %d\n",
               (*socks)->accept_socket
               );
      (void)rtcp_SendAckn(
                          &((*socks)->accept_socket),
                          hdr->reqtype
                          );
      (void)rtcp_CloseConnection(
                                 &((*socks)->accept_socket)
                                 );
      free((*socks)->proc_socket[i]);
      (*socks)->proc_socket[i] = NULL;
      (*socks)->nb_proc_sockets--;
      break;
    case RTCP_TAPE_REQ :
      tapereq->err.severity = RTCP_OK;
    case RTCP_TAPEERR_REQ :
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): TAPE request update received on socket %d, unit=%s\n",
               (*socks)->accept_socket,
               tapereq->unit
               );
      (void) rtcpc_UpdateProcStatus(
                                    tape,
                                    tapereq,
                                    NULL,
                                    dumptape
                                    );
      if ( tapereq->tprc != 0 ) {
        rtcp_log(
                 LOG_DEBUG,
                 "rtcpc_runReq(): TAPE request RC=%d, severity=0x%x, serrno=%d, max_tpretry=%d, max_cpretry=%d\n",
                 tapereq->tprc,
                 tapereq->err.severity,
                 tapereq->err.errorcode,
                 tapereq->err.max_tpretry,
                 tapereq->err.max_cpretry
                 );
        if ( !(tapereq->err.severity & RTCP_OK) ) {
          if ( save_serrno <= 0 ) save_serrno = tapereq->err.errorcode;
          retval = -1;
        }
      }
      break;
    case RTCP_FILE_REQ :
      filereq->err.severity = RTCP_OK;
    case RTCP_FILEERR_REQ :
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): FILE request update received on socket %d\n",
               (*socks)->accept_socket
               );
      thr = (rtcpcThrData_t *)calloc(1,sizeof(rtcpcThrData_t));
      if ( thr == NULL ) {
        if ( save_serrno <= 0 ) save_serrno = errno;
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_runReq() malloc(): %s\n",
                 sstrerror(errno)
                 );
        retval = -1;
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "malloc() error",
                                        RTCP_FAILED|RTCP_SYERR,
                                        retval
                                        );
        break;
      }
      thr->hdr = *hdr;
      thr->tape = tape;
      thr->tapereq = tpreq;
      thr->filereq = flreq;
      thr->reqID = reqID;
      thr->dumptape = dumptape;
      thr->pipe = internalPipe[1];
      thr->s = (*socks)->accept_socket;
      thr->index = socketIndex;
      if ( (dispatchRoutine != NULL) && (socketIndex >= 0) )
        *((*socks)->proc_socket[socketIndex]) = INVALID_SOCKET;
      (void)Cmutex_lock(activeThreads,-1);
      CLIST_INSERT(activeThreads,thr);
      (void)Cmutex_unlock(activeThreads);
      rc = internalDispatchRoutine(rtcpc_processReqUpdate,(void *)thr);
      if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"rtcpc_internalDispatch() failed: %s\n",
                 sstrerror(serrno));
        retval = -1;
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "rtcpc_internalDispatch() error",
                                        RTCP_FAILED|RTCP_SYERR,
                                        retval
                                        );
        break;
      }
      if ( rc == -1 ) retval = -1;
      break;
    case GIVE_OUTP:
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): output request received on socket %d\n",
               (*socks)->accept_socket
               );
      if ( hdr->len > CA_MAXLINELEN ) {
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_runReq(): print buffer overflow\n"
                 );
        rc = retval = -1;
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "print buffer overflow",
                                        RTCP_FAILED|RTCP_SYERR,
                                        retval
                                        );
        break;
      }
      rc = rtcp_GetMsg(
                       &((*socks)->accept_socket),
                       tpdumpbuf,
                       hdr->len
                       );
      if ( rc == -1 ) {
        if ( save_serrno <= 0 ) save_serrno = serrno;
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_runReq() rtcp_GetMsg(): %s\n",
                 sstrerror(serrno)
                 );
        retval = -1;
        (void)rtcpc_SetLocalErrorStatus(
                                        tape,
                                        save_serrno,
                                        "rtcp_GetMsg() error",
                                        RTCP_FAILED|RTCP_SYERR,
                                        retval
                                        );
        break;
      }
      /*
       * Use LOG_NOTICE to distinguish dump text from normal info.
       * messages.
       */
      rtcp_log(
               LOG_NOTICE,
               "%s",
               tpdumpbuf
               );
      break;
    default:
      rtcp_log(
               LOG_INFO,
               "INTERNAL ERROR: UNKNOWN REQUEST TYPE %d\n",
               hdr->reqtype
               );
      break;
    }
    if ( hdr->reqtype != GIVE_OUTP && hdr->reqtype != RTCP_ENDOF_REQ &&
         hdr->reqtype != RTCP_KILLJID_REQ && hdr->reqtype != RTCP_RSLCT_REQ &&
         hdr->reqtype != 0 &&
         (dumptape == 1 || (hdr->reqtype != RTCP_FILE_REQ &&
                            hdr->reqtype != RTCP_FILEERR_REQ)) ) {
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): send 0x%x request acknowledge\n",
               hdr->reqtype
               );
      rc = rtcp_SendAckn(
                         &((*socks)->accept_socket),
                         hdr->reqtype
                         );
      if ( rc == -1 ) {
        if ( save_serrno <= 0 ) save_serrno = serrno;
        rtcp_log(
                 LOG_ERR,
                 "rtcpc_runReq() rtcp_SendAckn(): %s\n",
                 sstrerror(serrno)
                 );
        if ( retval != -1 ) {
          retval = -1;
          (void)rtcpc_SetLocalErrorStatus(
                                          tape,
                                          save_serrno,
                                          "rtcp_SendAckn() error",
                                          RTCP_FAILED|RTCP_SYERR,
                                          retval
                                          );
        }
        break;
      }
    }

    rtcp_log(
             LOG_DEBUG,
             "rtcpc_runReq(): rc = %d, nb_proc_sockets = %d, tape->file=0x%lx reqtype=0x%x\n",
             rc,
             (*socks)->nb_proc_sockets,
             tape->file,hdr->reqtype
             );
    if ( rc == -1 ) break;
    /*
     * Dumptape request ends with RTCP_ENDOF_REQ whereas normal
     * requests end when all file requests have been satisfied. 
     */
    if ( dumptape == TRUE ) { 
      if ( hdr->reqtype == RTCP_ENDOF_REQ ) break;
    } else if ( rtcpc_EndOfRequest(tape) == 1 && 
                (*socks)->nb_proc_sockets == 0) break;
  } /* for (;;) */

  if ( rtcpc_killed == 0 ) {
    if ( retval == -1 ) {
      hdr->reqtype = RTCP_ABORT_REQ;
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): Send ABORT\n"
               );
      (void)rtcpc_finished(
                           socks,
                           hdr,
                           tape
                           );
    } else {
      hdr->reqtype = RTCP_ENDOF_REQ;
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_runReq(): Send End of Request\n"
               );
      retval = rtcpc_finished(
                              socks,
                              hdr,
                              tape
                              );
      if ( retval == -1 ) {
        if ( save_serrno <= 0 ) save_serrno = serrno;
      } else {
        retval = 0;
      }
    }
  }

  if ( retval == -1 && save_serrno > 0 ) serrno = save_serrno;
  return(retval);
}

/** rtcpc_runReq() - send RTCOPY request to server and process callbacks up to end of request
 *
 * @param rtcpc_socket_t *socks - RTCOPY client socket structure initialised by rtcpc_InitReq()
 * @param tape_list_t *tape - pointer to head tape element of an RTCOPY request list
 * @param char *realVID - real VID in case the volume is VIDMAPped in TMS (soon obsolete)
 */
int rtcpc_runReq(
                 rtcpHdr_t *hdr,
                 rtcpc_sockets_t **socks,
                 tape_list_t *tape
                 ) {
  return(rtcpc_runReq_ext(hdr,socks,tape,NULL));
}

int rtcpc_CheckRetry(tape_list_t *tape) {
  tape_list_t *tl;
  file_list_t *fl;

  if ( tape == NULL ) return(FALSE);
  /*
   * Make sure nothing failed seriously
   */
  CLIST_ITERATE_BEGIN(tape,tl) {
    if ( (tl->tapereq.err.severity & RTCP_FAILED) != 0 ) return(FALSE);
    CLIST_ITERATE_BEGIN(tl->file,fl) {
      if ( (fl->filereq.err.severity & RTCP_FAILED) != 0 ) return(FALSE);
    } CLIST_ITERATE_END(tl->file,fl);
  } CLIST_ITERATE_END(tape,tl);
  /*
   * Now check if a retry is possible
   */
  CLIST_ITERATE_BEGIN(tape,tl) {
    if ( (tl->tapereq.err.severity & RTCP_RESELECT_SERV) != 0 &&
         (tl->tapereq.err.severity & RTCP_FAILED) == 0 ) return(TRUE);
    CLIST_ITERATE_BEGIN(tl->file,fl) {
      if ( (fl->filereq.err.severity & RTCP_RESELECT_SERV) != 0 &&
           (fl->filereq.err.severity & RTCP_FAILED) == 0 ) return(TRUE);
    } CLIST_ITERATE_END(tl->file,fl);
  } CLIST_ITERATE_END(tape,tl);
  return(FALSE);
}

void rtcpc_FreeReqLists(tape_list_t **tape) {
  tape_list_t *tl;
  file_list_t *fl;

  if ( tape == NULL ) return;

  while ( *tape != NULL ) {
    tl = *tape;
    fl = tl->file;
    while ( tl->file != NULL ) {
      CLIST_DELETE(tl->file,fl);
      rtcp_log(
               LOG_DEBUG,
               "rtcpc_FreeReqLists(): free file list at 0x%lx\n",
               fl
               );
      free(fl);
      fl = tl->file;
    }
    CLIST_DELETE(*tape,tl);
    rtcp_log(
             LOG_DEBUG,
             "rtcpc_FreeReqLists(): free tape list at 0x%lx\n",
             tl
             );
    free(tl);
  }
  return;
}

int rtcpc_validCksumAlg(char *cksumAlg) {
  char validCksumAlgs[][CA_MAXCKSUMNAMELEN+1] = RTCP_VALID_CKSUMS;
  int i;

  if ( cksumAlg == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  /* No checksum is OK */
  if ( *cksumAlg == '\0' ) return(1);
  for (i=0; *validCksumAlgs[i] != '\0'; i++) {
    if ( strcmp(validCksumAlgs[i],cksumAlg) == 0 ) return(1);
  }
  return(0);
}

