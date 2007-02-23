/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpd_stageupdc.c - RTCOPY interface to stageupdc
 */

#include <stdlib.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
#include <stdlib.h>
#include <stdio.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#endif /* _WIN32 */

#include <signal.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/stat.h>

#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <Ctape_api.h>
#include <stage.h>
#include <Cuuid.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <serrno.h>
#include <stage_api.h>

extern processing_cntl_t proc_cntl;

/*
 * Global variable needed to flag that an ENOSPC has been sent to the
 * stager and thus no further stageupdc calls should take place until
 * next retry with a new file. This flag is reset in MainCntl after
 * having waited for completion of all subrequests up to the ENOSPC
 * failure. This total synchronisation is needed for SHIFT stagers since
 * they expect all stageupdc in sequence.
 */
extern int ENOSPC_occurred; 

void rtcp_stglog(int type, char *msg) {
    if ( type == MSG_ERR ) rtcp_log(LOG_ERR,msg);
    return;
}

int rtcpd_init_stgupdc(tape_list_t *tape) {
    int rc = 0;

    if ( tape == NULL || tape->file == NULL ||
         *tape->file->filereq.stageID == '\0' ) return(0);

    rc = stage_setlog(rtcp_stglog);
    return(rc); 
}

/*
 * This routine is needed to serialize the stageupdc calls to SHIFT
 * stagers (i.e. filereq->stageSubreqID == -1 ). They expect the stageupdc
 * calls to come in sequence: tppos(file1) - filcp(file1) - tppos(file2) -...
 */
int rtcpd_LockForTpPos(const int lock) {
    static int nb_waiters = 0;
    static int next_entry = 0;
    static int *wait_list = NULL;

    rtcp_log(LOG_DEBUG,"rtcpd_LockForTpPos(%d), current_lock=%d\n",
             lock,proc_cntl.TpPos);
    return(rtcpd_SerializeLock(lock,&proc_cntl.TpPos,proc_cntl.TpPos_lock,
                               &nb_waiters,&next_entry,&wait_list));
}

int rtcpd_stageupdc(tape_list_t *tape,
                    file_list_t *file) {
    int rc, status, retval, save_serrno, WaitTime, TransferTime;
    file_list_t *fl;
    rtcpFileRequest_t *filereq;
    rtcpTapeRequest_t *tapereq;
    char newpath[CA_MAXPATHLEN+1];
    char recfm[10];   /* Record format (could include ,bin or ,-f77) */
    u_signed64 nb_bytes;
    int retry = 0, tpPosCalled = 0;

    if ( tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    filereq = &file->filereq;
    tapereq = &tape->tapereq;

    rc = save_serrno = retval = 0;
    *newpath = '\0';
    /*
     * Check if this is a stage request
     */
    if ( filereq == NULL || *filereq->stageID == '\0' ) return(0);
    WaitTime = filereq->TEndPosition - tapereq->TStartRequest;
    TransferTime = max((time_t)filereq->TEndTransferDisk,
                       (time_t)filereq->TEndTransferTape) -
                       (time_t)filereq->TEndPosition;
    if ( WaitTime < 0 ) WaitTime = 0;
    if ( TransferTime < 0 ) TransferTime = 0;

    strcpy(recfm,filereq->recfm);
    if ( (filereq->convert & NOF77CW) != 0 ) {
        if ( *filereq->recfm == 'U' ) strcat(recfm,",bin");
        else if ( *filereq->recfm == 'F' ) strcat(recfm,",-f77");
    }


    rtcp_log(LOG_DEBUG,"rtcpd_stageupdc(): fseq=%d, status=%d, concat=%d, err=%d\n",
             filereq->tape_fseq,filereq->proc_status,filereq->concat,filereq->err.errorcode);

    for ( retry=0; retry<RTCP_STGUPDC_RETRIES; retry++ ) {
        status = filereq->err.errorcode;
        /*
         * Local status variable is used for tppos only (filcp uses the local
         * variable "retval"). It cannot take any other values than 0 or
         * ETFSQ (invalid tape file sequence number). In case there was
         * a warning while copying the data from tape to memory we must
         * reset for small files (<128MB) the entire file may be already
         * in memory when the disk IO starts up.
         */
        if ( status == ERTLIMBYSZ || status == ERTBLKSKPD ||
             status == ERTMNYPARY ) status = 0;
        if ( filereq->proc_status == RTCP_POSITIONED && status == 0 ||
             (filereq->proc_status == RTCP_FINISHED && 
              (filereq->concat & NOCONCAT_TO_EOD) != 0 && status == ETFSQ) ) { 

            if ( retry == 0 && filereq->stageSubreqID == -1 &&
                 (rtcpd_LockForTpPos(1) == -1) ) {
                rtcp_log(LOG_ERR,"rtcpd_stageupdc() rtcpd_LockForTpPos(1): returned without lock (serrno=%d, errno=%d)\n",
                         serrno,errno);
                return(-1);
            }
            if ( ENOSPC_occurred == TRUE ) {
                if ( filereq->stageSubreqID == -1 )
                    (void)rtcpd_LockForTpPos(0);
                return(0);
            }

            if ( (rtcpd_CheckProcError() & (RTCP_FAILED | 
                                            RTCP_RESELECT_SERV)) != 0 ) {
                 if ( filereq->stageSubreqID == -1 ) 
                     (void)rtcpd_LockForTpPos(0);
                 return(0);
            }
            tpPosCalled = 1;

            rtcp_log(LOG_DEBUG,"rtcpd_stageupdc() stage_updc_tppos(%s,%d,%d,%d,%s,%s,%d,%d,%s,%s)\n",
                     filereq->stageID,
                     filereq->stageSubreqID,
                     status,
                     filereq->blocksize,
                     tapereq->unit,
                     filereq->fid,
                     filereq->tape_fseq,
                     filereq->recordlength,
                     recfm,
                     newpath);

            rc = stage_updc_tppos(filereq->stageID,
                                  filereq->stageSubreqID,
                                  status,
                                  filereq->blocksize,
                                  tapereq->unit,
                                  filereq->fid,
                                  filereq->tape_fseq,
                                  filereq->recordlength,
                                  recfm,
                                  newpath);
            save_serrno = serrno;
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"rtcpd_stageupdc() stage_updc_tppos(): %s\n",
                         sstrerror(save_serrno));
                switch (save_serrno) {
                case EINVAL:
                    rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED|RTCP_USERR);
                    if ( filereq->stageSubreqID == -1 ) {
                        rtcpd_SetProcError(RTCP_FAILED|RTCP_USERR);
                        (void)rtcpd_LockForTpPos(0);
                    }
                    serrno = save_serrno;
                    return(-1);
                case SECOMERR:
                case SESYSERR:
                    break;
                default:
                    if ( save_serrno == 0 ) save_serrno = SEINTERNAL;
                    rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED|RTCP_SYERR);
                    if ( filereq->stageSubreqID == -1 ) {
                        rtcpd_SetProcError(RTCP_FAILED|RTCP_SYERR);
                        (void)rtcpd_LockForTpPos(0);
                    }
                    serrno = save_serrno;
                    return(-1);
                }
                continue;
            } 
            if ( (filereq->concat & CONCAT_TO_EOD) != 0 ||
                 ((file->next->filereq.concat & CONCAT) != 0 &&
                   file->next != tape->file) ) {
                if ( filereq->stageSubreqID == -1 ) (void)rtcpd_LockForTpPos(0);
            }
        } 

        nb_bytes = 0;
        if ( tapereq->mode == WRITE_ENABLE ) nb_bytes = filereq->bytes_in;
        else {
            if ( (filereq->concat & (CONCAT|CONCAT_TO_EOD)) == 0 ) {
                nb_bytes = filereq->bytes_out;
            } else {
                /*
                 * If concatenating to disk we have to give the total
                 * size (stager only deals with disk files).
                 */
                fl = file;
                while ( (fl->filereq.concat & (CONCAT|CONCAT_TO_EOD)) != 0 ) {
                    nb_bytes += fl->filereq.bytes_out;
                    if ( fl == tape->file ) break;
                    fl = fl->prev;
                } 
                /*
                 * First file in a normal concatenation is not "concatenated".
                 */
                if ( (filereq->concat & CONCAT) != 0 ) 
                    nb_bytes += fl->filereq.bytes_out;
            }
        }

        if ( (filereq->proc_status == RTCP_POSITIONED &&
              ((filereq->err.errorcode == ENOSPC && 
                tapereq->mode == WRITE_DISABLE) ||
               (filereq->err.errorcode != 0 && filereq->err.max_cpretry<=0))) || 
             (filereq->proc_status == RTCP_FINISHED && 
              (nb_bytes > 0 || (filereq->concat & CONCAT_TO_EOD) != 0) &&  
              ((file->next->filereq.concat & CONCAT) == 0 ||
                file->next == tape->file)) ) {

            if ( (rtcpd_CheckProcError() & (RTCP_FAILED | 
                                            RTCP_RESELECT_SERV)) != 0 ) {
                 if ( filereq->stageSubreqID == -1 ) 
                     (void)rtcpd_LockForTpPos(0);
                 return(0);
            }

            /*
             * Always give the return code
             */
            retval = 0;
            if ( filereq->err.errorcode == ENOSPC &&
                 tapereq->mode == WRITE_DISABLE ) {
                retval = ENOSPC;
                ENOSPC_occurred = TRUE;
            } else rc = rtcp_RetvalCASTOR(tape,file,&retval);

            if ( (filereq->concat & CONCAT_TO_EOD) != 0 ) {
                filereq = &file->prev->filereq;
                nb_bytes = file->diskbytes_sofar;
                TransferTime = (int)((time_t)filereq->TEndTransferDisk - 
                                     (time_t)filereq->TEndPosition);
            }

            *newpath = '\0';

            rtcp_log(LOG_DEBUG,"rtcpd_stageupdc() stage_updc_filcp(%s,%d,%d,%s,%d,%d,%d,%d,%s,%s,%d,%d,%s,%s)\n",
                     filereq->stageID,
                     filereq->stageSubreqID,
                     -retval,
                     filereq->ifce,
                     (int)nb_bytes,
                     WaitTime,
                     TransferTime,
                     filereq->blocksize,
                     tapereq->unit,
                     filereq->fid,
                     filereq->tape_fseq,
                     filereq->recordlength,
                     recfm,
                     newpath);

            rc = stage_updc_filcp(filereq->stageID,
                                  filereq->stageSubreqID,
                                  -retval,
                                  filereq->ifce,
                                  nb_bytes,
                                  WaitTime,
                                  TransferTime,
                                  filereq->blocksize,
                                  tapereq->unit,
                                  filereq->fid,
                                  filereq->tape_fseq,
                                  filereq->recordlength,
                                  recfm,
                                  newpath);
            save_serrno = serrno;

            if ( rc == 0 && (filereq->concat & CONCAT_TO_EOD) == 0 &&
                 filereq->stageSubreqID == -1 && 
                 (rtcpd_LockForTpPos(0) == -1) ) {
                rtcp_log(LOG_DEBUG,"rtcpd_stageupdc() rtcpd_LockForTpPos(0): error releasing lock (serrno=%d, errno=%d)\n",
                         serrno,errno);
                return(-1); 
            }   

            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"rtcpd_stageupdc() stage_updc_filcp(): %s\n",
                         sstrerror(save_serrno));
                switch (save_serrno) {
                case EINVAL:
                    rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED|RTCP_USERR);
                    if ( filereq->stageSubreqID == -1 ) {
                        rtcpd_SetProcError(RTCP_FAILED|RTCP_USERR);
                        (void)rtcpd_LockForTpPos(0);
                    }
                    serrno = save_serrno;
                    return(-1);
                case ENOSPC:
                    if ( retval == ENOSPC ) 
                        rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED|RTCP_USERR);
                    else 
                        rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED|RTCP_SYERR);
                    if ( filereq->stageSubreqID == -1 ) {
                        if ( retval == ENOSPC )
                            rtcpd_SetProcError(RTCP_FAILED|RTCP_USERR);
                        else
                            rtcpd_SetProcError(RTCP_FAILED|RTCP_SYERR); 
                        (void)rtcpd_LockForTpPos(0);
                    }
                    serrno = save_serrno;
                    return(-1);
                case SECOMERR:
                case SESYSERR:
                    break;
                default:
                    if ( save_serrno == 0 ) save_serrno = SEINTERNAL;
                    rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED|RTCP_SYERR);
                    if ( filereq->stageSubreqID == -1 ) {
                        rtcpd_SetProcError(RTCP_FAILED|RTCP_SYERR);
                        (void)rtcpd_LockForTpPos(0);
                    }
                    serrno = save_serrno;
                    return(-1);
                }
            } else break;
        }
        if ( rc == 0 ) break;
    } /* for ( retry=0; retry < RTCP_STGUPDC_RETRIES; retry++ ) */
    if ( rc == -1 ) {
        if ( save_serrno == SECOMERR || save_serrno == SESYSERR ) {
            rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED|RTCP_SYERR);
            if ( filereq->stageSubreqID == -1 )
                rtcpd_SetProcError(RTCP_FAILED|RTCP_SYERR);
        } else {
            if ( save_serrno == 0 ) save_serrno = SEINTERNAL;
            rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED|RTCP_UNERR);
            if ( filereq->stageSubreqID == -1 ) 
                rtcpd_SetProcError(RTCP_FAILED|RTCP_UNERR);
        }
        if ( filereq->stageSubreqID == -1 ) (void)rtcpd_LockForTpPos(0);
        serrno = save_serrno;
        return(-1);
    }

    /*
     * Reset error if we retry with a new path
     */
    if ( rc == 0 && tapereq->mode == WRITE_DISABLE && retval == ENOSPC ) 
        filereq->err.errorcode = 0;

    if ( rc == 0 &&  tapereq->mode == WRITE_DISABLE && *newpath != '\0' && 
         strcmp(newpath,filereq->file_path) ) {
        rtcp_log(LOG_INFO,"New path obtained from stager: %s\n",
            newpath);
        strcpy(filereq->file_path,newpath);
        return(NEWPATH);
    }
    rtcp_log(LOG_DEBUG,"rtcpd_stageupdc() stageupdc returns %d, %s\n",
             rc,newpath);
    if ( (*newpath == '\0' && tapereq->mode == WRITE_DISABLE) || rc != 0 ) {
        if ( rc != 0 ) {
            rtcpd_AppendClientMsg(NULL,file,
                           "stageupdc failed, rc=%d, path=%s\n",rc,newpath);
            if ( save_serrno != ENOSPC ) rtcpd_SetReqStatus(NULL,file,
                                         save_serrno,RTCP_FAILED | RTCP_SYERR);
            else rtcpd_SetReqStatus(NULL,file,save_serrno,
                                    RTCP_FAILED | RTCP_USERR);
            serrno = save_serrno;
            return(-1);
        }
        /* only report empty path if stage_updc_tppos() was really called */
        if ( (tpPosCalled == 1) && 
             (*newpath == '\0') && (tapereq->mode == WRITE_DISABLE) &&
             (filereq->proc_status == RTCP_POSITIONED) &&
             (strcmp(filereq->file_path,".") == 0) ) {
            if ( save_serrno == 0 ) save_serrno = SEINTERNAL;
            rtcpd_AppendClientMsg(NULL,file,
                                  "stageupdc failed, no path returned\n");
            rtcpd_SetReqStatus(NULL,file,save_serrno,RTCP_FAILED | RTCP_SYERR);
            serrno = save_serrno;
            return(-1);
        }
    }
    return(0);
}

