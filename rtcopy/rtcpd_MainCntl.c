/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_MainCntl.c,v $ $Revision: 1.19 $ $Date: 2000/01/24 17:42:48 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_MainCntl.c - RTCOPY server main control thread
 */


#if defined(_WIN32)
#include <winsock2.h>
#include <time.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#endif /* _WIN32 */

#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <signal.h>

#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <trace.h>
#include <osdep.h>
#include <net.h>
#include <Cthread_api.h>
#include <vdqm_api.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <Ctape_api.h>
#include <serrno.h>

char rtcp_cmds[][10] = RTCOPY_CMD_STRINGS;

extern char *getconfent(const char *, const char *, int);

extern int Debug;
int Debug = FALSE;

extern int nb_bufs;
int nb_bufs = NB_RTCP_BUFS;

extern size_t bufsz;
size_t bufsz = RTCP_BUFSZ;

extern processing_cntl_t proc_cntl;
processing_cntl_t proc_cntl;

extern processing_status_t proc_stat;
processing_status_t proc_stat;

extern buffer_table_t **databufs;
buffer_table_t **databufs;

extern int AbortFlag;
int AbortFlag = 0;

/*
 * Set Debug flag if requested
 */
static void rtcpd_SetDebug() {
    Debug = FALSE;
    if ( getenv("RTCOPY_DEBUG") != NULL ||
         getconfent("RTCOPY","DEBUG",1) != NULL ) Debug = TRUE;
    else NOTRACE;                 /* Switch off tracing */
    return;
}

int rtcpd_CheckNoMoreTapes() {
    struct stat st;

    if ( stat(NOMORETAPES,&st) == 0 ) {
        rtcp_log(LOG_INFO," tape service momentarily interrupted.\n");
        return(-1);
    }
    return(0);
}

/*
 * Routine to check and authorize client.
 * Normally all checks have been done by VDQM but we may want
 * add local checks for the future.
 */
static int rtcpd_CheckClient(rtcpClientInfo_t *client) {
    return(0);
}

/*
 * Allocate array of internal data buffers. A fix buffer size is
 * set for all requests. Requests will use nearest multiple of
 * block sizes below the actual buffer size as actual buffer size.
 * Note that within a request there may well be file requests with
 * varying blocksizes. Thus, the actual buffer size has to be re-
 * calculated for each new file request.
 */
static int rtcpd_AllocBuffers() {
    int i, rc;
    char *p;

    if ( (p = getconfent("RTCOPYD","NB_BUFS",0)) != NULL ) {
        nb_bufs = atoi(p);
    }
    if ( (p = getconfent("RTCOPYD","BUFSZ",0)) != NULL ) {
        bufsz = atoi(p);
    }
    rtcp_log(LOG_DEBUG,"rtcpd_AllocBuffers() allocate %d x %d buffers\n",
        nb_bufs,bufsz);

    databufs = (buffer_table_t **)calloc((size_t)nb_bufs, sizeof(buffer_table_t *));
    serrno = errno;
    if ( databufs == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_AllocBuffers() malloc(%d): %s\n",
            nb_bufs * sizeof(buffer_table_t *),
            sstrerror(serrno));
        return(-1);
    }
    for (i=0; i<nb_bufs; i++) {
        databufs[i] = (buffer_table_t *)calloc(1,sizeof(buffer_table_t));
        serrno = errno;
        if ( databufs[i] == NULL ) {
            rtcp_log(LOG_ERR,"rtcp_AllocBuffers() malloc(%d): %s\n",
                sizeof(buffer_table_t),sstrerror(serrno));
            return(-1);
        }
        databufs[i]->buffer = (char *)malloc(bufsz);
        serrno = errno;
        if ( databufs[i]->buffer == NULL ) {
            rtcp_log(LOG_ERR,"rtcpd_AllocBuffers() malloc(%d): %s\n",
                bufsz,sstrerror(serrno));
            return(-1);
        }
        databufs[i]->maxlength = bufsz;
        databufs[i]->end_of_tpfile = FALSE;
        databufs[i]->last_buffer = FALSE;
        databufs[i]->nb_waiters = 0;
        databufs[i]->nbrecs = 0;
        databufs[i]->lrecl_table = NULL;
        /*
         * Initialize semaphores
         */
        databufs[i]->flag = BUFFER_EMPTY;
        rc = Cthread_mutex_lock(&databufs[i]->flag);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,
                "rtcpd_AllocBuffers() Cthread_mutex_lock(buf[%d]): %s\n",
                i,sstrerror(serrno));
            return(-1);
        }
        rc = Cthread_mutex_unlock(&databufs[i]->flag);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,
                "rtcpd_AllocBuffers() Cthread_mutex_unlock(buf[%d]): %s\n",
                i,sstrerror(serrno));
            return(-1);
        }
        /*
         * Get direct pointer to Cthread structure to allow for
         * low overhead locks
         */
        databufs[i]->lock = Cthread_mutex_lock_addr(&databufs[i]->flag);
        if ( databufs[i]->lock == NULL ) {
            rtcp_log(LOG_ERR,
                "rtcpd_AllocBuffers() Cthread_mutex_lock_addr(buf[%d]): %s\n",
                i,sstrerror(serrno));
            return(-1);
        }
    }

    return(0);
}

static int rtcpd_InitProcCntl() {
    int rc;

    /*
     * Initialize global processing control variables
     */
    rc = Cthread_mutex_lock(&proc_cntl);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_lock(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    rc = Cthread_mutex_unlock(&proc_cntl);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_unlock(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    /*
     * Get direct pointer to Cthread structure to allow for
     * low overhead locks
     */
    proc_cntl.cntl_lock = Cthread_mutex_lock_addr(&proc_cntl);
    if ( proc_cntl.cntl_lock == NULL ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_lock_addr(proc_cntl): %s\n",
            sstrerror(serrno));
        return(-1);
    }

    /*
     * Initialize global error processing control variable
     */
    rc = Cthread_mutex_lock(&proc_cntl.ProcError);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_lock(ProcError): %s\n",
            sstrerror(serrno));    
        return(-1);
    }
    rc = Cthread_mutex_unlock(&proc_cntl.ProcError);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_unlock(ProcError): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    /*
     * Get direct pointer to Cthread structure to allow for
     * low overhead locks
     */
    proc_cntl.ProcError_lock = Cthread_mutex_lock_addr(&proc_cntl.ProcError);
    if ( proc_cntl.ProcError_lock == NULL ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_lock_addr(ProcError): %s\n",
            sstrerror(serrno));
        return(-1);
    }

    /*
     * Initialize global request status processing control variable
     */
    rc = Cthread_mutex_lock(&proc_cntl.ReqStatus);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_lock(ReqStatus): %s\n",
            sstrerror(serrno));    
        return(-1);
    }
    rc = Cthread_mutex_unlock(&proc_cntl.ReqStatus);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_unlock(ReqStatus): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    /*
     * Get direct pointer to Cthread structure to allow for
     * low overhead locks
     */
    proc_cntl.ReqStatus_lock = Cthread_mutex_lock_addr(&proc_cntl.ReqStatus);
    if ( proc_cntl.ReqStatus_lock == NULL ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_lock_addr(ReqStatus): %s\n",
            sstrerror(serrno));
        return(-1);
    }

    /*
     * Initialize exclusive lock for disk file append access.
     */ 
    rc = Cthread_mutex_lock(&proc_cntl.DiskFileAppend); 
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_lock(DiskFileAppend): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    rc = Cthread_mutex_unlock(&proc_cntl.DiskFileAppend);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_unlock(DiskFileAppend): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    /*
     * Get direct pointer to Cthread structure to allow for
     * low overhead locks
     */
    proc_cntl.DiskFileAppend_lock = Cthread_mutex_lock_addr(&proc_cntl.DiskFileAppend);
    if ( proc_cntl.DiskFileAppend_lock == NULL ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_lock_addr(DiskFileAppend_lock): %s\n",
            sstrerror(serrno));
        return(-1);
    }

    return(0);
}

static int rtcpd_FreeBuffers() {
    int i, *q;
    char *p;

    if ( databufs == NULL ) return(0);
    for (i=0; i<nb_bufs; i++) {
        if ( databufs[i] != NULL ) {
            rtcp_log(LOG_DEBUG,"rtcpd_FreeBuffers() free data buffer at 0x%lx\n",
                databufs[i]->buffer);
            if ( (p = databufs[i]->buffer) != NULL ) free(p);
            databufs[i]->buffer = NULL;
            if ( (q = databufs[i]->lrecl_table) != NULL ) free(q);
            free(databufs[i]);
            databufs[i] = NULL;
        }
    }
    free(databufs);
    databufs = NULL;
    return(0);
}

int rtcpd_CalcBufSz(tape_list_t *tape, file_list_t *file) {
    int blksiz, lrecl, current_bufsz;

    if ( file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    blksiz = file->filereq.blocksize;
    lrecl = file->filereq.recordlength;
    if ( blksiz<= 0 ) {
        /*
         * Block size not specified. Assume a default.
         * For future: get defaults from Ctape
         */
        if ( strstr(tape->tapereq.dgn,"SD3") != NULL ) {
            blksiz = 256*1024;
            if ( lrecl > 0 ) {
                blksiz = (blksiz/lrecl)*lrecl;
            }
        } else {
            if ( lrecl > 0 ) blksiz = lrecl;
            else blksiz = 32*1024;
        }
    }
    current_bufsz = (bufsz/blksiz) * blksiz;
    if ( current_bufsz <= 0 ) current_bufsz = blksiz;

    return(current_bufsz);
}

int rtcpd_AdmUformatInfo(file_list_t *file, int indxp) {
    int blksiz, nb_blks;

    if ( file == NULL || *(file->filereq.recfm) != 'U' ||
         file->filereq.blocksize <= 0 || indxp < 0 || 
         indxp > nb_bufs ) {
        rtcp_log(LOG_ERR,"rtcpd_AdmUformatInfo() invalid parameter(s)\n");
        serrno = EINVAL;
        return(-1);
    }
    blksiz = file->filereq.blocksize;
    nb_blks = databufs[indxp]->length / blksiz;
    if ( nb_blks <= 0 ) {
        rtcp_log(LOG_ERR,"rtcpd_AdmUformatInfo() internal error: buffer too small!!!\n");
        serrno = SEINTERNAL;
        return(-1);
    }
    if ( nb_blks > databufs[indxp]->nbrecs ) {
        if ( databufs[indxp] != NULL ) {
            free(databufs[indxp]->lrecl_table);
            databufs[indxp]->lrecl_table = NULL;
            databufs[indxp]->nbrecs = 0;
        }
        databufs[indxp]->lrecl_table = (int *)calloc((size_t)nb_blks,sizeof(int));
        if ( databufs[indxp]->lrecl_table == NULL ) {
            serrno = errno;
            rtcp_log(LOG_ERR,"rtcpd_AdmUformatInfo() calloc(): %s\n",
                sstrerror(serrno));
            return(-1);
        }
        databufs[indxp]->nbrecs = nb_blks;
    }
    return(0);
}

static void rtcpd_FreeResources(SOCKET **client_socket,
                         rtcpClientInfo_t **client,
                         tape_list_t **tape) {
    tape_list_t *nexttape;
    file_list_t *nextfile, *tmpfile;
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;
    char ifce[5];
    u_signed64 totSz = 0;
    int totKBSz = 0;
    int Twait = 0;
    int Tservice = 0;
    int Ttransfer = 0;
    int mode;

    /*
     * Close and free client listen socket if still active. This
     * will certainly kill off the client listen thread if it is 
     * still there.
     */
    if ( client_socket != NULL && *client_socket != NULL ) {
        rtcp_log(LOG_DEBUG,"rtcpd_FreeResources() close and free client socket\n");
        rtcp_CloseConnection(*client_socket);
        free(*client_socket);
        *client_socket = NULL;
    }

    if ( client != NULL && *client != NULL ) {
        free(*client);
        *client = NULL;
    }
    (void)rtcpd_FreeBuffers();
    (void)Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
    if ( tape != NULL && *tape != NULL ) {
        CLIST_ITERATE_BEGIN(*tape,nexttape) {
            tapereq = &(nexttape->tapereq);
            mode = tapereq->mode;
            Tservice = (tapereq->TEndUnmount - tapereq->TStartRequest);
            if ( Twait == 0 ) 
                Twait = (tapereq->TStartMount - tapereq->TStartRequest);
            Twait += (tapereq->TEndMount - tapereq->TStartMount);
            Twait += (tapereq->TEndUnmount - tapereq->TStartUnmount);
            tmpfile = nextfile = nexttape->file;
            while ( nextfile != NULL ) {
                filereq = &(nextfile->filereq);
                strcpy(ifce,filereq->ifce);
                Ttransfer += 
                  max(filereq->TEndTransferTape,filereq->TEndTransferDisk) -
                  min(filereq->TStartTransferTape,filereq->TStartTransferDisk);
                if ( tapereq->mode == WRITE_ENABLE )
                    totSz += filereq->bytes_in;
                else
                    totSz += filereq->bytes_out;
                
                CLIST_DELETE(nextfile,tmpfile);
                rtcp_log(LOG_DEBUG,"rtcpd_FreeResources() free file element 0x%lx\n",
                    tmpfile);
                free(tmpfile);
                tmpfile = nextfile;
            }
        } CLIST_ITERATE_END(*tape,nexttape);
        
        nexttape = *tape;
        while ( *tape != NULL ) {
            CLIST_DELETE(*tape,nexttape);
            rtcp_log(LOG_DEBUG,"rtcpd_FreeResources() free tape element 0x%lx\n",
                nexttape);
            free(nexttape);
            nexttape = *tape;
        }
    }
    totKBSz = (int)(totSz / 1024);

    if ( (rtcpd_CheckProcError() & RTCP_OK) != 0 ) {
        rtcp_log(LOG_INFO,"total number of Kbytes transferred is %d\n",totKBSz);
        rtcp_log(LOG_INFO,"waiting time was %d seconds\n",Twait);
        rtcp_log(LOG_INFO,"service time was %d seconds\n",Tservice);
        if ( Ttransfer > 0 ) {
            if ( mode == WRITE_ENABLE )
                rtcp_log(LOG_INFO,"cpdsktp: Data transfer bandwidth (%s) is %d KB/sec\n",
                         ifce,totKBSz/Ttransfer); 
            else
                rtcp_log(LOG_INFO,"cptpdsk: Data transfer bandwidth (%s) is %d KB/sec\n",
                         ifce,totKBSz/Ttransfer);
        }
        rtcp_log(LOG_INFO,"request successful\n");
    } else {
        rtcp_log(LOG_INFO,"request failed\n");
    }

    rtcpd_SetProcError(RTCP_OK);
    return;
}

int rtcpd_AbortHandler(int sig) {
    if ( sig == SIGTERM ) AbortFlag = 2;
    else AbortFlag = 1;
    return(0);
}

void rtcpd_BroadcastException() {
    int i;

    if ( databufs != NULL ) {
        for (i=0;i<nb_bufs;i++) {
            if ( databufs[i] != NULL ) {
                (void)Cthread_mutex_lock_ext(databufs[i]->lock);
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            }
        }
    }
    (void)Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);

    (void)Cthread_mutex_lock_ext(proc_cntl.ReqStatus_lock);
    (void)Cthread_cond_broadcast_ext(proc_cntl.ReqStatus_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.ReqStatus_lock);

    (void)Cthread_mutex_lock_ext(proc_cntl.DiskFileAppend_lock);
    (void)Cthread_cond_broadcast_ext(proc_cntl.DiskFileAppend_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.DiskFileAppend_lock);
}

static int rtcpd_ProcError(int *code) {
    static int global_code, rc;

    if ( AbortFlag != 0 ) {
        if ( AbortFlag == 1 ) 
            rtcp_log(LOG_DEBUG,"rtcpd_ProcError() REQUEST ABORTED by user\n");
        else
            rtcp_log(LOG_DEBUG,"rtcpd_ProcError() REQUEST ABORTED by operator\n");
        return(proc_cntl.ProcError);
    }
    rc = Cthread_mutex_lock_ext(proc_cntl.ProcError_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_ProcError(): Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        rc = RTCP_FAILED;
    } else {
        if ( code != NULL && AbortFlag == 0 ) {
            /*
             * Never reset FAILED status
             */
            if ( (proc_cntl.ProcError & RTCP_FAILED) == 0 ) {
                proc_cntl.ProcError = *code;
            }
        }
        rc = proc_cntl.ProcError;
    }
    (void) Cthread_mutex_unlock_ext(proc_cntl.ProcError_lock);
    return(rc);
}

void rtcpd_SetProcError(int code) {
    int set_code, rc, i;

    set_code = code;
    rc = rtcpd_ProcError(&set_code);
    if ( (rc != code) && 
         ((rc & RTCP_FAILED) == 0) && 
         (AbortFlag == 0) ) {
        /*
         * There was an error setting the global ProcError.
         * Since global control has failed we must assure that
         * the error is set (although thread-unsafe).
         */
        rtcp_log(LOG_ERR,"rtcpd_SetProcError() force FAILED status\n");
        proc_cntl.ProcError = RTCP_FAILED;
    }
    if ( (rc & (RTCP_FAILED | RTCP_EOD)) != 0 ) rtcpd_BroadcastException(); 

    return;
}

int rtcpd_CheckProcError() {
    return(rtcpd_ProcError(NULL));
}

void rtcpd_SetReqStatus(tape_list_t *tape,
                        file_list_t *file,
                        int status,
                        int severity) {
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;
    int rc,i;

    rtcp_log(LOG_DEBUG,"rtcpd_SetReqStatus() status=%d, severity=%d\n",
             status,severity);
    if ( tape == NULL && file == NULL ) return;
    if ( tape != NULL ) tapereq = &tape->tapereq;
    if ( file != NULL ) filereq = &file->filereq;

    rc = Cthread_mutex_lock_ext(proc_cntl.ReqStatus_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_SetReqStatus() Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        return;
    }
    if ( tapereq != NULL ) {
        /*
         * Don't reset a FAILED status
         */
        if ( (tapereq->err.severity & RTCP_FAILED) == 0 ) {
            tapereq->err.errorcode = status;
            if ( (severity & (RTCP_FAILED | RTCP_RESELECT_SERV | RTCP_USERR |
                              RTCP_SYERR | RTCP_UNERR | RTCP_SEERR)) != 0 ) {
                tapereq->err.severity = tapereq->err.severity & ~RTCP_OK;
                tapereq->err.severity = tapereq->err.severity & ~RTCP_RETRY_OK;
                tapereq->err.severity |= severity;
            } else { 
                tapereq->err.severity |= severity;
            } 
        }
    }
    if ( filereq != NULL ) {
        /*
         * Don't reset a FAILED status
         */
        if ( (filereq->err.severity & RTCP_FAILED) == 0 ) {
            filereq->err.errorcode = status;
            if ( (severity & (RTCP_FAILED | RTCP_RESELECT_SERV | RTCP_USERR | 
                              RTCP_SYERR | RTCP_UNERR | RTCP_SEERR)) != 0 ) {
                filereq->err.severity = filereq->err.severity & ~RTCP_OK;
                filereq->err.severity = filereq->err.severity & ~RTCP_RETRY_OK;
                filereq->err.severity |= severity;
            } else {
                filereq->err.severity |= severity;
            } 
        }
    }
    (void)Cthread_mutex_unlock_ext(proc_cntl.ReqStatus_lock);

    return;
}

void rtcpd_CheckReqStatus(tape_list_t *tape,
                          file_list_t *file,
                          int *status,
                          int *severity) {
    int _status, _severity, rc;
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;
    if ( (tape == NULL && file == NULL) ||
         (tape != NULL && file != NULL) ||
         (status == NULL && severity == NULL) ) return;

    if ( tape != NULL ) tapereq = &tape->tapereq;
    if ( file != NULL ) filereq = &file->filereq;

    rc = Cthread_mutex_lock_ext(proc_cntl.ReqStatus_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_CheckReqStatus() Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        return;
    }
    if ( tapereq != NULL ) {
        _status = tapereq->err.errorcode;
        _severity = tapereq->err.severity;
    }
    if ( filereq != NULL ) {
        _status = filereq->err.errorcode;
        _severity = filereq->err.severity;
    }
    (void)Cthread_mutex_unlock_ext(proc_cntl.ReqStatus_lock);
    if ( status != NULL ) *status = _status;
    if ( severity != NULL ) *severity = _severity;
    return;
}


int rtcpd_MainCntl(SOCKET *accept_socket) {
    rtcpTapeRequest_t tapereq;
    rtcpFileRequest_t filereq;
    rtcpHdr_t hdr;
    SOCKET *client_socket;
    rtcpClientInfo_t *client;
    tape_list_t *tape, *nexttape;
    file_list_t *nextfile;
    int rc, retry, reqtype, errmsglen,status, CLThId;
    char *errmsg;
    static int thPoolId = -1;
    static int thPoolSz = -1;
    struct passwd *pwd;
    char *cmd = NULL;
#if !defined(_WIN32)
    struct sigaction sigact;
    sigset_t sigset;
#endif /* _WIN32 */

    (void)setpgrp();
    AbortFlag = 0;
#if !defined(_WIN32)
    (void)sigemptyset(&sigset);
    sigact.sa_mask = sigset;
    sigact.sa_handler = SIG_IGN;
    sigaction(SIGPIPE,&sigact,NULL);
    /*
     * Should normally use sigwait(). 
     */
    sigact.sa_handler = (void (*)(int))rtcpd_AbortHandler;
    sigaction(SIGTERM,&sigact,NULL);
#else /* _WIN32 */
    signal(SIGPIPE,SIG_IGN);
    signal(SIGTERM,(void (*)(int))rtcpd_AbortHandler);
#endif /* _WIN32 */
    rtcp_log(LOG_DEBUG,"rtcpd_MainCntl() called\n");
    rtcpd_SetDebug();
    client = (rtcpClientInfo_t *)calloc(1,sizeof(rtcpClientInfo_t));
    if ( client == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() malloc(): %s\n",
            sstrerror(errno));
        return(-1);
    }

    /*
     * Receive the request
     */
    memset(&tapereq,'\0',sizeof(tapereq));
    memset(&filereq,'\0',sizeof(tapereq));
    rc = rtcp_RecvReq(accept_socket,
                      &hdr,
                      client,
                      &tapereq,
                      &filereq);

    if ( rc == -1 ) {
        errmsg = sstrerror(serrno);
        errmsglen = 0;
        if ( errmsg != NULL ) errmsglen = strlen(errmsg);
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcp_RecvReq(): %s\n",
            (errmsg == NULL ? "Unknown error" : errmsg));
        /*
         * Need to tell VDQM that we failed to receive the
         * request. If it didn't originate from VDQM there
         * is no harm since we're going to exit anyway.
         */
        (void)vdqm_AcknClientAddr(*accept_socket,
                                  rc,
                                  errmsglen,
                                  errmsg);
        return(-1);
    } else reqtype = rc;

    if ( reqtype != VDQM_CLIENTINFO ) {
        /*
         * This is an old client
         */
        rtcp_log(LOG_INFO,"Running old (SHIFT) client request\n");
        rc = rtcp_RunOld(accept_socket,&hdr);
        (void)rtcp_CloseConnection(accept_socket);
        return(rc);
    }

    /*
     * If local nomoretapes is set, we don't acknowledge VDQM message.
     * VDQM will then requeue the request and put assigned drive in
     * UNKNOWN status
     */
    if ( rtcpd_CheckNoMoreTapes() != 0 ) {
        (void)rtcp_CloseConnection(accept_socket);
        return(-1);
    }

    rc = vdqm_AcknClientAddr(*accept_socket,reqtype,0,NULL);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() vdqm_AcknClientAddr(): %s\n",
            sstrerror(serrno));
        (void)rtcp_CloseConnection(accept_socket);
        return(-1);
    }

    /*
     * We've got the client address so we don't need VDQM anymore
     */
    rc = rtcp_CloseConnection(accept_socket);
    if ( rc == -1 ) {
        /*
         * There was an error closing the connection to VDQM.
         * Probably not serious, so we just log it and continue.
         */
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcp_CloseConnection(): %s\n",
            sstrerror(serrno));
    }

    /*
     * Contact the client and get the request. First check if OK.
     */
    rc = rtcpd_CheckClient(client);
    if ( rc == -1 ) {
        /*
         * Client not authorised
         */
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() client (%d,%d)@%s not authorised\n",
                 client->uid,client->gid,client->clienthost);
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        return(-1);
    }

    /*
     * Client OK, connect to him
     */
    client_socket = (SOCKET *)malloc(sizeof(SOCKET));
    if ( client_socket == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() malloc(): %s\n",
            sstrerror(errno));
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        return(-1);
    }
    *client_socket = INVALID_SOCKET;
    rc = rtcpd_ConnectToClient(client_socket,
                               client->clienthost,
                               &client->clientport);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcp_ConnectToClient(%s,%d): %s\n",
                 client->clienthost,client->clientport,sstrerror(serrno));
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        return(-1);
    }
    rc = rtcpd_InitProcCntl();

    /*
     * Loop to get the full request
     */
    rc = 0;
    reqtype = 0;
    tape = NULL;
    while ( reqtype != RTCP_NOMORE_REQ  ) {
        memset(&filereq,'\0',sizeof(filereq));
        rc = rtcp_RecvReq(client_socket,
            &hdr,client,&tapereq,&filereq);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcp_RecvReq(): %s\n",
                     sstrerror(serrno));
            break;
        }
        reqtype = hdr.reqtype;
        rc = rtcp_SendAckn(client_socket,reqtype);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcp_SendAckn(): %s\n",
                sstrerror(serrno));
            break;
        }
        if ( reqtype == RTCP_TAPE_REQ || reqtype == RTCP_TAPEERR_REQ ) {
            /*
             * Tape request, copy and add to global tape list
             */
            nexttape = (tape_list_t *)calloc(1,sizeof(tape_list_t));
            if ( nexttape == NULL ) {
                rtcp_log(LOG_ERR,"rtcpd_MainCntl() calloc(): %s\n",
                    sstrerror(errno));
                rc = -1;
                break;
            }
            tapereq.TStartRtcpd = (int)time(NULL);
            if ( tapereq.VolReqID != client->VolReqID ) {
                rtcp_log(LOG_ERR,"rtcpd_MainCntl() wrong VolReqID %d, should be %d\n",
                    tapereq.VolReqID, client->VolReqID);
                rc = -1;
                break;
            }
            nexttape->tapereq = tapereq;
            CLIST_INSERT(tape,nexttape);
            rtcp_log(LOG_DEBUG,"Tape VID: %s, DGN: %s, unit %s, VolReqID=%d,mode=%d\n",
                tapereq.vid,tapereq.dgn,tapereq.unit,tapereq.VolReqID,
                tapereq.mode);
        }
        if ( reqtype == RTCP_FILE_REQ || reqtype == RTCP_FILEERR_REQ ) {
            /*
             * File request, copy and add to list anchored with
             * last tape list element
             */
            nextfile = (file_list_t *)calloc(1,sizeof(file_list_t));
            if ( nextfile == NULL ) {
                rtcp_log(LOG_ERR,"rtcpd_MainCntl() calloc(): %s\n",
                    sstrerror(errno));
                rc = -1;
                break;
            }
            nextfile->filereq = filereq;
            if ( nexttape == NULL ) {
                rtcp_log(LOG_ERR,"rtcpd_MainCntl() invalid request sequence\n");
                rc = -1;
                break;
            }
            if ( !VALID_PROC_STATUS(nextfile->filereq.proc_status) )
                nextfile->filereq.proc_status = RTCP_WAITING;
            CLIST_INSERT(nexttape->file,nextfile);
            rtcp_log(LOG_DEBUG,"   File: %s, FSEQ %d, Blksz %d\n",
                filereq.file_path,filereq.tape_fseq,filereq.blocksize);
            nextfile->tape = nexttape;
        }
    } /* End while ( reqtype != RTCP_NOMORE_REQ ) */

    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() request loop finished with error\n");
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(-1);
    }
    /*
     * Log start message
     */
    pwd = getpwuid(client->uid);
    if ( tapereq.mode == WRITE_ENABLE )
        rtcp_log(LOG_INFO,"cpdsktp request by %s (%d,%d) from %s\n",
                 pwd->pw_name,client->uid,client->gid,client->clienthost);
    else
        rtcp_log(LOG_INFO,"cptpdsk request by %s (%d,%d) from %s\n",
                 pwd->pw_name,client->uid,client->gid,client->clienthost);

    cmd = rtcp_cmds[tapereq.mode];
            
    /*
     * On UNIX: set client UID/GID
     */
#if !defined(_WIN32)
    rc = setgid(client->gid);
    if ( rc == -1 )
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() setgid(%d): %s\n",
            client->gid,sstrerror(errno));
    else {
        rc = setuid(client->uid);
        if ( rc == -1 )
            rtcp_log(LOG_ERR,"rtcpd_MainCntl() setuid(%d): %s\n",
                client->uid,sstrerror(errno));
    }
    if ( rc == -1 ) {
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(-1);
    }
#endif /* _WIN32 */
    /*
     * Check the request and set defaults
     */
    rc = rtcp_CheckReq(client_socket,tape);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcp_CheckReq(): %s\n",
            sstrerror(serrno));
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(-1);
    }

    /*
     * Allocate the buffers
     */
    rc = rtcpd_AllocBuffers();
    if ( rc == -1 ) {
        (void)rtcpd_SetReqStatus(tape,NULL,serrno,RTCP_RESELECT_SERV);
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() failed to allocate buffers\n");
        (void)rtcpd_AppendClientMsg(tape,NULL,RT105,sstrerror(serrno));
        (void)tellClient(client_socket,tape,NULL,-1);
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(-1);
    }
    /*
     * Reset global error status
     */
    rtcpd_SetProcError(RTCP_OK);

    /*
     * Start a thread to listen to client socket (to receive ABORT).
     */
    CLThId = rtcpd_ClientListen(*client_socket);
    if ( CLThId == -1 ) {
        (void)rtcpd_SetReqStatus(tape,NULL,serrno,RTCP_RESELECT_SERV);
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcpd_ClientListen(): %s\n",
            sstrerror(serrno));
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(-1);
    }

    /*
     * Start up the disk IO thread pool (only once on NT).
     */
    if ( thPoolId == -1 ) thPoolId = rtcpd_InitDiskIO(&thPoolSz);
    if ( thPoolId == -1 || thPoolSz <= 0 ) {
        (void)rtcpd_SetReqStatus(tape,NULL,serrno,RTCP_RESELECT_SERV);
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcpd_InitDiskIO(0x%lx): %s\n",
            &thPoolSz,sstrerror(serrno));
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(-1);
    }
    /*
     * Start up monitoring thread
     */
    rc = rtcpd_StartMonitor(thPoolSz);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcp_StartMonitor(%d): %s\n",
            thPoolSz,sstrerror(serrno));
    }
    /*
     * Local retry loop to break out from
     */
    retry = 0;
    for (;;) {
        /*
         * Start tape control and I/O thread. From now on the
         * deassign will be done in the tape IO thread.
         */
        rc = rtcpd_StartTapeIO(client,tape);
        if ( rc == -1 ) {
            (void)rtcpd_SetReqStatus(tape,NULL,serrno,RTCP_RESELECT_SERV);
            rtcp_log(LOG_ERR,
                "rtcpd_MainCntl() failed to start Tape I/O thread\n");
            (void)rtcpd_AppendClientMsg(tape,NULL,"rtcpd_StartTapeIO(): %s\n",
                  sstrerror(serrno));
            (void)tellClient(client_socket,tape,NULL,-1);
            (void)rtcpd_Deassign(client->VolReqID,&tapereq);
            break;
        }

        /*
         * Start disk and network IO thread
         */
        rc = rtcpd_StartDiskIO(client,tape,tape->file,thPoolId,thPoolSz);
        if ( rc == -1 ) {
            (void)rtcpd_SetReqStatus(tape,NULL,serrno,RTCP_RESELECT_SERV);
            rtcp_log(LOG_ERR,"rtcpd_MainCntl() failed to start disk I/O thread\n");
            rtcpd_SetProcError(RTCP_FAILED);
        }

        /*
         * Request is running to its end. Wait for the
         * tape control and I/O thread.
         */
        rc = rtcpd_WaitTapeIO(&status);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcpd_WaitTapeIO(): %s\n",
                sstrerror(serrno));
        }
        rtcp_log(LOG_DEBUG,"rtcpd_MainCntl() tape I/O thread returned status=%d\n",
            status);
        if ( (rtcpd_CheckProcError() & RTCP_LOCAL_RETRY) != 0 ) { 
            retry++;
            rtcp_log(LOG_INFO,"Automatic retry number %d\n",retry);
        } else break;
        /*
         * do a local retry
         */
        rtcpd_SetProcError(RTCP_OK);
    } /* for (;;) */

    /*
     * Wait for the client listen thread to return.
     */
    rc = rtcpd_WaitCLThread(CLThId,&status);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcpd_WaitCLThread(%d): %s\n",
            CLThId,sstrerror(serrno));
    }
    rtcp_log(LOG_DEBUG,"rtcpd_MainCntl() Client Listen thread returned status=%d\n",
        status);

    proc_stat.tapeIOstatus.current_activity = RTCP_PS_FINISHED;

    /*
     * Clean up allocated resources and return
     */
    rtcpd_CleanUpDiskIO(thPoolId);
    rtcpd_FreeResources(&client_socket,&client,&tape);

    return(0);
}

