/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_Ctape.c,v $ $Revision: 1.7 $ $Date: 2000/01/17 08:20:29 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_Ctape.c - RTCOPY interface to Ctape
 */


#if defined(_WIN32)
#include <process.h>
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
#include <sys/stat.h>

#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <vdqm_api.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <Ctape_api.h>
#include <serrno.h>

#define CTP_ERRTXT rtcpd_CtapeErrMsg()

static int Ctape_key = -1;
static char Unkn_errorstr[] = "unknown error";
extern int AbortFlag;

extern char *getconfent(char *, char *, int);
static char tape_path[CA_MAXPATHLEN+1];  /* Needed for Ctape_kill */

int rtcpd_CtapeInit() {
    char *errbuf;
    int errbufsiz = CA_MAXLINELEN+1;

    Cglobals_get(&Ctape_key,(void **)&errbuf,errbufsiz);
    if ( errbuf == NULL ) return(-1);

    Ctape_seterrbuf(errbuf,errbufsiz);
    tape_path[0] = '\0';
    return(0);
}

char *rtcpd_CtapeErrMsg() {
    char *errbuf;
    int errbufsiz = CA_MAXLINELEN+1;

    Cglobals_get(&Ctape_key,(void **)&errbuf,errbufsiz);
    if ( errbuf == NULL ) return(Unkn_errorstr);
    return(errbuf);
}

int rtcpd_CtapeFree() {
    char *errbuf;
    int errbufsiz = CA_MAXLINELEN+1;

    Cglobals_get(&Ctape_key,(void **)&errbuf,errbufsiz);
    if ( errbuf != NULL ) free(errbuf);
    return(0);
}

int rtcpd_Assign(tape_list_t *tape) {
    int rc, status, value, jobID;
    rtcpTapeRequest_t *tapereq;

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    tapereq = &tape->tapereq;

    /*
     * Assign the drive to current job
     */
    status = VDQM_UNIT_ASSIGN;
    value = tapereq->VolReqID;
    jobID = getpid();

    rtcp_log(LOG_DEBUG,"rtcpd_Assign() VolReqID=%d, dgn=%s, unit=%s, jobID=%d\n",
        tapereq->VolReqID,tapereq->dgn,tapereq->unit,jobID);

    rc = vdqm_UnitStatus(NULL,NULL,tapereq->dgn,NULL,tapereq->unit,
        &status,&value,jobID);

    tapereq->tprc = rc;
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_Assign() vdqm_UnitStatus() %s\n",
            sstrerror(serrno));
        return(-1);
    }

    rtcp_log(LOG_DEBUG,"rtcpd_Assign() vdqm_UnitStatus() jobID=%d\n",
        value);

    tapereq->jobID = value;

    return(rc);
}

/*
 * To be used if request fails before any Ctape call (e.g.
 * bad request structure or client did an exit...).
 */
int rtcpd_Deassign(int VolReqID,
                   rtcpTapeRequest_t *tapereq) {
    int rc, status, jobID, value;

    if ( tapereq == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_Deassign() called with NULL tapereq\n");
        serrno = EINVAL;
        return(-1);
    }
    rtcp_log(LOG_DEBUG,"rtcpd_Deassign() called\n");
    if ( tapereq->jobID <= 0 ) {
        /*
         * We need to assign the unit first (for safety VDQM
         * only allows to RELEASE with jobID's).
         */
        status = VDQM_UNIT_ASSIGN;
        if ( VolReqID > 0 ) value = VolReqID;
        else value = tapereq->VolReqID;
        jobID = (int)getpid();

        rtcp_log(LOG_DEBUG,"rtcpd_Deassign() ASSIGN volreq ID %d -> jobID %d\n",
            value,jobID);
        rc = vdqm_UnitStatus(NULL,NULL,tapereq->dgn,NULL,tapereq->unit,
            &status,&value,jobID);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_Deassign() vdqm_UnitStatus(UNIT_ASSIGN) %s\n",
                sstrerror(serrno));
        }
    } else jobID = tapereq->jobID;
    status = VDQM_UNIT_RELEASE;

    rtcp_log(LOG_DEBUG,"rtcpd_Deassign() RELEASE job ID %d\n",jobID);
    rc = vdqm_UnitStatus(NULL,NULL,tapereq->dgn,NULL,tapereq->unit,
        &status,NULL,jobID);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_Deassign() vdqm_UnitStatus(UNIT_RELEASE) %s\n",
            sstrerror(serrno));
    }
    return(rc);
}

void rtcpd_CtapeKill() {
    if ( *tape_path != '\0' ) Ctape_kill(tape_path);
    return;
}

int rtcpd_Reserve(tape_list_t *tape) {
    struct dgn_rsv rsv;
    int rc, count, save_serrno;
    rtcpTapeRequest_t *tapereq;

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    tapereq = &tape->tapereq;

    count = 1;
    rtcp_log(LOG_DEBUG,"rtcpd_Reserve() dgn=%s\n",tapereq->dgn);

    strcpy(rsv.name,tapereq->dgn);
    rsv.num = 1;

    rc = Ctape_reserve(count,&rsv);

    tapereq->tprc = rc;
    save_serrno = serrno;
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcp_Reserve() Ctape_reserve(): %s\n",
            CTP_ERRTXT);
        rtcpd_SetReqStatus(tape,NULL,save_serrno,RTCP_FAILED);
        rtcpd_AppendClientMsg(tape, NULL, "%s\n",CTP_ERRTXT);
        return(-1);
    }

    rtcp_log(LOG_DEBUG,"rtcpd_Reserve() successful\n");
    return(0);
}

int rtcpd_Mount(tape_list_t *tape) {
    int rc, j, save_serrno,severity, jobID;
    char *p, confparam[16];
    struct stat st;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;

    if ( tape == NULL || tape->file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    tapereq = &tape->tapereq;
    filereq = &tape->file->filereq;
    jobID = getpid();

    if ( *filereq->tape_path == '\0' ) 
        sprintf(filereq->tape_path,"PATH%d",jobID);

    j = 0;
    while ( (rc = stat(filereq->tape_path,&st)) == 0 ) {
        sprintf(filereq->tape_path,"PATH%d.%d",jobID,++j);
    }

    rtcp_log(LOG_DEBUG,"rtcpd_Mount() path=%s, vid=%s@unit=%s\n",
        filereq->tape_path,tapereq->vid,tapereq->unit);

    severity = RTCP_OK;

    serrno = errno = 0;
    /*
     * We need to save tape_path for Ctape_kill() in case there is 
     * an ABORT. The reason is that main control thread may have 
     * free'd the tape and file request structures while aborting 
     * and the tape_path information gone lost.
     */
    strcpy(tape_path,filereq->tape_path);
    tapereq->TStartMount = (int)time(NULL);
    rc = Ctape_mount(filereq->tape_path,
                     tapereq->vid,
                     tapereq->partition,
                     tapereq->dgn,
                     tapereq->density,
                     tapereq->unit,
                     tapereq->mode,
                     tapereq->vsn,
                     tapereq->label,
                     tapereq->VolReqID);
    
    tapereq->tprc = rc;
    save_serrno = (serrno != 0 ? serrno : errno);
    tapereq->TEndMount = (int)time(NULL);
    if ( AbortFlag == 1 ) {
        rtcp_log(LOG_ERR,"rtcpd_Mount() ABORTING! Calling Ctape_kill(%s)\n",
            tape_path);
        Ctape_kill(tape_path);
        *tape_path = '\0';
        severity = RTCP_FAILED | RTCP_USERR;
        rtcpd_SetReqStatus(tape,NULL,save_serrno,severity);
        return(-1);
    }
    if ( rc == -1 ) {
        /*
         * If there is an error (including medium errors) on
         * MOUNT the Ctape_mount() routine will always free
         * or even "down" the unit. Therefore we should not 
         * retry on this drive but rather tell the client to
         * call VDQM for a new drive
         */
        rtcp_log(LOG_ERR,"rtcpd_Mount() Ctape_mount() %s\n",
            CTP_ERRTXT);
        rtcpd_AppendClientMsg(tape, NULL, "%s\n",CTP_ERRTXT);
        severity = 0;
        switch ( save_serrno ) {
        case EIO:
        case ETPARIT:
            /*
             * Check if there is any configured error action
             * for this medium errors (like sending a mail to
             * operator or raising an alarm).
             */
            sprintf(confparam,"%s_ERRACTION",tapereq->dgn);
            if ( (p = getconfent("RTCOPYD",confparam,0)) != NULL ) {
                j = atoi(p);
                severity = j;
            }
        case ETHWERR:
        case ETNOSNS:
            if ( severity & RTCP_NORETRY ) {
                tapereq->err.max_tpretry = -1;
            } else {
                tapereq->err.max_tpretry--;
                severity |= RTCP_RESELECT_SERV;
            }
            break;
        case EEXIST:
            /*
             * This should not happen...
             */
        case ENXIO:
        case ETRSLT:
            severity = RTCP_RESELECT_SERV;
            break;
        case SYERR:
        case SECOMERR:
        case SENOSSERV:
        case SENOSHOST:
        case ETDNP:
        case ETIDN:
        case ETVBSY:
            severity = RTCP_FAILED | RTCP_SYERR;
            break;
        case EINVAL: /* Wrong parameter...*/
        case EACCES: /* TMS denied access */
        case ETINTR:
        case ETBLANK:
        case ETCOMPA:
        case ETUNREC:
            severity = RTCP_FAILED | RTCP_USERR;
            break;
        case ETNDV:
        case ETIDG:
        case ETNRS:
            severity = RTCP_FAILED | RTCP_SEERR;
            break;
        case EINTR:
            severity = RTCP_FAILED | RTCP_USERR;
            (void)Ctape_kill(tape_path);
            break;
        default:
            severity = RTCP_FAILED | RTCP_UNERR;
            break;
        }
        rtcpd_SetReqStatus(tape,NULL,save_serrno,severity);
    }

    if ( rc == 0 ) 
        rtcp_log(LOG_DEBUG,"rtcpd_Mount() Ctape_mount() successful\n");

    *tape_path = '\0';
    return(rc);
}

int rtcpd_Position(tape_list_t *tape,
                   file_list_t *file) {
    int rc, j, save_serrno, flags,filstat, do_retry, severity;
    char *p, confparam[32];
    rtcpFileRequest_t *prevreq,*filereq;
    rtcpTapeRequest_t *tapereq;

    if ( tape == NULL || file == NULL ||
         !VALID_CONVERT(&file->filereq) ||
         !VALID_CONCAT(&file->filereq) ) {
        serrno = EINVAL;
        return(-1);
    }
    tapereq = &tape->tapereq;
    filereq = &file->filereq;
    prevreq = (file == tape->file ? NULL : &file->prev->filereq);

    if ( *filereq->tape_path == '\0' ) {
        if ( prevreq != NULL ) 
            strcpy(filereq->tape_path,prevreq->tape_path);
        else {
            rtcpd_AppendClientMsg(NULL, file,"rtcpd_Position(): cannot determine tape path\n");
            rtcp_log(LOG_DEBUG,"rtcpd_Position(): cannot determine tape path\n");
            serrno = SEINTERNAL;
            filereq->err.severity = RTCP_FAILED;
            filereq->err.errorcode = serrno;
            return(-1);
        }
    }

    rtcp_log(LOG_DEBUG,"rtcpd_Position(%s) vid=%s, fseq=%d, fsec=%d\n",
        filereq->tape_path,tapereq->vid,filereq->tape_fseq,
        file->tape_fsec);

    flags = filereq->tp_err_action;
    /*
     * Find out whether we need to reposition or not
     */
    if ( prevreq != NULL ) {
        /*
         * We don't need to reposition if tape file numbers are
         * consecutive or if we already positioned to EOI for append.
         */
        if ((filereq->tape_fseq == prevreq->tape_fseq + 1) ||
            ((filereq->position_method & TPPOSIT_EOI) != 0 && 
             (prevreq->position_method & TPPOSIT_EOI) != 0) ) { 
            if ( prevreq->cprc == 0 || (prevreq->cprc == LIMBYSZ &&
                tapereq->mode == WRITE_ENABLE ) ) {
                flags |= NOPOS;
            }
        }
    } else if ((filereq->concat == CONCAT_TO_EOD) &&
               (tapereq->mode == WRITE_DISABLE) ) {
        flags |= NOPOS;
    }


    filstat = filereq->check_fid;

    do_retry = 1;

    filereq->TStartPosition = (int)time(NULL);
    /*
     * We need to save tape_path for Ctape_kill() in case there is
     * an ABORT. The reason is that main control thread may have 
     * free'd the tape and file request structures while aborting 
     * and the tape_path information gone lost.
     */
    strcpy(tape_path,filereq->tape_path);
    while (do_retry) {
        rtcp_log(LOG_DEBUG,"rtcpd_Position() Ctape_position(%s,0x%x,%d,%d,%u,%d,%d,0x%x,%s,%s,%d,%d,%d,0x%x)\n",filereq->tape_path,filereq->position_method,filereq->tape_fseq,file->tape_fsec,filereq->blockid,tapereq->start_file,tapereq->end_file,filstat,filereq->fid,filereq->recfm,filereq->blocksize,filereq->recordlength,filereq->retention,flags);
        serrno = errno = 0;
        rc = Ctape_position(filereq->tape_path,
                            filereq->position_method,
                            filereq->tape_fseq,
                            file->tape_fsec,
                            (unsigned int)filereq->blockid,
                            tapereq->start_file,
                            tapereq->end_file,
                            filstat,
                            filereq->fid,
                            filereq->recfm,
                            filereq->blocksize,
                            filereq->recordlength,
                            filereq->retention,
                            flags);
        save_serrno = (serrno > 0 ? serrno : errno);
        filereq->TEndPosition = (int)time(NULL);
        filereq->cprc = rc;
        if ( AbortFlag == 1 ) {
            rtcp_log(LOG_ERR,"rtcpd_Position() ABORTING! Calling Ctape_kill(%s)\n",
                tape_path);
            severity = RTCP_FAILED | RTCP_USERR;
            save_serrno = EINTR;
            rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
            Ctape_kill(tape_path);
            *tape_path = '\0';
            return(-1);
        }
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_Position() Ctape_position() serrno=%d, %s\n",
                save_serrno,CTP_ERRTXT);
            rtcpd_AppendClientMsg(NULL, file, "%s\n",CTP_ERRTXT);
            severity = 0;

            switch (save_serrno) {
            case ENXIO:      /* Drive not operational */
                /*
                 * This is the only case when Ctape_position()
                 * puts the drive down. Send back the request
                 * and don't release (since the drive is already
                 * down).
                 */
                severity = RTCP_RESELECT_SERV | RTCP_NORLS;
                rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                break;
            case EACCES:    /* File not expired */
            case EINVAL:     /* Invalid parameter */
            case ETLBL:      /* Bad label structure */
            case ETEOV:      /* EOV found in multivolume set */
            case ETBLANK:    /* Blank tape        */
                severity = RTCP_FAILED | RTCP_USERR;
                rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                break;
            case ETFSQ:      /* Bad file sequence number */
                if ( filereq->concat == CONCAT_TO_EOD ) {
                    /*
                     * We reached last tape file when concatenating
                     * to a disk file. This is not an error since
                     * it can only happen if the user explicitly 
                     * specified to concatenate all tape files until EOD
                     * into a single disk file
                     */
                    severity = RTCP_OK | RTCP_EOD;
                    rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                    (void)rtcpd_stageupdc(tape,file);
                    rc = 0;
                    break;
                } else if ( filereq->concat == NOCONCAT_TO_EOD ) {
                    /*
                     * The client asked to read until EOD and
                     * specified more disk files than there was 
                     * tape files. This is an user error but probably
                     * intentional. The client didn't know how many
                     * tape files there were and just specified an
                     * arbitrary large number of disk files. Make sure
                     * that the stager (if any) knows how many files
                     * there are on the tape.
                     */
                    severity = RTCP_FAILED | RTCP_EOD | RTCP_USERR;
                    rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                    rc = rtcpd_stageupdc(tape,file);
                    if ( rc == -1 ) rtcpd_SetReqStatus(NULL,file,serrno,
                        RTCP_FAILED | RTCP_EOD | RTCP_SYERR);
                    break;
                } else {
                    /*
                     * Client addressed a file beyond EOD. This is
                     * a true user error.
                     */
                    severity = RTCP_FAILED | RTCP_USERR;
                    rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                    break;
                }
            case SENOSHOST:  /* Host unknown      */
            case SENOSSERV:  /* Service unknown   */
            case SECOMERR:   /* Communication error */
            case ETDNP:      /* Tape daemon is not running */
            case ETHWERR:    /* Device malfunction */
            case ETNRDY:     /* Drive not ready */
                /*
                 * Something is wrong with server or drive. We
                 * tell client to retry elsewhere.
                 */
                severity = RTCP_RESELECT_SERV | RTCP_SYERR;
                rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                break;
            case ETCOMPA:    /* Drive/cartridge compatibility problems */
            case ETUNREC:    /* Unrecoverable media error */
                /*
                 * Problem with the tape. Don't retry.
                 */
                filereq->err.max_tpretry = -1;
                severity = RTCP_FAILED | RTCP_SYERR;
                rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                break;
            case EIO:        /* I/O error */
            case ETPARIT:    /* Parity error */
                /*
                 * Check if there is any configured error action
                 * for this medium errors (like sending a mail to
                 * operator or raising an alarm).
                 */
                sprintf(confparam,"%s_ERRACTION",tapereq->dgn);
                if ( (p = getconfent("RTCOPYD",confparam,0)) != NULL ) {
                    j = atoi(p);
                    severity = j;
                }
            case ETNOSNS:    /* No sense data available */
                if ( !(severity & RTCP_NORETRY) ) {
                    severity = RTCP_RESELECT_SERV | RTCP_SYERR;
                }
                rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                break;
            case EINTR:
                severity = RTCP_FAILED | RTCP_USERR;
                (void)Ctape_kill(tape_path);
                rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                break;
            default:
                severity = RTCP_FAILED | RTCP_UNERR;
                rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                break;
            }
            if ( (severity & RTCP_NORETRY) != 0 ) {
                /*
                 * If configured error action says noretry we
                 * reset max_tpretry so that the client won't retry
                 * on another server
                 */
                filereq->err.max_tpretry = -1;
                do_retry = 0;
            } else {
                if ( (severity & RTCP_LOCAL_RETRY) ||
                     (severity & RTCP_RESELECT_SERV) )
                     filereq->err.max_tpretry--;

                if ( !(severity & RTCP_LOCAL_RETRY) ||
                      (filereq->err.max_tpretry<0) ) do_retry = 0;
            }
        } else do_retry = 0;
    } /* end while (do_retry) */
    if ( rc == 0 ) 
        rtcp_log(LOG_DEBUG,"rtcpd_Position() Ctape_position() successful\n");
    *tape_path = '\0';
    return(rc);
}

int rtcpd_Info(tape_list_t *tape, file_list_t *file) {
    int rc, save_serrno, severity;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    char unit[CA_MAXUNMLEN+1];

    if ( tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    tapereq = &tape->tapereq;
    filereq = &file->filereq;

    rtcp_log(LOG_DEBUG,"rtcpd_Info(%s) vid=%s, fseq=%d, fsec=%d\n",
        filereq->tape_path,tapereq->vid,filereq->tape_fseq,
        file->tape_fsec);

    rc = Ctape_info( filereq->tape_path,
                    &filereq->blocksize,
                    (unsigned int *)&filereq->blockid,
                     tapereq->density,
                     tapereq->devtype,
                     unit,
                     filereq->fid,
                    &filereq->tape_fseq,
                    &filereq->recordlength,
                     filereq->recfm);
    filereq->cprc = rc;
    save_serrno = serrno;
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_Info() Ctape_info() %s\n",    
            CTP_ERRTXT);
        rtcpd_AppendClientMsg(NULL, file, "%s\n",CTP_ERRTXT);
        switch (save_serrno) {
        case SENOSHOST:
        case SENOSSERV:
        case SECOMERR:
        case ETDNP:
            severity = RTCP_RESELECT_SERV | RTCP_SYERR;
            break;
        case EINTR:
            if ( filereq != NULL ) {
                severity = RTCP_FAILED | RTCP_UNERR;
                (void)Ctape_kill(filereq->tape_path);
            }
            break;
        default:
            severity = RTCP_FAILED | RTCP_UNERR;
            break;
        }
        rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
    }
    if ( rc == 0 && strcmp(unit,tapereq->unit) != 0 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_Info() tape mount on wrong unit, expected %s found %s\n",
            tapereq->unit,unit);
        rc = -1;
    }
    if ( filereq->blocksize <= 0 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_Info() Ctape_info() returns blocksize = %d\n",
            filereq->blocksize);
        rtcpd_AppendClientMsg(NULL, file, 
            "rtcpd_Info() Ctape_info() returns blocksize = %d\n",
            filereq->blocksize);
        rtcpd_SetReqStatus(NULL,file,SEINTERNAL,RTCP_FAILED | RTCP_UNERR);
        rc = -1;
    }
    if ( rc == 0 )
        rtcp_log(LOG_DEBUG,"rtcpd_Info() Ctape_info() successful, unit=%s, blocksize=%d, recordlength=%d\n",
             unit,filereq->blocksize,filereq->recordlength);

    return(rc);
}


int rtcpd_Release(tape_list_t *tape, file_list_t *file) {
    int rc, save_serrno, flags,severity;
    char *path = NULL;
    rtcpFileRequest_t *filereq = NULL;
    rtcpTapeRequest_t *tapereq = NULL;

    if ( file == NULL ) flags = TPRLS_ALL;
    else {
        filereq = &file->filereq;
        path = filereq->tape_path;
        flags = TPRLS_PATH | TPRLS_KEEP_RSV;
    }
    if ( tape != NULL ) tapereq = &tape->tapereq;

    rtcp_log(LOG_DEBUG,"rtcpd_Release() called with path=%s\n",
        (path == NULL ? "(nil)" : path));

    if ( tapereq != NULL ) tapereq->TStartUnmount = (int)time(NULL);
    rc = Ctape_rls(path,flags);

    save_serrno = serrno;

    if ( tapereq != NULL ) tapereq->TEndUnmount = (int)time(NULL);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_Release() Ctape_rls() %s\n",
            CTP_ERRTXT);
        if ( file != NULL ) {
            rtcpd_AppendClientMsg(NULL, file, "%s\n",CTP_ERRTXT);
            severity = 0;
        }

        switch (save_serrno) {
        case EINVAL:
            path = NULL;
            flags = TPRLS_ALL;
            log(LOG_ERR,"rtcpd_Release() retry with TPRLS_ALL\n");
            (void)Ctape_rls(path,flags);
            break;
        case ETFSQ:      /* Bad file sequence number */
            if ( (filereq->concat & (CONCAT_TO_EOD | NOCONCAT_TO_EOD)) != 0 )
                rc = 0;
            break;
        case EINTR:
            if ( filereq != NULL ) {
                severity = RTCP_FAILED | RTCP_UNERR;
                (void)Ctape_kill(filereq->tape_path);
            }
            break;
        default:
            break;
        }
        if ( file != NULL && rc == -1 ) 
            rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
    } else
        rtcp_log(LOG_DEBUG,"rtcpd_Release() Ctape_rls() successful\n");

    serrno = save_serrno;
    return(rc);
}

    
