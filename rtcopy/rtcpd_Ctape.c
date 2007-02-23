/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpd_Ctape.c - RTCOPY interface to Ctape
 */

#include <stdlib.h>
#include <string.h>
#include <time.h>
#if defined(_WIN32)
#include <process.h>
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <unistd.h>                      /* Network "data base"          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#endif /* _WIN32 */

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
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <Ctape_api.h>
#include <serrno.h>

#define CTP_ERRTXT rtcpd_CtapeErrMsg()
#ifndef EBUSY_RETRIES
#define EBUSY_RETRIES 10
#endif /* EBUSY_RETRIES */

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

char *rtcpd_GetCtapeErrBuf() {
    char *errbuf;
    int errbufsiz = CA_MAXLINELEN+1;

    Cglobals_get(&Ctape_key,(void **)&errbuf,errbufsiz);
    return(errbuf);
}

char *rtcpd_CtapeErrMsg() {
    char *errbuf;
    int save_serrno;

    save_serrno = serrno > 0 ? serrno : errno;
    errbuf = rtcpd_GetCtapeErrBuf();
    if ( errbuf == NULL ) return(Unkn_errorstr);
    if ( *errbuf == '\0' ) {
        if ( save_serrno > 0 ) return(sstrerror(save_serrno));
        else return(Unkn_errorstr);
    }
    return(errbuf);
}

void rtcpd_ResetCtapeError() {
    char *errbuf;

    errbuf = rtcpd_GetCtapeErrBuf();
    if ( errbuf != NULL ) *errbuf = '\0';
    serrno = 0;
    return;
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
    jobID = rtcpd_jobID();

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
                   rtcpTapeRequest_t *tapereq,
                   rtcpClientInfo_t *client) {
    int rc, status, jobID, value, save_serrno, save_errno;
    char vid[CA_MAXVIDLEN+1];
    tape_list_t *tape, *nexttape;
    file_list_t *nextfile = NULL;

    save_serrno = serrno;
    save_errno = errno;
    if ( tapereq == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_Deassign() called with NULL tapereq\n");
        serrno = EINVAL;
        return(-1);
    }
    rtcp_log(LOG_DEBUG,"rtcpd_Deassign() called\n");

    /*
     * Check whether there is a volume physically mounted (could be
     * the case if this is a job re-using an already mounted volume).
     */
    status = VDQM_UNIT_QUERY;
    *vid = '\0';
    jobID = 0;
    rc = vdqm_UnitStatus(NULL,vid,tapereq->dgn,NULL,tapereq->unit,
                         &status,&value,jobID);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_Deassign() vdqm_UnitStatus(UNIT_QUERY) %s\n",
                sstrerror(serrno));
    } else if ( (status & (VDQM_UNIT_ASSIGN|VDQM_FORCE_UNMOUNT)) != 0 ) {
        /*
         * If tape software has already assigned the unit or called for
         * force unmount, the cleanup will not be needed here.
         */
        rtcp_log(LOG_INFO,"rtcpd_Deassign() unit status=0x%x. Cleanup not needed\n",
                 status);
        return(0);
    } else if ( *vid != '\0' ) {
        /*
         * There is a volume on the unit! we must make sure that it is
         * physically unmounted.
         */
        rtcp_log(LOG_INFO,"rtcpd_Deassign() volume on drive. Trying to unmount\n");
        tape = NULL;
        nexttape = (tape_list_t *)calloc(1,sizeof(tape_list_t));
        CLIST_INSERT(tape,nexttape);
        nextfile = (file_list_t *)calloc(1,sizeof(file_list_t));
        CLIST_INSERT(tape->file,nextfile);
        nexttape->tapereq = *tapereq;
        if ( tapereq->VolReqID <= 0 ) nexttape->tapereq.VolReqID = VolReqID;
        strcpy(nexttape->tapereq.vid,vid);
        *(nexttape->tapereq.density) = '\0';
        *(nexttape->tapereq.vsn) = '\0';
        *(nexttape->tapereq.label) = '\0';

        /*
         * In case we are still root
         */
        if ( client != NULL && getgid() != (gid_t)client->gid ) 
            (void)setgid((gid_t)client->gid);
        if ( client != NULL && getuid() != (uid_t)client->uid ) 
            (void)setuid((uid_t)client->uid);
#if defined(CTAPE_DUMMIES)
        /*
         * If we run with dummy Ctape we need to assign the
         * drive here
         */
        (void) rtcpd_Assign(tape);
#endif /* CTAPE_DUMMIES */
        (void)rtcpd_Reserve(tape);
        *(nexttape->tapereq.dgn) = '\0';
        nexttape->tapereq.mode = WRITE_DISABLE;
        rc = rtcpd_Mount(tape);
        /*
         * Make sure we release without NOWAIT
         */
        tape->tapereq.tprc = -1;
        (void)rtcpd_Release(tape,NULL);

        free(nextfile);
        free(nexttape);
        return(rc);
    }

    /*
     * If status is already FREE or DOWN we don't need to do anything more
     */
    if ( rc != -1 && ((status == (VDQM_UNIT_UP|VDQM_UNIT_FREE)) ||
                      (status & VDQM_UNIT_DOWN) != 0) ) return(0);
    /*
     * We're here because there was no tape physically mounted on the drive.
     * Just make sure that the job is released in VDQM.
     */

    if ( tapereq->jobID <= 0 ) {
        /*
         * We need to assign the unit first (for safety VDQM
         * only allows to RELEASE with jobID's).
         */
        status = VDQM_UNIT_ASSIGN;
        if ( VolReqID > 0 ) value = VolReqID;
        else value = tapereq->VolReqID;
        jobID = rtcpd_jobID();

        rtcp_log(LOG_INFO,"rtcpd_Deassign() ASSIGN volreq ID %d -> jobID %d\n",
            value,jobID);
        rc = vdqm_UnitStatus(NULL,NULL,tapereq->dgn,NULL,tapereq->unit,
            &status,&value,jobID);
        if ( rc == -1 ) {
            rtcp_log(LOG_INFO,"rtcpd_Deassign() vdqm_UnitStatus(UNIT_ASSIGN) %s\n",
                sstrerror(serrno));
        }
    } else jobID = tapereq->jobID;
    status = VDQM_UNIT_RELEASE;

    rtcp_log(LOG_INFO,"rtcpd_Deassign() RELEASE job ID %d\n",jobID);
    rc = vdqm_UnitStatus(NULL,NULL,tapereq->dgn,NULL,tapereq->unit,
        &status,NULL,jobID);
    if ( rc == -1 ) {
        rtcp_log(LOG_INFO,"rtcpd_Deassign() vdqm_UnitStatus(UNIT_RELEASE) %s\n",
            sstrerror(serrno));
    }
    serrno = save_serrno;
    errno = save_errno;
    return(rc);
}

void rtcpd_CtapeKill() {
    if ( tape_path[0] != '\0' ) Ctape_kill(tape_path);
    return;
}

int rtcpd_Reserve(tape_list_t *tape) {
    struct dgn_rsv rsv;
    int rc, count, save_serrno, severity;
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

    rtcpd_ResetCtapeError();
    rc = Ctape_reserve(count,&rsv);

    tapereq->tprc = rc;
    save_serrno = serrno;
    if ( AbortFlag == 1 ) {
        rtcp_log(LOG_ERR,"rtcpd_Reserve() ABORTING!\n");
        severity = RTCP_FAILED | RTCP_USERR;
        rtcpd_SetReqStatus(tape,NULL,save_serrno,severity);
        return(-1);
    }

    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcp_Reserve() Ctape_reserve(): %s\n",
            CTP_ERRTXT);
        if ( save_serrno == ETNDV ) {
            rtcpd_SetReqStatus(tape,NULL,save_serrno,RTCP_RESELECT_SERV);
        } else {
            rtcpd_SetReqStatus(tape,NULL,save_serrno,RTCP_FAILED);
        }
        rtcpd_AppendClientMsg(tape, NULL, "%s\n",CTP_ERRTXT);
        return(-1);
    }

    rtcp_log(LOG_DEBUG,"rtcpd_Reserve() successful\n");
    return(0);
}

int rtcpd_Mount(tape_list_t *tape) {
    int rc, j, save_serrno,severity, retry, jobID;
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
    jobID = rtcpd_jobID();

    if ( *filereq->tape_path == '\0' ) 
        sprintf(filereq->tape_path,"PATH%d",jobID);

    j = 0;
    while ( (rc = stat(filereq->tape_path,&st)) == 0 ) {
        sprintf(filereq->tape_path,"PATH%d.%d",jobID,++j);
    }

    /*
     * Retry loop on EBUSY.
     */
    for (retry=0; retry<EBUSY_RETRIES; retry++) {
        rtcp_log(LOG_DEBUG,"Ctape_mount(%s,%s,%d,%s,%s,%s,%d,%s,%s,%d)\n",
                 filereq->tape_path,
                 tapereq->vid,
                 tapereq->side,
                 tapereq->dgn,
                 tapereq->density,
                 tapereq->unit,
                 tapereq->mode,
                 tapereq->vsn,
                 tapereq->label,
                 tapereq->VolReqID);

        severity = RTCP_OK;

        serrno = errno = 0;
        /*
         * We need to save tape_path for Ctape_kill() in case there is 
         * an ABORT. The reason is that main control thread may have 
         * free'd the tape and file request structures while aborting 
         * and the tape_path information gone lost.
         */
        strcpy(tape_path,filereq->tape_path);
        rtcpd_ResetCtapeError();
        tapereq->TStartMount = (int)time(NULL);
        rc = Ctape_mount(filereq->tape_path,
                         (*tapereq->vid != '\0' ? tapereq->vid : NULL),
                         tapereq->side,
                         (*tapereq->dgn != '\0' ? tapereq->dgn : NULL),
                         (*tapereq->density != '\0' ? tapereq->density : NULL),
                         (*tapereq->unit != '\0' ? tapereq->unit : NULL),
                         tapereq->mode,
                         (*tapereq->vsn != '\0' ? tapereq->vsn : NULL),
                         (*tapereq->label != '\0' ? tapereq->label : NULL),
                         tapereq->VolReqID);
    
        tapereq->tprc = rc;
        tape_path[0] = '\0';
        save_serrno = (serrno != 0 ? serrno : errno);
        tapereq->TEndMount = (int)time(NULL);
        if ( AbortFlag == 1 ) {
            rtcp_log(LOG_ERR,"rtcpd_Mount() ABORTING!\n");
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
            if ( save_serrno != ETVBSY && save_serrno != EBUSY ) 
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
                    tapereq->err.max_tpretry = 0;
                } else {
                    tapereq->err.max_tpretry--;
                    severity |= RTCP_RESELECT_SERV;
                }
                break;
            case ETVBSY:
                /*
                 * Retry forever on a volume busy
                 */
                tapereq->err.max_tpretry = MAX_TPRETRY;
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
            case ETBADMIR:
                severity = RTCP_FAILED | RTCP_SYERR;
                break;
            case EINVAL: /* Wrong parameter...*/
            case EACCES: /* TMS denied access */
            case ETWLBL: /* Wrong label taype */
            case ETWVSN: /* Wrong vsn */
            case ETHELD: /* Volume held in TMS */
            case ETABSENT: /* Volume absent in */
            case ETWPROT: /* Cartridge write protected (physical or TMS) */
            case ETARCH: /* Volume archived */
            case ETVUNKN: /* Volume unknown */
            case ETOPAB: /* Operator cancel (should this be a SYERR rather?) */
            case ETINTR:
            case ETBLANK:
            case ETCOMPA:
            case ETUNREC:
                severity = RTCP_FAILED | RTCP_USERR;
                break;
            case ETNDV:
                severity = RTCP_RESELECT_SERV;
                break;
            case ETIDG:
            case ETNRS:
                severity = RTCP_FAILED | RTCP_SEERR;
                break;
            case EINTR:
                severity = RTCP_FAILED | RTCP_USERR;
                break;
            case EBUSY:
                rtcp_log(LOG_INFO,"rtcpd_Mount() retry %d on EBUSY, drive=%s\n",
                         retry,tapereq->unit);
                sleep(5);
                break;
            default:
                severity = RTCP_FAILED | RTCP_UNERR;
                break;
            }
            if ( save_serrno != EBUSY ) 
                rtcpd_SetReqStatus(tape,NULL,save_serrno,severity);
        } /* if ( rc == -1 ) */
        /*
         * Retry on EBUSY since this can be a transient status while 
         * previous job is release from the drive
         */
        if ( rc == 0 || (rc == -1 && save_serrno != EBUSY) ) break;
    } /* for (retry==0; retry<EBUSY_RETRIES; retry++) */
    /*
     * Retries didn't help...
     */
    if ( rc == -1 && save_serrno == EBUSY ) {
         rtcp_log(LOG_ERR,"rtcpd_Mount() giving up after %d retries on EBUSY\n",
                  retry);
         severity = RTCP_FAILED | RTCP_USERR;
         rtcpd_SetReqStatus(tape,NULL,save_serrno,severity);
    }

    if ( rc == 0 ) 
        rtcp_log(LOG_DEBUG,"rtcpd_Mount() Ctape_mount() successful\n");

    return(rc);
}

int rtcpd_Position(tape_list_t *tape,
                   file_list_t *file) {
    int rc, j, save_serrno, flags, do_retry, severity;
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
        if ( *tape->file->filereq.tape_path != '\0' ) 
            strcpy(filereq->tape_path,tape->file->filereq.tape_path);
        else {
            rtcpd_AppendClientMsg(NULL, file,"rtcpd_Position(): cannot determine tape path\n");
            rtcp_log(LOG_DEBUG,"rtcpd_Position(): cannot determine tape path\n");
            severity = RTCP_FAILED|RTCP_UNERR;
            save_serrno = SEINTERNAL;
            rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
            serrno = save_serrno;
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
            if ( (prevreq->err.severity == RTCP_OK && 
                  tape->local_retry == 0) || 
                ((prevreq->err.severity & RTCP_LIMBYSZ) != 0 &&
                 tapereq->mode == WRITE_ENABLE ) ) {
                flags |= NOPOS;
            }
        }
    }

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
        rtcp_log(LOG_DEBUG,"rtcpd_Position() Ctape_position(%s,0x%x,%d,%d,%d:%d:%d:%d,%d,%d,0x%x,%s,%s,%s,%d,%d,%d,0x%x)\n",
                 filereq->tape_path,
                 filereq->position_method,
                 filereq->tape_fseq,
                 file->tape_fsec,
                 (int)filereq->blockid[0],(int)filereq->blockid[1],
                 (int)filereq->blockid[2],(int)filereq->blockid[3],
                 tapereq->start_file,
                 tapereq->end_file,
                 filereq->check_fid,
                 filereq->fid,
                 tapereq->vsn,
                 filereq->recfm,
                 filereq->blocksize,
                 filereq->recordlength,
                 filereq->retention,flags);
        serrno = errno = 0;
        rtcpd_ResetCtapeError();
        rc = Ctape_position(filereq->tape_path,
                            filereq->position_method,
                            filereq->tape_fseq,
                            file->tape_fsec,
                            filereq->blockid,
                            tapereq->start_file,
                            tapereq->end_file,
                            filereq->check_fid,
                            filereq->fid,
                            tapereq->vsn,
                            filereq->recfm,
                            filereq->blocksize,
                            filereq->recordlength,
                            filereq->retention,
                            flags);
        save_serrno = (serrno > 0 ? serrno : errno);
        filereq->TEndPosition = (int)time(NULL);
        filereq->cprc = rc;
        tape_path[0] = '\0';
        if ( AbortFlag == 1 ) {
            rtcp_log(LOG_ERR,"rtcpd_Position() ABORTING!\n");
            severity = RTCP_FAILED | RTCP_USERR;
            save_serrno = EINTR;
            rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
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
            case ETNXPD:     /* File not expired */
            case ENOENT:     /* File not found */
            case EINVAL:     /* Invalid parameter */
            case ETLBL:      /* Bad label structure */
            case ETEOV:      /* EOV found in multivolume set */
            case ETBLANK:    /* Blank tape        */
                severity = RTCP_FAILED | RTCP_USERR;
                rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                break;
            case ETFSQ:      /* Bad file sequence number */
                if ( (filereq->concat & CONCAT_TO_EOD) != 0 ) {
                    /*
                     * We reached last tape file when concatenating
                     * to a disk file. This is not an error since
                     * it can only happen if the user explicitly 
                     * specified to concatenate all tape files until EOD
                     * into a single disk file
                     */
                    severity = RTCP_OK | RTCP_EOD;
                    rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                    rc = 0;
                    break;
                } else if ( (filereq->concat & NOCONCAT_TO_EOD) != 0 &&
                            file != tape->file ) {
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
                    severity = RTCP_OK | RTCP_EOD;
                    rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
                    rc = 0;
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
                filereq->err.max_tpretry = 0;
                severity = RTCP_FAILED | RTCP_USERR;
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
                filereq->err.max_tpretry = 0;
                do_retry = 0;
            } else {
                if ( (severity & RTCP_LOCAL_RETRY) ||
                     (severity & RTCP_RESELECT_SERV) )
                     filereq->err.max_tpretry--;

                if ( !(severity & RTCP_LOCAL_RETRY) ||
                      (filereq->err.max_tpretry<=0) ) do_retry = 0;
            }
        } else do_retry = 0;
    } /* end while (do_retry) */
    if ( rc == 0 ) 
        rtcp_log(LOG_DEBUG,"rtcpd_Position() Ctape_position() successful\n");
    return(rc);
}

int rtcpd_drvinfo(tape_list_t *tape) {
    struct devinfo devInfo;
    int rc;
    file_list_t *fl;

    if ( tape == NULL || tape->file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    fl = tape->file;
    rtcp_log(LOG_DEBUG,"rtcpd_drvinfo() called with vid=%s, unit=%s, mode=%d, check_fid=%d\n",
        tape->tapereq.vid,tape->tapereq.unit,
        tape->tapereq.mode,fl->filereq.check_fid);
    if ( tape->tapereq.mode == WRITE_DISABLE ||
         fl->filereq.check_fid == CHECK_FILE ) return(0);

    rc = Ctape_drvinfo(tape->tapereq.unit,&devInfo);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_drvinfo() (ingored) Ctape_drvinfo() %s\n",    
            CTP_ERRTXT);
        return(0);
    }
    rtcp_log(LOG_DEBUG,"rtcpd_drvinfo() Ctape_drvinfo() returned default blocksize=%d\n",
        devInfo.defblksize);
    CLIST_ITERATE_BEGIN(tape->file,fl) {
        if ( fl->filereq.blocksize <= 0 ) {
            rtcp_log(LOG_DEBUG,"rtcpd_drvinfo() set default blocksize (%d) for fseq=%d, file=%s\n",
                devInfo.defblksize,fl->filereq.tape_fseq,fl->filereq.file_path);
            fl->filereq.blocksize = devInfo.defblksize;
        } else {
            rtcp_log(LOG_DEBUG,"rtcpd_drvinfo() blocksize (%d) already set by client for fseq=%d, file=%s\n",
                fl->filereq.blocksize,fl->filereq.tape_fseq,fl->filereq.file_path);
        }
    } CLIST_ITERATE_END(tape->file,fl);
    return(0);
}

int rtcpd_Info(tape_list_t *tape, file_list_t *file) {
    int rc, fseq, save_serrno, severity;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    char unit[CA_MAXUNMLEN+1];
    char recfm[CA_MAXRECFMLEN+1];

    if ( tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    tapereq = &tape->tapereq;
    filereq = &file->filereq;
    fseq = filereq->tape_fseq;

    rtcp_log(LOG_DEBUG,"rtcpd_Info(%s) vid=%s, fseq=%d, fsec=%d\n",
        filereq->tape_path,tapereq->vid,filereq->tape_fseq,
        file->tape_fsec);

    *recfm = '\0';
    rtcpd_ResetCtapeError();
    rc = Ctape_info( filereq->tape_path,
                    &filereq->blocksize,
                     filereq->blockid,
                     tapereq->density,
                     tapereq->devtype,
                     unit,
                     filereq->fid,
                    &filereq->tape_fseq,
                    &filereq->recordlength,
                     recfm);
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
            if ( filereq != NULL ) severity = RTCP_FAILED | RTCP_UNERR;
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
    if ( fseq > 0 && filereq->blocksize <= 0 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_Info() Ctape_info() returns blocksize = %d\n",
            filereq->blocksize);
        rtcpd_AppendClientMsg(NULL, file, 
            "rtcpd_Info() Ctape_info() returns blocksize = %d\n",
            filereq->blocksize);
        rtcpd_SetReqStatus(NULL,file,SEINTERNAL,RTCP_FAILED | RTCP_UNERR);
        rc = -1;
    }
    /*
     * Only set recfm if user didn't specify it.
     */
    if ( *filereq->recfm == '\0' ) strcpy(filereq->recfm,recfm);
    if ( *filereq->recfm == '\0' ) strcpy(filereq->recfm,"U");
    if ( rc == 0 )
        rtcp_log(LOG_DEBUG,"rtcpd_Info(%s) returned vid=%s, fseq=%d, fsec=%d, unit=%s, blocksize=%d, recordlength=%d, recfm=%s, blockid=%d:%d:%d:%d\n",
                 filereq->tape_path,tapereq->vid,filereq->tape_fseq,
                 file->tape_fsec,unit,filereq->blocksize,filereq->recordlength,
                 recfm,(int)filereq->blockid[0],(int)filereq->blockid[1],
                 (int)filereq->blockid[2],(int)filereq->blockid[3]);

    return(rc);
}


int rtcpd_Release(tape_list_t *tape, file_list_t *file) {
    int rc, save_serrno, flags,severity;
    char *path = NULL;
    rtcpFileRequest_t *filereq = NULL;
    rtcpTapeRequest_t *tapereq = NULL;

    if ( file == NULL ) {
        if ( tape != NULL && tape->tapereq.tprc == -1 ) flags = TPRLS_ALL;
        else flags = TPRLS_ALL | TPRLS_NOWAIT;
    } else {
        filereq = &file->filereq;
        path = filereq->tape_path;
        flags = TPRLS_PATH | TPRLS_KEEP_RSV;
    }
    if ( tape != NULL ) tapereq = &tape->tapereq;

    rtcp_log(LOG_DEBUG,"rtcpd_Release() called with path=%s\n",
        (path == NULL ? "(nil)" : path));

    rtcpd_ResetCtapeError();
    if ( tapereq != NULL ) tapereq->TStartUnmount = (int)time(NULL);
    rc = Ctape_rls(path,flags);

    save_serrno = serrno;

    if ( tapereq != NULL ) tapereq->TEndUnmount = (int)time(NULL);
    if ( rc == -1 && save_serrno != ETRLSP) {
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
            rtcpd_ResetCtapeError();
            (void)Ctape_rls(path,flags);
            break;
        case ETFSQ:      /* Bad file sequence number */
            if ( (filereq->concat & (CONCAT_TO_EOD | NOCONCAT_TO_EOD)) != 0 )
                rc = 0;
            break;
        case EINTR:
            if ( filereq != NULL ) severity = RTCP_FAILED | RTCP_UNERR;
            break;
        default:
            break;
        }
        if ( file != NULL && rc == -1 ) 
            rtcpd_SetReqStatus(NULL,file,save_serrno,severity);
    } else {
        rc = 0;
        rtcp_log(LOG_DEBUG,"rtcpd_Release() Ctape_rls() successful\n");
    }

    serrno = save_serrno;
    return(rc);
}

int rtcpd_DmpInit(tape_list_t *tape) {
    int rc, code, save_serrno;

    if ( tape == NULL || tape->file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( tape->dumpreq.convert == EBCCONV ) code = DMP_EBC;
    else code = DMP_ASC;

    rtcp_log(LOG_DEBUG,"rtcpd_DmpInit() called\n");

    rtcp_log(LOG_DEBUG,"Ctape_dmpinit(%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d)\n",
             tape->file->filereq.tape_path,
             tape->tapereq.vid,
             tape->tapereq.density,
             tape->tapereq.unit,
             tape->tapereq.devtype,
             tape->dumpreq.blocksize,
             tape->dumpreq.fromblock,
             tape->dumpreq.toblock,
             tape->dumpreq.maxbyte,
             tape->dumpreq.startfile,
             tape->dumpreq.maxfile,
             code,
             tape->dumpreq.tp_err_action);


    rc = Ctape_dmpinit(tape->file->filereq.tape_path,
                       tape->tapereq.vid,
                       tape->tapereq.density,
                       tape->tapereq.unit,
                       tape->tapereq.devtype,
                       tape->dumpreq.blocksize,
                       tape->dumpreq.fromblock,
                       tape->dumpreq.toblock,
                       tape->dumpreq.maxbyte,
                       tape->dumpreq.startfile,
                       tape->dumpreq.maxfile,
                       code,
                       tape->dumpreq.tp_err_action);

    save_serrno = serrno;
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_DmpInit() Ctape_dmpinit(): %s\n",
                 sstrerror(save_serrno));
        serrno = save_serrno;
    } else rtcp_log(LOG_DEBUG,"rtcpd_DmpInit() Ctape_dmpinit() successful\n");
    return(rc);
}

int rtcpd_DmpFile(tape_list_t *tape, file_list_t *file) {
    int rc, save_serrno;
    rtcpFileRequest_t filereq;

    if ( tape == NULL || tape->file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    rtcp_log(LOG_DEBUG,"rtcpd_DmpFile() called\n");

    memset(&filereq,'\0',sizeof(filereq));

   rtcp_log(LOG_DEBUG,"call Ctape_dmpfil(%s,%s,%d,%s,%d,%d,%d,%s,%d)\n",
            tape->file->filereq.tape_path,
            tape->tapereq.label,
            filereq.blocksize,
            filereq.fid,
            tape->file->tape_fsec,
            filereq.tape_fseq,
            filereq.recordlength,
            filereq.recfm,
            (int)filereq.maxsize);

    rc = Ctape_dmpfil(tape->file->filereq.tape_path,
                      tape->tapereq.label,
                      &filereq.blocksize,
                      filereq.fid,
                      &(tape->file->tape_fsec),
                      &filereq.tape_fseq,
                      &filereq.recordlength,
                      filereq.recfm,
                      &filereq.maxsize);

   rtcp_log(LOG_DEBUG,"returned Ctape_dmpfil(%s,%s,%d,%s,%d,%d,%d,%s,%d)\n",
            tape->file->filereq.tape_path,
            tape->tapereq.label,
            filereq.blocksize,
            filereq.fid,
            tape->file->tape_fsec,
            filereq.tape_fseq,
            filereq.recordlength,
            filereq.recfm,
            (int)filereq.maxsize);


     save_serrno = serrno;
     if ( rc == -1 ) {
         rtcp_log(LOG_ERR,"rtcpd_DmpFile() Ctape_dmpfil(): %s\n",
                  sstrerror(save_serrno));
         serrno = save_serrno;
     } else {
         rtcp_log(LOG_DEBUG,"rtcpd_DmpFile() Ctape_dmpfil() successful\n");
         if ( file != NULL ) file->filereq = filereq;
     }
     return(rc);
}

int rtcpd_DmpEnd() {
    rtcp_log(LOG_DEBUG,"rtcpd_DmpEnd() called\n");
    (void)Ctape_dmpend();
    rtcp_log(LOG_DEBUG,"rtcpd_DmpEnd() Ctape_dmpend() successful\n");
    return(0);
}
