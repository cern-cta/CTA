/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_MainCntl.c,v $ $Revision: 1.73 $ $Date: 2000/06/13 16:37:27 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_MainCntl.c - RTCOPY server main control thread
 */

#include <stdlib.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#endif /* _WIN32 */

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <signal.h>

#include <pwd.h>
#include <grp.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <Cpwd.h>
#include <sacct.h>
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
#if defined(sgi)
/*
 * Workaround for SGI flags in grp.h
 */
extern struct group *getgrent();
#endif /* IRIX */

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

extern int SHIFTclient;
int SHIFTclient = FALSE;

extern int Dumptape;
int Dumptape = FALSE;

/*
 * Global variable needed to flag that an ENOSPC has been sent to the
 * stager and thus no further stageupdc calls should take place until
 * next retry with a new file. This flag is reset in MainCntl after
 * having waited for completion of all subrequests up to the ENOSPC
 * failure. This total synchronisation is needed for SHIFT stagers since
 * they expect all stageupdc in sequence.
 */
extern int ENOSPC_occurred;
int ENOSPC_occurred = FALSE;

/*
 * Set Debug flag if requested
 */
static void rtcpd_SetDebug() {
    Debug = FALSE;
    if ( getenv("RTCOPY_DEBUG") != NULL ||
         getconfent("RTCOPY","DEBUG",1) != NULL ) Debug = TRUE;
    else NOTRACE;                 /* Switch off tracing */
    if ( Debug == TRUE ) initlog("rtcopyd",LOG_DEBUG,RTCOPY_LOGFILE);
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
 * Check that client account code exists for requested group 
 */
static int rtcpd_ChkNewAcct(char *acctstr,struct passwd *pwd,gid_t gid) {
    char buf[BUFSIZ] ;
    char * def_acct ;
    struct group * gr ;
    char * getacctent() ;

    if ( acctstr == NULL || pwd == NULL ) return(-1);
    acctstr[0]= '\0' ;
    /* get default account */
    if ( getacctent(pwd,NULL,buf,sizeof(buf)) == NULL ) return(-1);
    if ( strtok(buf,":") == NULL || (def_acct= strtok(NULL,":")) == NULL ) return(-1);
    if ( strlen(def_acct) == 6 && *(def_acct+3) == '$' &&   /* uuu$gg */
         (gr= getgrgid((gid_t)gid)) ) {
        strncpy(acctstr,def_acct,4) ;
        strcpy(acctstr+4,gr->gr_name) ; /* new uuu$gg */
        if ( getacctent(pwd,acctstr,buf,sizeof(buf)) )
            return(0);      /* newacct was executed */
    }
    acctstr[0]= '\0' ;
    return(-1);
}


/*
 * Routine to check and authorize client. This routine should be called
 * from main thread. It's inherently thread unsafe because of getgrent(). 
 */
int rtcpd_CheckClient(int _uid, int _gid, char *name, 
                      char *acctstr, int *rc) {
    struct passwd *pw;
    struct group *gr;
    char **gr_mem;
    uid_t uid;
    gid_t gid;

    if ( name == NULL || acctstr == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    uid = (uid_t)_uid;
    gid = (gid_t)_gid;
    if ( rc != NULL ) *rc = 0;

    if ( uid < 100 ) {
        rtcp_log(LOG_ERR,"request from uid smaller than 100 are rejected\n");
        if ( rc != NULL ) *rc = PERMDENIED;
        return(-1);
    }
    if ( (pw = Cgetpwuid(uid)) == NULL ) {
        rtcp_log(LOG_ERR,"your uid is not defined on this server\n");
        if ( rc != NULL ) *rc = UNKNOWNUID;
        return(-1);
    }
    if ( strcmp(pw->pw_name,name) != 0 ) {
        rtcp_log(LOG_ERR,"your uid does not match your login name\n");
        if ( rc != NULL ) *rc = UIDMISMATCH;
        return(-1);
    }
    if ( pw->pw_gid != gid ) {
        setgrent();
        while ( (gr = getgrent()) ) {
            if ( pw->pw_gid == gr->gr_gid ) continue;
            for ( gr_mem = gr->gr_mem; gr_mem != NULL && *gr_mem != NULL;
                  gr_mem++ ) if ( !strcmp(*gr_mem,name) ) break;

            if ( gr_mem != NULL && *gr_mem != NULL && 
                 !strcmp(*gr_mem,name) ) break;
        }
        endgrent();
        if ( (gr_mem == NULL || *gr_mem == NULL || 
              strcmp(*gr_mem,name)) &&
             (rtcpd_ChkNewAcct(acctstr,pw,gid) < 0) ) {
            rtcp_log(LOG_ERR,"your gid does not match your uid\n") ;
            if ( rc != NULL ) *rc = UNKNOWNGID;
            return(-1);
        }
    }

    return(0);
}

/*
 * Log command line a'la SHIFT.
 */
#define LOGLINE_APPEND(X,Y) sprintf(&logline[strlen(logline)],(X),(Y))
static int rtcpd_PrintCmd(tape_list_t *tape) {
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    rtcpDumpTapeRequest_t *dumpreq;
    tape_list_t *tl;
    file_list_t *fl;
    char *vid;
    char *vsn;
    char logline[CA_MAXLINELEN+1], common_fid[CA_MAXFIDLEN+1];
    int mode,fseq;
    u_signed64 common_sz;

    if ( tape == NULL ) return(-1);

    *logline = '\0';
    tapereq = &tape->tapereq;
    vid = tapereq->vid;
    vsn = tapereq->vsn;
    mode = tapereq->mode;
    if ( tape->file != NULL ) 
        sprintf(logline,"%s %s %s",(mode == WRITE_ENABLE ? "tpwrite" : "tpread"),
                (*vid != '\0' ? "-V" : "-v"),(*vid != '\0' ? vid : vsn));
    else
        sprintf(logline,"dumptape %s %s",(*vid != '\0' ? "-V" : "-v"),
                                         (*vid != '\0' ? vid : vsn));
    CLIST_ITERATE_BEGIN(tape,tl) {
        if ( tl != tape ) {
            tapereq = &tl->tapereq;
            LOGLINE_APPEND(":%s",
                    (*vid != '\0' ? tapereq->vid : tapereq->vsn));
        }
    } CLIST_ITERATE_END(tape,tl);

    if ( tape->file == NULL ) {
        dumpreq = &tape->dumpreq;
        if ( dumpreq->maxbyte >= 0 ) LOGLINE_APPEND(" -B %d",dumpreq->maxbyte);
        if ( dumpreq->blocksize >= 0 ) LOGLINE_APPEND(" -b %d",dumpreq->blocksize);
        if ( dumpreq->maxfile >= 0 ) LOGLINE_APPEND(" -F %d",dumpreq->maxfile);
        if ( dumpreq->tp_err_action == IGNOREEOI ) 
            LOGLINE_APPEND(" -E %s","ignoreeoi");
        if ( dumpreq->convert == EBCCONV ) LOGLINE_APPEND(" -C %s","ebcdic");
        if ( dumpreq->fromblock >= 0 && dumpreq->toblock >= 0 ) {
            LOGLINE_APPEND(" -N %d",dumpreq->fromblock);
            LOGLINE_APPEND(",%d",dumpreq->toblock);
        } else if ( dumpreq->toblock >= 0 ) {
            LOGLINE_APPEND(" -N %d",dumpreq->toblock);
        }
        if ( *logline != '\0' ) rtcp_log(LOG_INFO,"%s\n",logline);
        return(0);
    }
    filereq = &tape->file->filereq;
    if ( *filereq->recfm != '\0' ) {
        LOGLINE_APPEND(" -F %s",filereq->recfm);
        if ( filereq->convert > 0 &&
             (filereq->convert & NOF77CW) != 0 ) LOGLINE_APPEND(",%s","-f77");
    }
    if ( filereq->blocksize > 0 )
        LOGLINE_APPEND(" -b %d",filereq->blocksize);
    if ( filereq->recordlength > 0 ) 
        LOGLINE_APPEND(" -L %d",filereq->recordlength);
    if ( filereq->def_alloc > 0 )
        LOGLINE_APPEND(" -A %s","deferred");
    if ( filereq->rtcp_err_action == SKIPBAD ) 
        LOGLINE_APPEND(" -E %s","skip");
    if ( filereq->rtcp_err_action == KEEPFILE ) 
        LOGLINE_APPEND(" -E %s","keep");
    if ( filereq->tp_err_action == IGNOREEOI ) 
        LOGLINE_APPEND(" -E %s","ignoreeoi");
    if ( filereq->check_fid == CHECK_FILE )
        LOGLINE_APPEND(" %s","-o");
    if ( filereq->check_fid == NEW_FILE )
        LOGLINE_APPEND(" %s","-n");
    if ( *filereq->stageID != '\0' )
        LOGLINE_APPEND(" -Z %s",filereq->stageID);
    if ( filereq->convert > 0 && (filereq->convert & (EBCCONV|FIXVAR)) != 0 ) { 
        LOGLINE_APPEND("%s"," -C "); 
        if ( (filereq->convert & EBCCONV) != 0 ) {
            LOGLINE_APPEND("%s","ebcdic");
        } else LOGLINE_APPEND("%s","ascii");
        if ( (filereq->convert & FIXVAR) != 0 )
            LOGLINE_APPEND(",%s","block");
    } 

    if ( filereq->maxnbrec > 0 )
        LOGLINE_APPEND(" -N %d",(int)filereq->maxnbrec);

    if ( filereq->position_method == TPPOSIT_EOI ||
         filereq->position_method == TPPOSIT_FID ) {
        fseq = 0;
        CLIST_ITERATE_BEGIN(tape->file,fl) {
            if ( (fl->filereq.concat & CONCAT) == 0 ) fseq++;
        } CLIST_ITERATE_END(tape->file,fl);
        LOGLINE_APPEND(" -q %s",
            (filereq->position_method == TPPOSIT_EOI ? "n" : "u"));
        if ( fseq > 1 ) LOGLINE_APPEND("%d",fseq);
    }

    fseq = filereq->tape_fseq;
    if ( filereq->position_method == TPPOSIT_FSEQ ) {
        CLIST_ITERATE_BEGIN(tape->file,fl) {
            filereq = &fl->filereq;
            if ( fl == tape->file ) LOGLINE_APPEND(" -q %d",fseq);
            else if ( filereq->tape_fseq != fseq ) {
                if ( strlen(logline) + 11 >= CA_MAXLINELEN ) {
                    if ( logline[strlen(logline)-1] == '-' )
                        if ( (filereq->concat & 
                              (NOCONCAT_TO_EOD|CONCAT_TO_EOD))==0 ) 
                            rtcp_log(LOG_INFO,"%s%d \\\n",logline,fseq);
                        else
                            rtcp_log(LOG_INFO,"%s \\\n",logline);
                    else
                        rtcp_log(LOG_INFO,"%s, \\\n",logline);
                    *logline = '\0';
                }
                if ( (filereq->concat & (NOCONCAT_TO_EOD|CONCAT_TO_EOD))==0 ) {
                    if ( filereq->tape_fseq == fseq + 1 ) {
                        if ( *logline!='\0' && 
                             logline[strlen(logline)-1] != '-' &&
                             (fl->next->filereq.concat & 
                              (NOCONCAT_TO_EOD|CONCAT_TO_EOD)) == 0 &&
                             (fl->next->next->filereq.concat & 
                              NOCONCAT_TO_EOD)  == 0 ) {
                            LOGLINE_APPEND("%s","-");
                        } else if ( *logline == '\0' ||
                            (logline[strlen(logline)-1] == '-' && 
                             ((fl->next->filereq.concat & CONCAT_TO_EOD) != 0 ||
                              (fl->next->next->filereq.concat & 
                               NOCONCAT_TO_EOD) != 0)) ) {
                            LOGLINE_APPEND("%d",filereq->tape_fseq);
                        }
                    } else {
                        if ( *logline!='\0' && 
                             logline[strlen(logline)-1] != '-' ) {
                            LOGLINE_APPEND("%s",",");
                        } else if ( *logline != '\0' && fseq > 0 ) {
                            LOGLINE_APPEND("%d,",fseq);
                        }
                        LOGLINE_APPEND("%d",filereq->tape_fseq);
                    }
                } else {
                    if ( fl != tape->file && 
                         (fl->prev->filereq.concat & (NOCONCAT_TO_EOD|CONCAT_TO_EOD))==0 ) {
                        if ( *logline!='\0' ) LOGLINE_APPEND("%s",",");
                        if ( (filereq->concat & NOCONCAT_TO_EOD) != 0 ) 
                            LOGLINE_APPEND("%d",fseq);
                        else LOGLINE_APPEND("%d",filereq->tape_fseq);
                    }
                    if ( logline[strlen(logline)-1] != '-' )
                        LOGLINE_APPEND("%s","-");
                }
                fseq = filereq->tape_fseq;
            }
        } CLIST_ITERATE_END(tape->file,fl);
        if ( logline[strlen(logline)-1] == '-' ) {
            if ( (tape->file->prev->filereq.concat &
                 (CONCAT_TO_EOD|NOCONCAT_TO_EOD)) == 0 ) {
                if ( strlen(logline) + 11 >= CA_MAXLINELEN ) 
                    rtcp_log(LOG_INFO,"%s%d \\\n",logline,fseq);
                else LOGLINE_APPEND("%d",fseq);
            }
        } else if ( (tape->file->prev->filereq.concat &
                     (CONCAT_TO_EOD|NOCONCAT_TO_EOD)) != 0 ) {
            if ( strlen(logline) + 4 >= CA_MAXLINELEN )
                 rtcp_log(LOG_INFO,"%s- \\\n",logline);
            else LOGLINE_APPEND("%s","-");
        }
    }

    filereq = &(tape->file->filereq);
    *common_fid = '\0';
    if ( *filereq->fid != '\0' ) {
        strcpy(common_fid,filereq->fid);
        CLIST_ITERATE_BEGIN(tape->file,fl) {
            if ( strcmp(common_fid,fl->filereq.fid) != 0  ) {
                *common_fid = '\0';
                break;
            }
        } CLIST_ITERATE_END(tape->file,fl);
        if ( *common_fid != '\0' ) LOGLINE_APPEND(" -f %s",common_fid);
    }

    if ( *filereq->fid != '\0' && *common_fid == '\0' ) {
        fseq = filereq->tape_fseq;
        CLIST_ITERATE_BEGIN(tape->file,fl) {
            filereq = &fl->filereq;
            if ( fl == tape->file ) LOGLINE_APPEND(" -f %s",filereq->fid);
            else if ( filereq->tape_fseq != fseq || filereq->tape_fseq == -1 ) {
                if ( !(mode==WRITE_ENABLE && (filereq->concat & CONCAT)!=0) ) { 
                    if ( strlen(logline)+strlen(filereq->fid)+5<CA_MAXLINELEN )
                        LOGLINE_APPEND(":%s",filereq->fid);
                    else {
                        rtcp_log(LOG_INFO,"%s: \\\n",logline);
                        *logline = '\0';
                        LOGLINE_APPEND("%s",filereq->fid);
                    }
                }
                fseq = filereq->tape_fseq;
            }
        } CLIST_ITERATE_END(tape->file,fl);
    }

    filereq = &(tape->file->filereq);
    common_sz = 0;
    if ( filereq->maxsize > 0 ) {
        common_sz = filereq->maxsize;
        CLIST_ITERATE_BEGIN(tape->file,fl) {
            if ( fl->filereq.maxsize != common_sz ) {
                common_sz = 0;
                break;
            }
        } CLIST_ITERATE_END(tape->file,fl);
        if ( common_sz != 0 ) LOGLINE_APPEND(" -s %d",
                               (unsigned int)(filereq->maxsize/(1024*1024)));
    }
    if ( filereq->maxsize > 0 && common_sz == 0 ) {
        fseq = filereq->tape_fseq;
        CLIST_ITERATE_BEGIN(tape->file,fl) {
            filereq = &fl->filereq;
            if ( fl == tape->file )  LOGLINE_APPEND(" -s %d",
                                (unsigned int)(filereq->maxsize/(1024*1024)));
            else if ( (filereq->concat & CONCAT) == 0 ) {
                if ( strlen(logline)+17<CA_MAXLINELEN )
                    LOGLINE_APPEND(":%d",
                                  (unsigned int)(filereq->maxsize/(1024*1024)));
                else {
                    rtcp_log(LOG_INFO,"%s: \\\n",logline);
                    *logline = '\0';
                    LOGLINE_APPEND("%d",
                                  (unsigned int)(filereq->maxsize/(1024*1024)));
                }
            }
        } CLIST_ITERATE_END(tape->file,fl);
    }

    CLIST_ITERATE_BEGIN(tape->file,fl) {
        filereq = &fl->filereq;
        if ( !(mode==WRITE_DISABLE && (filereq->concat & CONCAT) != 0) ) {
            if ( strlen(logline)+strlen(filereq->file_path)+4<CA_MAXLINELEN ) {
                LOGLINE_APPEND(" %s",filereq->file_path);
            } else {
                rtcp_log(LOG_INFO,"%s \\\n",logline);
                *logline = '\0';
                LOGLINE_APPEND("%s",filereq->file_path);
            }
        }
    } CLIST_ITERATE_END(tape->file,fl);
    if ( *logline != '\0' ) rtcp_log(LOG_INFO,"%s\n",logline);

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
static int rtcpd_ResetRequest(tape_list_t *tape) {
    int i, mode;
    tape_list_t *tl;
    file_list_t *fl, *fltmp;
    rtcpFileRequest_t *filereq = NULL;

    /*
     * Reset the data buffers
     */
    for (i=0; i<nb_bufs; i++) {
        databufs[i]->maxlength = bufsz;
        databufs[i]->data_length = 0;
        databufs[i]->length = 0;
        databufs[i]->bufendp = 0;
        databufs[i]->end_of_tpfile = FALSE;
        databufs[i]->last_buffer = FALSE;
        databufs[i]->nb_waiters = 0;
        databufs[i]->nbrecs = 0;
        databufs[i]->lrecl_table = NULL;
        /*
         * Initialize semaphores
         */
        databufs[i]->flag = BUFFER_EMPTY;
    }
    /*
     * Reset all non-finished requests
     */
    CLIST_ITERATE_BEGIN(tape,tl) {
        mode = tl->tapereq.mode;
        CLIST_ITERATE_BEGIN(tape->file,fl) {
            filereq = &fl->filereq;
            if ( filereq->proc_status == RTCP_WAITING ||
                 filereq->proc_status == RTCP_POSITIONED ) {
                rtcp_log(LOG_DEBUG,"rtcpd_ResetRequest() reset filereq(%d,%d,%s)\n",
                         filereq->tape_fseq,filereq->disk_fseq,
                         filereq->file_path);
                filereq->proc_status = RTCP_WAITING;
                filereq->bytes_out = 0;
                fl->diskbytes_sofar = 0;
                fl->tapebytes_sofar = 0;
                fl->end_index = -1;
                if ( mode == WRITE_DISABLE ) filereq->bytes_in = 0;
                /*
                 * On tape write the limited by size status is set
                 * already in rtcp_CheckReq() since disk file sizes are
                 * all known. Therefor we cannot reset that status here. 
                 */
                if ( !(mode == WRITE_ENABLE &&
                       filereq->err.severity == RTCP_OK | RTCP_LIMBYSZ) ) {
                    filereq->err.severity = RTCP_OK;
                    filereq->err.errorcode = 0;
                }
                *filereq->err.errmsgtxt = '\0';
                tl->tapereq.err.severity = RTCP_OK;
                tl->tapereq.err.errorcode = 0;
                *tl->tapereq.err.errmsgtxt = '\0';
            }
            if ( (((filereq->concat & CONCAT) != 0) && 
                  ((fl->next->filereq.concat & CONCAT) == 0)) ||
                 (((filereq->concat & CONCAT_TO_EOD) != 0) &&
                  (((fl->next->filereq.concat & CONCAT_TO_EOD) == 0) ||
                   (fl == tape->file))) ) {
                /*
                 * Last file in a concatenation. Check if it has finished
                 * successfully. If not, we must reset the status of all
                 * files in this concatenation.
                 */
                if ( filereq->proc_status != RTCP_FINISHED ) {
                    rtcp_log(LOG_DEBUG,"rtcpd_ResetRequest() reset filereq(%d,%d,%s,concat:%d)\n",
                         filereq->tape_fseq,filereq->disk_fseq,
                         filereq->file_path,filereq->concat);
                    filereq->proc_status = RTCP_WAITING;
                    filereq->err.severity = RTCP_OK;
                    filereq->err.errorcode = 0;
                    *filereq->err.errmsgtxt = '\0'; 
                    tl->tapereq.err.severity = RTCP_OK;
                    tl->tapereq.err.errorcode = 0;
                    *tl->tapereq.err.errmsgtxt = '\0';
                    fltmp = fl;
                    while ( fltmp->prev != fl && 
                       ((fltmp->filereq.concat & CONCAT) != 0 ||
                        (fltmp->prev->filereq.concat & CONCAT_TO_EOD) != 0) ) {
                        fltmp = fltmp->prev;
                        rtcp_log(LOG_DEBUG,"rtcpd_ResetRequest() reset filereq(%d,%d,%s,concat:%d)\n",
                               fltmp->filereq.tape_fseq,
                               fltmp->filereq.disk_fseq,
                               fltmp->filereq.file_path,
                               fltmp->filereq.concat);
                        fltmp->filereq.proc_status = RTCP_WAITING;
                        fltmp->filereq.err = filereq->err;
                        fltmp->filereq.bytes_out = 0;
                        fltmp->diskbytes_sofar = 0;
                        fltmp->tapebytes_sofar = 0;
                        fltmp->end_index = -1;
                        if ( mode == WRITE_DISABLE ) fltmp->filereq.bytes_in = 0;
                    }
                }
            }
        } CLIST_ITERATE_END(tape->file,fl);
    } CLIST_ITERATE_END(tape,tl);

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

    /*
     * Initialize exclusive lock for stage_updc_tppos() 
     */
    rc = Cthread_mutex_lock(&proc_cntl.TpPos);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_lock(TpPos): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    rc = Cthread_mutex_unlock(&proc_cntl.TpPos);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_unlock(TpPos): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    /*
     * Get direct pointer to Cthread structure to allow for
     * low overhead locks
     */
    proc_cntl.TpPos_lock = Cthread_mutex_lock_addr(&proc_cntl.TpPos);
    if ( proc_cntl.TpPos_lock == NULL ) {
        rtcp_log(LOG_ERR,
            "rtcpd_InitProcCntl() Cthread_mutex_lock_addr(TpPos): %s\n",
            sstrerror(serrno));
        return(-1);
    }

    return(0);
}

int rtcpd_SerializeLock(const int lock, int *lockflag, void *lockaddr, 
                        int *nb_waiters, int *next_entry, int **wait_list) {
    int rc, severity, i, *my_wait_entry;
    int *loc_wait_list, loc_next_entry;

    if ( lockflag == NULL || lockaddr == NULL || nb_waiters == NULL ||
         next_entry == NULL || wait_list == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    rc = Cthread_mutex_lock_ext(lockaddr);
    if ( (rtcpd_CheckProcError() &
          (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV)) != 0 ) {
        *lockflag = lock;
        (void)Cthread_cond_broadcast_ext(lockaddr);
        (void)Cthread_mutex_unlock_ext(lockaddr);
        return(-1);
    }

    if ( rc == -1 ) return(-1);
    if ( *wait_list == NULL ) {
        /*
         * There cannot be more waiters than the number of disk IO threads.
         */
        *wait_list = (int *)calloc(proc_stat.nb_diskIO,sizeof(int));
        if ( *wait_list == NULL ) {
            *lockflag = lock;
            (void)Cthread_cond_broadcast_ext(lockaddr);
            (void)Cthread_mutex_unlock_ext(lockaddr);
            return(-1);
        }
        rtcp_log(LOG_DEBUG,"rtcpd_SerializeLock() allocated wait_list at 0x%lx\n",
                 *wait_list);
    }

    if ( lock != 0 ) {
        /*
         * Use a local wait list to improve readability of the code
         */
        loc_wait_list = *(int **)wait_list;
        loc_next_entry = *next_entry;
        loc_wait_list[loc_next_entry] = *nb_waiters;
        my_wait_entry = &loc_wait_list[loc_next_entry];
        *next_entry = ((*next_entry) + 1) % proc_stat.nb_diskIO;
        (*nb_waiters)++;
        while ( *lockflag == lock || *my_wait_entry > 0 ) {
            rtcp_log(LOG_DEBUG,"rtcpd_SerializeLock() nb_waiters=%d, next_entry=%d, my_index=%d, my_wait_entry=%d(0x%lx)\n",*nb_waiters,*next_entry,loc_next_entry,*my_wait_entry,my_wait_entry);
            for (i=0;i<proc_stat.nb_diskIO;i++)
                rtcp_log(LOG_DEBUG,"rtcpd_SerializeLock() loc_wait_list[%d](0x%lx)=%d\n",i,&loc_wait_list[i],loc_wait_list[i]);
            rc = Cthread_cond_wait_ext(lockaddr);
            if ( rc == -1 || (rtcpd_CheckProcError() &
                 (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV)) != 0 ) {
                *lockflag = lock;
                (*nb_waiters)--;
                for (i=0; i<proc_stat.nb_diskIO; i++) loc_wait_list[i]--;
                (void)Cthread_cond_broadcast_ext(lockaddr);
                (void)Cthread_mutex_unlock_ext(lockaddr);
                return(-1);
            }
            rtcp_log(LOG_DEBUG,"rtcpd_SerializeLock() woke up with nb_waiters=%d, next_entry=%d, my_wait_entry=%d(0x%lx)\n",*nb_waiters,*next_entry,*my_wait_entry,my_wait_entry);
        }
        (*nb_waiters)--;
        for (i=0; i<proc_stat.nb_diskIO; i++) loc_wait_list[i]--;
    }
    rtcp_log(LOG_DEBUG,"rtcpd_SerializeLock() change lock from %d to %d\n",
             *lockflag,lock);
    *lockflag = lock;
    if ( lock == 0 ) {
        rc = Cthread_cond_broadcast_ext(lockaddr);
    }
    (void)Cthread_mutex_unlock_ext(lockaddr);
    if ( rc == -1 ) return(-1);
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
    char ifce[5],dgn[CA_MAXDGNLEN+1],unit[CA_MAXUNMLEN+1];
    u_signed64 totSz = 0;
    int totKBSz = 0;
    int totMBSz = 0;
    int Twait = 0;
    int Tservice = 0;
    int Ttransfer = 0;
    int mode,jobID,status,rc;

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
            jobID = tapereq->jobID;
            strcpy(unit,tapereq->unit);
            strcpy(dgn,tapereq->dgn);
            Tservice = ((time_t)tapereq->TEndUnmount - 
                        (time_t)tapereq->TStartRequest);
            if ( Twait == 0 ) 
                Twait = ((time_t)tapereq->TStartMount - 
                         (time_t)tapereq->TStartRequest);
            Twait += ((time_t)tapereq->TEndMount - 
                      (time_t)tapereq->TStartMount);
            Twait += ((time_t)tapereq->TEndUnmount - 
                      (time_t)tapereq->TStartUnmount);
            tmpfile = nextfile = nexttape->file;
            while ( nextfile != NULL ) {
                filereq = &(nextfile->filereq);
                strcpy(ifce,filereq->ifce);
                if ( filereq->proc_status == RTCP_FINISHED ||
                     filereq->proc_status == RTCP_PARTIALLY_FINISHED ||
                     filereq->proc_status == RTCP_EOV_HIT ) {
                    /*
                     * Transfer time of tape file.
                     */
                    if ( tapereq->mode == WRITE_DISABLE ||
                        (filereq->concat & CONCAT) == 0 ) {
                        Ttransfer += (time_t)max(
                                  (time_t)filereq->TEndTransferDisk,
                                  (time_t)filereq->TEndTransferTape) -
                                  (time_t)max(
                                      (time_t)filereq->TStartTransferDisk,
                                      (time_t)filereq->TStartTransferTape);
                    }

                    if ( tapereq->mode == WRITE_ENABLE &&
                         filereq->proc_status == RTCP_FINISHED )
                        totSz += filereq->bytes_in;
                    else if ( tapereq->mode == WRITE_DISABLE )
                        totSz += filereq->bytes_out;
                }
                
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
    totMBSz = totKBSz / 1024;
    if ( totMBSz > 0 ) {
        status = VDQM_UNIT_MBCOUNT;
        rc = vdqm_UnitStatus(NULL,NULL,dgn,NULL,unit,&status,&totMBSz,jobID);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"vdqm_UnitStatus(VDQM_UNIT_MBCOUNT,%d MB): %s\n",
                     totMBSz,sstrerror(serrno));
        }
    }

    if ( (rtcpd_CheckProcError() & RTCP_OK) != 0 ) {
        rtcp_log(LOG_INFO,"total number of Kbytes transferred is %d\n",totKBSz);
        rtcp_log(LOG_INFO,"waiting time was %d seconds\n",Twait);
        rtcp_log(LOG_INFO,"service time was %d seconds\n",Tservice);
        if ( Ttransfer <= 0 ) Ttransfer = 1;
        if ( mode == WRITE_ENABLE )
            rtcp_log(LOG_INFO,"cpdsktp: Data transfer bandwidth (%s) is %d KB/sec\n",
                     ifce,totKBSz/Ttransfer); 
        else
            rtcp_log(LOG_INFO,"cptpdsk: Data transfer bandwidth (%s) is %d KB/sec\n",
                     ifce,totKBSz/Ttransfer);
        rtcp_log(LOG_INFO,"request successful\n");
    } else {
        rtcp_log(LOG_INFO,"request failed\n");
    }

    rtcpd_SetProcError(RTCP_OK);
    return;
}

void rtcpd_BroadcastException() {
    int i;

    /*
     * Broadcast to all control conditions. These are normally not
     * blocked since they are used as pure semaphores.
     */
    rtcp_log(LOG_DEBUG,"rtcpd_BroadcastException() broadcast to cntl_lock\n");
    (void)Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
    (void)Cthread_cond_broadcast_ext(proc_cntl.cntl_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);

    rtcp_log(LOG_DEBUG,"rtcpd_BroadcastException() broadcast to ReqStatus_lock\n");
    (void)Cthread_mutex_lock_ext(proc_cntl.ReqStatus_lock);
    (void)Cthread_cond_broadcast_ext(proc_cntl.ReqStatus_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.ReqStatus_lock);

    rtcp_log(LOG_DEBUG,"rtcpd_BroadcastException() broadcast to DiskFileAppend_lock\n");
    (void)Cthread_mutex_lock_ext(proc_cntl.DiskFileAppend_lock);
    (void)Cthread_cond_broadcast_ext(proc_cntl.DiskFileAppend_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.DiskFileAppend_lock);

    rtcp_log(LOG_DEBUG,"rtcpd_BroadcastException() broadcast to TpPos_lock\n");
    (void)Cthread_mutex_lock_ext(proc_cntl.TpPos_lock);
    (void)Cthread_cond_broadcast_ext(proc_cntl.TpPos_lock);
    (void)Cthread_mutex_unlock_ext(proc_cntl.TpPos_lock);

    /*
     * Finally, signal all data buffers. Since those conditions also protects
     * the memory buffers (in addition to being semaphores), this could 
     * potentially block if for instance, a diskIO thread is blocked in a
     * rfio_...() call. Although this is normally correct (condition broadcast
     * is intended for global thread synchronisation), it could prevent proper
     * exit of the process in case of a killjid.
     * However, a killjid triggers a process exit call in the tapeIO thread
     * (after clean-up) and the only possibility to cause a global hang is
     * if the tapeIO thread is blocked in a system call.
     */
    if ( databufs != NULL ) {
        for (i=0;i<nb_bufs;i++) {
            if ( databufs[i] != NULL ) {
                rtcp_log(LOG_DEBUG,
                    "rtcpd_BroadcastException() broadcast to databuf[%d]\n",i);
                (void)Cthread_mutex_lock_ext(databufs[i]->lock);
                (void)Cthread_cond_broadcast_ext(databufs[i]->lock);
                (void)Cthread_mutex_unlock_ext(databufs[i]->lock);
            }
        }
    }

    rtcp_log(LOG_DEBUG,"rtcpd_BroadcastException() finished\n");
    return; 
}

static int rtcpd_ProcError(int *code) {
    static int global_code, rc;

    if ( AbortFlag != 0 && code == NULL ) {
        return(proc_cntl.ProcError);
    }
    rc = Cthread_mutex_lock_ext(proc_cntl.ProcError_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_ProcError(): Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        rc = RTCP_FAILED;
    } else {
        if ( code != NULL ) {
            /*
             * Never reset FAILED status
             */
            if ( (proc_cntl.ProcError & RTCP_FAILED) == 0 &&
                  ((proc_cntl.ProcError & RTCP_RESELECT_SERV) == 0 ||
                    AbortFlag == 0) ) {
                proc_cntl.ProcError = *code;
            }
        }
        rc = proc_cntl.ProcError;
    }
    (void) Cthread_cond_broadcast_ext(proc_cntl.ProcError_lock);
    (void) Cthread_mutex_unlock_ext(proc_cntl.ProcError_lock);
    return(rc);
}

void rtcpd_SetProcError(int code) {
    int set_code, rc, i;

    set_code = code;
    rtcp_log(LOG_DEBUG,"rtcpd_SetProcError(%d) called\n",set_code);
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
        rc = proc_cntl.ProcError = RTCP_FAILED;
    }
    if ( (rc & (RTCP_LOCAL_RETRY|RTCP_FAILED|RTCP_RESELECT_SERV|RTCP_EOD)) != 0 ) 
        rtcpd_BroadcastException(); 

    return;
}

int rtcpd_CheckProcError() {
    return(rtcpd_ProcError(NULL));
}

int rtcpd_WaitProcStatus(int what) {
    int rc;

    rc = Cthread_mutex_lock_ext(proc_cntl.ProcError_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_WaitProcStatus(): Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        return(-1);
    } 
    rtcp_log(LOG_DEBUG,"rtcpd_WaitProcStatus() current: %d, waiting for %d\n",
             proc_cntl.ProcError,what);
    while ((proc_cntl.ProcError & (what|RTCP_FAILED|RTCP_RESELECT_SERV)) == 0) {
        rc = Cthread_cond_wait_ext(proc_cntl.ProcError_lock);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_WaitProcStatus(): Cthread_wait_cond_ext(): %s\n",
            sstrerror(serrno));
            (void) Cthread_mutex_unlock_ext(proc_cntl.ProcError_lock);
            return(-1);
        }
        rtcp_log(LOG_DEBUG,"rtcpd_WaitProcStatus() current: %d, waiting for %d\n",
                 proc_cntl.ProcError,what);
    }
    rc = Cthread_mutex_unlock_ext(proc_cntl.ProcError_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_WaitProcStatus(): Cthread_mutex_unlock_ext(): %s\n",
                 sstrerror(serrno));
        return(-1);
    }
    return(0);
}

int rtcpd_WaitCompletion(tape_list_t *tape, file_list_t *file) {
    tape_list_t *tl;
    file_list_t *fl;
    int all_finished = 0;
    int rc;

    while (all_finished == 0) {
       all_finished = 1;
       rc = Cthread_mutex_lock_ext(proc_cntl.cntl_lock);
       if ( rc == -1 ) {
           rtcp_log(LOG_ERR,"rtcpd_WaitCompletion() Cthread_mutex_lock_ext() : %s\n",
                    sstrerror(serrno));
           return(-1);
       }

       CLIST_ITERATE_BEGIN(tape,tl) {
           CLIST_ITERATE_BEGIN(tl->file,fl) {
               if ( fl == file ) break;
               if ( fl->filereq.proc_status < RTCP_PARTIALLY_FINISHED ) {
                   all_finished = 0;
                   break;
               }
           } CLIST_ITERATE_END(tl->file,fl);
           if ( all_finished == 0 ) break;
       } CLIST_ITERATE_END(tape,tl); 
       if ( all_finished == 0 ) {
           rc = Cthread_cond_wait_ext(proc_cntl.cntl_lock);
           if ( rc == -1 ) {
               rtcp_log(LOG_ERR,"rtcpd_WaitCompletion() Cthread_cond_wait_ext(): %s\n",
                        sstrerror(serrno));
               (void)Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
               return(-1);
           } 
       } /* if ( all_finished == 0 ) */
       rc = Cthread_mutex_unlock_ext(proc_cntl.cntl_lock);
       if ( rc == -1 ) {
           rtcp_log(LOG_ERR,"rtcpd_WaitCompletion() Cthread_cond_wait_ext(): %s\n",
                    sstrerror(serrno));
           return(-1);
       }
    } /* while (all_finished == 0)  */
    return(0);
}

void rtcpd_SetReqStatus(tape_list_t *tape,
                        file_list_t *file,
                        int status,
                        int severity) {
    file_list_t *fl;
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
        if ( severity != 0 && (severity & RTCP_OK) == 0 ) 
            tapereq->err.severity = tapereq->err.severity & ~RTCP_OK;
        /*
         * Don't reset failed status
         */
        if ( (tapereq->err.severity & RTCP_FAILED) == 0 ) {
            tapereq->err.errorcode = status;
            if ( (severity & (RTCP_FAILED | RTCP_RESELECT_SERV | RTCP_USERR |
                              RTCP_SYERR | RTCP_UNERR | RTCP_SEERR)) != 0 ) {
                tapereq->err.severity = tapereq->err.severity & ~RTCP_OK;
                tapereq->err.severity = tapereq->err.severity & ~RTCP_RETRY_OK;
                tapereq->err.severity = tapereq->err.severity & ~RTCP_LOCAL_RETRY;
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
                filereq->err.severity = filereq->err.severity & ~RTCP_LOCAL_RETRY;
                filereq->err.severity |= severity;
                fl = file;
                /*
                 * On tape write we must set same status for all 
                 * concatenated requests so far.
                 */
                if ( file->tape->tapereq.mode == WRITE_ENABLE ) {
                    while ( fl != file->tape->file &&
                           (fl->filereq.concat & CONCAT) != 0 ) {
                        fl = fl->prev;
                        fl->filereq.err.severity = filereq->err.severity;
                    }
                }
            } else {
                filereq->err.severity |= severity;
                if ( (severity & RTCP_LOCAL_RETRY) != 0 ) {
                    fl = file;
                    /*
                     * On tape write we must set same status for all
                     * concatenated requests so far.
                     */
                    if ( file->tape->tapereq.mode == WRITE_ENABLE ) {
                        while ( fl != file->tape->file &&
                                (fl->filereq.concat & CONCAT) != 0 ) {
                            fl = fl->prev;
                            fl->filereq.err.severity = filereq->err.severity;
                        }
                    }
                }
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
         (status == NULL && severity == NULL) ) return;

    if ( tape != NULL ) tapereq = &tape->tapereq;
    if ( file != NULL ) filereq = &file->filereq;

    rc = Cthread_mutex_lock_ext(proc_cntl.ReqStatus_lock);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_CheckReqStatus() Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        return;
    }
    _severity = 0;
    _status = 0;
    if ( tapereq != NULL ) {
        _status = tapereq->err.errorcode;
        _severity = tapereq->err.severity;
    }
    if ( filereq != NULL ) {
        if ( _status == 0 ) _status = filereq->err.errorcode;
        if ( _severity == 0 || (_severity & RTCP_OK) != 0 ) 
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
    rtcpDumpTapeRequest_t dumpreq;
    rtcpHdr_t hdr;
    SOCKET *client_socket = NULL;
    rtcpClientInfo_t *client = NULL;
    tape_list_t *tape, *nexttape;
    file_list_t *nextfile;
    int rc, retry, reqtype, errmsglen,status, save_errno, CLThId;
    char *errmsg, *msgtxtbuf;
    static int thPoolId = -1;
    static int thPoolSz = -1;
    struct passwd *pwd;
    char *cmd = NULL;
    char acctstr[7] = "";
    char envacct[20];

    (void)setpgrp();
    AbortFlag = 0;
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
        SHIFTclient = TRUE;
        rtcp_log(LOG_INFO,"Running old (SHIFT) client request\n");
        rc = rtcp_RunOld(accept_socket,&hdr);

        (void)rtcp_CloseConnection(accept_socket);
        if ( client != NULL ) {
            free(client);
            client = NULL;
        }
        return(rc);
    }

    /*
     * If local nomoretapes is set, we don't acknowledge VDQM message.
     * VDQM will then requeue the request and put assigned drive in
     * UNKNOWN status
     */
    if ( rtcpd_CheckNoMoreTapes() != 0 ) {
        (void)rtcp_CloseConnection(accept_socket);
        rtcpd_FreeResources(NULL,&client,NULL);
        return(-1);
    }

    rc = vdqm_AcknClientAddr(*accept_socket,reqtype,0,NULL);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() vdqm_AcknClientAddr(): %s\n",
            sstrerror(serrno));
        (void)rtcp_CloseConnection(accept_socket);
        rtcpd_FreeResources(NULL,&client,NULL);
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
    rc = rtcpd_CheckClient((int)client->uid,(int)client->gid,
                           client->name,acctstr,NULL);
    if ( rc == -1 ) {
        /*
         * Client not authorised
         */
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() %s(%d,%d)@%s not authorised\n",
                 client->name,client->uid,client->gid,client->clienthost);
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        rtcpd_FreeResources(NULL,&client,NULL);
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
        rtcpd_FreeResources(NULL,&client,NULL);
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
        rtcpd_FreeResources(&client_socket,&client,NULL);
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
        filereq.err.severity = RTCP_OK;
        rc = rtcp_RecvReq(client_socket,
            &hdr,client,&tapereq,&filereq);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcp_RecvReq(): %s\n",
                     sstrerror(serrno));
            break;
        }
        reqtype = hdr.reqtype;
        if ( reqtype == RTCP_DUMPTAPE_REQ ) {
            rc = rtcp_RecvTpDump(client_socket,&dumpreq);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcp_RecvTpDump(): %s\n",
                         sstrerror(serrno));
                break;
            }
            if ( nexttape == NULL ) {
                rtcp_log(LOG_ERR,"rtcpd_MainCntl() invalid request sequence\n");
                rc = -1;
                break;
            }
            nexttape->dumpreq = dumpreq;
        }
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
            nexttape->tapereq = tapereq;
            nexttape->tapereq.err.severity = nexttape->tapereq.err.severity &
                                             ~RTCP_RESELECT_SERV;
            CLIST_INSERT(tape,nexttape);
            if ( tapereq.VolReqID != client->VolReqID ) {
                rtcp_log(LOG_ERR,"rtcpd_MainCntl() wrong VolReqID %d, should be %d\n",
                    tapereq.VolReqID, client->VolReqID);
                rc = -1;
                break;
            }
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
            nextfile->end_index = -1;
            nextfile->filereq = filereq;
            nextfile->filereq.err.severity = nextfile->filereq.err.severity &
                                             ~RTCP_RESELECT_SERV; 
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
    if ( tapereq.mode == WRITE_ENABLE ) {
        rtcp_log(LOG_INFO,"cpdsktp request by %s (%d,%d) from %s\n",
                 client->name,client->uid,client->gid,client->clienthost);
    } else {
        if ( tape->file != NULL ) { 
            rtcp_log(LOG_INFO,"cptpdsk request by %s (%d,%d) from %s\n",
                     client->name,client->uid,client->gid,client->clienthost);
        } else {
            rtcp_log(LOG_INFO,"tpdump request by %s (%d,%d) from %s\n",
                     client->name,client->uid,client->gid,client->clienthost);
        }
    }

    (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPCMDR);

    cmd = rtcp_cmds[tapereq.mode];
            
    /*
     * On UNIX: set client UID/GID
     */
#if !defined(_WIN32)
    rc = setgid((gid_t)client->gid);
    save_errno = errno;
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() setgid(%d): %s\n",
            client->gid,sstrerror(save_errno));
        (void)rtcpd_AppendClientMsg(tape,NULL,"setgid(%d): %s",
            client->gid,sstrerror(save_errno));
    } else {
        rc = setuid((uid_t)client->uid);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_MainCntl() setuid(%d): %s\n",
                client->uid,sstrerror(save_errno));
            (void)rtcpd_AppendClientMsg(tape,NULL,"setuid(%d): %s",
                client->uid,sstrerror(save_errno));
        }
        if ( acctstr != '\0' ) {
            (void) sprintf(envacct,"ACCOUNT=%s",acctstr);
            if ( putenv(envacct) != 0 ) {
                rtcp_log(LOG_ERR,"putenv(%s) failed\n",envacct);
                (void)rtcpd_AppendClientMsg(tape,NULL,"putenv(%s): %s",
                      acctstr,sstrerror(save_errno));
            }
        }
    }
#endif /* !_WIN32 */

    (void)rtcpd_PrintCmd(tape);
    if ( rc == -1 ) {
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPEMSG);
        (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPCMDC);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(-1);
    }
    /*
     * Check the request and set defaults
     */
    rc = rtcp_CheckReq(client_socket,client,tape);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcp_CheckReq(): %s\n",
            sstrerror(serrno));
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPCMDC);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(-1);
    }
    (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPCMDD);

    /*
     * Start a thread to listen to client socket (to receive ABORT).
     */
    CLThId = rtcpd_ClientListen(*client_socket);
    if ( CLThId == -1 ) {
        (void)rtcpd_SetReqStatus(tape,NULL,serrno,RTCP_RESELECT_SERV);
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcpd_ClientListen(): %s\n",
            sstrerror(serrno));
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPEMSG);
        (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPCMDC);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(-1);
    }
    /*
     * Reset global error status
     */
    rtcpd_SetProcError(RTCP_OK);

    /*
     * Dumptape request?
     */
    if ( tape->file == NULL ) {
        Dumptape = TRUE;
        rc = rtcpd_tpdump(client,tape);
        rtcp_InitLog(NULL,NULL,NULL,NULL);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcpd_tpdump(): %s\n",
                     sstrerror(serrno));
            if ( *tape->tapereq.err.errmsgtxt == '\0' ) {
                (void)rtcpd_AppendClientMsg(tape,NULL,"tpdump(): %s\n",
                         sstrerror(serrno));
                (void)rtcp_WriteAccountRecord(client,tape,NULL,RTCPEMSG);
            }
            (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        }
        (void)rtcp_WriteAccountRecord(client,tape,NULL,RTCPCMDC);
        (void)rtcpd_WaitCLThread(CLThId,&status);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(rc);
    }

    /*
     * Allocate the buffers
     */
    rc = rtcpd_AllocBuffers();
    if ( rc == -1 ) {
        (void)rtcpd_SetReqStatus(tape,NULL,serrno,RTCP_RESELECT_SERV);
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() failed to allocate buffers\n");
        (void)rtcpd_AppendClientMsg(tape,NULL,RT105,cmd,sstrerror(serrno));
        (void)tellClient(client_socket,tape,NULL,-1);
        (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPEMSG);
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPCMDC);
        (void)rtcpd_WaitCLThread(CLThId,&status);
        rtcpd_FreeResources(&client_socket,&client,&tape);
        return(-1);
    }
    /*
     * Initialise the stage API
     */
    rc = rtcpd_init_stgupdc(tape);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcpd_init_stgupdc(): %s\n",
                 sstrerror(serrno));
    }

    /*
     * Start up the disk IO thread pool (only once on NT).
     */
    if ( thPoolId == -1 ) thPoolId = rtcpd_InitDiskIO(&thPoolSz);
    if ( thPoolId == -1 || thPoolSz <= 0 ) {
        (void)rtcpd_SetReqStatus(tape,NULL,serrno,RTCP_RESELECT_SERV);
        rtcp_log(LOG_ERR,"rtcpd_MainCntl() rtcpd_InitDiskIO(0x%lx): %s\n",
            &thPoolSz,sstrerror(serrno));
        (void)rtcpd_AppendClientMsg(tape,NULL,"rtcpd_InitDiskIO(): %s\n",
              sstrerror(serrno));
        (void)rtcpd_Deassign(client->VolReqID,&tapereq);
        (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPEMSG);
        (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPCMDC);
        (void)rtcpd_WaitCLThread(CLThId,&status);
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
    rtcpd_ResetRequest(tape);
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
            (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPEMSG);
            break;
        }

        /*
         * Start disk and network IO thread
         */
        rc = rtcpd_StartDiskIO(client,tape,tape->file,thPoolId,thPoolSz);
        if ( rc == -1 ) {
            (void)rtcpd_SetReqStatus(tape,NULL,serrno,RTCP_RESELECT_SERV);
            rtcp_log(LOG_ERR,"rtcpd_MainCntl() failed to start disk I/O thread\n");
            (void)rtcpd_AppendClientMsg(tape,NULL,"rtcpd_StartDiskIO(): %s\n",
                  sstrerror(serrno));
            (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPEMSG);
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
        rtcpd_WaitProcStatus(RTCP_RETRY_OK);
        rtcpd_ResetRequest(tape);
        rtcpd_SetProcError(RTCP_OK);
        ENOSPC_occurred = FALSE;
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
    (void)rtcp_WriteAccountRecord(client,tape,tape->file,RTCPCMDC);
    rtcpd_FreeResources(&client_socket,&client,&tape);

    return(0);
}

