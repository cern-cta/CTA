/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_stageupdc.c,v $ $Revision: 1.14 $ $Date: 2000/02/28 07:41:04 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_stageupdc.c - RTCOPY interface to stageupdc
 */

#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
#include <stdlib.h>
#include <stdio.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#endif /* _WIN32 */

#include <signal.h>
#include <stdlib.h>
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
#include <rtcp.h>
#include <rtcp_server.h>
#include <vdqm_api.h>
#include <Ctape_api.h>
#include <serrno.h>

#if !defined(USE_STAGEAPI)
#define ADD_COPT(X,Y,Z) {if (Z!=NULL && *Z!='\0') sprintf(&X[strlen(X)],Y,Z);}
#define ADD_NOPT(X,Y,Z) {if (Z>0) sprintf(&X[strlen(X)],Y,Z);}
#define ADD_SWITCH(X,Y,Z) {if (Z>0) sprintf(&X[strlen(X)],Y);}
#if defined(_WIN32)
#define POPEN(X,Y) _popen(X,Y)
#define PCLOSE(X) _pclose(X)
#else /* _WIN32 */
#define POPEN(X,Y) popen(X,Y)
#define PCLOSE(X) pclose(X)
#endif /* _WIN32 */
#if !defined(STGCMD)
#define STGCMD "stageupdc"
#endif /* STGCMD */
#endif /* USE_STAGEAPI */

int rtcpd_stageupdc(tape_list_t *tape,
                    file_list_t *file) {
    int rc, status, retval, want_reply, save_serrno, WaitTime, TransferTime;
    rtcpFileRequest_t *filereq;
    rtcpTapeRequest_t *tapereq;
    char newpath[CA_MAXPATHLEN+1];
#if !defined(USE_STAGEAPI)
    char stageupdc_cmd[CA_MAXLINELEN+1];
    FILE *stgupdc_fd;
#else  /* !USE_STAGEAPI */
    u_signed64 nb_bytes;
#endif /* !USE_STAGEAPI */

    if ( tape == NULL || file == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    filereq = &file->filereq;
    tapereq = &tape->tapereq;

    rc = save_serrno = 0;
    /*
     * Check if this is a stage request
     */
    if ( *filereq->stageID == '\0' ) return(0);
    WaitTime = filereq->TEndPosition - tapereq->TStartRequest;
    TransferTime = max((time_t)filereq->TEndTransferDisk,
                       (time_t)filereq->TEndTransferTape) -
                       (time_t)filereq->TEndPosition;


#if defined(USE_STAGEAPI)
    status = filereq->err.errorcode;
    want_reply = 0 ;
    if ( filereq->proc_status == RTCP_POSITIONED ) {
        if ( status != ETFSQ && status != ETEOV ) status = 0;
        if ( strcmp(filereq->file_path,".") == 0 ) want_reply = 1;
        rc = stage_updc_tppos(filereq->stageID,
                              status,
                              filereq->blocksize,
                              tapereq->unit,
                              filereq->fid,
                              filereq->tape_fseq,
                              filereq->recordlength,
                              filereq->recfm,
                              newpath,
                              want_reply);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_stageupdc() stage_updc_tppos(): %s\n",
                     sstrerror(serrno));
            return(-1);
        }
    } else if ( filereq->cprc != 0 || filereq->proc_status == RTCP_FINISHED ) {
        /*
         * Always give the return code
         */
        retval = 0;
        rc = rtcp_RetvalSHIFT(tape,file,&retval);
        if ( tapereq->mode == WRITE_ENABLE ) nb_bytes = filereq->bytes_in;
        else nb_bytes = filereq->bytes_out;
        if ( status == ENOSPC ) want_reply = 1;
        strcpy(newpath,filereq->file_path);

        rc = stage_updc_filcp(filereq->stageID,
                              retval,
                              filereq->ifce,
                              nb_bytes,
                              WaitTime,
                              TransferTime,
                              filereq->blocksize,
                              tapereq->unit,
                              filereq->fid,
                              filereq->tape_fseq,
                              filereq->recordlength,
                              filereq->recfm,
                              newpath,
                              want_reply);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_stageupdc() stage_updc_filcp(): %s\n",
                     sstrerror(serrno));
            return(-1);
        }

    }
#else /* USE_STAGEAPI */
    sprintf(stageupdc_cmd,"%s/%s -Z %s ",BIN,STGCMD,filereq->stageID);
    if ( filereq->cprc < 0 && 
        (filereq->err.severity & (RTCP_FAILED | RTCP_EOD)) ) {
            /*
             * SHIFT tpmnt error 211 == CASTOR ETFSQ
             * stageupdc stageID -R 211
             */
            ADD_NOPT(stageupdc_cmd," -R %d ",211);
    } else {
        /*
         * stageupdc stageID -I ifce -W lastw -T lastt -R cprc \
         *                   -s lastnbt -b ... 
         */
        if ( filereq->proc_status == RTCP_FINISHED ||
             filereq->cprc != 0 ) {
            if ( filereq->proc_status == RTCP_FINISHED ) {
                /*
                 * This file request has finished
                 */
                ADD_NOPT(stageupdc_cmd," -W %d ",WaitTime);
                ADD_NOPT(stageupdc_cmd," -T %d ",TransferTime);
            }
            /*
             * Always give the return code
             */
            retval = 0;
            rc = rtcp_RetvalSHIFT(tape,file,&retval);
            sprintf(&stageupdc_cmd[strlen(stageupdc_cmd)]," -R %d ",retval);
            ADD_NOPT(stageupdc_cmd," -s %d ",(int)filereq->bytes_out);
        }
        if ( (filereq->err.severity & RTCP_SYERR) == 0 ) {
            ADD_COPT(stageupdc_cmd," -I %s ",filereq->ifce);
            ADD_NOPT(stageupdc_cmd," -b %d ",filereq->blocksize);
            ADD_NOPT(stageupdc_cmd," -L %d ",filereq->recordlength);
            ADD_NOPT(stageupdc_cmd," -q %d ",filereq->tape_fseq);
            ADD_COPT(stageupdc_cmd," -F %s ",filereq->recfm);
            ADD_COPT(stageupdc_cmd," -f %s ",filereq->fid);
            ADD_COPT(stageupdc_cmd," -D %s ",tapereq->unit);
        }
    }
    rtcp_log(LOG_DEBUG,"rtcpd_stageupdc() cmd=%s\n",stageupdc_cmd);
    /*
     * Must change SIGCHLD disposition since pclose() needs to wait
     * for child. This is all rather hazardous in a threaded env.
     * but normally we should use the stager API rather than then
     * stageupdc command.....
     */
    signal(SIGCHLD,SIG_DFL);
    stgupdc_fd = POPEN(stageupdc_cmd,"r");
    if ( stgupdc_fd == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_stageupdc() popen(%s): %s\n",
            stageupdc_cmd,sstrerror(errno));
        signal(SIGCHLD,SIG_IGN);
        return(-1);
    }
    while ( !feof(stgupdc_fd) ) {
        if ( fgets(newpath,CA_MAXPATHLEN+1,stgupdc_fd) == NULL ) {
            if ( errno > 0 ) 
                rtcp_log(LOG_ERR,"rtcpd_stageupdc() fgets(): %s\n",
                         sstrerror(errno));
        }
    }
    rc = PCLOSE(stgupdc_fd);
    signal(SIGCHLD,SIG_IGN);
#endif /* !USE_STAGEAPI */

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
        rtcp_log(LOG_ERR,"rtcpd_stageupdc() stageupdc returned, rc=%d, path=%s\n",
                 rc,newpath);
        if ( rc != 0 ) {
            rtcpd_AppendClientMsg(NULL,file,
                           "stageupdc failed, rc=%d, path=%s\n",rc,newpath);
            if ( rc != ENOSPC ) rtcpd_SetReqStatus(NULL,file,rc,
                                                   RTCP_FAILED | RTCP_SYERR);
            else rtcpd_SetReqStatus(NULL,file,rc,RTCP_FAILED | RTCP_USERR);
            return(-1);
        }
        if ( *newpath == '\0' && tapereq->mode == WRITE_DISABLE &&
             filereq->proc_status == RTCP_POSITIONED &&
             strcmp(filereq->file_path,".") == 0 ) {
            rtcp_log(LOG_ERR,
                    "rtcpd_stageupdc() deferred allocation and no new path received\n");
            return(-1);
        }
    }
    return(0);
}

