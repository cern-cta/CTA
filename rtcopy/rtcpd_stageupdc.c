/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_stageupdc.c,v $ $Revision: 1.3 $ $Date: 2000/01/19 15:19:56 $ CERN IT-PDP/DM Olof Barring";
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
    int rc, retval, save_serrno, WaitTime, TransferTime;
    rtcpFileRequest_t *filereq;
    rtcpTapeRequest_t *tapereq;
#if !defined(USE_STAGEAPI)
    char stageupdc_cmd[CA_MAXLINELEN+1];
    char newpath[CA_MAXPATHLEN+1];
    FILE *stgupdc_fd;
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

#if defined(USE_STAGEAPI)
    if ( filereq->proc_status == RTCP_POSITIONED ) {
        status = filereq->err.errorcode;
        if ( status != ETFSQ ) status = 0;
        rc = stage_updc_tppos(filereq->stageID,
                              status,
                              filereq->blocksize,
                              tapereq->unit,
                              filereq->fid,
                              filereq->tape_fseq,
                              filereq->recordlength,
                              filereq->recfm,
                              newpath);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_stageupdc() stage_updc_tppos(): %s\n",
                     sstrerror(serrno));
            return(-1);
        }
    } else if ( filereq->proc_status == RTCP_FINISHED ) {
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
        if ( filereq->proc_status == RTCP_FINISHED ) {
            /*
             * This file request has finished
             */
            WaitTime = filereq->TEndPosition - tapereq->TStartRequest;
            TransferTime = min((time_t)filereq->TStartTransferDisk,
                (time_t)filereq->TStartTransferTape);
            TransferTime = max((time_t)filereq->TEndTransferDisk,
                (time_t)filereq->TEndTransferTape) - TransferTime;
            ADD_NOPT(stageupdc_cmd," -W %d ",WaitTime);
            ADD_NOPT(stageupdc_cmd," -T %d ",TransferTime);
            /*
             * Always give the return code
             */
            retval = 0;
            rc = rtcpd_RetvalSHIFT(tape,file,&retval);
            sprintf(&stageupdc_cmd[strlen(stageupdc_cmd)]," -R %d ",retval);
            ADD_NOPT(stageupdc_cmd," -s %d ",(int)filereq->bytes_out);
        }
        if ( filereq->cprc != RTCP_SYERR ) {
            ADD_COPT(stageupdc_cmd," -I %s ",filereq->ifce);
            ADD_NOPT(stageupdc_cmd," -b %d ",filereq->blocksize);
            ADD_NOPT(stageupdc_cmd," -L %d ",filereq->recordlength);
            ADD_NOPT(stageupdc_cmd," -q %d ",filereq->tape_fseq);
            ADD_COPT(stageupdc_cmd," -F %s ",filereq->recfm);
            ADD_COPT(stageupdc_cmd," -f %s ",filereq->fid);
            ADD_COPT(stageupdc_cmd," -D %s ",tapereq->unit);
        }
    }
    stgupdc_fd = POPEN(stageupdc_cmd,"r");
    if ( stgupdc_fd == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_stageupdc() popen(%s): %s\n",
            stageupdc_cmd,sstrerror(errno));
        return(-1);
    }
    while ( !feof(stgupdc_fd) ) {
        if ( fgets(newpath,CA_MAXPATHLEN+1,stgupdc_fd) == NULL ) {
            rtcp_log(LOG_ERR,"rtcpd_stageupdc() fgets(): %s\n",
                sstrerror(errno));
            PCLOSE(stgupdc_fd);
            return(-1);
        }
    }
    PCLOSE(stgupdc_fd);
    if ( *newpath != '\0' && !strcmp(newpath,filereq->file_path) ) {
        rtcp_log(LOG_INFO,"New path obtained from stager: %s\n",
            newpath);
        strcpy(filereq->file_path,newpath);
        return(NEWPATH);
    }
#endif /* !USE_STAGEAPI */
    return(0);
}

