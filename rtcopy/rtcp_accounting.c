/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

#include <sys/types.h>
#include <string.h>
#include <Castor_limits.h>
#include <osdep.h>
#include <net.h>
#include <sacct.h>
#include <log.h>
#include <Ctape_constants.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <rtcp_server.h>
#include <rfio_api.h>
#include <stdlib.h>
#include <stdio.h>

extern int AbortFlag;

int rtcp_wacct(int   subtype,
               uid_t uid,
               gid_t gid,
               int   jid,
               int   stgreqid,
               char  reqtype,
               char  *ifce,
               char  *vid,
               int   size,
               int   retryn,
               int   exitcode,
               char  *clienthost,
               char  *dsksrvr,
               int   fseq,
               char  *errmsgtxt) {
    int acctreclen;
    struct acctrtcp acctrtcp;
    char *getconfent();
    char *p;

    if ((p = getconfent("ACCT", "RTCOPY", 0)) == NULL ||
        (strcmp (p, "YES") != 0 && strcmp (p, "yes")) != 0) return(0);
    memset ((char *) &acctrtcp, 0, sizeof(struct acctrtcp));
    acctrtcp.subtype = subtype;
    acctrtcp.uid = uid;
    acctrtcp.gid = gid;
    acctrtcp.jid = jid;
    acctrtcp.stgreqid = stgreqid;
    acctrtcp.reqtype = reqtype;
    if ( ifce != NULL ) strncpy (acctrtcp.ifce, ifce,sizeof(acctrtcp.ifce));
    if ( vid != NULL ) strncpy (acctrtcp.vid, vid,sizeof(acctrtcp.vid));
    acctrtcp.size = size;
    acctrtcp.retryn = retryn;
    acctrtcp.exitcode = exitcode;
    acctrtcp.fseq = fseq;
    if ( clienthost != NULL ) strncpy (acctrtcp.clienthost, clienthost,sizeof(acctrtcp.clienthost));
    if ( dsksrvr != NULL )  strncpy (acctrtcp.dsksrvr, dsksrvr,sizeof(acctrtcp.dsksrvr));
    if ( errmsgtxt != NULL ) strncpy (acctrtcp.errmsgtxt, errmsgtxt,sizeof(acctrtcp.errmsgtxt));
    acctreclen = ((char *) acctrtcp.errmsgtxt - (char *) &acctrtcp) +
                 strlen(errmsgtxt)+ 1;

    wsacct (ACCTRTCOPY, &acctrtcp, acctreclen);
    return(0);
}

int rtcp_AccountTiming(tape_list_t *tape, file_list_t *file) {
    struct acctrtcp_timing rtcptiming;
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;
    int acctreclen;

    if ( file == NULL ) return(-1);
    filereq = &file->filereq;
    if ( tape != NULL ) tapereq = &tape->tapereq;
    else tapereq = &file->tape->tapereq;

    rtcptiming.jid = filereq->jobID;
    if ( rtcptiming.jid <= 0 ) rtcptiming.jid = rtcpd_jobID();

    /*
     * File sizes
     */
    if ( tapereq->mode == WRITE_DISABLE ) {
        rtcptiming.disk_KB = (int)(filereq->bytes_out/1024);
        rtcptiming.tape_KB = (int)(filereq->bytes_in/1024);
    } else {
        rtcptiming.tape_KB = (int)(filereq->bytes_out/1024);
        rtcptiming.disk_KB = (int)(filereq->bytes_in/1024);
    }
    rtcptiming.host_KB = (int)(filereq->host_bytes/1024);

    /*
     * Timing info.
     */
    rtcptiming.TStartPosition = filereq->TStartPosition;
    rtcptiming.TEndPosition = filereq->TEndPosition;
    rtcptiming.TStartTransferDisk = filereq->TStartTransferDisk;
    rtcptiming.TEndTransferDisk = filereq->TEndTransferDisk;
    rtcptiming.TStartTransferTape = filereq->TStartTransferTape;
    rtcptiming.TEndTransferTape = filereq->TEndTransferTape;
    rtcptiming.TStartRequest = tapereq->TStartRequest;
    rtcptiming.TStartRtcpd = tapereq->TStartRtcpd;
    rtcptiming.TStartMount = tapereq->TStartMount;
    rtcptiming.TEndMount = tapereq->TEndMount;
    rtcptiming.TStartUnmount = tapereq->TStartUnmount;
    rtcptiming.TEndUnmount = tapereq->TEndUnmount;

    acctreclen = sizeof(rtcptiming);

    wsacct (ACCTRTCPTIM, &rtcptiming, acctreclen);
    return(0);
}

int rtcp_WriteAccountRecord(rtcpClientInfo_t *client,
                            tape_list_t *tape,
                            file_list_t *file,
                            int subtype) {
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;
    int jobID = 0;
    int stager_reqID = 0;
    int KBytes = 0;
    int retry_nb = 0;
    int mode = WRITE_DISABLE;
    int exitcode = 0;
    int fseq = 0;
    int rc;
    char charcom, *p, stageID[CA_MAXSTGRIDLEN+1];
    char *disksrv = NULL;
    char *errmsgtxt = NULL;
    char *ifce = NULL;
    char file_path[CA_MAXPATHLEN+1];

#if defined(ACCTON)
    if ( client == NULL || tape == NULL ) return(-1);
    tapereq = &tape->tapereq;
    if ( file != NULL ) {
        filereq = &file->filereq;
        mode = tapereq->mode;
        if ( mode == WRITE_ENABLE ) charcom = 'w';
        else charcom = 'r';
    } else {
        charcom = 'd';
        mode = WRITE_DISABLE;
    }
    jobID = tapereq->jobID;
    if ( jobID <= 0 ) jobID = rtcpd_jobID();

    if ( file != NULL ) {
        if ( *filereq->stageID != '\0' ) {
            strcpy(stageID,filereq->stageID);
            p = strstr(stageID,".");
            if ( p != NULL ) {
                *p = '\0';
                stager_reqID = atoi(stageID);
            }
        }
        KBytes = 0;
        if ( subtype == RTCPPRC || subtype == RTCPPRR ) {
            if ( mode == WRITE_ENABLE ) KBytes = (int)(filereq->bytes_in/1024);
            else KBytes = (int)(filereq->bytes_out/1024);
            
        } 
        ifce = filereq->ifce;
        fseq = filereq->tape_fseq;

        if ( subtype == RTCPCMDC ) {
            if ( AbortFlag == 0 ) rc = rtcp_RetvalSHIFT(tape,NULL,&exitcode);
            if ( AbortFlag == 1 ) exitcode = USERR;
            if ( AbortFlag == 2 ) exitcode = SYERR;
        } else rc = rtcp_RetvalSHIFT(tape,file,&exitcode);

        retry_nb = MAX_CPRETRY - filereq->err.max_cpretry;
        if ( retry_nb <= 0 ) retry_nb = MAX_TPRETRY - filereq->err.max_tpretry;
        if ( retry_nb <= 0 ) retry_nb = MAX_TPRETRY - tapereq->err.max_tpretry;
        if ( retry_nb < 0 ) retry_nb = 0;

        strcpy(file_path,filereq->file_path);
        (void)rfio_parse(file_path,&disksrv,&p); 
        if ( disksrv == NULL ) disksrv = client->clienthost;  

        errmsgtxt = filereq->err.errmsgtxt;
    }
    if ( errmsgtxt == NULL || *errmsgtxt == '\0' ) 
        errmsgtxt = tapereq->err.errmsgtxt;

    if ( subtype == RTCPEMSG && *errmsgtxt == '\0' ) {
        if ( AbortFlag == 0 ) {
            rtcp_log(LOG_ERR,"%s\n","rtcp_WriteAccountRecord(RTCPEMSG) without msg txt");
            return(-1);
        } else {
            if ( AbortFlag == 1 ) {
                exitcode = USERR;
                sprintf(errmsgtxt,"%s\n","request aborted by user");
            } else if ( AbortFlag == 2 ) {
                exitcode = SYERR;
                sprintf(errmsgtxt,"%s\n","request aborted by operator");
            }
        }
    }
    /*
     * Make sure to account for all failed requests
     */
    if ( subtype == RTCPCMDC && exitcode == 0 && 
         (rtcpd_CheckProcError() & RTCP_FAILED) != 0 ) {
        rtcp_log(LOG_ERR,"rtcp_WriteAccountRecord(RTCPCMDC) severity=0x%x but exitcode=%d. Force exitcode=%d!\n",rtcpd_CheckProcError(),exitcode,UNERR);
        exitcode = UNERR;
    }

#ifdef MONITOR

    rtcp_log(LOG_DEBUG, "MONITOR - Before rtcopy status sent\n");

    Cmonit_send_rtcopy_status(subtype,(uid_t)client->uid,(gid_t)client->gid,jobID,
                    stager_reqID,charcom,ifce,tapereq->vid,
                    KBytes,retry_nb,exitcode,client->clienthost,disksrv,
                    fseq,errmsgtxt, tapereq->unit);

    rtcp_log(LOG_DEBUG, "MONITOR - After rtcopy status sent\n");

#endif

    rc = rtcp_wacct(subtype,(uid_t)client->uid,(gid_t)client->gid,jobID,
                    stager_reqID,charcom,ifce,tapereq->vid,
                    KBytes,retry_nb,exitcode,client->clienthost,disksrv,
                    fseq,errmsgtxt);
    if ( subtype == RTCPPRC || subtype == RTCPPRR ) 
        rc = rtcp_AccountTiming(tape,file);
#endif /* ACCTON */
    return(0);
}

