/*
 * Copyright (C) 1995-2000 by CERN IT-PDP/DM Olof Barring
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcp_accounting.c,v $ $Revision: 1.3 $ $Date: 2000/02/23 15:56:52 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

#include <sys/types.h>
#include <string.h>
#include <Castor_limits.h>
#include <osdep.h>
#include <net.h>
#include <sacct.h>
#include <Ctape_constants.h>
#include <rtcp_constants.h>
#include <rtcp.h>

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
    strcpy (acctrtcp.ifce, ifce);
    strcpy (acctrtcp.vid, vid);
    acctrtcp.size = size;
    acctrtcp.retryn = retryn;
    acctrtcp.exitcode = exitcode;
    strcpy (acctrtcp.clienthost, clienthost);
    strcpy (acctrtcp.dsksrvr, dsksrvr);
    strcpy (acctrtcp.errmsgtxt, errmsgtxt);
    acctreclen = ((char *) acctrtcp.errmsgtxt - (char *) &acctrtcp) +
                 strlen(errmsgtxt)+ 1;

    wsacct (ACCTRTCOPY, &acctrtcp, acctreclen);
    return(0);
}

int rtcp_WriteAccountRecord(rtcpClientInfo_t *client,
                            file_list_t *file,
                            int subtype) {
    tape_list_t *tape;
    rtcpTapeRequest_t *tapereq = NULL;
    rtcpFileRequest_t *filereq = NULL;
    int jobID, rc, stager_reqID, KBytes, retry_nb, mode;
    int severity, exitcode;
    char charcom, *p, stageID[CA_MAXSTGRIDLEN+1];
    char *disksrv, *errmsgtxt, file_path[CA_MAXPATHLEN+1];

#if defined(ACCTON)
    if ( file == NULL || file->tape == NULL ) return(-1);
    filereq = &file->filereq;
    tape = file->tape;
    tapereq = &tape->tapereq;
    mode = tapereq->mode;
    if ( mode == WRITE_ENABLE ) charcom = 'w';
    else charcom = 'r';
    jobID = tapereq->jobID;
    if ( jobID <= 0 ) jobID = getpgrp();
    stager_reqID = 0;
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
        if ( mode == WRITE_ENABLE ) KBytes = (int)(filereq->bytes_in / 1024);
        else KBytes = (int)(filereq->bytes_out / 1024);
    } 

    if ( subtype == RTCPCMDC ) rc = rtcp_RetvalSHIFT(tape,NULL,&exitcode);
    else rc = rtcp_RetvalSHIFT(tape,file,&exitcode);

    retry_nb = MAX_CPRETRY - filereq->err.max_cpretry;
    if ( retry_nb <= 0 ) retry_nb = MAX_TPRETRY - filereq->err.max_tpretry;
    if ( retry_nb <= 0 ) retry_nb = MAX_TPRETRY - tapereq->err.max_tpretry;
    if ( retry_nb < 0 ) retry_nb = 0;

    strcpy(file_path,filereq->file_path);
    if ( (p = strstr(file_path,":")) != NULL ) {
        *p = '\0';
        disksrv = file_path;
    } else disksrv = client->clienthost;

    errmsgtxt = filereq->err.errmsgtxt;
    if ( *errmsgtxt == '\0' ) errmsgtxt = tapereq->err.errmsgtxt;

    rc = rtcp_wacct(subtype,(uid_t)client->uid,(gid_t)client->gid,jobID,
                    stager_reqID,charcom,filereq->ifce,tapereq->vid,
                    KBytes,retry_nb,exitcode,client->clienthost,disksrv,
                    errmsgtxt);
#endif /* ACCTON */
    return(0);
}
