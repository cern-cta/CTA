/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * killjid.c - command to kill local running job.
 */

#include <stdio.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else /* _WIN32 */
#include <regex.h>
#endif /* _WIN32 */
#include <time.h>
#include <Castor_limits.h>
#include <serrno.h>
#include <net.h>
#include <osdep.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <vdqm_api.h>
#include <Cuuid.h>
#include <Ctape_constants.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <stdlib.h>
#include <unistd.h>
extern int rtcp_InitLog(char *, FILE *, FILE *, SOCKET *);
extern char * optarg;
extern int optind;

#define USAGE_ERROR {fprintf(stderr,"Usage: %s [-r] [jid]\n",argv[0]); exit(2);}

int main(int argc, char *argv[]) {
    int rc, jid, reselect;
    int match_reqid = 0;
    vdqmVolReq_t volreq, match_volreq;
    vdqmDrvReq_t drvreq, match_drvreq;
    vdqmnw_t *nw = NULL;
    rtcpHdr_t hdr;
    static char errtxt[1024];
    char server[CA_MAXHOSTNAMELEN+1];
    SOCKET client_socket = INVALID_SOCKET;

    rtcp_InitLog(errtxt,NULL,stderr,NULL);
    if ( argc <= 1 ) USAGE_ERROR;

    reselect = FALSE;
    while ( (rc = getopt(argc,argv,"jr")) != EOF ) {
        switch(rc) {
        case 'j' :
                  match_reqid = 1;
                  break;
        case 'r' : 
                  reselect = TRUE;
                  break;
        case '?' :
                  USAGE_ERROR;
                  break;
        }
    }


    jid = atoi(argv[optind]);
    gethostname(server,sizeof(server)-1);
    if ( jid <= 0 ) USAGE_ERROR;

    /*
     * Get drive status
     */
    rc = 0;
    memset(&drvreq,'\0',sizeof(drvreq));
    memset(&match_drvreq,'\0',sizeof(match_drvreq));
    strcpy(drvreq.server,server);
    do {
       rc = vdqm_NextDrive(&nw,&drvreq);
       if ( match_reqid == 0 ) {
           if ( drvreq.jobID == jid ) match_drvreq = drvreq;
       } else {
           if ( drvreq.VolReqID == jid ) match_drvreq = drvreq;
       }
    } while (rc != -1);

    if ( match_drvreq.jobID != jid ) {
        if ( match_reqid == 0 ) {
            fprintf(stderr,"Could not find volume request for jobid=%d\n",jid);
            exit(2);
        } else {
            fprintf(stderr,"Could not find running request for VolReqID=%d\n",
                    jid);
            match_drvreq.VolReqID = jid;
        } 
    }

    nw = NULL;
    /*
     * Get volume queue
     */
    rc = 0;
    memset(&volreq,'\0',sizeof(volreq));
    memset(&match_volreq,'\0',sizeof(match_volreq));
    do {
       strcpy(volreq.dgn,match_drvreq.dgn);
       rc = vdqm_NextVol(&nw,&volreq);
       if ( volreq.VolReqID == match_drvreq.VolReqID ) match_volreq = volreq;
    } while (rc != -1);
    if ( *match_volreq.client_host == '\0' || match_volreq.client_port <= 0 ) {
        fprintf(stderr,"Could not find client for job ID %d\n",jid);
        exit(1);
    }

    fprintf(stdout,"Client is (%d,%d)@%s:%d\n",
            match_volreq.clientUID,match_volreq.clientGID,
            match_volreq.client_host,match_volreq.client_port);

    hdr.magic = RTCOPY_MAGIC;
    if ( reselect == TRUE ) {
        hdr.reqtype = RTCP_RSLCT_REQ;
        fprintf(stdout,"Sending RSLCT to %s:%d\n",match_volreq.client_host,
                match_volreq.client_port);
    } else {
        hdr.reqtype = RTCP_KILLJID_REQ;
        fprintf(stdout,"Sending KILLJID to %s:%d\n",match_volreq.client_host,
                match_volreq.client_port);
    }

    hdr.len = 0;
    rc = rtcp_Connect(&client_socket,match_volreq.client_host,
                      &match_volreq.client_port, RTCP_CONNECT_TO_CLIENT);
    if ( rc == -1 ) {
        fprintf(stderr,"rtcp_Connect(): %s\n",sstrerror(serrno));
        fprintf(stderr,"... trying to remove the request from VDQM\n");
        rc = vdqm_DelVolumeReq(NULL,match_volreq.VolReqID, match_volreq.volid, match_volreq.dgn, NULL, NULL, match_volreq.client_port);
        if ( rc == -1 ) {
            fprintf(stderr,"vdqm_DelVolumeReq(): %s\n",sstrerror(serrno));
            exit(1);
        }
        exit(0);
    }
    rc = rtcp_SendReq(&client_socket,&hdr,NULL,NULL,NULL);
    if ( rc == -1 ) {
        fprintf(stderr,"rtcp_SendReq(): %s\n",sstrerror(serrno));
        (void)rtcp_CloseConnection(&client_socket);
        exit(1);
    }
    rc = rtcp_RecvAckn(&client_socket, hdr.reqtype);
    if ( rc == -1 ) {
        fprintf(stderr,"rtcp_RecvAckn(): %s\n",sstrerror(serrno));
        (void)rtcp_CloseConnection(&client_socket);
        exit(1);
    }
    rc = rtcp_CloseConnection(&client_socket);
    exit(0);
}
