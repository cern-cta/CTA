/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_SHIFTClients.c,v $ $Revision: 1.1 $ $Date: 1999/12/17 12:08:35 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_SHIFTClients.c - RTCOPY routines to serve old SHIFT clients
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
#include <osdep.h>
#include <net.h>
#include <marshall.h>
#include <Cthread_api.h>
#include <vdqm_api.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <Ctape_api.h>
#include <serrno.h>

/*
 * Special unmarshall_STRING macro to avoid unnecessary copies.
 */
#define unmarshall_STRING_nc(ptr,str) { str=ptr; INC_PTR(ptr,strlen(str)+1);}


static int rtcp_GetOldCMsg(SOCKET *s, 
                           rtcpHdr_t *hdr,
                           shift_client_t **req) {
    int argc = -1;
    int rc, rfio_key, mask, i;
    char **argv = NULL;
    char *p;
    char *msgbuf = NULL;
    static char commands[][20]={"tpread","tpwrite","dumptape"};

    if ( s == NULL || *s == INVALID_SOCKET || 
         hdr == NULL || req == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    /*
     * Header has already been received in rtcp_RecvReq()
     */
    rc = 0;
    if ( hdr->len > 0 ) {
        *req = (shift_client_t *)calloc(1,sizeof(shift_client_t));
        if ( *req == NULL ) {
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() calloc(1,%d): %s\n",
                sizeof(shift_client_t),sstrerror(errno));
            return(-1);
        }
        msgbuf = (char *)malloc(hdr->len);
        if ( msgbuf == NULL ) {
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() malloc(%d): %s\n",
                hdr->len,sstrerror(errno));
            free(*req);
            *req = NULL;
            return(-1);
        }

        rc = netread(*s,msgbuf,hdr->len);
        switch (rc) {
        case -1:
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() netread(%d): %s\n",
                hdr->len,neterror());
            return(-1);
        case 0:
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() netread(%d): connection dropped\n",
                hdr->len);
            return(-1);
        }

        p = msgbuf;
        unmarshall_STRING(p,(*req)->name);
        unmarshall_WORD(p,(*req)->uid);
        unmarshall_WORD(p,(*req)->gid);
        if ( hdr->reqtype == RQST_INFO ) {
            unmarshall_STRING(p,(*req)->dgn);
        } else if ( hdr->reqtype == RQST_DKTP ||
                    hdr->reqtype == RQST_TPDK ||
                    hdr->reqtype == RQST_DPTP ) {
            unmarshall_WORD(p,rfio_key);
            unmarshall_WORD(p,mask);
            unmarshall_LONG(p,argc);
            if ( argc < 0 ) {
                serrno = SEINTERNAL;
                return(-1);
            }
            argv = (char **)malloc((argc+1) * sizeof(char *));
            if ( hdr->reqtype == RQST_TPDK ) argv[0] = commands[0];
            else if ( hdr->reqtype == RQST_DKTP ) argv[0] = commands[1];
            else if ( hdr->reqtype == RQST_DPTP ) argv[0] = commands[2];
            
            /*
             * Don't duplicate the strings. We must make sure
             * to not free msgbuf until the command has been parsed.
             */
            for (i=1;i<argc; i++) unmarshall_STRING_nc(p,argv[i]);
            argv[argc] = NULL;
            rc = rtcpc_BuildReq(&((*req)->tape),argc,argv);
            free(msgbuf);
        } else {
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() unknown request type 0x%x\n",
                hdr->reqtype);
            serrno = SEINTERNAL;
            return(-1);
        }
    }
    return(rc);
}


static int rtcp_GetOldCinfo(shift_client_t *req) {
    vdqmVolReq_t VolReq;
    vdqmDrvReq_t DrvReq;
    vdqmnw_t *nw = NULL;
    char server[CA_MAXHOSTNAMELEN+1];
    int namelen = CA_MAXHOSTNAMELEN;
    int nb_queued, nb_running, nb_drv, rc;


    if ( req == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    gethostname(server,namelen);

    memset(&VolReq,'\0',sizeof(vdqmVolReq_t));
    memset(&DrvReq,'\0',sizeof(vdqmDrvReq_t));

    strcpy(VolReq.server,server);
    strcpy(DrvReq.server,server);
    strcpy(VolReq.dgn,req->dgn);
    strcpy(DrvReq.dgn,req->dgn);

    while ( (rc = vdqm_NextVol(&nw,&VolReq)) != -1 ) {
        if ( *VolReq.volid == '\0' ) continue;
        nb_queued++;
    }

    nw = NULL;
    while ( (rc = vdqm_NextDrive(&nw,&DrvReq)) != -1 ) {
        if ( *DrvReq.drive == '\0' ) continue;
        if ( DrvReq.VolReqID > 0 ) nb_running++;
        nb_drv++;
    }

    req->info.status = AVAILABLE;
    req->info.queue = nb_queued;
    req->info.nb_units = nb_drv;
    return(0);
}

int rtcp_RunOld(SOCKET *s, rtcpHdr_t *hdr) {
    int rc, CLThId;
    shift_client_t *req = NULL;
    tape_list_t *tl;
    file_list_t *fl;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    char client_msg_buf[256];

    if ( s == NULL || hdr == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    rtcp_InitLog(client_msg_buf,NULL,NULL,s);

    rc = rtcp_GetOldCMsg(s,hdr,&req);
    if ( rc == -1 ) return(-1);

    rc = rtcp_SendOldCAckn(s,hdr);
    if ( rc == -1 ) return(-1);

    if ( hdr->reqtype == RQST_INFO ) {
        rc = rtcp_GetOldCinfo(req);
        if ( rc == -1 ) return(-1);

        rc = rtcp_SendOldCinfo(s,hdr,req);
        if ( rc == -1 ) return(-1);

        return(0);
    }
    /*
     * Kick off client listen thread to answer pings etc.
     */
    CLThId = rtcpd_ClientListen(*s);
    if ( CLThId == -1 ) return(-1);

    CLIST_ITERATE_BEGIN(req->tape,tl) {
        tapereq = &tl->tapereq;
        dumpTapeReq(tl);
        CLIST_ITERATE_BEGIN(tl->file,fl) {
            filereq = &fl->filereq;
            dumpFileReq(fl);
        } CLIST_ITERATE_END(tl->file,fl);
    } CLIST_ITERATE_END(req->tape,tl);

    return(0);
}