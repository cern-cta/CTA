/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_SHIFTClients.c,v $ $Revision: 1.6 $ $Date: 1999/12/28 15:14:51 $ CERN IT-PDP/DM Olof Barring";
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
#include <Cnetdb.h>
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
    int rc, rfio_key, i;
    mode_t mask;
    char **argv = NULL;
    char *p;
    char *msgbuf = NULL;
    struct sockaddr_in from;
    struct hostent *hp;
    int fromlen; 
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

        fromlen = sizeof(from);
        rc = getpeername(*s,(struct sockaddr *)&from,&fromlen);
        if ( rc == SOCKET_ERROR ) {
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() getpeername(): %s\n",
                     neterror());
            return(-1);
        }
        hp = Cgethostbyaddr((void *)&(from.sin_addr),sizeof(struct in_addr),
                            from.sin_family);
        if ( hp == NULL ) {
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() Cgethostbyaddr(): h_errno=%d, %s\n",
                     h_errno,neterror());
            return(-1);
        }
        strcpy((*req)->clienthost,hp->h_name);

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
            if ( hdr->magic == RTCOPY_MAGIC_SHIFT ) unmarshall_WORD(p,rfio_key);
            unmarshall_WORD(p,mask);
            umask(mask);
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
    int nb_queued, nb_used, nb_units, rc;


    if ( req == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    memset(&req->info,'\0',sizeof(tpqueue_info_t));
    if ( rtcpd_CheckNoMoreTapes() != 0 ) {
        req->info.status = NOTAVAIL;
        return(0);
    }
    gethostname(server,namelen);

    memset(&VolReq,'\0',sizeof(vdqmVolReq_t));
    memset(&DrvReq,'\0',sizeof(vdqmDrvReq_t));

    strcpy(VolReq.server,server);
    strcpy(DrvReq.server,server);
    strcpy(VolReq.dgn,req->dgn);
    strcpy(DrvReq.dgn,req->dgn);

    nb_queued = nb_used = nb_units = 0;

    while ( (rc = vdqm_NextVol(&nw,&VolReq)) != -1 ) {
        if ( *VolReq.volid == '\0' ) continue;
        nb_queued++;
    }

    nw = NULL;
    while ( (rc = vdqm_NextDrive(&nw,&DrvReq)) != -1 ) {
        if ( *DrvReq.drive == '\0' ) continue;
        if ( DrvReq.VolReqID > 0 ) nb_used++;
        if ( (DrvReq.status & VDQM_UNIT_UP) != 0 ) nb_units++;
    }

    req->info.status = AVAILABLE;
    req->info.nb_queued = nb_queued;
    req->info.nb_units = nb_units;
    req->info.nb_used = nb_used;
    return(0);
}

static int rtcp_SendRC(SOCKET *s,
                        rtcpHdr_t *hdr,
                        int *status,
                        shift_client_t *req) {
    tape_list_t *tl;
    file_list_t *fl;
    rtcpErrMsg_t *err;
    int retval, rc;
    char msgbuf[4*LONGSIZE], *p;

    if ( s == NULL || *s == INVALID_SOCKET || hdr == NULL || 
         status == NULL || req == NULL ) {
        serrno = EINVAL;
        return;
    }

    err = NULL;
    CLIST_ITERATE_BEGIN(req->tape,tl) {
        if ( tl->tapereq.tprc != 0 ) {
            err = &(tl->tapereq.err);
            break;
        }
        CLIST_ITERATE_BEGIN(tl->file,fl) {
            if ( fl->filereq.cprc != 0 ) {
                err = &(fl->filereq.err);
                break;
            }
        } CLIST_ITERATE_END(tl->file,fl);
        if ( fl->filereq.cprc != 0 ) break;
    } CLIST_ITERATE_END(req->tape,tl);

    if ( err != NULL ) {
        if ( (err->severity & RTCP_FAILED) != 0 ) {
            if ( (err->severity & RTCP_RESELECT_SERV) != 0 ) retval == RSLCT;
            else if ( (err->severity & RTCP_USERR) != 0 ) retval = USERR;
            else if ( (err->severity & RTCP_SYERR) != 0 ) retval = SYERR;
            else if ( (err->severity & RTCP_UNERR) != 0 ) retval = UNERR;
            else if ( (err->severity & RTCP_SEERR) != 0 ) retval = SEERR;
            else retval = UNERR;
        } else {
            if ( (err->severity & RTCP_BLKSKPD) != 0 ) retval = BLKSKPD;
            else if ( (err->severity & RTCP_TPE_LSZ) != 0 ) retval = TPE_LSZ;
            else if ( (err->severity & RTCP_MNYPARY) != 0 ) retval = MNYPARY;
            else if ( (err->severity & RTCP_LIMBYSZ) != 0 ) retval = LIMBYSZ;
            else retval = 0;
        } 
    } else retval = 0;

    *status = retval;
    p = msgbuf;
    marshall_LONG(p,hdr->magic);
    marshall_LONG(p,GIVE_RESU);
    marshall_LONG(p,LONGSIZE);
    marshall_LONG(p,retval);
    rc = netwrite(*s, msgbuf, 4*LONGSIZE);
    switch (rc) {
    case -1:
        rtcp_log(LOG_ERR,"rtcp_SendRC() netwrite(): %s\n",neterror());
        return(-1);
    case 0:
        rtcp_log(LOG_ERR,"rtcp_SendRC() netwrite(): connection dropped\n");
        return(-1);
    }
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

    rc = rtcp_GetOldCMsg(s,hdr,&req);
    if ( rc == -1 ) return(-1);

    rc = rtcp_SendOldCAckn(s,hdr);
    if ( rc == -1 ) return(-1);

    if ( hdr->reqtype == RQST_INFO ) {
        rtcp_log(LOG_INFO,"info request by user %s (%d,%d) from %s\n",
                 req->name,req->uid,req->gid,req->clienthost);

        rc = rtcp_GetOldCinfo(req);
        if ( rc == -1 ) return(-1);
        rtcp_log(LOG_INFO,"info request returns status=%d, used=%d, queue=%d, units=%d\n",req->info.status,req->info.nb_used,req->info.nb_queued,req->info.nb_units);

        rc = rtcp_SendOldCinfo(s,hdr,req);
        if ( rc == -1 ) return(-1);

        return(0);
    }
    /*
     * Kick off client listen thread to answer pings etc.
     */
    CLThId = rtcpd_ClientListen(*s);
    if ( CLThId == -1 ) return(-1);

    /*
     * Start the request
     */

#if !defined(_WIN32)
    if ( setgid(req->gid) == -1 ) {
        rtcp_log(LOG_ERR,"setgid(%d): %s\n",req->gid,sstrerror(errno));
        return(-1);
    }
    if ( setuid(req->uid) == -1 ) {
        rtcp_log(LOG_ERR,"setuid(%d): %s\n",req->uid,sstrerror(errno));
        return(-1);
    }
#endif /* !_WIN32 */

    /*
     * Redirect logging to client socket
     */
    rtcp_InitLog(client_msg_buf,NULL,NULL,s);

    rc = rtcpc(req->tape);
    (void) rtcp_SendRC(s,hdr,&rc,req); 

    return(rc);
}

