/*
 * Copyright (C) 1999-2003 by CERN
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqm_SendRecv.c,v $ $Revision: 1.2 $ $Date: 2005/03/15 22:57:11 $ CERN IT/ADC Olof Barring";
#endif /* not lint */

/*
 * vdqm_SendRecv.c - Send and receive VDQM request and acknowledge messages.
 */

#include <stdlib.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#endif /* _WIN32 */

#include <errno.h>

#include <Castor_limits.h>
#include <rtcp_constants.h>             /* Definition of RTCOPY_MAGIC   */
#include <net.h>
#include <log.h>
#include <osdep.h>
#include <Cnetdb.h>
#include <marshall.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <serrno.h>
#include <common.h>
extern char *getconfent();

#define REQTYPE(Y,X) ( X == VDQM_##Y##_REQ || \
    X == VDQM_DEL_##Y##REQ || \
    X == VDQM_GET_##Y##QUEUE || (!strcmp(#Y,"VOL") && X == VDQM_PING) || \
    (!strcmp(#Y,"DRV") && X == VDQM_DEDICATE_DRV) )
#define ADMINREQ(X) ( X == VDQM_HOLD || X == VDQM_RELEASE || \
    X == VDQM_SHUTDOWN )

typedef enum direction {SendTo,ReceiveFrom} direction_t;

#define DO_MARSHALL(X,Y,Z,W) { \
    if ( W == SendTo ) {marshall_##X(Y,Z);} \
    else {unmarshall_##X(Y,Z);} }

#define DO_MARSHALL_STRING(Y,Z,W,N) { \
    if ( W == SendTo ) {marshall_STRING(Y,Z);} \
    else { if(unmarshall_STRINGN(Y,Z,N)) { serrno=EINVAL; return -1; } } }



static int vdqm_Transfer(vdqmnw_t *nw,
                         vdqmHdr_t *hdr,
                         vdqmVolReq_t *volreq,
                         vdqmDrvReq_t *drvreq,
                         direction_t whereto) {
    
    char hdrbuf[VDQM_HDRBUFSIZ], buf[VDQM_MSGBUFSIZ];
    char servername[CA_MAXHOSTNAMELEN+1];
    char *p,*domain;
    struct sockaddr_in from;
    struct hostent *hp;
    int fromlen;
    int magic,reqtype,len,local_access; 
    int rc;
    SOCKET s;
    
    /*
     * Sanity checks
     */
    if ( nw == NULL                                   ||
        (nw->accept_socket == INVALID_SOCKET          &&
        nw->connect_socket == INVALID_SOCKET) ) {
        serrno = EINVAL;
        return(-1);
    }
    
    reqtype = -1;
    *servername = '\0';
    local_access = 0;
    magic = len = 0;
    if ( (s = nw->accept_socket) == INVALID_SOCKET ) {
        rc = gethostname(servername,CA_MAXHOSTNAMELEN);
        s = nw->connect_socket;
    }
    
    if ( whereto == ReceiveFrom ) {
        rc = netread_timeout(s,hdrbuf,VDQM_HDRBUFSIZ,VDQM_TIMEOUT);
        switch (rc) {
        case -1: 
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer() netread(HDR): %s\n",
                neterror());
#endif /* VDQMSERV */
            serrno = SECOMERR;
            return(-1);
        case 0:
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer() netread(HDR): connection dropped\n");
#endif /* VDQMSERV */
            serrno = SECONNDROP;
            return(-1);
        }
        p = hdrbuf;
        DO_MARSHALL(LONG,p,magic,whereto);
        DO_MARSHALL(LONG,p,reqtype,whereto);
        DO_MARSHALL(LONG,p,len,whereto);
        if ( hdr != NULL ) {
            hdr->magic = magic;
            hdr->reqtype = reqtype;
            hdr->len = len;
        }
        if ( VALID_VDQM_MSGLEN(len) ) {
            rc = netread_timeout(s,buf,len,VDQM_TIMEOUT);
            switch (rc) {
            case -1:
#if defined(VDQMSERV)
                log(LOG_ERR,"vdqm_Transfer() netread(REQ): %s\n",
                    neterror());
#endif /* VDQMSERV */
                serrno = SECOMERR;
                return(-1);
            case 0:
#if defined(VDQMSERV)
                log(LOG_ERR,"vdqm_Transfer() netread(REQ): connection dropped\n");
#endif /* VDQMSERV */
                serrno = SECONNDROP;
                return(-1);
            }
        } else if ( len > 0 ) {
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer() netread(REQ): invalid message length %d\n",
                len);
#endif /* VDQMSERV */
            serrno = SEUMSG2LONG;
            return(-1);
        }
        
        fromlen = sizeof(from);
        if ( (rc = getpeername(s,(struct sockaddr *)&from,&fromlen)) == SOCKET_ERROR ) {
#if defined(VDQMSERV)
          log(LOG_ERR,"vdqm_Transfer(): getpeername() %s\n",neterror());
#endif /* VDQMSERV */
          return(-1);
        } 
        if ( (hp = Cgethostbyaddr((void *)&(from.sin_addr),sizeof(struct in_addr),from.sin_family)) == NULL ) {
#if defined(VDQMSERV)
          log(LOG_ERR,"vdqm_Transfer(): Cgethostbyaddr() h_errno=%d, %s\n",
              h_errno,neterror());
#endif /* VDQMSERV */
          return(-1);
        }
        if ( (REQTYPE(VOL,reqtype) && volreq == NULL) ||
             (REQTYPE(DRV,reqtype) && drvreq == NULL) ) {
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer(): no buffer for reqtype=0x%x\n",
                reqtype);
#endif /* VDQMSERV */
            serrno = EINVAL;
            return(-1);
        } else if ( REQTYPE(DRV,reqtype) ) {
            /* 
             * We need to authorize request host if not same as server name.
             */
            strcpy(drvreq->reqhost,hp->h_name);
            if ( isremote(from.sin_addr,drvreq->reqhost) == 1 &&
                 getconfent("VDQM","REMOTE_ACCESS",1) == NULL ) {
#if defined(VDQMSERV)
                log(LOG_ERR,"vdqm_Transfer(): remote access attempted, host=%s\n",drvreq->reqhost);
                return(-1);
#endif /* VDQMSERV */
            } else {
               local_access = 1;
               if ( (domain = strstr(drvreq->reqhost,".")) != NULL ) 
                    *domain = '\0';
            }
        }
        if ( ADMINREQ(reqtype) ) {
#if defined(VDQMSERV)
          log(LOG_INFO,"ADMIN request (0x%x) from %s\n",
              reqtype,hp->h_name);
#endif /* VDQMSERV */
          if ( (isadminhost(s,hp->h_name) != 0) ) {
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer(): unauthorised ADMIN request (0x%x) from %s\n",
                reqtype,hp->h_name);
#endif /* VDQMSERV */
            serrno = EPERM;
            return(-1);
          }   
        }
    }
    if ( whereto == SendTo ) {
        if ( hdr != NULL && VDQM_VALID_REQTYPE(hdr->reqtype) ) reqtype = hdr->reqtype;
        else if ( volreq != NULL ) reqtype = VDQM_VOL_REQ;
        else if ( drvreq != NULL ) reqtype = VDQM_DRV_REQ;
        else {
            log(LOG_ERR,"vdqm_Transfer(): cannot determine request type to send\n");
            return(-1);
        }
        if ( *servername != '\0' ) {
            if ( reqtype == VDQM_VOL_REQ && *volreq->client_host == '\0' ) 
                strcpy(volreq->client_host,servername);
            else if ( reqtype == VDQM_DRV_REQ && *drvreq->server == '\0' )
                strcpy(drvreq->server,servername);
        }
    }
    
    p = buf;
    if ( REQTYPE(VOL,reqtype) && volreq != NULL ) {
        DO_MARSHALL(LONG,p,volreq->VolReqID,whereto);
        DO_MARSHALL(LONG,p,volreq->DrvReqID,whereto);
        DO_MARSHALL(LONG,p,volreq->priority,whereto);
        DO_MARSHALL(LONG,p,volreq->client_port,whereto);
        DO_MARSHALL(LONG,p,volreq->clientUID,whereto);
        DO_MARSHALL(LONG,p,volreq->clientGID,whereto);
        DO_MARSHALL(LONG,p,volreq->mode,whereto);
        DO_MARSHALL(LONG,p,volreq->recvtime,whereto);
        DO_MARSHALL_STRING(p,volreq->client_host,whereto, sizeof(volreq->client_host));
        DO_MARSHALL_STRING(p,volreq->volid,whereto, sizeof(volreq->volid));
        DO_MARSHALL_STRING(p,volreq->server,whereto, sizeof(volreq->server));
        DO_MARSHALL_STRING(p,volreq->drive,whereto, sizeof(volreq->drive));
        DO_MARSHALL_STRING(p,volreq->dgn,whereto, sizeof(volreq->dgn));
        DO_MARSHALL_STRING(p,volreq->client_name,whereto, sizeof(volreq->client_name));
    }
    if ( REQTYPE(DRV,reqtype) && drvreq != NULL ) {
        DO_MARSHALL(LONG,p,drvreq->status,whereto);
        DO_MARSHALL(LONG,p,drvreq->DrvReqID,whereto);
        DO_MARSHALL(LONG,p,drvreq->VolReqID,whereto);
        DO_MARSHALL(LONG,p,drvreq->jobID,whereto);
        DO_MARSHALL(LONG,p,drvreq->recvtime,whereto);
        DO_MARSHALL(LONG,p,drvreq->resettime,whereto);
        DO_MARSHALL(LONG,p,drvreq->usecount,whereto);
        DO_MARSHALL(LONG,p,drvreq->errcount,whereto);
        DO_MARSHALL(LONG,p,drvreq->MBtransf,whereto);
        DO_MARSHALL(LONG,p,drvreq->mode,whereto);
        DO_MARSHALL(HYPER,p,drvreq->TotalMB,whereto);
        DO_MARSHALL_STRING(p,drvreq->volid,whereto, sizeof(drvreq->volid));
        DO_MARSHALL_STRING(p,drvreq->server,whereto, sizeof(drvreq->server));
        DO_MARSHALL_STRING(p,drvreq->drive,whereto, sizeof(drvreq->drive));
        DO_MARSHALL_STRING(p,drvreq->dgn,whereto, sizeof(drvreq->dgn));
        DO_MARSHALL_STRING(p,drvreq->dedicate,whereto, sizeof(drvreq->dedicate));
        if ( (whereto == ReceiveFrom) && (local_access == 1) &&
             (domain = strstr(drvreq->server,".")) != NULL ) *domain = '\0';
    }
 
    if ( whereto == SendTo ) {
        /*
         * reqtype has already been determined above
         */
        if ( hdr != NULL && hdr->magic != 0 ) magic = hdr->magic;
        else magic = VDQM_MAGIC;
        len = 0;
        if ( REQTYPE(VOL,reqtype)) 
             len = VDQM_VOLREQLEN(volreq);
        else if ( REQTYPE(DRV,reqtype) ) 
               len = VDQM_DRVREQLEN(drvreq);
        else if ( ADMINREQ(reqtype) ) len = 0;
        else if ( hdr != NULL ) len = hdr->len;
        p = hdrbuf;
        DO_MARSHALL(LONG,p,magic,whereto);
        DO_MARSHALL(LONG,p,reqtype,whereto);
        DO_MARSHALL(LONG,p,len,whereto);
        rc = netwrite_timeout(s,hdrbuf,VDQM_HDRBUFSIZ,VDQM_TIMEOUT);
        switch (rc) {
        case -1:
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer() netwrite(HDR): %s\n",
                neterror());
#endif /* VDQMSERV */
            serrno = SECOMERR;
            return(-1);
        case 0:
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer() netwrite(HDR): connection dropped\n");
#endif /*VDQMSERV */
            serrno = SECONNDROP;
            return(-1);
        }
        if ( len > 0 ) {
            rc = netwrite_timeout(s,buf,len,VDQM_TIMEOUT);
            switch (rc) {
            case -1:
#if defined(VDQMSERV)
                log(LOG_ERR,"vdqm_Transfer() netwrite(REQ): %s\n",
                    neterror());
#endif /* VDQMSERV */
                serrno = SECOMERR;
                return(-1);
            case 0:
#if defined(VDQMSERV)
                log(LOG_ERR,"vdqm_Transfer() netwrite(REQ): connection dropped\n");
#endif /*VDQMSERV */
                serrno = SECONNDROP;
                return(-1);
            }
        }
    } else if ( whereto == ReceiveFrom ) {
      if ( REQTYPE(DRV,reqtype) && (reqtype != VDQM_GET_DRVQUEUE) ) {
        if ( (strcmp(drvreq->reqhost,drvreq->server) != 0) &&
             (isadminhost(s,drvreq->reqhost) != 0) ) {
#if defined(VDQMSERV)
          log(LOG_ERR,"vdqm_Transfer(): unauthorised drive request (0x%x) for %s@%s from %s\n",
              reqtype,drvreq->drive,drvreq->server,drvreq->reqhost);
#endif /* VDQMSERV */
          serrno = EPERM;
          return(-1);
        }
      }
    }
    return(reqtype);
}

int vdqm_SendReq(vdqmnw_t *nw, 
                 vdqmHdr_t *hdr, 
                 vdqmVolReq_t *volreq,
                 vdqmDrvReq_t *drvreq) {
    direction_t whereto = SendTo;
    return(vdqm_Transfer(nw,hdr,volreq,drvreq,whereto));
}

int vdqm_RecvReq(vdqmnw_t *nw, 
                 vdqmHdr_t *hdr, 
                 vdqmVolReq_t *volreq,
                 vdqmDrvReq_t *drvreq) {
    direction_t whereto = ReceiveFrom;
    return(vdqm_Transfer(nw,hdr,volreq,drvreq,whereto));
}

int vdqm_SendPing(vdqmnw_t *nw,
                  vdqmHdr_t *hdr,
                  vdqmVolReq_t *volreq) {
    direction_t whereto = SendTo;
    vdqmHdr_t tmphdr, *tmphdr_p;
    if ( hdr == NULL ) {
        tmphdr.magic = VDQM_MAGIC; 
        tmphdr.reqtype = VDQM_PING;
        tmphdr.len = -1;
        tmphdr_p = &tmphdr;
    } else {
        hdr->reqtype = VDQM_PING;
        tmphdr_p = hdr;
    }
    return(vdqm_Transfer(nw,tmphdr_p,volreq,NULL,whereto));
}

#if defined(VDQMSERV)
int vdqm_Hangup(vdqmnw_t *nw) {
	direction_t whereto = SendTo;
	vdqmHdr_t hdr;
	hdr.magic = VDQM_MAGIC;
	hdr.reqtype = VDQM_HANGUP;
	hdr.len = -1;
	return(vdqm_Transfer(nw,&hdr,NULL,NULL,whereto));
}

int vdqm_GetReplica(vdqmnw_t *nw, vdqmReplica_t *Replica) {
    int fromlen;
    struct hostent *hp = NULL;
    struct sockaddr_in from;
    char *p;
    SOCKET s;

    if ( nw == NULL || nw->accept_socket == INVALID_SOCKET || 
         Replica == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    s = nw->accept_socket;
    fromlen = sizeof(from);
    if ( getpeername(s,(struct sockaddr *)&from,&fromlen) == SOCKET_ERROR ) {
        log(LOG_ERR,"vdqm_GetReplica() getpeername(): %s\n",neterror());
        serrno = SECOMERR;
        return(-1);
    }
    if ( (hp = Cgethostbyaddr((void *)&(from.sin_addr),sizeof(struct in_addr),from.sin_family)) == NULL ) {
        log(LOG_ERR,"vdqm_Transfer(): Cgethostbyaddr() h_errno=%d, %s\n",
            h_errno,neterror());
        serrno = SECOMERR;
        return(-1);
    }
    /*
     * Remove domain
     */
    strcpy(Replica->host,hp->h_name);
    if ( (p = strchr(Replica->host,'.')) != NULL ) *p = '\0';
    return(0);
}
#endif /* VDQMSERV */

static int vdqm_TransAckn(vdqmnw_t *nw, int reqtype, int *data, 
                          direction_t whereto) {
    char hdrbuf[VDQM_HDRBUFSIZ];
    int magic, recvreqtype, len, rc;
    char *p;
    SOCKET s;
    
    if ( nw == NULL                                   ||
        (nw->accept_socket == INVALID_SOCKET          &&
        nw->connect_socket == INVALID_SOCKET) ) {
        serrno = EINVAL;
        return(-1);
    }
    if ( (s = nw->accept_socket) == INVALID_SOCKET )
        s = nw->connect_socket;
    
    magic = VDQM_MAGIC;
    len = 0;
    recvreqtype = reqtype;
    if ( data != NULL ) len = *data;
    
    if ( whereto == ReceiveFrom ) {
        rc = netread_timeout(s,hdrbuf,VDQM_HDRBUFSIZ,VDQM_TIMEOUT);
        switch (rc) {
        case -1: 
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_TransAckn() netread(HDR): %s\n",
                neterror());
#endif /* VDQMSERV */
            serrno = SECOMERR;
            return(-1);
        case 0:
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_TransAckn() netread(HDR): connection dropped\n");
#endif /* VDQMSERV */
            serrno = SECONNDROP;
            return(-1);
        }
    }
    p = hdrbuf;
    DO_MARSHALL(LONG,p,magic,whereto);
    DO_MARSHALL(LONG,p,recvreqtype,whereto);
    DO_MARSHALL(LONG,p,len,whereto);
    
    if ( whereto == SendTo ) {
        magic = VDQM_MAGIC;
        len = 0;
        p = hdrbuf;
        rc = netwrite_timeout(s,hdrbuf,VDQM_HDRBUFSIZ,VDQM_TIMEOUT);
        switch (rc) {
        case -1: 
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_TransAckn() netwrite(HDR): %s\n",
                neterror());
#endif /* VDQMSERV */
            serrno = SECOMERR;
            return(-1);
        case 0:
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_TransAckn() netwrite(HDR): connection dropped\n");
#endif /* VDQMSERV */
            serrno = SECONNDROP;
            return(-1);
        }
    }
    
    if ( data != NULL ) *data = len;
    return(recvreqtype);
}

#if defined(VDQMSERV)
int vdqm_AcknRollback(vdqmnw_t *nw) {
    direction_t whereto = SendTo;
    int reqtype; 
    reqtype = vdqm_GetError();
    
    return(vdqm_TransAckn(nw,reqtype,NULL,whereto));
}
#endif /* VDQMSERV */

int vdqm_AcknCommit(vdqmnw_t *nw) {
    direction_t whereto = SendTo;
    int reqtype = VDQM_COMMIT;
    
    return(vdqm_TransAckn(nw,reqtype,NULL,whereto));
}

int vdqm_RecvAckn(vdqmnw_t *nw) {
    direction_t whereto = ReceiveFrom;
    int reqtype = 0;
    
    return(vdqm_TransAckn(nw,reqtype,NULL,whereto));
}

#if defined(VDQMSERV)
int vdqm_AcknPing(vdqmnw_t *nw,int queuepos) {
    direction_t whereto = SendTo;
    int reqtype = VDQM_PING;
    int data;
    
    if (queuepos >= 0 ) data = queuepos;
    else data = -vdqm_GetError();
    return(vdqm_TransAckn(nw,reqtype,&data,whereto));
}

int vdqm_AcknHangup(vdqmnw_t *nw) {
    direction_t whereto = SendTo;
    int reqtype = VDQM_HANGUP;

    return(vdqm_TransAckn(nw,reqtype,NULL,whereto));
}
#endif /* VDQMSERV */

int vdqm_RecvPingAckn(vdqmnw_t *nw) {
    direction_t whereto = ReceiveFrom;
    int reqtype = 0;
    int data = 0;
    
    reqtype = vdqm_TransAckn(nw,reqtype,&data,whereto);
    
    if ( reqtype != VDQM_PING || data < 0 )  {
        if ( data < 0 ) serrno = -data;
        else serrno = SEINTERNAL;
        return(-1);
    } else return(data);
}

static int vdqm_MarshallRTCPReq(char *buf,
                                vdqmVolReq_t *VolReq,
                                vdqmDrvReq_t *DrvReq,
                                direction_t whereto) {

    char *p;
    int reqtype,magic,len;

    if ( buf == NULL || VolReq == NULL ) return(-1);
    p = buf;
    magic = RTCOPY_MAGIC_OLD0;
    reqtype = VDQM_CLIENTINFO;
    len = 4*LONGSIZE + strlen(VolReq->client_name) + 
        strlen(VolReq->client_host) + strlen(DrvReq->dgn) + 
        strlen(DrvReq->drive) + 4;
    DO_MARSHALL(LONG,p,magic,whereto);
    DO_MARSHALL(LONG,p,reqtype,whereto);
    DO_MARSHALL(LONG,p,len,whereto);
    if ( (magic != RTCOPY_MAGIC && magic != RTCOPY_MAGIC_OLD0) || reqtype != VDQM_CLIENTINFO ) return(-1);
    DO_MARSHALL(LONG,p,VolReq->VolReqID,whereto);
    DO_MARSHALL(LONG,p,VolReq->client_port,whereto);
    DO_MARSHALL(LONG,p,VolReq->clientUID,whereto);
    DO_MARSHALL(LONG,p,VolReq->clientGID,whereto);
    DO_MARSHALL_STRING(p,VolReq->client_host,whereto, sizeof(VolReq->client_host));
    DO_MARSHALL_STRING(p,DrvReq->dgn,whereto, sizeof(VolReq->dgn));
    DO_MARSHALL_STRING(p,DrvReq->drive,whereto, sizeof(VolReq->drive));
    DO_MARSHALL_STRING(p,VolReq->client_name,whereto, sizeof(VolReq->client_name));
    return(len+3*LONGSIZE);
}

static int vdqm_MarshallRTCPAckn(char *buf,
                                 int *status,
                                 int *errmsglen,
                                 char *errmsg,
                                 direction_t whereto) {
    char *p;
    int reqtype,magic,len;
    int rc,msglen;
    if ( buf == NULL ) return(-1);

    rc = (status == NULL ? 0 : *status);
    msglen = (errmsglen == NULL ? 0 : *errmsglen);
    p = buf;
    magic = RTCOPY_MAGIC_OLD0;
    reqtype = VDQM_CLIENTINFO;
    len = LONGSIZE + msglen +1;
    DO_MARSHALL(LONG,p,magic,whereto);
    DO_MARSHALL(LONG,p,reqtype,whereto);
    DO_MARSHALL(LONG,p,len,whereto);
    if ( (magic != RTCOPY_MAGIC && magic != RTCOPY_MAGIC_OLD0) || reqtype != VDQM_CLIENTINFO ) return(-1);

    DO_MARSHALL(LONG,p,rc,whereto);
    if ( status != NULL ) *status = rc;
    if ( errmsglen != NULL && errmsg != NULL) {
        if ( whereto == ReceiveFrom ) {
            msglen = len - LONGSIZE -1;
            msglen = msglen < *errmsglen-1 ? msglen : *errmsglen-1;
            strncpy(errmsg,p,msglen);
            errmsg[msglen] = '\0';
            *errmsglen = msglen;
        } else {
            DO_MARSHALL(STRING,p,errmsg,SendTo);
        }
    }
    return(len+3*LONGSIZE);
}

int vdqm_GetRTCPReq(char *buf,
                    vdqmVolReq_t *VolReq,
                    vdqmDrvReq_t *DrvReq) {
    direction_t whereto = ReceiveFrom;
    return(vdqm_MarshallRTCPReq(buf,VolReq,DrvReq,whereto));
}

int vdqm_SendRTCPAckn(SOCKET connect_socket,
                      int *status,
                      int *errmsglen,
                      char *errmsg) {
    char buf[VDQM_MSGBUFSIZ];
    direction_t whereto = SendTo;
    int rc,len,msglen;

    len = msglen = 0;
    if ( errmsglen != NULL && *errmsglen > 0 && errmsg != NULL ) {
        msglen = (*errmsglen>VDQM_MSGBUFSIZ-4*LONGSIZE-1 ?
            VDQM_MSGBUFSIZ-4*LONGSIZE-1: *errmsglen);
    }

    len = vdqm_MarshallRTCPAckn(buf,status,&msglen,
            errmsg,whereto);

    rc = netwrite_timeout(connect_socket,buf,len,VDQM_TIMEOUT);
    return(rc);
}

#if defined(VDQMSERV)
int vdqm_ConnectToRTCP(SOCKET *connect_socket, char *RTCPserver) {
    SOCKET s;
    char servername[CA_MAXHOSTNAMELEN+1];
    int port;
    struct sockaddr_in sin;
    struct hostent *hp;
#ifdef CSEC
    Csec_context_t sec_ctx;
    char *p;
    int n;
#endif

#if defined(DEBUG)
    log(LOG_DEBUG,"vdqm_ConnectToRTCP(): DEBUG MODE! no action\n");
    return(0);
#endif
    if ( connect_socket == NULL ) return(-1);
    if ( RTCPserver == NULL ) gethostname(servername,CA_MAXHOSTNAMELEN);
    else strcpy(servername,RTCPserver);

    port = vdqm_GetRTCPPort();
    if ( port <= 0 ) {
        log(LOG_ERR,"vdqm_ConnectToRTCP(%s): no RTCOPY port number defined\n",
            servername);
        return(-1);
    }

    s = socket(AF_INET,SOCK_STREAM,0);
    if ( s == INVALID_SOCKET ) {
        log(LOG_ERR,"vdqm_ConnectToRTCP(%s): socket() %s\n",servername,
            neterror());
        return(-1);
    }
    if ( (hp = Cgethostbyname(servername)) == (struct hostent *)NULL ) {
        log(LOG_ERR,"vdqm_ConnectToRTCP(%s): gethostbyname() %s\n",servername,
            neterror());
        return(-1);
    }

    sin.sin_port = htons((short)port);
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

    if ( connect(s, (struct sockaddr *)&sin, sizeof(sin)) == SOCKET_ERROR) {
        log(LOG_ERR,"vdqm_ConnectToRTCP(%s): connect() %s\n",
            servername,neterror());
        closesocket(s);
        return(-1);
    }
#ifdef CSEC

    if (Csec_client_initContext(&sec_ctx, CSEC_SERVICE_TYPE_CENTRAL, NULL) <0) {
      log(LOG_ERR, "vdqm_ConnectToRTCP(%s): Could not init context\n", servername);
      closesocket(s);
      serrno = ESEC_CTX_NOT_INITIALIZED;
      return(-1);
    }
	
    if(Csec_client_establishContext(&sec_ctx, s)< 0) {
      log(LOG_ERR, "vdqm_ConnectToRTCP(%s): Could not establish context\n", servername);
      closesocket(s);
      serrno = ESEC_NO_CONTEXT;
      return(-1);
    }
	
    Csec_clearContext(&sec_ctx);
#endif

    *connect_socket = s;
    return(0);
}
#endif /* VDQMSERV */ 

int vdqm_SendToRTCP(SOCKET connect_socket, vdqmVolReq_t *VolReq,
                    vdqmDrvReq_t *DrvReq) {
    char buf[VDQM_MSGBUFSIZ];
    char errmsg[1024];
    char *p;
    int len,msglen,rc,magic,reqtype,status;

#if defined(DEBUG)
    log(LOG_DEBUG,"vdqm_ConnectToRTCP(): DEBUG MODE! no action\n");
    return(0);
#endif
    if ( VolReq == NULL || DrvReq == NULL ) return(-1);
    p = buf;
    len = vdqm_MarshallRTCPReq(p,VolReq,DrvReq,SendTo);
    rc = netwrite_timeout(connect_socket,buf,len,VDQM_TIMEOUT);
    switch (rc) {
    case -1:
        log(LOG_ERR,"vdqm_SendToRTCP(): netwrite() %s\n",neterror());
        serrno = SECOMERR;
        return(-1);
    case 0:
        log(LOG_ERR,"vdqm_SendToRTCP(): netwrite() connection dropped\n");
        serrno = SECONNDROP;
        return(-1);
    }
 
    rc = netread_timeout(connect_socket,buf,LONGSIZE*3,VDQM_TIMEOUT);
    switch (rc) {
    case -1:
        log(LOG_ERR,"vdqm_SendToRTCP(): netread(HDR) %s\n",neterror());
        serrno = SECOMERR;
        return(-1);
    case 0:
        log(LOG_ERR,"vdqm_SendToRTCP(): netread(HDR) connection dropped\n");
        serrno = SECONNDROP;
        return(-1);
    }
    p = buf;
    unmarshall_LONG(p,magic);
    unmarshall_LONG(p,reqtype);
    unmarshall_LONG(p,len);
    rc = 0;
    if ( len > 0 ) {
        if ( len > VDQM_MSGBUFSIZ - 3*LONGSIZE ) {
            log(LOG_ERR,"vdqm_SendToRTCP() too large errmsg buffer requested %d (%d)\n",
                len,VDQM_MSGBUFSIZ-3*LONGSIZE);
            len = VDQM_MSGBUFSIZ - 3*LONGSIZE;
        }
        rc = netread_timeout(connect_socket,p,len,VDQM_TIMEOUT);
        switch (rc) {
        case -1:
            log(LOG_ERR,"vdqm_SendToRTCP(): netread(REQ) %s\n",neterror());
            serrno = SECOMERR;
            return(-1);
        case 0:
            log(LOG_ERR,"vdqm_SendToRTCP(): netread(REQ) connection dropped\n");
            serrno = SECONNDROP;
            return(-1);
        }
        /*
         * Acknowledge message
         */
        msglen = 1024;
        p = buf;
        *errmsg = '\0';
        status = 0;
        rc = vdqm_MarshallRTCPAckn(p,&status,&msglen,errmsg,ReceiveFrom);
        if ( msglen > 0 ) {
            log(LOG_ERR,"vdqm_SendToRTCP(): rtcopyd returned %d, %s\n",
                status,errmsg);
        }
    }
    return(rc);
}
