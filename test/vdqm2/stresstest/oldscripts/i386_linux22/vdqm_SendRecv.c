/*
 * $Id: vdqm_SendRecv.c,v 1.1 2008/01/31 09:57:44 murrayc3 Exp $
 * $Log: vdqm_SendRecv.c,v $
 * Revision 1.1  2008/01/31 09:57:44  murrayc3
 * Added old vdqm test scripts so they are not lost
 *
 * Revision 1.1  2008/01/31 09:15:55  murrayc3
 * Added old vdqm test scripts so they are not lost
 *
 */

/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * vdqm_SendRecv.c - Send and receive VDQM request and acknowledge messages.
 */

#include <stdlib.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <errno.h>

#include <Castor_limits.h>
#include <rtcopy.h>                     /* Definition of RTCOPY_MAGIC   */
#include <net.h>
#include <log.h>
#include <osdep.h>
#include <marshall.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <serrno.h>

#if !defined(linux)
extern char *sys_errlist[];
#else /* linux */
#include <stdio.h>   /* Contains definition of sys_errlist[] */
#endif /* linux */


#define REQTYPE(Y,X) ( X == VDQM_##Y##_REQ || \
    X == VDQM_DEL_##Y##REQ || \
    X == VDQM_GET_##Y##QUEUE || (!strcmp(#Y,"VOL") && X == VDQM_PING) )
#define ADMINREQ(X) ( X == VDQM_HOLD || X == VDQM_RELEASE || \
    X == VDQM_SHUTDOWN )

typedef enum direction {SendTo,ReceiveFrom} direction_t;

#define DO_MARSHALL(X,Y,Z,W) { \
    if ( W == SendTo ) {marshall_##X(Y,Z);} \
    else {unmarshall_##X(Y,Z);} }

static int vdqm_Transfer(vdqmnw_t *nw,
                         vdqmHdr_t *hdr,
                         vdqmVolReq_t *volreq,
                         vdqmDrvReq_t *drvreq,
                         direction_t whereto) {
    
    char hdrbuf[VDQM_HDRBUFSIZ], buf[VDQM_MSGBUFSIZ];
    char servername[CA_MAXHOSTNAMELEN+1];
    char *p;
    int magic,reqtype,len;
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
    if ( (s = nw->accept_socket) == INVALID_SOCKET ) {
        rc = gethostname(servername,CA_MAXHOSTNAMELEN);
        s = nw->connect_socket;
    }
    
    if ( whereto == ReceiveFrom ) {
        rc = netread(s,hdrbuf,VDQM_HDRBUFSIZ);
        switch (rc) {
        case -1: 
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer() netread(HDR): %s\n",
                NWERRTXT);
#endif /* VDQMSERV */
            return(-1);
        case 0:
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer() netread(HDR): connection dropped\n");
#endif /* VDQMSERV */
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
        if ( len > 0 ) {
            rc = netread(s,buf,len);
            switch (rc) {
            case -1:
#if defined(VDQMSERV)
                log(LOG_ERR,"vdqm_Transfer() netread(REQ): %s\n",
                    NWERRTXT);
#endif /* VDQMSERV */
                return(-1);
            case 0:
#if defined(VDQMSERV)
                log(LOG_ERR,"vdqm_Transfer() netread(REQ): connection dropped\n");
#endif /* VDQMSERV */
                return(-1);
            }
        }
        
        if ( (REQTYPE(VOL,reqtype) && volreq == NULL) ||
             (REQTYPE(DRV,reqtype) && drvreq == NULL) ) {
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer(): no buffer for reqtype=0x%x\n",
                reqtype);
#endif /* VDQMSERV */
            serrno = EINVAL;
            return(-1);
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
        DO_MARSHALL(LONG,p,volreq->recvtime,whereto);
        DO_MARSHALL(STRING,p,volreq->client_host,whereto);
        DO_MARSHALL(STRING,p,volreq->volid,whereto);
        DO_MARSHALL(STRING,p,volreq->server,whereto);
        DO_MARSHALL(STRING,p,volreq->drive,whereto);
        DO_MARSHALL(STRING,p,volreq->dgn,whereto);
    }
    if ( REQTYPE(DRV,reqtype) && drvreq != NULL ) {
        DO_MARSHALL(LONG,p,drvreq->status,whereto);
        DO_MARSHALL(LONG,p,drvreq->DrvReqID,whereto);
        DO_MARSHALL(LONG,p,drvreq->VolReqID,whereto);
        DO_MARSHALL(LONG,p,drvreq->jobID,whereto);
        DO_MARSHALL(LONG,p,drvreq->recvtime,whereto);
        DO_MARSHALL(STRING,p,drvreq->volid,whereto);
        DO_MARSHALL(STRING,p,drvreq->server,whereto);
        DO_MARSHALL(STRING,p,drvreq->drive,whereto);
        DO_MARSHALL(STRING,p,drvreq->dgn,whereto);
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
        rc = netwrite(s,hdrbuf,VDQM_HDRBUFSIZ);
        switch (rc) {
        case -1:
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer() netwrite(HDR): %s\n",
                NWERRTXT);
#endif /* VDQMSERV */
            return(-1);
        case 0:
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_Transfer() netwrite(HDR): connection dropped\n");
#endif /*VDQMSERV */
            return(-1);
        }
        if ( len > 0 ) {
            rc = netwrite(s,buf,len);
            switch (rc) {
            case -1:
#if defined(VDQMSERV)
                log(LOG_ERR,"vdqm_Transfer() netwrite(REQ): %s\n",
                    NWERRTXT);
#endif /* VDQMSERV */
                return(-1);
            case 0:
#if defined(VDQMSERV)
                log(LOG_ERR,"vdqm_Transfer() netwrite(REQ): connection dropped\n");
#endif /*VDQMSERV */
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
    return(vdqm_Transfer(nw,hdr,volreq,NULL,whereto));
}

static int vdqm_TransAckn(vdqmnw_t *nw, int reqtype, int *data, 
                          direction_t whereto) {
    char hdrbuf[VDQM_HDRBUFSIZ];
    int magic, len, rc;
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
    if ( data != NULL ) len = *data;
    
    if ( whereto == ReceiveFrom ) {
        rc = netread(s,hdrbuf,VDQM_HDRBUFSIZ);
        switch (rc) {
        case -1: 
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_TransAckn() netread(HDR): %s\n",
                NWERRTXT);
#endif /* VDQMSERV */
            return(-1);
        case 0:
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_TransAckn() netread(HDR): connection dropped\n");
#endif /* VDQMSERV */
            return(-1);
        }
    }
    p = hdrbuf;
    DO_MARSHALL(LONG,p,magic,whereto);
    DO_MARSHALL(LONG,p,reqtype,whereto);
    DO_MARSHALL(LONG,p,len,whereto);
    
    if ( whereto == SendTo ) {
        magic = VDQM_MAGIC;
        len = 0;
        p = hdrbuf;
        rc = netwrite(s,hdrbuf,VDQM_HDRBUFSIZ);
        switch (rc) {
        case -1: 
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_TransAckn() netwrite(HDR): %s\n",
                NWERRTXT);
#endif /* VDQMSERV */
            return(-1);
        case 0:
#if defined(VDQMSERV)
            log(LOG_ERR,"vdqm_TransAckn() netwrite(HDR): connection dropped\n");
#endif /* VDQMSERV */
            return(-1);
        }
    }
    
    if ( data != NULL ) *data = len;
    return(reqtype);
}

int vdqm_AcknRollback(vdqmnw_t *nw) {
    direction_t whereto = SendTo;
    int reqtype = VDQM_ROLLBACK;
    
    return(vdqm_TransAckn(nw,reqtype,NULL,whereto));
}

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

int vdqm_AcknPing(vdqmnw_t *nw,int queuepos) {
    direction_t whereto = SendTo;
    int reqtype = VDQM_PING;
    int data;
    
    data = queuepos;
    return(vdqm_TransAckn(nw,reqtype,&data,whereto));
}

int vdqm_RecvPingAckn(vdqmnw_t *nw) {
    direction_t whereto = ReceiveFrom;
    int reqtype = 0;
    int data = 0;
    
    reqtype = vdqm_TransAckn(nw,reqtype,&data,whereto);
    
    if ( reqtype != VDQM_PING || data == -1 ) return(-1);
    else return(data);
}

static int vdqm_MarshallRTCPReq(char *buf,
                                vdqmVolReq_t *VolReq,
                                vdqmDrvReq_t *DrvReq,
                                direction_t whereto) {

    char *p;
    int reqtype,magic,len;

    if ( buf == NULL || VolReq == NULL ) return(-1);
    p = buf;
    magic = RTCOPY_MAGIC;
    reqtype = VDQM_CLIENTINFO;
    len = 2*LONGSIZE + strlen(VolReq->client_host) + 
        strlen(DrvReq->dgn) + strlen(DrvReq->drive) + 3;
    DO_MARSHALL(LONG,p,magic,whereto);
    DO_MARSHALL(LONG,p,reqtype,whereto);
    DO_MARSHALL(LONG,p,len,whereto);
    if ( magic != RTCOPY_MAGIC || reqtype != VDQM_CLIENTINFO ) return(-1);
    DO_MARSHALL(LONG,p,VolReq->VolReqID,whereto);
    DO_MARSHALL(LONG,p,VolReq->client_port,whereto);
    DO_MARSHALL(STRING,p,VolReq->client_host,whereto);
    DO_MARSHALL(STRING,p,DrvReq->dgn,whereto);
    DO_MARSHALL(STRING,p,DrvReq->drive,whereto);
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
    magic = RTCOPY_MAGIC;
    reqtype = VDQM_CLIENTINFO;
    len = LONGSIZE + msglen +1;
    DO_MARSHALL(LONG,p,magic,whereto);
    DO_MARSHALL(LONG,p,reqtype,whereto);
    DO_MARSHALL(LONG,p,len,whereto);
    if ( magic != RTCOPY_MAGIC || reqtype != VDQM_CLIENTINFO ) return(-1);

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

    rc = netwrite(connect_socket,buf,len);
    return(rc);
}

#if defined(VDQMSERV)
int vdqm_ConnectToRTCP(SOCKET *connect_socket, char *RTCPserver) {
    SOCKET s;
    char servername[CA_MAXHOSTNAMELEN+1];
    int port;
    struct sockaddr_in sin;
    struct hostent *hp;

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
            NWERRTXT);
        return(-1);
    }
    if ( (hp = gethostbyname(servername)) == (struct hostent *)NULL ) {
        log(LOG_ERR,"vdqm_ConnectToRTCP(%s): gethostbyname() %s\n",servername,
            NWERRTXT);
        return(-1);
    }

    sin.sin_port = htons((short)port);
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

    if ( connect(s, (struct sockaddr *)&sin, sizeof(sin)) == SOCKET_ERROR) {
        log(LOG_ERR,"vdqm_ConnectToRTCP(%s): connect() %s\n",
            servername,NWERRTXT);
        closesocket(s);
        return(-1);
    }
    
    *connect_socket = s;
    return(0);
}

int vdqm_SendToRTCP(SOCKET connect_socket, vdqmVolReq_t *VolReq,
                    vdqmDrvReq_t *DrvReq) {
    char buf[VDQM_MSGBUFSIZ];
    char errmsg[1024];
    char *p;
    int len,msglen,rc,magic,reqtype,status;

    if ( VolReq == NULL || DrvReq == NULL ) return(-1);
    p = buf;
    len = vdqm_MarshallRTCPReq(p,VolReq,DrvReq,SendTo);
    rc = netwrite(connect_socket,buf,len );
    switch (rc) {
    case -1:
        log(LOG_ERR,"vdqm_SendToRTCP(): netwrite() %s\n",NWERRTXT);
        return(-1);
    case 0:
        log(LOG_ERR,"vdqm_SendToRTCP(): netwrite() connection dropped\n");
        return(-1);
    }
 
    rc = netread(connect_socket,buf,LONGSIZE*3);
    switch (rc) {
    case -1:
        log(LOG_ERR,"vdqm_SendToRTCP(): netread(HDR) %s\n",NWERRTXT);
        return(-1);
    case 0:
        log(LOG_ERR,"vdqm_SendToRTCP(): netread(HDR) connection dropped\n");
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
        rc = netread(connect_socket,p,len);
        switch (rc) {
        case -1:
            log(LOG_ERR,"vdqm_SendToRTCP(): netread(REQ) %s\n",NWERRTXT);
            return(-1);
        case 0:
            log(LOG_ERR,"vdqm_SendToRTCP(): netread(REQ) connection dropped\n");
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
#endif /* VDQMSERV */
