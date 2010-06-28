#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <errno.h>
#include <Castor_limits.h>
#include <rtcp_constants.h>             /* Definition of RTCOPY_MAGIC   */
#include <net.h>
#include <log.h>
#include <osdep.h>
#include <marshall.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <vdqm_api.h>
#define CTHREAD
#include <Cthread_api.h>

#include <stdio.h> 
#define RTCPD_LOGFILE "rtcpd.log"
#define RTCPD_PORT 8889
#if defined(SOMAXCONN)
#define RTCPD_BACKLOG SOMAXCONN
#else /* SOMAXCONN */
#define RTCPD_BACKLOG 5
#endif /* SOMAXCONN */

#define NWERRTXT strerror(errno)
#define Sleep(X) sleep(X/1000)

#define ERRTXT strerror(errno)

int rtcpd_InitNW(SOCKET *s) {
    struct sockaddr_in sin;
    extern char * getenv();
    char *p;
    int port, rc;

    if ( s == NULL ) return(-1);

    if ( (p = getenv("RTCOPY_PORT")) != (char *)NULL ) {
        port = atoi(p);
    } else {
        port = RTCPD_PORT;
    }
    log(LOG_INFO,"rtcp_InitNW() rtcopy port set to %d\n",port);

    *s = socket(AF_INET,SOCK_STREAM,0);
    if ( *s == INVALID_SOCKET ) {
        log(LOG_ERR,"rtcp_InitNW() socket(): %s\n",NWERRTXT);
        return(-1);
    }

    rc = 1;
    if ( setsockopt(*s,SOL_SOCKET,SO_REUSEADDR,(char *)&rc,sizeof(rc)) == 
        SOCKET_ERROR ) {
        log(LOG_ERR,"rtcp_InitNW() setsockopt(): %s\n",NWERRTXT);
    }

    (void)memset(&sin,'\0',sizeof(sin));
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    sin.sin_family = AF_INET;
    sin.sin_port = htons((u_short)port);

    rc = bind(*s,(struct sockaddr *)&sin,sizeof(sin));
    if ( rc == SOCKET_ERROR ) {
        (void)log(LOG_ERR,"rtcp_InitNW() bind(): %s\n",NWERRTXT);
        closesocket(*s);
        return(-1);
    }

    rc = listen(*s,RTCPD_BACKLOG);
    if ( rc == SOCKET_ERROR ) {
        (void)log(LOG_ERR,"rtcp_InitNW() listen(): %s\n",NWERRTXT);
        shutdown(*s,SD_BOTH);
        closesocket(*s);
        return(-1);
    }

    return(0);
}

int rtcpd_Listen(SOCKET s, SOCKET *ns) {
    fd_set rfds, rfds_copy;
    struct sockaddr_in from;
    int fromlen, maxfd, rc;

    if ( s == INVALID_SOCKET || ns == NULL ) return(-1);
    FD_ZERO(&rfds);
    FD_SET(s,&rfds);
    maxfd =0;

    for (;;) {
        *ns = INVALID_SOCKET;
        maxfd = s + 1;
        rfds_copy = rfds;
        if ( (rc = select(maxfd,&rfds_copy,NULL,NULL,NULL)) > 0 ) {
            if ( FD_ISSET(s,&rfds_copy) ) {
                fromlen = sizeof(from);
                *ns = accept(s,(struct sockaddr *)&from,&fromlen);
                if ( *ns == INVALID_SOCKET ) {
                    (void)log(LOG_ERR,"rtcpd_Listen() accpet(): %s\n",
                        NWERRTXT);
                    if ( errno != EINTR ) {
                        return(-1);
                    } else {
                        continue;
                    }
                }
            }
            break;
        } else {
            (void)log(LOG_ERR,"rtcpd_Listen() select(): %s\n",NWERRTXT);
            if ( errno != EINTR ) {
                return(-1);
            } else {
                continue;
            }
        }
        break;
    }
    return(0);
}

int rtcpd_CloseConn(SOCKET *s) {
    int rc;
    
    if ( s == NULL || *s == INVALID_SOCKET ) return(-1);

    rc = shutdown(*s,SD_BOTH);
    if ( rc == SOCKET_ERROR ) {
        (void)log(LOG_ERR,"rtcpd_CloseConn() shutdown(): %s\n",
            NWERRTXT);
    }
    rc = closesocket(*s);
    if ( rc == SOCKET_ERROR ) {
        (void)log(LOG_ERR,"rtcpd_CloseConn() closesocket(): %s\n",
            NWERRTXT);
    }
    *s = INVALID_SOCKET;
    return(rc);
}

int rtcpd_GetClientAddr(SOCKET s,
                        char *clienthost,
                        int *clientport,
                        int *VolReqID,
                        char *dgn,
                        char *tapeunit) {
    char buf[1024],name[80],*p;
    int magic,reqtype,len,rc, uid, gid;

    log(LOG_INFO,"rtcpd_GetClientAddr() called\n");
    if ( s == INVALID_SOCKET || clienthost == NULL ||
        clientport == NULL || tapeunit == NULL ) return(-1);
    /*
     * Read the header
     */
    rc = netread(s,buf,3*LONGSIZE);
    switch (rc) {
    case -1:
        log(LOG_ERR,"rtcpd_GetClientAddr() netread(HDR): %s\n",
                NWERRTXT);
        return(-1);
    case 0:
        log(LOG_ERR,"rtcpd_GetClientAddr() netread(HDR): connection dropped\n");    
        return(-1);
    }
    p = buf;
    unmarshall_LONG(p,magic);
    unmarshall_LONG(p,reqtype);
    unmarshall_LONG(p,len);
    log(LOG_INFO,"rtcpd_GetClientAddr() reading %d bytes\n",len);
    if (len > 0) {
        rc = netread(s,p,len);
        switch (rc) {
        case -1:
            log(LOG_ERR,"rtcpd_GetClientAddr() netread(HDR): %s\n",
                    NWERRTXT);
            return(-1);
        case 0:
            log(LOG_ERR,"rtcpd_GetClientAddr() netread(HDR): connection dropped\n");    
            return(-1);
        }
    }
    rc = vdqm_GetClientAddr(buf,clienthost,clientport,VolReqID,
                            &uid,&gid,name,dgn,tapeunit);
    if ( rc == -1 ) {
        log(LOG_ERR,"rtcpd_GetClientAddr() vdqm_GetClientAddr() returned error\n");
    }
    log(LOG_INFO,"rtcpd_GetClientAddr(): host=%s, port=%d, VolReqID=%d, dgn=%s, drive=%s\n",
        clienthost,*clientport,*VolReqID,dgn,tapeunit);
    log(LOG_INFO,"rtcpd_GetClientAddr(): uid=%d, gid=%d, name=%s\n",
        uid,gid,name);
    return(rc);
}

int rtcpd_ConnectToClient(SOCKET *connect_socket,
                          char *client_host,
                          int *client_port) {
	struct hostent *hp = NULL;
	struct sockaddr_in sin ; /* Internet address */

    if ( connect_socket == NULL || client_host == NULL || 
        client_port == NULL ) return(-1);
	*connect_socket = socket(AF_INET,SOCK_STREAM,0);
	if ( *connect_socket == INVALID_SOCKET ) {
		log(LOG_ERR,"rtcpd_ConnectToClient() socket(): %s\n",NWERRTXT);
		return(-1);
	}
	if ( (hp = gethostbyname(client_host)) == NULL ) {
		log(LOG_ERR,"rtcpd_ConnectToClient(): gethostbyname(%s) %s\n",
			client_host,NWERRTXT);
        closesocket(*connect_socket);
		return(-1);
	}
	sin.sin_port = htons((short)*client_port);
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

	if ( connect(*connect_socket,(struct sockaddr *)&sin, sizeof(struct sockaddr_in)) ==
		SOCKET_ERROR) {
		log(LOG_ERR,"rtcpd_ConnectToClient() connect(): %s\n",NWERRTXT);
        closesocket(*connect_socket);
		return(-1);
	}
    return(0);
}

int rtcpd_GetClientReq(SOCKET connect_socket,
                       int *rtcp_reqtype,
                       char *buf) {
    int rc,magic,reqtype,len;
    char hdrbuf[3*LONGSIZE],*p;

    log(LOG_DEBUG,"\trtcpd_GetClientReq() called\n");
    if ( connect_socket == INVALID_SOCKET || buf == NULL ||
        rtcp_reqtype == NULL ) return(-1);
    *rtcp_reqtype = 0;
    /*
     * Read the header
     */
    rc = netread(connect_socket,hdrbuf,3*LONGSIZE);
    switch (rc) {
    case -1: 
        log(LOG_ERR,"rtcpd_GetClientReq() netread(HDR): %s\n", 
            NWERRTXT);
        return(-1);
    case 0:
        log(LOG_ERR,"rtcpd_GetClientReq() netread(HDR): connection dropped\n");
        return(-1);
    }
    p = hdrbuf;
    unmarshall_LONG(p,magic);
    unmarshall_LONG(p,reqtype);
    unmarshall_LONG(p,len);
    if ( magic != RTCOPY_MAGIC ) return(-1);
    *rtcp_reqtype = reqtype;
    log(LOG_DEBUG,"\trtcpd_GetClientReq() receive req body size=%d\n",
        len);
    rc = netread(connect_socket,buf,len);
    switch (rc) {
    case -1: 
        log(LOG_ERR,"rtcpd_GetClientReq() netread(REQ): %s\n", 
            NWERRTXT);
        return(-1);
    case 0:
        log(LOG_ERR,"rtcpd_GetClientReq() netread(REQ): connection dropped\n");    
        return(-1);
    }
    /*
     * Acknowledge
     */
    len = 0;
    p = hdrbuf;
    marshall_LONG(p,magic);
    marshall_LONG(p,reqtype);
    marshall_LONG(p,len);
    log(LOG_DEBUG,"\trtcpd_GetClientReq() send ackn.\n");
    rc = netwrite(connect_socket,hdrbuf,3*LONGSIZE);
    switch (rc) {
    case -1:
        log(LOG_ERR,"rtcpd_GetClientReq() netread(REQ): %s\n",
            NWERRTXT);
        return(-1);
    case 0:
        log(LOG_ERR,"rtcpd_GetClientReq() netread(REQ): connection dropped\n");
        return(-1);
    }
    log(LOG_DEBUG,"\rtcpd_GetClientReq() exit\n");
    return(0);
}

int rtcpd_init() {
    initlog("rtcpd",LOG_DEBUG,RTCPD_LOGFILE);
    return(0);
}

int rtcpd_exit(SOCKET s1, SOCKET s2, char *msg) {
    int rc;
    if ( s1 != INVALID_SOCKET ) {
        shutdown(s1,SD_BOTH);
        closesocket(s1);
    }
    if ( s2 != INVALID_SOCKET ) {
        shutdown(s2,SD_BOTH);
        closesocket(s2);
    }
    log(LOG_ERR,"rtcpd_exit() %s\n",msg);
    exit(0);
    return(0);
}

int rtcpd_CtapeInit(char *dgn, char *unit) {
    int rc, status, value;

    status = VDQM_UNIT_UP | VDQM_UNIT_FREE;
    value = 0;
    log(LOG_INFO,"rtcpd_CtapeInit() configuring %s UP|FREE for DGN %s\n",
        unit,dgn);
    rc = vdqm_UnitStatus(NULL,NULL,dgn,NULL,unit,&status,&value,0);
    log(LOG_INFO,"rtcpd_CtapeInit() vdqm_UnitStatus() returned %d, value=%d\n",
        rc,value);
    if ( rc == -1 ) return(-1);
    else return(value);
}
int rtcpd_CtapeAssign(char *dgn, char *drive, int VolReqID, int *jobID) {
    int rc, status, value;

    if ( dgn == NULL || drive == NULL || jobID == NULL ) return(-1);
    log(LOG_INFO,"rtcpd_CtapeAssign(%s,%s,%d) called\n",
        dgn,drive,VolReqID);
    /* 
     * Assign the drive
     */
    status = VDQM_UNIT_ASSIGN;
    value = VolReqID;
    *jobID = getpid();
    rc = vdqm_UnitStatus(NULL,NULL,dgn,NULL,drive,&status,&value,*jobID);
    log(LOG_INFO,"rtcpd_CtapeAssign() vdqm_UnitStatus(ASSIGN) returned %d, value=%d\n",
        rc,value);
    if ( rc == -1 ) return(-1);
    else return(value);
}
int rtcpd_CtapeMount(char *dgn, char *drive, char *vid, int jobID) {
    int rc, status,value;

    /*
     * Mount the tape
     */
    status = VDQM_VOL_MOUNT;
    value = 0;
    Sleep(5000);
    rc = vdqm_UnitStatus(NULL,vid,dgn,NULL,drive,&status,&value,jobID);
    log(LOG_INFO,"rtcpd_CtapeMount() vdqm_UnitStatus(MOUNT) returned %d, value=%d\n",
        rc,value);

    if ( rc == -1 ) return(-1);
    else return(value);
}

int rtcpd_CtapeRelease(char *dgn, char *drive, int jobID) {
    int rc, status, value;
    char vid[32];

    if ( dgn == NULL || drive == NULL ) return(-1);
    status = VDQM_UNIT_RELEASE;
    value = 0;
    *vid = '\0';
    rc = vdqm_UnitStatus(NULL,vid,dgn,NULL,drive,&status,&value,jobID);
    log(LOG_INFO,"rtcpd_CtapeRelease() vdqm_UnitStatus(RELEASE) returned %d, value=%d\n",
        rc,value);
    if ( rc == -1 ) return(-1);

    if ( status & VDQM_VOL_UNMOUNT ) {
        log(LOG_INFO,"rtcpd_CtapeRelease() unmount volid %s\n",vid);
        Sleep(5000);
        rc = vdqm_UnitStatus(NULL,vid,dgn,NULL,drive,&status,NULL,jobID);
        log(LOG_INFO,"rtcpd_CtapeRelease() vdqm_UnitStatus(UNMOUNT) returned %d, value=%d\n",
            rc,value);
        if ( rc == -1 ) return(-1);

        log(LOG_INFO,"rtcpd_CtapeRelease() set drive FREE\n");
        status = VDQM_UNIT_FREE | VDQM_UNIT_UP;
        rc = vdqm_UnitStatus(NULL,NULL,dgn,NULL,drive,&status,&value,0);
        log(LOG_INFO,"rtcpd_CtapeRelease() vdqm_UnitStatus(FREE|UP) returned %d, value=%d\n",
            rc,value);
    }
    if ( rc == -1 ) return(-1);
    else return(value);
}
const int failure = -1;
const int success = 0;
static int started;
void *rtcpd_worker(void *arg) {
    char rtcpmsg[1024],clienthost[32],tapeunit[32],dgn[32],vid[32], *p;
    int reqtype,rc,clrc,clientport,jobID,vdqm_VolReqID,VolReqID;
    SOCKET vdqm_socket, client_socket;

    if ( arg == NULL ) return((void *)&failure);
    vdqm_socket = *(SOCKET *)arg;
    free(arg);
    if ( vdqm_socket == INVALID_SOCKET ) return((void *)&failure);
    Cthread_cond_broadcast(&started);
    clientport = -1;
    *clienthost = *tapeunit = '\0';
    rc = rtcpd_GetClientAddr(vdqm_socket,
        clienthost,&clientport,&vdqm_VolReqID,dgn,tapeunit);
    if ( rc == -1 ) {
        log(LOG_ERR,"main(): rtcpd_GetClientAddr() returned error\n");
        return((void *)&failure);
    }

    rc = vdqm_AcknClientAddr(vdqm_socket,rc,0,NULL);
    if ( rc == -1 ) {
        log(LOG_ERR,"main(): vdqm_AcknClientAddr() returned error\n");
        return((void *)&failure);
    }
    rtcpd_CloseConn(&vdqm_socket);

    client_socket = INVALID_SOCKET;
    rc = rtcpd_ConnectToClient(&client_socket,clienthost,
        &clientport);
    if ( rc == -1 ) {
        log(LOG_ERR,"main(): rtcpd_ConnectToClient(%s,%d) returned error\n",
            clienthost,clientport);
        return((void *)&failure);
    }
    clrc = rtcpd_GetClientReq(client_socket,&reqtype,rtcpmsg);
    if ( rc == -1 ) {
        log(LOG_ERR,"main(): rtcpd_GetClientReq() returned error\n");
    }
    log(LOG_INFO,"main(): tapeunit = %s, received client messages: %s\n",
        tapeunit,rtcpmsg);
    rtcpd_CloseConn(&client_socket);

    VolReqID = -1;
    p = strtok(rtcpmsg," ");
    if ( p != NULL ) VolReqID = atoi(p);
    p = strtok(NULL," ");
    if ( p != NULL ) strcpy(vid,p);
    log(LOG_INFO,"worker(): VolReqID=%d, vid=%s\n",VolReqID,vid);
    if ( vdqm_VolReqID != VolReqID ) {
        log(LOG_ERR,"worker(): wrong VolReqID! VDQM says %d while client claims %d\n",
            vdqm_VolReqID,VolReqID);
        exit(1);
    }

    rc = rtcpd_CtapeAssign(dgn,tapeunit,VolReqID,&jobID);
    if ( rc == -1 || jobID == 0) {
        log(LOG_ERR,"main(): rtcpd_CtapeAssign() returned error\n");
        return((void *)&failure);
    }

    if ( clrc != -1 ) {
        for (;;) {
            rc = rtcpd_CtapeMount(dgn,tapeunit,vid,jobID);
            if ( rc == -1 ) {
                log(LOG_ERR,"rtcpd_CtapeMount(%s,%s,%s,%d) returned error\n",
                    dgn,tapeunit,vid,jobID);
                break;
            }
            Sleep(10000);
            rc = rtcpd_CtapeRelease(dgn,tapeunit,jobID);
            if ( rc == -1 ) {
                log(LOG_ERR,"rtcpd_CtapeRelease(%s,%s,%s,%d) returned error\n",
                    dgn,tapeunit,vid,jobID);
                break;
            }
            break;
        }
    }
    return((void *)&success);
}

#define RTCPD_EXIT(X) rtcpd_exit(listen_socket,vdqm_socket,X)
int main(int argc, char *argv[]) {
    char tapeunits[10][32],dgn[32];
    int rc,i;
    SOCKET listen_socket,vdqm_socket,*arg;

    rc = rtcpd_init();

    /*
     * Must initialize listen socket before configuring
     * drives.
     */
    listen_socket = vdqm_socket = INVALID_SOCKET;
    rc = rtcpd_InitNW(&listen_socket);
    if ( rc == -1 || listen_socket == INVALID_SOCKET ) 
        RTCPD_EXIT("rtcpd_InitNW() error");

    if ( argc > 1 ) strcpy(dgn,argv[1]);
    else strcpy(dgn,"DLT2");
    if ( argc > 2 ) {
        for ( i=2; i<argc; i++)  {
            strcpy(tapeunits[i],argv[i]);
            rc = rtcpd_CtapeInit(dgn,tapeunits[i]);
            if ( rc == -1 ) break;
        }
    } else {
        strcpy(tapeunits[0],"dlr01");
        rc = rtcpd_CtapeInit(dgn,tapeunits[0]);
    }
    if ( rc == -1 ) RTCPD_EXIT("could not initialise drives");


    for (;;) {
        vdqm_socket = INVALID_SOCKET;
        rc = rtcpd_Listen(listen_socket,&vdqm_socket);
        if ( rc == -1 || vdqm_socket == INVALID_SOCKET ) {
            log(LOG_ERR,"main(): rtcpd_Listen() returned error\n");
            break;
        }
        arg = (SOCKET *)malloc(sizeof(SOCKET));
        if ( arg == NULL ) {
            log(LOG_ERR,"main() malloc(): %s\n",ERRTXT);
            break;
        }
        *arg = vdqm_socket;
        Cthread_create_detached(rtcpd_worker,(void *)arg);
        Cthread_cond_wait(&started);
    }
    RTCPD_EXIT("End of rtcpd");
    return(0);
}
