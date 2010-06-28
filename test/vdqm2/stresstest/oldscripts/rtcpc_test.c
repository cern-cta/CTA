#include <stdlib.h>
#include <stdio.h>
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
#include <osdep.h>
#include <marshall.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <vdqm_api.h>

#if !defined(linux)
extern char *sys_errlist[];
#endif /* linux */

#define NWERRTXT neterror()

#if defined(SOMAXCONN)
#define RTCPC_BACKLOG SOMAXCONN
#else /* SOMAXCONN */
#define RTCPC_BACKLOG 5
#endif /* SOMAXCONN */

int rtcpc_InitNW(SOCKET *s, int *myport) {
    struct sockaddr_in sin;
    extern char * getenv();
    int rc, len;

    if ( s == NULL || myport == NULL ) return(-1);

    *s = socket(AF_INET,SOCK_STREAM,0);
    if ( *s == INVALID_SOCKET ) {
        fprintf(stderr,"rtcp_InitNW() socket(): %s\n",NWERRTXT);
        return(-1);
    }

    (void)memset(&sin,'\0',sizeof(sin));
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    sin.sin_family = AF_INET;
    sin.sin_port = 0;

    rc = bind(*s,(struct sockaddr *)&sin,sizeof(sin));
    if ( rc == SOCKET_ERROR ) {
        (void)fprintf(stderr,"rtcp_InitNW() bind(): %s\n",NWERRTXT);
        closesocket(*s);
        return(-1);
    }

    len = sizeof(sin);
    if ( getsockname(*s,(struct sockaddr *)&sin,&len) == SOCKET_ERROR ) {
        fprintf(stderr,"rtcpc_InitNW() getsockname(): %s\n",NWERRTXT);
        closesocket(*s);
        return(-1);
    }
    *myport = ntohs(sin.sin_port);

    rc = listen(*s,RTCPC_BACKLOG);
    if ( rc == SOCKET_ERROR ) {
        (void)fprintf(stderr,"rtcp_InitNW() listen(): %s\n",NWERRTXT);
        shutdown(*s,SD_BOTH);
        closesocket(*s);
        return(-1);
    }

    return(0);
}

int rtcpc_Listen(SOCKET s, SOCKET *ns) {
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
                    (void)fprintf(stderr,"rtcpd_Listen() accpet(): %s\n",
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
            (void)fprintf(stderr,"rtcpd_Listen() select(): %s\n",NWERRTXT);
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

int rtcpc_CloseConn(SOCKET *s) {
    int rc;
    
    if ( s == NULL || *s == INVALID_SOCKET ) return(-1);

    rc = shutdown(*s,SD_BOTH);
    if ( rc == SOCKET_ERROR ) {
        fprintf(stderr,"rtcpc_CloseConn() shutdown(): %s\n",
            NWERRTXT);
    }
    rc = closesocket(*s);
    if ( rc == SOCKET_ERROR ) {
        fprintf(stderr,"rtcpc_CloseConn() closesocket(): %s\n",
            NWERRTXT);
    }
    *s = INVALID_SOCKET;
    return(rc);
}

int rtcpc_SendReq(SOCKET accept_socket, int *reqtype, char *msg) {
    char buf[3*LONGSIZE+1024], *p;
    int magic, l_reqtype, len,rc;

    fprintf(stderr,"\trtcpc_SendReq() called\n");
    if ( accept_socket == INVALID_SOCKET || reqtype == NULL || 
        msg == NULL ) return(-1);

    magic = RTCOPY_MAGIC;
    len = strlen(msg) + 1;
    l_reqtype = *reqtype;
    p = buf;
    marshall_LONG(p,magic);
    marshall_LONG(p,l_reqtype);
    marshall_LONG(p,len);
    marshall_STRING(p,msg);
    fprintf(stderr,"\trtcpc_SendReq() write %d + %d = %dbytes\n",
        3*LONGSIZE,len,3*LONGSIZE+len);
    rc = netwrite(accept_socket,buf,3*LONGSIZE+len);
    switch (rc) {
    case -1:
        fprintf(stderr,"rtcpc_SendReq() netread(HDR+REQ): %s\n",
            NWERRTXT);
        return(-1);
    case 0:
        fprintf(stderr,"rtcpc_SendReq() netread(HDR+REQ): connection dropped\n");
        return(-1);
    }
 
    /*
     * Receive ackn.
     */
    fprintf(stderr,"\trtcpc_SendReq() receive ackn.\n");
    rc = netread(accept_socket,buf,3*LONGSIZE);
    switch (rc) {
    case -1: 
        fprintf(stderr,"rtcpc_SendReq() netread(ACKN): %s\n", 
            NWERRTXT);
        return(-1);
    case 0:
        fprintf(stderr,"rtcpc_SendReq() netread(ACKN): connection dropped\n");
        return(-1);
    }
    p = buf;
    unmarshall_LONG(p,magic);
    unmarshall_LONG(p,l_reqtype);
    unmarshall_LONG(p,len);
    if ( magic != RTCOPY_MAGIC ) return(-1);
    *reqtype = l_reqtype;
    fprintf(stderr,"\trtcpc_SendReq() exit\n");
    return(0);
}

int rtcpc_exit(SOCKET s1, SOCKET s2, int status) {
    int rc;
    if ( s1 != INVALID_SOCKET ) {
        shutdown(s1,SD_BOTH);
        closesocket(s1);
    }
    if ( s2 != INVALID_SOCKET ) {
        shutdown(s2,SD_BOTH);
        closesocket(s2);
    }
    fprintf(stderr,"rtcpc_exit() exit status %d\n",status);
    exit(status);
    return(0);
}

int main(int argc, char *argv[]) {
    char *dgn, *vid, *drive, *server;
    char msg[1024];
    int reqtype,reqID,rc,port;
    SOCKET listen_socket, accept_socket;

    if ( argc < 6 ) {
        fprintf(stderr,"Usage: %s vid dgn drive server reqtype\n",argv[0]);
        return(2);
    }

    vid = argv[1];
    dgn = argv[2];
    drive = argv[3];
    if ( *drive == '\0' ) drive = NULL;
    server = argv[4];
    if ( *server == '\0' ) server = NULL;
    reqtype = atoi(argv[5]);
    
    rc = rtcpc_InitNW(&listen_socket,&port);
    if ( rc == -1 ) {
        fprintf(stderr,"Cannot initialize network\n");
        rtcpc_exit(listen_socket,accept_socket,1);
    }
    fprintf(stderr,"Listen port is %d\n",port);

    reqID = 0;
    rc = vdqm_SendVolReq(NULL,&reqID,vid,dgn,server,drive,reqtype,port);
    if ( rc == -1 ) {
        fprintf(stderr,"Cannot contact VDQM\n");
        rtcpc_exit(listen_socket,accept_socket,1);
    }

    sprintf(msg,"%d %s %s %d",reqID,vid,dgn,reqtype);
    fprintf(stderr,"Volume ReqID = %d. Waiting for connection...\n",
        reqID);
    rc = rtcpc_Listen(listen_socket,&accept_socket);
    if ( rc == -1 ) {
        fprintf(stderr,"Listen returned error\n");
        rtcpc_exit(listen_socket,accept_socket,1);
    }
    fprintf(stderr,"send request message: %s\n",msg);
    rc = rtcpc_SendReq(accept_socket,&reqtype,msg);
    if ( rc == -1 ) {
        fprintf(stderr,"SendReq() returned error\n");
        rtcpc_exit(listen_socket,accept_socket,1);
    }
    rtcpc_exit(listen_socket,accept_socket,0);
    exit(0);
}
