/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * rtcp_InitNW.c - Initialise the RTCP network interface (server only).
 */

#ifndef lint
static char sccsid[] = "$RCSfile: rtcp_InitNW.c,v $ $Revision: 1.6 $ $Date: 2000/02/23 15:56:51 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
WSADATA wsadata;
#else /* _WIN32 */
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#endif /* _WIN32 */
#include <errno.h>
#include <Castor_limits.h>
#include <Cnetdb.h>
#include <log.h>
#include <net.h>
#include <osdep.h>
#include <serrno.h>

#include <rtcp_constants.h>
#include <rtcp.h>

typedef enum rtcp_type {client,server} rtcp_type_t;


int rtcp_InitNW(SOCKET **ListenSocket, int *port, rtcp_type_t type) {
    struct sockaddr_in sin ; /* Internet address */
    struct servent *sp ;   /* Service entry */
    extern char * getconfent() ;
    extern char * getenv() ;
    char *p ;
    int rtcp_port = -1;
    int rcode,rc,len;
    
    if ( ListenSocket == NULL || (*ListenSocket != NULL && 
        **ListenSocket != INVALID_SOCKET )) {
        serrno = EINVAL;
        return(-1); /* Socket already allocated and opened */
    }
    if ( port == NULL && type == client ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( port != NULL ) *port = rtcp_port;

#if defined(_WIN32)
    /*
     * Do a dummy test to see if WinSock DLL has been initialized
     */
    rc = socket(AF_INET,SOCK_STREAM,0);
    if ( rc == INVALID_SOCKET ) {
        if ( GetLastError() == WSANOTINITIALISED ) {
            /* Initialize the WinSock DLL */
            rc = WSAStartup(MAKEWORD(2,0), &wsadata);    
            if ( rc ) {
                rtcp_log(LOG_ERR,"rtcp_InitNW() WSAStartup(): %s\n",neterror());
                return(-1);
            }
        } else {
            rtcp_log(LOG_ERR,"rtcp_InitNW() WSAStartup(): %s\n",neterror());
            return(-1);
        }
    } else closesocket(rc);
#endif /* _WIN32 */
    
    if ( (*ListenSocket = (SOCKET *)calloc(1,sizeof(SOCKET))) == NULL ) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() calloc(): %s",sstrerror(errno));
        return(-1);
    }
    **ListenSocket = INVALID_SOCKET;

    if ( type == server ) {
        /*
         * Server wants to listen to the well-known RTCOPY port.
         * Set appropriate listen port in order of priority:
         *        (1) env variable if defined
         *        (2) configuration variable
         *        (3) service entry
         *        (4) compiler constant
         *        (5) -1 : return error
         */
#if !defined(DEBUG) && !defined(_DEBUG)
        if ( (p = getenv("RTCOPY_PORT")) != (char *)NULL ) {
            rtcp_port = atoi(p);
        } else if ( (p = getconfent("RTCOPY","PORT",0)) != (char *)NULL ) {
            rtcp_port = atoi(p);
        } else if ( (sp = Cgetservbyname("rtcopy","tcp")) != (struct servent *)NULL ) {
            rtcp_port = (int)ntohs(sp->s_port);
        } else {
#if defined(RTCOPY_PORT)
            rtcp_port = RTCOPY_PORT;
#endif /* RTCOPY_PORT */
        }
#else /* !DEBUG && !_DEBUG */
        rtcp_port = RTCOPY_PORT_DEBUG;
#endif /* !DEBUG && !_DEBUG */
    } else {
        /* 
         * Client just wants to listen to an arbitrary port
         */
        rtcp_port = 0;
    }

    if ( rtcp_port < 0 ) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() rtcp_port = %d\n",rtcp_port);
        return(-1);
    }
    
    **ListenSocket = socket(AF_INET,SOCK_STREAM,0);
    if ( **ListenSocket == INVALID_SOCKET ) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() socket(): %s\n",neterror());
        return(-1);
    }

    if ( type == server ) {
        rcode = 1;
        if ( setsockopt(**ListenSocket, SOL_SOCKET, SO_REUSEADDR, 
            (char *)&rcode,sizeof(rcode)) == SOCKET_ERROR ) {
            rtcp_log(LOG_ERR,"rtcp_InitNW() setsockopt(SO_REUSEADDR) not fatal: %s\n",
                neterror());
        }
    }
    
    (void)memset(&sin,'\0',sizeof(sin));
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    sin.sin_family = AF_INET;
    sin.sin_port = htons((u_short)rtcp_port);
    rc = bind(**ListenSocket, (struct sockaddr *)&sin, sizeof(sin));
    if ( rc == SOCKET_ERROR ) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() bind(): %s\n",neterror());
        return(-1);
    }
    
    if ( listen(**ListenSocket,RTCOPY_BACKLOG) == SOCKET_ERROR ) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() listen(): %s\n",neterror());
        return(-1);
    }
    if ( type == client ) {
        len = sizeof(sin);
        rc = getsockname(**ListenSocket,(struct sockaddr *)&sin,&len);
        if ( rc == SOCKET_ERROR ) {
            rtcp_log(LOG_ERR,"rtcp_InitNW() getsockname(): %s\n",neterror());
            return(-1);
        }
        rtcp_port = ntohs(sin.sin_port);
    }
    
    if ( port != NULL ) *port = rtcp_port;
    return(0);
}

/*
 * Server initialize NW routine
 */
int rtcpd_InitNW(SOCKET **ListenSocket) {
    rtcp_type_t type = server;
    return(rtcp_InitNW(ListenSocket,NULL,type));
}

/*
 * Client initialize NW routine
 */
int rtcpc_InitNW(SOCKET **ListenSocket, int *port) {
    rtcp_type_t type = client;
    return(rtcp_InitNW(ListenSocket,port,type));
}

/*
 * Cleanup routine to be used in a return statement like,
 *    return(rtcp_CleanUp(ListenSocket,-1));
 */
int rtcp_CleanUp(SOCKET **ListenSocket,int status) {
    if ( ListenSocket != NULL && *ListenSocket != NULL ) {
        if ( **ListenSocket != INVALID_SOCKET ) {
            closesocket(**ListenSocket);
            **ListenSocket = INVALID_SOCKET;
        }
        free(*ListenSocket);
        *ListenSocket = NULL;
    }
#if defined(_WIN32)
    WSACleanup();
#endif /* _WIN32 */
    return(status);
}
