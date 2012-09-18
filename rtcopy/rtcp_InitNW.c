/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcp_InitNW.c - Initialise the RTCP network interface (server only).
 */

#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>      /* Standard data types                */
#include <netdb.h>          /* Network "data base"                */
#include <sys/socket.h>     /* Socket interface                   */
#include <netinet/in.h>     /* Internet data types                */
#include <netinet/tcp.h>    /* S. Murray 31/03/09 TCP definitions */
#include <errno.h>
#include <Castor_limits.h>
#include <Cnetdb.h>
#include <log.h>
#include <unistd.h>
#include <net.h>
#include <Cuuid.h>
#include <osdep.h>
#include <serrno.h>

#include <rtcp_constants.h>
#include <rtcp.h>

typedef enum rtcp_type {client,server} rtcp_type_t;

int use_port = -1;

int rtcp_InitNW(int **ListenSocket, int *port, rtcp_type_t type, char *serviceName, char *cfgName) {
    struct sockaddr_in sin ; /* Internet address */
    struct servent *sp ;   /* Service entry */
    extern char * getconfent() ;
    extern char * getenv() ;
    char *p ;
    int rtcp_port = -1;
    int rcode,rc;
    socklen_t len;
    char *localServiceName = "rtcopy";
    char *localCfgName = "RTCOPY";
    char portEnv[CA_MAXLINELEN+1];
    
    if ( ListenSocket == NULL || (*ListenSocket != NULL && 
        **ListenSocket != -1 )) {
        serrno = EINVAL;
        return(-1); /* Socket already allocated and opened */
    }
    if ( port == NULL && type == client ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( port != NULL ) *port = rtcp_port;

    if ( serviceName != NULL ) localServiceName = serviceName;
    if ( cfgName != NULL ) localCfgName = cfgName;

    if ( (*ListenSocket = (int *)calloc(1,sizeof(int))) == NULL ) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() calloc(): %s",sstrerror(errno));
        serrno = SESYSERR;
        return(-1);
    }
    **ListenSocket = -1;

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
        sprintf(portEnv,"%s_PORT",localCfgName);
        if ( (p = getenv(portEnv)) != (char *)NULL ) {
            rtcp_port = atoi(p);
        } else if ( (p = getconfent(localCfgName,"PORT",0)) != (char *)NULL ) {
            rtcp_port = atoi(p);
        } else if ( (sp = Cgetservbyname(localServiceName,"tcp")) != (struct servent *)NULL ) {
            rtcp_port = (int)ntohs(sp->s_port);
        } else {
#if defined(RTCOPY_PORT)
            rtcp_port = RTCOPY_PORT;
#endif /* RTCOPY_PORT */
        }
        if ( use_port > 0 ) rtcp_port = use_port;
    } else {
        /* 
         * Client just wants to listen to an arbitrary port
         */
        rtcp_port = 0;
    }

    if ( rtcp_port < 0 ) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() rtcp_port = %d\n",rtcp_port);
        serrno = SEINTERNAL;
        return(-1);
    }
    
    **ListenSocket = socket(AF_INET,SOCK_STREAM,0);
    if ( **ListenSocket == -1 ) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() socket(): %s\n",neterror());
        serrno = SECOMERR;
        return(-1);
    }

    if ( type == server ) {
        rcode = 1;
        if ( setsockopt(**ListenSocket, SOL_SOCKET, SO_REUSEADDR, 
            (char *)&rcode,sizeof(rcode)) == -1 ) {
            rtcp_log(LOG_ERR,"rtcp_InitNW() setsockopt(SO_REUSEADDR) not fatal: %s\n",
                neterror());
        }
        { /* S. Murray 31/03/09 */
          int tcp_nodelay = 1;
          if ( setsockopt(**ListenSocket, IPPROTO_TCP, TCP_NODELAY,
            (char *)&tcp_nodelay,sizeof(tcp_nodelay)) < 0 ) {
            rtcp_log(LOG_ERR,
              "rtcp_InitNW() setsockopt(TCP_NODELAY) not fatal:%s\n",
              neterror());
          }
        }
    }
    
    (void)memset(&sin,'\0',sizeof(sin));
    /* Bind to localhost because the tapebridged daemon should be the only */
    /* client of the rtcpd daemon and the tapebridged daemon should always */
    /* be ran on the same host as the rtcpd daemon.                        */
    if(0 == inet_aton("127.0.0.1", &sin.sin_addr)) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() inet_aton(): Invalid address\n");
        serrno = SECOMERR;
        return(-1);
    }
    sin.sin_family = AF_INET;
    sin.sin_port = htons((u_short)rtcp_port);
    rc = bind(**ListenSocket, (struct sockaddr *)&sin, sizeof(sin));
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() bind(): %s\n",neterror());
        serrno = SECOMERR;
        return(-1);
    }
    
    if ( listen(**ListenSocket,RTCOPY_BACKLOG) == -1 ) {
        rtcp_log(LOG_ERR,"rtcp_InitNW() listen(): %s\n",neterror());
        serrno = SECOMERR;
        return(-1);
    }
    if ( type == client ) {
        len = sizeof(sin);
        rc = getsockname(**ListenSocket,(struct sockaddr *)&sin,&len);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcp_InitNW() getsockname(): %s\n",neterror());
            serrno = SECOMERR;
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
int rtcpd_InitNW(int **ListenSocket) {
    rtcp_type_t type = server;
    return(rtcp_InitNW(ListenSocket,NULL,type,NULL,NULL));
}

/*
 * Client initialize NW routine
 */
int rtcpc_InitNW(int **ListenSocket, int *port) {
    rtcp_type_t type = client;
    return(rtcp_InitNW(ListenSocket,port,type,NULL,NULL));
}

int rtcpcld_InitNW(int **rtcpdCallback) {
    int rc;
    rtcp_type_t type = server;
    rc = rtcp_InitNW(rtcpdCallback,NULL,type,"rtcpcld","RTCPCLD");
    return(rc);
}

/*
 * Cleanup routine to be used in a return statement like,
 *    return(rtcp_CleanUp(ListenSocket,-1));
 */
int rtcp_CleanUp(int **ListenSocket,int status) {
    if ( ListenSocket != NULL && *ListenSocket != NULL ) {
        if ( **ListenSocket != -1 ) {
            close(**ListenSocket);
            **ListenSocket = -1;
        }
        free(*ListenSocket);
        *ListenSocket = NULL;
    }
    return(status);
}
