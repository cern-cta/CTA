/*
 * $Id: vdqm_InitNW.c,v 1.3 2005/03/15 22:57:11 bcouturi Exp $
 *
 * Copyright (C) 1999-2001 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqm_InitNW.c,v $ $Revision: 1.3 $ $Date: 2005/03/15 22:57:11 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqm_InitNW.c - Initialise the VDQM network interface (server only).
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#if defined(_WIN32)
#include <winsock2.h>
WSADATA wsadata;
#else /* _WIN32 */
#include <unistd.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#endif /* _WIN32 */

#include <errno.h>
#include <Castor_limits.h>
#include <osdep.h>
#include <Cnetdb.h>
#include <log.h>
#include <net.h>
#include <serrno.h>
#include <vdqm_constants.h>
#include <vdqm.h>

static int InitNW(vdqmnw_t **nw, char *vdqm_host, int use_port) {
    struct sockaddr_in sin ; /* Internet address */
    struct servent *sp ;   /* Service entry */
    extern char * getconfent() ;
    extern char * getenv() ;
    struct hostent *hp = NULL;
    char *p ;
    int vdqm_port = -1;
    int rcode,rc;
    SOCKET s;
#ifdef CSEC
    int n;
    int secure_connection = 0;
#endif

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
                log(LOG_ERR,"InitNW() WSAStartup(): %s\n",neterror());
                return(-1);
            }
        } else {
            log(LOG_ERR,"InitNW() WSAStartup(): %s\n",neterror());
            return(-1);
        }
    } else closesocket(rc);
#endif /* _WIN32 */

    /*
     * Don't consider this as an error. We'll use it
     * to initialize Win32 socket library only.
     */
    if ( nw == NULL ) return(0);
#ifdef CSEC
    if (getenv("SECURE_CASTOR") != NULL) secure_connection++;
#endif
    if ( use_port > 0 ) {
        vdqm_port = use_port;
    } else {
#ifdef CSEC
      if (secure_connection) {
        /*
         * Set appropriate listen port in order of priority:
         *        (1) env variable if defined
         *        (2) configuration variable
         *        (3) service entry
         *        (4) compiler constant
         *        (5) -1 : return error
         */
        if ( (p = getenv("SVDQM_PORT")) != (char *)NULL ) {
            vdqm_port = atoi(p);
        } else if ( (p = getconfent("SVDQM","PORT",0)) != (char *)NULL ) {
            vdqm_port = atoi(p);
        } else if ( (sp = Cgetservbyname("svdqm","tcp")) != (struct servent *)NULL ) {
            vdqm_port = (int)ntohs(sp->s_port);
        } else {
#if defined(SVDQM_PORT)
            vdqm_port = SVDQM_PORT;
#endif /* SVDQM_PORT */
        }
      } else {
#endif
        /*
         * Set appropriate listen port in order of priority:
         *        (1) env variable if defined
         *        (2) configuration variable
         *        (3) service entry
         *        (4) compiler constant
         *        (5) -1 : return error
         */
        if ( (p = getenv("VDQM_PORT")) != (char *)NULL ) {
            vdqm_port = atoi(p);
        } else if ( (p = getconfent("VDQM","PORT",0)) != (char *)NULL ) {
            vdqm_port = atoi(p);
        } else if ( (sp = Cgetservbyname("vdqm","tcp")) != (struct servent *)NULL ) {
            vdqm_port = (int)ntohs(sp->s_port);
        } else {
#if defined(VDQM_PORT)
            vdqm_port = VDQM_PORT;
#endif /* VDQM_PORT */
        }
#ifdef CSEC
      }
#endif
    }
    if ( vdqm_port < 0 ) {
        log(LOG_ERR,"InitNW() vdqm_port = %d\n",vdqm_port);
        return(-1);
    }

    if ( (*nw = (vdqmnw_t *)calloc(1,sizeof(vdqmnw_t))) == NULL ) {
        log(LOG_ERR,"InitNW() calloc(): %s",sstrerror(errno));
        return(-1);
    }

    s = socket(AF_INET,SOCK_STREAM,0);
    if ( s == INVALID_SOCKET ) {
        log(LOG_ERR,"InitNW() socket(): %s\n",neterror());
        return(-1);
    }

    (void)memset(&sin,'\0',sizeof(sin));
    if ( vdqm_host != NULL && *vdqm_host != '\0' ) {
        (*nw)->connect_socket = s;
        if ( (hp = Cgethostbyname(vdqm_host)) == (struct hostent *)NULL ) {
            log(LOG_ERR,"InitNW() Cgethostbyname(%s): %s, h_errno=%d\n",
                vdqm_host,neterror(),h_errno);
            closesocket((*nw)->connect_socket);
            return(-1);
        }
        sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
    } else {
        (*nw)->listen_socket = s;
        rcode = 1;
        if ( setsockopt((*nw)->listen_socket, SOL_SOCKET, SO_REUSEADDR, 
             (char *)&rcode,sizeof(rcode)) == SOCKET_ERROR ) {
            log(LOG_ERR,"InitNW() setsockopt(SO_REUSEADDR): %s\n",
                neterror());
        }
        sin.sin_addr.s_addr = htonl(INADDR_ANY);
    }

    sin.sin_family = AF_INET;
    sin.sin_port = htons((unsigned short)vdqm_port);

    if ( vdqm_host == NULL || *vdqm_host == '\0' )  {
        rc = bind((*nw)->listen_socket, (struct sockaddr *)&sin, sizeof(sin));
        if ( rc == SOCKET_ERROR ) {
            (void)log(LOG_ERR,"InitNW() bind(): %s\n",neterror());
            closesocket((*nw)->listen_socket);
            return(-1);
        }

        if ( listen((*nw)->listen_socket,VDQM_BACKLOG) == SOCKET_ERROR ) {
            (void)log(LOG_ERR,"InitNW() listen(): %s\n",neterror());
            closesocket((*nw)->listen_socket);
            return(-1);
        }
        (*nw)->connect_socket = INVALID_SOCKET;
    } else {
        log(LOG_INFO,"InitNW() connecting to %d@%s\n",vdqm_port,vdqm_host);
        rc = connect((*nw)->connect_socket, (struct sockaddr *)&sin, 
                     sizeof(sin)); 
        if ( rc == SOCKET_ERROR ) {
            log(LOG_ERR,"InitNW() connect(): %s\n",neterror());
            closesocket((*nw)->connect_socket);
            return(-1);
        }
#ifdef CSEC
	if (secure_connection) {
	  if (Csec_client_initContext(&((*nw)->sec_ctx), CSEC_SERVICE_TYPE_CENTRAL, NULL) <0) {
	    log(LOG_ERR, "InitNW() Could not init context\n");
            closesocket((*nw)->connect_socket);
	    serrno = ESEC_CTX_NOT_INITIALIZED;
	    return -1;
	  }
	  
	  if(Csec_client_establishContext(&((*nw)->sec_ctx), (*nw)->connect_socket)< 0) {
	    log (LOG_ERR, "InitNW() Could not establish context\n");
            closesocket((*nw)->connect_socket);
	    serrno = ESEC_NO_CONTEXT;
	    return -1;
	  }
	
	  Csec_clearContext(&((*nw)->sec_ctx));
	}
#endif

        (*nw)->listen_socket = INVALID_SOCKET;
    }

    (*nw)->accept_socket = INVALID_SOCKET;
    return(0);
}

int vdqm_InitNW(vdqmnw_t **nw) {
    return(InitNW(nw,NULL,-1));
}

int vdqm_InitNWOnPort(vdqmnw_t **nw, int use_port) {
    return(InitNW(nw,NULL,use_port));
}

int vdqm_ConnectToVDQM(vdqmnw_t **nw, char *host) {
    return(InitNW(nw,host,-1));
}

int vdqm_CleanUp(vdqmnw_t *nw,int status) {
    if ( nw != NULL ) {
        if ( nw->accept_socket != INVALID_SOCKET ) vdqm_CloseConn(nw);
        closesocket(nw->listen_socket);
        free(nw);
    }
#if defined(_WIN32)
    WSACleanup();
#endif /* _WIN32 */
    return(status);
}
