/*
 * Copyright (C) 1999-2001 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqm_Listen.c,v $ $Revision: 1.2 $ $Date: 2005/03/15 22:57:11 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqm_Listen.c - accept requests from VDQM clients (server only) 
 */

#include <stdlib.h>

#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
#include <unistd.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#if defined(sgi)
#include <sys/select.h>
#include <bstring.h>
#endif /* sgi */
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#endif /* _WIN32 */

#include <errno.h>
#include <Castor_limits.h>
#include <serrno.h>
#include <osdep.h>
#include <net.h>
#include <log.h>
#include <vdqm_constants.h>
#include <vdqm.h>

int vdqm_Listen(vdqmnw_t *nw) {
    fd_set rfds,rfds_copy;
    struct sockaddr_in from;
    int fromlen, save_errno;
    int maxfd, rc;
#ifdef CSEC
    int c;
#endif
    
    if ( nw == NULL ) return(-1);
    
    if ( nw->listen_socket == INVALID_SOCKET &&
         nw->connect_socket == INVALID_SOCKET ) return(-1);
    FD_ZERO(&rfds);
    if ( nw->listen_socket != INVALID_SOCKET ) FD_SET(nw->listen_socket,&rfds);
    if ( nw->connect_socket != INVALID_SOCKET ) FD_SET(nw->connect_socket,&rfds);
    
    while (1) {
        /*
         * Loop on interrupted select()/accept() until a valid
         * connection is found or an error occured
         */
        nw->accept_socket = INVALID_SOCKET;
        maxfd = 0;
        if ( nw->listen_socket != INVALID_SOCKET ) maxfd = nw->listen_socket+1;
        if ( nw->connect_socket != INVALID_SOCKET &&
             nw->connect_socket >= maxfd ) maxfd = nw->connect_socket+1;
        rfds_copy = rfds;
		log(LOG_DEBUG,"vdqm_Listen() doing select(), maxfd = %d\n",maxfd);
        if ( (rc = select(maxfd,&rfds_copy,NULL,NULL,NULL)) > 0 ) {
			log(LOG_DEBUG,"vdqm_Listen() select() returned rc=%d\n",rc);
            if ( nw->listen_socket != INVALID_SOCKET &&
                 FD_ISSET(nw->listen_socket,&rfds_copy) ) {
                fromlen = sizeof(from);
				log(LOG_DEBUG,"vdqm_Listen() doing accept\n");
                nw->accept_socket = accept(nw->listen_socket,
                    (struct sockaddr *)&from,
                    &fromlen);
				save_errno = errno;
                if ( nw->accept_socket == INVALID_SOCKET ) {
                    (void)log(LOG_ERR,"vdqm_Listen() accept(): %s\n",neterror());
                    if ( save_errno != EINTR ) {
                        return(-1);
                    } else {
                        continue;
                    }
                }
#ifdef CSEC
		Csec_server_reinitContext(&(nw->sec_ctx), CSEC_SERVICE_TYPE_CENTRAL, NULL);
		if (Csec_server_establishContext(&(nw->sec_ctx),nw->accept_socket) < 0) {
		  (void)log(LOG_ERR, "Could not establish context: %s !\n", Csec_geterrmsg());
		  closesocket (nw->accept_socket);
		  nw->accept_socket = INVALID_SOCKET;
		  return (-1);
		}
		/* Connection could be done from another castor service */
		if ((c = Csec_server_isClientAService(&(nw->sec_ctx))) >= 0) {
		  (void)log(LOG_INFO, "CSEC: Client is castor service type: %d\n", c);
		  nw->Csec_service_type = c;
		} else {
	          char *username;
		  if (Csec_server_mapClientToLocalUser(&(nw->sec_ctx), &username, &(nw->Csec_uid), &(nw->Csec_gid))==0) {
		    (void)log(LOG_INFO, "CSEC: Client is %s (%d/%d)\n",
			      username,
			      nw->Csec_uid,
			      nw->Csec_gid);
		    nw->Csec_service_type = -1;
		  } else {
		    (void)log(LOG_ERR, "CSEC: Can't get client username\n");
		    closesocket (nw->accept_socket);
		    nw->accept_socket = INVALID_SOCKET;
		    return (-1);
		  }
		}
#endif /* CSEC */
            } else if ( nw->connect_socket != INVALID_SOCKET &&
                        FD_ISSET(nw->connect_socket,&rfds_copy) ) {
                nw->accept_socket = nw->connect_socket;
            }
            break;
        } else {
            (void)log(LOG_ERR,"vdqm_Listen() select(): %s\n",sstrerror(errno));
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
