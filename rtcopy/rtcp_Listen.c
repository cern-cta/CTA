/*
 * $Id: rtcp_Listen.c,v 1.9 2009/03/31 16:08:35 murrayc3 Exp $
 *
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcp_Listen.c - accept new or select on existing connections.
 */

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/param.h>
#include <sys/types.h>      /* Standard data types                */
#include <netdb.h>          /* Network "data base"                */
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>     /* Internet data types                */
#include <netinet/tcp.h>    /* S. Murray 31/03/09 TCP definitions */
#include <errno.h>
#include <Castor_limits.h>
#include <osdep.h>
#include <Cnetdb.h>
#include <net.h>
#include <serrno.h>
#include <log.h>
#include <common.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#ifdef RTCOPYCSEC
#include "Csec_api.h"
#endif


int rtcp_Listen(int s, int *ns, int timeout, int wherefrom) {
  fd_set rfds, rfds_copy;
  struct sockaddr_in from;
  int maxfd, rc;
  socklen_t fromlen;
  int save_errno, keepalive=1;
  struct timeval t_out, *t_out_p;
#ifdef RTCOPYCSEC
  uid_t Csec_uid;
  gid_t Csec_gid;
  Csec_context_t sec_ctx;
  int Csec_service_type;
  int c;
  char *p;
  int n;
#endif

  (void)wherefrom;

  if ( s == -1 ) {
    serrno = EINVAL;
    return(-1);
  }

  FD_ZERO(&rfds);
  maxfd =FD_SETSIZE;
  FD_SET(s,&rfds);

  t_out_p = NULL;
  if ( timeout > 0 ) {
    t_out.tv_sec = (time_t)timeout;
    t_out.tv_usec = 0;
    t_out_p = &t_out;
  }

  /*
   * Loop on select over all listen sockets. Break on valid
   * connection or true error.
   */
  for (;;) {
    if ( ns != NULL ) *ns = -1;
    rfds_copy = rfds;
    if ( (rc = select(maxfd,&rfds_copy,NULL,NULL,t_out_p)) > 0 ) {
      if ( FD_ISSET(s,&rfds_copy) ) {
        /*
         * Caller just wanted us to select on the socket ?
         */
        if ( ns == NULL ) return(0);
        /*
         * Loop on accept on current socket. Break on 
         * valid connection or true error.
         */
        for (;;) {
          fromlen = sizeof(from);
          *ns = accept(s,(struct sockaddr *)&from,&fromlen);
          if ( *ns == -1 ) {
            save_errno = errno;
            rtcp_log(LOG_ERR,"rtcp_Listen() accept(): %s\n",
                     neterror());
            if ( save_errno != EINTR ) {
              /*
               * True error.
               */
              serrno = save_errno;
              return(-1);
            } else {
              /*
               * An interrupt was received.
               * Continue as normal.
               */
              continue;
            }
          }
          /*
           * A valid connection has been found.
           */
          (void)setsockopt(
                           *ns,
                           SOL_SOCKET,
                           SO_KEEPALIVE,
                           (char *)&keepalive,
                           sizeof(int)
                           );
          { /* S. Murray 31/03/09 */
            int tcp_nodelay = 1;
            setsockopt(*ns, IPPROTO_TCP, TCP_NODELAY,
              (char *)&tcp_nodelay,sizeof(tcp_nodelay));
          }
#ifdef RTCOPYCSEC
          /*
           * Try to establish secure connection.
           */
          if (wherefrom == RTCP_ACCEPT_FROM_CLIENT) { /* We are the server */
            Csec_server_initContext(&sec_ctx, CSEC_SERVICE_TYPE_CENTRAL, NULL);
            if (Csec_server_establishContext(&sec_ctx, *ns) < 0) {
              rtcp_log(LOG_ERR,"rtcp_Listen(): CSEC: Could not establish cotext\n");
              close(*ns);
              *ns = -1;
              return(-1);
            }
            rtcp_log(LOG_INFO,"rtcp_Listen() CSEC: server context established\n");
            /* Connection could be done from another castor service */
            if ((c = Csec_server_isClientAService(&sec_ctx)) >= 0) {
              rtcp_log(LOG_ERR,"rtcp_Listen(): CSEC: Client is castor service type %d\n", c);
              Csec_service_type = c;
            }
            else {
	      char *username;
              if (Csec_server_mapClientToLocalUser(&sec_ctx, &username, &Csec_uid, &Csec_gid) == 0) {
                rtcp_log(LOG_ERR,"rtcp_Listen(): CSEC: Client is %s (%d/%d)\n",
			 username,
                         Csec_uid,
                         Csec_gid);
                Csec_service_type = -1;
              }
              else {
                close(*ns);
                *ns = -1;
                return(-1);
              }
            }
          }
          else { /* We are the client */
            if (Csec_client_initContext(&sec_ctx, CSEC_SERVICE_TYPE_CENTRAL, NULL) <0) {
              rtcp_log(LOG_ERR, "rtcp_Listen() Could not init client context\n");
              close(*ns);
              *ns = -1;
              serrno = ESEC_CTX_NOT_INITIALIZED;
              return(-1);
            }
                      
            if(Csec_client_establishContext(&sec_ctx, *ns)< 0) {
              rtcp_log(LOG_ERR, "rtcp_Listen() Could not establish client context\n");
              close(*ns);
              *ns = -1;
              serrno = ESEC_NO_CONTEXT;
              return(-1);
            }
                      
            Csec_clearContext(&sec_ctx);
          }
#endif   /* RTCOPYCSEC */
          /*
           * Break out of infinite accept loop.
           */
          break;
        }
      }
      /*
       * A valid connection has been found. Break out of
       * infinite select loop and return connection to caller.
       */
      break;
    } else if ( rc == -1 ){
      save_errno = errno;
      rtcp_log(LOG_ERR,"rtcp_Listen() select(): %s\n",neterror());
      serrno = save_errno;
      return(-1);
    } else break; /* timeout limit expired */
  }
  return(0);
}

int rtcp_CheckConnect(const int s, tape_list_t *tape) {
  char peerhost[CA_MAXHOSTNAMELEN+1], tmp[CA_MAXHOSTNAMELEN+1];
  char *tpserver;
  int l, rc;

  *peerhost = '\0';
  rtcp_log(LOG_DEBUG,"rtcp_CheckConnect() entered\n");
  if ( s == -1 ) return(-1);

  if ( (rc = isadminhost(s,peerhost)) == -1 && serrno != SENOTADMIN ) {
    rtcp_log(LOG_ERR,"rtcp_CheckConnect() failed to lookup connection\n");
    return(-1);
  }
  if ( *peerhost == '\0' ) {
    rtcp_log(LOG_ERR,"rtcp_CheckConnect() peer host name is empty\n");
    return(0);
  }

  l = strlen(peerhost);
  if ( l > CA_MAXHOSTNAMELEN ) {
    strncpy(tmp,peerhost,10);
    tmp[10] = '\0';
    rtcp_log(LOG_ERR,"rtcp_CheckConnect() peer host name length %d exceeds limit %d. Name begins with: %s\n",
             l,CA_MAXHOSTNAMELEN,tmp);
    return(0);
  }
 
  if ( rc == 0 ) {
    rtcp_log(LOG_INFO,"rtcp_CheckConnect() connection from authorised host %s\n",
             peerhost);
    return(1);
  }

  tpserver = NULL;
  if ( tape != NULL ) tpserver = tape->tapereq.server;
  rtcp_log(LOG_DEBUG,"rtcp_CheckConnect() compare host %s with tape server %s\n",peerhost,tpserver);
  if ( tpserver != NULL && *tpserver != '\0' &&
       strcmp(peerhost,tpserver) == 0 ) {
    rtcp_log(LOG_DEBUG,"rtcp_CheckConnect() host %s matches tape server %s\n",
             peerhost,tpserver);
    return(1);
  }
  rtcp_log(LOG_INFO,"rtcp_CheckConnect() reject connection from unauthorised host %s\n",(peerhost != NULL ? peerhost : "'unknown'"));
  rtcp_log(LOG_DEBUG,"rtcp_CheckConnect() exits with status 0 (unauthorised connection)\n");
  return(0);
}
