#include <stdlib.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <errno.h>
#include <Castor_limits.h>
#include <net.h>
#include <log.h>
#include <vdqm_constants.h>
#include <vdqm.h>

#if !defined(linux)
extern char     *sys_errlist[] ;        /* System error list            */
#else /* linux */
#include <stdio.h>   /* Contains definition of sys_errlist[] */
#endif /* linux */ 

int vdqm_Listen(vdqmnw_t *nw) {
  fd_set rfds,rfds_copy;
  struct sockaddr_in from;
  int fromlen;
  int maxfd, rc;
  
  if ( nw == NULL ) return(-1);

  if ( nw->listen_socket == INVALID_SOCKET ) return(-1);
  FD_ZERO(&rfds);
  FD_SET(nw->listen_socket,&rfds);

  while (1) {
	/*
	 * Loop on interrupted select()/accept() until a valid
	 * connection is found or an error occured
	 */
	nw->accept_socket = INVALID_SOCKET;
    maxfd = nw->listen_socket+1;
    rfds_copy = rfds;
	if ( (rc = select(maxfd,&rfds_copy,NULL,NULL,NULL)) > 0 ) {
	  if ( FD_ISSET(nw->listen_socket,&rfds_copy) ) {
		fromlen = sizeof(from);
		nw->accept_socket = accept(nw->listen_socket,
		 						(struct sockaddr *)&from,
								&fromlen);
		if ( nw->accept_socket == INVALID_SOCKET ) {
		  (void)log(LOG_ERR,"vdqm_Listen() accept(): %s\n",NWERRTXT);
		  if ( errno != EINTR ) {
		    return(-1);
		  } else {
			continue;
		  }
		}
	  }
	  break;
	} else {
	  (void)log(LOG_ERR,"vdqm_Listen() select(): %s\n",sys_errlist[errno]);
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
