#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <errno.h>
#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <vdqm_constants.h>
#include <vdqm.h>

#if !defined(linux)
extern char *sys_errlist[];
#endif /* linux */

int vdqm_InitNW(vdqmnw_t **nw) {
  struct sockaddr_in sin ; /* Internet address */
  struct servent *sp ;   /* Service entry */
  extern char * getconfent() ;
  extern char * getenv() ;
  char *p ;
  int vdqm_port = -1;
  int rcode,rc;

  if ( (*nw = (vdqmnw_t *)calloc(1,sizeof(vdqmnw_t))) == NULL ) {
	log(LOG_ERR,"vdqm_InitNW() calloc(): %s",ERRTXT);
	return(-1);
  }

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
  } else if ( (sp = getservbyname("vdqm","tcp")) != (struct servent *)NULL ) {
	vdqm_port = (int)sp->s_port;
  } else {
#if defined(VDQM_PORT)
    vdqm_port = VDQM_PORT;
#endif /* VDQM_PORT */
  }
  if ( vdqm_port < 0 ) {
	log(LOG_ERR,"vdqm_InitNW() vdqm_port = %d\n",vdqm_port);
	return(-1);
  }

  (*nw)->listen_socket = socket(AF_INET,SOCK_STREAM,0);
  if ( (*nw)->listen_socket == INVALID_SOCKET )
  {
	log(LOG_ERR,"vdqm_InitNW() socket(): %s\n",NWERRTXT);
	return(-1);
  }

  rcode = 1;
  if ( setsockopt((*nw)->listen_socket, SOL_SOCKET, SO_REUSEADDR, 
	  (char *)&rcode,sizeof(rcode)) == SOCKET_ERROR )
  {
	log(LOG_ERR,"vdqm_InitNW() setsockopt(SO_REUSEADDR) not fatal: %s\n",
	  NWERRTXT);
  }

  (void)memset(&sin,'\0',sizeof(sin));
  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  sin.sin_family = AF_INET;
  sin.sin_port = htons((u_short)vdqm_port);
  rc = bind((*nw)->listen_socket, (struct sockaddr *)&sin, sizeof(sin));
  if ( rc == SOCKET_ERROR )
  {
	(void)log(LOG_ERR,"vdqm_InitNW() bind(): %s\n",NWERRTXT);
	return(-1);
  }

  if ( listen((*nw)->listen_socket,VDQM_BACKLOG) == SOCKET_ERROR ) {
	(void)log(LOG_ERR,"vdqm_InitNW() listen(): %s\n",NWERRTXT);
	return(-1);
  }

  (*nw)->accept_socket = INVALID_SOCKET;
  (*nw)->connect_socket = INVALID_SOCKET;
  return(0);
}
int vdqm_CleanUp(vdqmnw_t *nw,int status) {
	if ( nw->accept_socket != INVALID_SOCKET ) vdqm_CloseConn(nw);
	shutdown(nw->listen_socket,SD_BOTH);
	closesocket(nw->listen_socket);
	free(nw);
	return(status);
}
