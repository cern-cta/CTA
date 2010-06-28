#include <stdlib.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <sys/socket.h>
#include <errno.h>
#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#if !defined(linux)
extern char *sys_errlist[];
#else /* linux */
#include <stdio.h>   /* Contains definition of sys_errlist[] */
#endif /* linux */

int vdqm_CloseConn(vdqmnw_t *nw) {
	SOCKET s;
	int status = 0;

	if ( nw == NULL ) return(-1);
	if ( (s = nw->accept_socket) == INVALID_SOCKET ) 
		s = nw->connect_socket;

	if ( s == INVALID_SOCKET ) return(-1);
	status = shutdown(s,SD_BOTH);
	if ( status == SOCKET_ERROR ) {
		log(LOG_ERR,"vdqm_CloseConn(): shutdown() %s\n",NWERRTXT);
	}
	status = closesocket(s);
	if ( status == SOCKET_ERROR ) {
		log(LOG_ERR,"vdqm_CloseConn(): closesocket() %s\n",NWERRTXT);
	}
	return(status);
}
