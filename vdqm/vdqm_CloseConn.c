/*
 * $Id: vdqm_CloseConn.c,v 1.4 1999/09/02 15:20:38 obarring Exp $
 * $Log: vdqm_CloseConn.c,v $
 * Revision 1.4  1999/09/02 15:20:38  obarring
 * Add osdep.h because of new u_signed64 decl. in vdqm.h
 *
 * Revision 1.3  1999/09/01 15:07:11  obarring
 * Fix sccsid string
 *
 * Revision 1.2  1999/07/29 09:17:50  obarring
 * Replace TABs with 4 SPACEs
 *
 * Revision 1.1  1999/07/27 09:19:42  obarring
 * First version
 *
 */

/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * vdqm_CloseConn.c - close a VDQM connections.
 */

#ifndef lint
static char sccsid[] = "@(#)$Id: vdqm_CloseConn.c,v 1.4 1999/09/02 15:20:38 obarring Exp $";
#endif /* not lint */

#include <stdlib.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <sys/socket.h>
#endif /* _WIN32 */
#include <errno.h>
#include <Castor_limits.h>
#include <log.h>
#include <osdep.h>
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
