/*
 * $Id: vdqm_PoolOp.c,v 1.1 1999/07/27 09:20:47 obarring Exp $
 * $Log: vdqm_PoolOp.c,v $
 * Revision 1.1  1999/07/27 09:20:47  obarring
 * First version
 *
 */

/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * vdqm_PoolOp.c - Assign a thread and dispatch request (server only).
 */

#ifndef lint
static char cvsId[] = "$Id";
#endif /* not lint */

#if defined(_WIN32)
#include <winsock2.h>    /* Needed for SOCKET definition */
#endif /* _WIN32 */
#include <stdlib.h>
#include <errno.h>
#include <Castor_limits.h>
#include <net.h>
#include <log.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <Cpool_api.h>

#if !defined(linux)
extern char *sys_errlist[];
#else /* linux */
#include <stdio.h>   /* Contains definition of sys_errlist[] */
#endif /* linux */

int vdqm_GetPool(int poolID, vdqmnw_t *nw, vdqmnw_t nwtable[]) {
    extern void *vdqm_ProcReq(void *);
    vdqmnw_t *tmpnw;
	int rc;

	if ( nw == NULL || nwtable == NULL ) {
		log(LOG_ERR,"vdqm_GetPool() invalid network structure\n"); 
		return(-1);
	}
	rc = Cpool_next_index(poolID);
	if ( rc == -1 ) return(-1);

	tmpnw = &nwtable[rc];
	*tmpnw = *nw;
    log(LOG_DEBUG,"vdqm_GetPool(): next thread index is %d, nw=0x%x\n",
        rc,tmpnw);
	rc = Cpool_assign(poolID,vdqm_ProcReq,(void *)tmpnw,-1);
	return(rc);
}
int vdqm_ReturnPool(vdqmnw_t *nw) {
	if ( nw == NULL ) return(-1);
    log(LOG_DEBUG,"vdqm_ReturnPool(0x%x)\n",nw);
	nw->accept_socket = nw->connect_socket = 
		nw->listen_socket = INVALID_SOCKET;
	return(0);
}
