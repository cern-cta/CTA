/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqm_PoolOp.c,v $ $Revision: 1.6 $ $Date: 1999/09/27 15:23:47 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqm_PoolOp.c - Assign a thread and dispatch request (server only).
 */

#if defined(_WIN32)
#include <winsock2.h>    /* Needed for SOCKET definition */
#endif /* _WIN32 */
#include <stdlib.h>
#include <errno.h>
#include <Castor_limits.h>
#include <serrno.h>
#include <osdep.h>
#include <net.h>
#include <log.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <Cpool_api.h>

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
    if ( rc == -1 ) {
      log(LOG_ERR,"vdqm_GetPool() Cpool_assign(): %s\n",ssterror(serrno));
    }
    return(rc);
}
int vdqm_ReturnPool(vdqmnw_t *nw) {
    if ( nw == NULL ) return(-1);
    log(LOG_DEBUG,"vdqm_ReturnPool(0x%x)\n",nw);
    nw->accept_socket = nw->connect_socket = 
        nw->listen_socket = INVALID_SOCKET;
    return(0);
}
