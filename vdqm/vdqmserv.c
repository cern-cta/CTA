/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqmserv.c,v $ $Revision: 1.6 $ $Date: 1999/12/17 14:05:34 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqmserv.c - VDQM server main routine (server only).
 */

#if defined(_WIN32)
#include <winsock2.h>    /* Needed for SOCKET definition */
#endif /* _WIN32 */
#include <stdlib.h>
#include <errno.h>
#include <Castor_limits.h>
#include <serrno.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <vdqm_constants.h>
#include <vdqm.h>

void initlog(char *, int, char *);
int vdqm_shutdown;

int main() {
    vdqmnw_t *nw, *nwtable;
    int rc, poolID;
    extern int vdqm_shutdown;

    initlog("vdqm",LOG_INFO,VDQM_LOG_FILE);
    log(LOG_INFO,"main:\n\n ******* VDQM SERVER START ******\n\n");
    rc = vdqm_InitNW(&nw);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_InitNw(): %s\n",neterror());
        return(vdqm_CleanUp(nw,1));
    }
    rc = vdqm_StartRollbackThread();
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_StartRollbackThread(): %s\n",sstrerror(serrno));
        return(vdqm_CleanUp(nw,1));
    }
    rc = vdqm_InitPool(&nwtable);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_InitPool(): %s\n",sstrerror(serrno));
        return(vdqm_CleanUp(nw,1));
    }
    poolID = rc;
    vdqm_shutdown = 0;
    
    for (;;) {
        rc = vdqm_Listen(nw);
        if ( vdqm_shutdown ) break;
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_Listen(): %s\n",neterror());
            continue;
        }
        rc = vdqm_GetPool(poolID,nw,nwtable);
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_GetPool(): %s\n",sstrerror(serrno));
            break;
        }
    }
    log(LOG_INFO,"main:\n\n ******* VDQM SERVER EXIT ******\n\n");
    return(vdqm_CleanUp(nw,1));
}
