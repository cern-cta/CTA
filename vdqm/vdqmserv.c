/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqmserv.c,v $ $Revision: 1.12 $ $Date: 2000/08/25 08:03:44 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqmserv.c - VDQM server main routine (server only).
 */

#if defined(_WIN32)
#include <winsock2.h>    /* Needed for SOCKET definition */
#endif /* _WIN32 */

#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#if defined(VDQMSERV)
#if !defined(_WIN32)
#include <regex.h>
#endif /* _WIN32 */
#endif /* VDQMSERV */

#include <Castor_limits.h>
#include <osdep.h>
#include <serrno.h>
#include <log.h>
#include <net.h>
#include <Cinit.h>
#include <vdqm_constants.h>
#include <vdqm.h>

void initlog(char *, int, char *);
int vdqm_shutdown, vdqm_restart;

int vdqm_main(struct main_args *main_args) {
    vdqmnw_t *nw, *nwtable;
    int rc, poolID;

    vdqm_shutdown = vdqm_restart = 0;

#if !defined(_WIN32)
    signal(SIGPIPE,SIG_IGN);
#endif /* _WIN32 */

    initlog("vdqm",LOG_INFO,VDQM_LOG_FILE);
#if defined(__DATE__) && defined (__TIME__)
    log(LOG_INFO,"******* VDQM server generated at %s %s.\n",
             __DATE__,__TIME__);
#endif /* __DATE__ && __TIME__ */
    rc = vdqm_InitNW(&nw);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_InitNw(): %s\n",neterror());
        return(vdqm_CleanUp(nw,1));
    }
    rc = vdqm_StartReplicaThread();
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_StartReplicaThread(): %s\n",sstrerror(serrno));
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
    rc = vdqm_InitQueueLock();
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_InitQueueLock(): %s\n",sstrerror(serrno));
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
        if ( vdqm_restart == 1 ) {
            vdqm_restart = 0;
            rc = vdqm_StartReplicaThread();
            if ( rc == -1 ) {
                log(LOG_ERR,"vdqm_StartReplicaThread(): %s\n",sstrerror(serrno));
                return(vdqm_CleanUp(nw,1));
            }
        }
    }
    log(LOG_INFO,"main:\n\n ******* VDQM SERVER EXIT ******\n\n");
    return(vdqm_CleanUp(nw,1));
}

int main() {

#if defined(_WIN32)
    if ( Cinitservice("vdqm",vdqm_main) == -1 ) exit(1);
#else /* _WIN32 */
    if ( Cinitdaemon("vdqm",NULL) == -1 ) exit(1);
    exit(vdqm_main(NULL));
#endif /* _WIN32 */
}
