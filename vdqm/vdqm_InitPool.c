/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqm_InitPool.c,v $ $Revision: 1.6 $ $Date: 2000/02/28 17:59:01 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqm_InitPool.c - Initialise the VDQM thread pool (server only).
 */

#if defined(_WIN32)
#include <winsock2.h>    /* Needed for SOCKET definition */
#endif /* _WIN32 */

#if defined(VDQMSERV)
#if !defined(_WIN32)
#include <regex.h>
#else /* _WIN32 */
typedef void * regex_t
#endif /* _WIN32 */
#endif /* VDQMSERV */

#include <stdlib.h>
#include <errno.h>
#include <Castor_limits.h>
#include <osdep.h>
#include <net.h>
#include <log.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <Cthread_api.h>
#include <Cpool_api.h>

int vdqm_InitPool(vdqmnw_t **nwtable) {
    extern char *getenv();
    extern char *getconfent();
    char *p;
    int poolsize,rc;
    
    if ( (p = getenv("VDQM_THREAD_POOL")) != (char *)NULL ) {
        poolsize = atoi(p);
    } else if ( ( p = getconfent("VDQM","THREAD_POOL",0)) != (char *)NULL ) {
        poolsize = atoi(p);
    } else {
#if defined(VDQM_THREAD_POOL)
        poolsize = VDQM_THREAD_POOL;
#else /* VDQM_THREAD_POOL */
        poolsize = 10;         /* Set some reasonable default */
#endif /* VDQM_TRHEAD_POOL */
    }
    
    rc = Cpool_create(poolsize,&poolsize);
    log(LOG_INFO,"vdqm_InitPool() thread pool (id=%d): pool size = %d\n",
        rc,poolsize);
    *nwtable = (vdqmnw_t *)malloc(poolsize * sizeof(vdqmnw_t));
    if ( *nwtable == NULL ) {
      log(LOG_ERR,"vdqm_InitPool() malloc(): %s\n",sstrerror(errno));
      return(-1);
    }
    return(rc);
}
