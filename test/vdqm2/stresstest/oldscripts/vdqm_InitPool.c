#include <stdlib.h>
#include <errno.h>
#include <Castor_limits.h>
#include <net.h>
#include <log.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <Cthread_api.h>
#include <Cpool_api.h>

#if !defined(linux)
extern char *sys_errlist[];
#else /* linux */
#include <stdio.h>   /* Contains definition of sys_errlist[] */
#endif /* linux */

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
	poolsize = 10;            /* Set some reasonable default */
#endif /* VDQM_TRHEAD_POOL */
  }

  rc = Cpool_create(poolsize,&poolsize);
  log(LOG_INFO,"vdqm_InitPool() thread pool (id=%d): pool size = %d\n",
	rc,poolsize);
  *nwtable = (vdqmnw_t *)malloc(poolsize * sizeof(vdqmnw_t));
  if ( *nwtable == NULL ) return(-1);
  return(rc);
}
