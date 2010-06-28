#include <stdlib.h>
#include <errno.h>
#include <Castor_limits.h>
#include <log.h>
#include <net.h>
extern char *geterr();
#include <vdqm_constants.h>
#include <vdqm.h>

#if !defined(linux)
extern char *sys_errlist[];
#else /* linux */
#include <stdio.h> /* Contains def. of sys_errlist[] */
#endif /* linux */

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
		log(LOG_ERR,"vdqm_InitNw(): %s\n",NWERRTXT);
		return(vdqm_CleanUp(nw,1));
	}
	rc = vdqm_InitPool(&nwtable);
	if ( rc == -1 ) {
		log(LOG_ERR,"vdqm_InitPool(): %s\n",ERRTXT);
		return(vdqm_CleanUp(nw,1));
	}
	poolID = rc;
    vdqm_shutdown = 0;
	
	for (;;) {
		rc = vdqm_Listen(nw);
        if ( vdqm_shutdown ) break;
		if ( rc == -1 ) {
			log(LOG_ERR,"vdqm_Listen(): %s\n",NWERRTXT);
			continue;
		}
		rc = vdqm_GetPool(poolID,nw,nwtable);
		if ( rc == -1 ) {
			log(LOG_ERR,"vdqm_GetPool(): %s\n",ERRTXT);
			break;
		}
	}
    log(LOG_INFO,"main:\n\n ******* VDQM SERVER EXIT ******\n\n");
	return(vdqm_CleanUp(nw,1));
}
