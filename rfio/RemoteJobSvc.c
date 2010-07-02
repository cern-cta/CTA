
/*
 * $Id: RemoteJobSvc.c,v 1.1 2008/07/28 16:23:57 waldron Exp $
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdlib.h>
#include <errno.h>
#include "serrno.h"
#include "stager_macros.h"
#include "RemoteJobSvc.h"
#include "Cglobals.h"
#include "stager_catalogInterface.h"

/* ----------------------------------------------------------------- */
/* stager_getRemJobAndDbSvc                                          */
/*                                                                   */
/* Purpose: Create/get the REMOTE JOB service inside a DB service    */
/*                                                                   */
/* Input:  (struct Cstager_IJobSvc_t ***) jobSvc [JOB service]       */
/*         (struct C_Services_t ***) dbSvc [DB service]              */
/*                                                                   */
/* Output: Fills stgSvc                                              */
/* ----------------------------------------------------------------- */
int stager_getRemJobAndDbSvc(jobSvc,dbSvc)
     struct Cstager_IJobSvc_t ***jobSvc;
     struct C_Services_t ***dbSvc;
{
  char *func = "stager_getRemJobAndDbSvc";
  struct C_Services_t **svcs = NULL;
  struct C_IService_t *iSvc = NULL;
  struct Cstager_IJobSvc_t **svc;
  static int SvcsKey = -1;        /* Used by Cglobals_get */
  int rc;

  STAGER_LOG_ENTER();

  if ((jobSvc == NULL) || (dbSvc == NULL)) {
    serrno = EFAULT;
    rc = -1;
    goto stager_getStgSvcReturn;
  }
  svc = NULL;
  rc = Cglobals_get(&SvcsKey,(void **)&svc,sizeof(struct Cstager_IJobSvc_t **));
  if ((rc == -1) || (svc == NULL)) {
    STAGER_LOG_ERROR(NULL,"Cglobals_get");
    rc = -1;
    goto stager_getStgSvcReturn;
  }

  if ( *svc == NULL ) {
    if (stager_getDbSvc(&svcs) < 0) {
      rc = -1;
      goto stager_getStgSvcReturn;
    }
    rc = C_Services_service(*svcs,"RemoteJobSvc",SVC_REMOTEJOBSVC, &iSvc);
    if ((rc == -1) || (iSvc == NULL)) {
      serrno = SEINTERNAL;
      STAGER_LOG_DB_ERROR(NULL,"C_Services_service",C_Services_errorMsg(*svcs));
      rc = -1;
      goto stager_getStgSvcReturn;
    }
    *svc = Cstager_IJobSvc_fromIService(iSvc);
  } else {
    if (stager_getDbSvc(&svcs) < 0) {
      rc = -1;
      goto stager_getStgSvcReturn;
    }
  }

  *jobSvc = svc;
  *dbSvc = svcs;

  rc = 0;

 stager_getStgSvcReturn:
  STAGER_LOG_RETURN(rc);
}
