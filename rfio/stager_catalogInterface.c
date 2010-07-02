/*
 * $Id: stager_catalogInterface.c,v 1.2 2008/07/31 07:09:14 sponcec3 Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/*
 * Credits go to Olof Barring - I cut/pasted his original well-done catalog interface */

/* ============== */
/* System headers */
/* ============== */
#include <errno.h>

/* ============= */
/* Local headers */
/* ============= */
#include "serrno.h"
#include "Cglobals.h"
#include "stager_macros.h"
#include "stager_catalogInterface.h"

/* ---------------------------------------------------- */
/* stager_getDbSvc                                      */
/*                                                      */
/* Purpose: Create/get the DB service                   */
/*                                                      */
/* Input:  (struct C_Services_t ***) dbSvc [DB service] */
/*                                                      */
/* Output: Fills dbSvc                                  */
/* ---------------------------------------------------- */
int stager_getDbSvc(dbSvc)
     struct C_Services_t ***dbSvc;
{
  char *func = "stager_getDbSvc";
  struct C_Services_t **svc;
  int rc;
  static int svcsKey = -1;        /* Used by Cglobals_get */

  STAGER_LOG_ENTER();

  if (dbSvc == NULL) {
    serrno = EFAULT;
    rc = -1;
    goto stager_getDbSvcReturn;
  }
  svc = NULL;
  rc = Cglobals_get(&svcsKey,(void **)&svc,sizeof(struct C_Services_t **));
  if ((rc == -1) || (svc == NULL)) {
    serrno = SEINTERNAL;
    STAGER_LOG_ERROR(NULL,"Cglobals_get");
    rc = -1;
    goto stager_getDbSvcReturn;
  }

  if ( *svc == NULL ) {
    if (C_Services_create(svc) < 0) {
      STAGER_LOG_ERROR(NULL,"C_Services_create");
      goto stager_getDbSvcReturn;
    }
  }
  *dbSvc = svc;

  rc = 0;

 stager_getDbSvcReturn:
  STAGER_LOG_RETURN(rc);
}

/* ----------------------------------------------------------------- */
/* stager_getStgAndDbSvc                                             */
/*                                                                   */
/* Purpose: Create/get the STAGER service inside a DB service        */
/*                                                                   */
/* Input:  (struct Cstager_IStagerSvc_t ***) stgSvc [STAGER service] */
/*         (struct C_Services_t ***) dbSvc [DB service]              */
/*                                                                   */
/* Output: Fills stgSvc                                              */
/* ----------------------------------------------------------------- */
int stager_getStgAndDbSvc(stgSvc,dbSvc)
     struct Cstager_IStagerSvc_t ***stgSvc;
     struct C_Services_t ***dbSvc;
{
  char *func = "stager_getStgAndDbSvc";
  struct C_Services_t **svcs = NULL;
  struct C_IService_t *iSvc = NULL;
  struct Cstager_IStagerSvc_t **svc;
  static int SvcsKey = -1;        /* Used by Cglobals_get */
  int rc;

  STAGER_LOG_ENTER();

  if ((stgSvc == NULL) || (dbSvc == NULL)) {
    serrno = EFAULT;
    rc = -1;
    goto stager_getStgSvcReturn;
  }
  svc = NULL;
  rc = Cglobals_get(&SvcsKey,(void **)&svc,sizeof(struct Cstager_IStagerSvc_t **));
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
    rc = C_Services_service(*svcs,"DbStagerSvc",SVC_DBSTAGERSVC, &iSvc);
    if ((rc == -1) || (iSvc == NULL)) {
      serrno = SEINTERNAL;
      STAGER_LOG_DB_ERROR(NULL,"C_Services_service",C_Services_errorMsg(*svcs));
      rc = -1;
      goto stager_getStgSvcReturn;
    }
    *svc = Cstager_IStagerSvc_fromIService(iSvc);
  } else {
    if (stager_getDbSvc(&svcs) < 0) {
      rc = -1;
      goto stager_getStgSvcReturn;
    }
  }

  *stgSvc = svc;
  *dbSvc = svcs;

  rc = 0;

 stager_getStgSvcReturn:
  STAGER_LOG_RETURN(rc);
}


/* ----------------------------------------------------------------- */
/* stager_getJobSvc                                                  */
/*                                                                   */
/* Purpose: Create/get the JOB service                               */
/*                                                                   */
/* Input:  (struct Cstager_IJobSvc_t ***) jobSvc                  */
/*                                                                   */
/*                                                                   */
/* Output: Fills jobSvc                                              */
/* ----------------------------------------------------------------- */


int stager_getJobSvc(jobSvc)
     struct Cstager_IJobSvc_t ***jobSvc;

{
  char *func = "stager_getJobSvc";
  struct C_Services_t **svcs = NULL;
  struct C_IService_t *iSvc = NULL;
  struct Cstager_IJobSvc_t **svc;
  static int SvcsKey = -1;        /* Used by Cglobals_get */
  int rc;

  STAGER_LOG_ENTER();

  if ((jobSvc == NULL)) {
    serrno = EFAULT;
    rc = -1;
    goto stager_getJobSvcReturn;
  }
  svc = NULL;
  rc = Cglobals_get(&SvcsKey,(void **)&svc,sizeof(struct Cstager_IJobSvc_t **));
  if ((rc == -1) || (svc == NULL)) {
    STAGER_LOG_ERROR(NULL,"Cglobals_get");
    rc = -1;
    goto stager_getJobSvcReturn;
  }

  if ( *svc == NULL ) {
    if (stager_getDbSvc(&svcs) < 0) {
      rc = -1;
      goto stager_getJobSvcReturn;
    }
    rc = C_Services_service(*svcs,"DbJobSvc",SVC_DBJOBSVC, &iSvc);
    if ((rc == -1) || (iSvc == NULL)) {
      serrno = SEINTERNAL;
      STAGER_LOG_DB_ERROR(NULL,"C_Services_service",C_Services_errorMsg(*svcs));
      rc = -1;
      goto stager_getJobSvcReturn;
    }
    *svc = Cstager_IJobSvc_fromIService(iSvc);
  } else {
    if (stager_getDbSvc(&svcs) < 0) {
      rc = -1;
      goto stager_getJobSvcReturn;
    }
  }

  *jobSvc = svc;


  rc = 0;

 stager_getJobSvcReturn:
  STAGER_LOG_RETURN(rc);
}
