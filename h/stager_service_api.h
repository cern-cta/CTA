/*
 * $Id*
 */

#ifndef __stager_service_h
#define __stager_service_h

#include "osdep.h"
#include <sys/types.h>
#ifndef _WIN32
#include <sys/time.h>
#else
#include <time.h>
#endif

/* Define one type of service per stager pool - please update file stager_service2name.h accordingly */
enum {
  STAGER_SERVICE_DB      = 0,
  STAGER_SERVICE_QUERY   = 1,
  STAGER_SERVICE_GETNEXT = 2,
  STAGER_SERVICE_JOB     = 3,
  STAGER_SERVICE_GC      = 4,

  STAGER_SERVICE_MAX     = 5        /* Nb of services - Do not forget to update this variable */
};

struct stagerService {
  int serviceType;
  int serviceTimeout;
  int (*selectFunc) _PROTO((void **));
  int (*processFunc) _PROTO((void *));
  int serviceSelectTimer;
  int serviceProcessTimer;
  int nbActiveThreads;
  int nbNotifyThreads;
  int notified;
  int notTheFirstTime;
};


EXTERN_C int DLL_DECL stager_serviceInit _PROTO(());
EXTERN_C int DLL_DECL stager_notifyServices _PROTO(());
EXTERN_C int DLL_DECL stager_notifyService _PROTO((int));
EXTERN_C int DLL_DECL stager_runService _PROTO((int, int,  int (*) _PROTO((void **)), int (*) _PROTO((void *))));
EXTERN_C int DLL_DECL stager_statService _PROTO((int, int,  int (*) _PROTO((void **)), int (*) _PROTO((void *))));

#endif /* __stager_service_h */
