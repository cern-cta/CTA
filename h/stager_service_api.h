/*
 * $Id*
 */

#ifndef __stager_service_h
#define __stager_service_h

#include "osdep.h"

struct stagerService {
  int serviceType;
  int serviceTimeout;
  int (*serviceSelect) _PROTO((void **));
  int (*serviceProcess) _PROTO((void *));
  int serviceSelectTimer;
  int serviceProcessTimer;
};


EXTERN_C int DLL_DECL stager_serviceInit _PROTO(());
EXTERN_C int DLL_DECL stager_notifyServices _PROTO(());
EXTERN_C int DLL_DECL stager_notifyService _PROTO((int));
EXTERN_C int DLL_DECL stager_runService _PROTO((int, int,  int (*) _PROTO((void **)), int (*) _PROTO((void *))));
EXTERN_C int DLL_DECL stager_statService _PROTO((int, int,  int (*) _PROTO((void **)), int (*) _PROTO((void *))));

#endif /* __stager_service_h */
