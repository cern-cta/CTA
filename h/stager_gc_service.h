/*
 * $Id: stager_gc_service.h,v 1.1 2005/02/09 17:05:36 sponcec3 Exp $
 */

#ifndef __stager_gc_service_h
#define __stager_gc_service_h

#include "osdep.h"

EXTERN_C int DLL_DECL stager_gc_select _PROTO((void **));
EXTERN_C int DLL_DECL stager_gc_process _PROTO((void *));

#endif /* __stager_gc_service_h */
