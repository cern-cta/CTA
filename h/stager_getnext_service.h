/*
 * $Id: stager_getnext_service.h,v 1.1 2004/11/01 11:34:47 jdurand Exp $
 */

#ifndef __stager_getnext_service_h
#define __stager_getnext_service_h

#include "osdep.h"

EXTERN_C int DLL_DECL stager_getnext_select _PROTO((void **));
EXTERN_C int DLL_DECL stager_getnext_process _PROTO((void *));

#endif /* __stager_getnext_service_h */
