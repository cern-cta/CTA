/*
 * $Id: stager_update_service.h,v 1.1 2004/11/01 11:34:47 jdurand Exp $
 */

#ifndef __stager_update_service_h
#define __stager_update_service_h

#include "osdep.h"

EXTERN_C int DLL_DECL stager_update_select _PROTO((void **));
EXTERN_C int DLL_DECL stager_update_process _PROTO((void *));

#endif /* __stager_update_service_h */
