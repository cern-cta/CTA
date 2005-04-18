/*
 * $Id: stager_error_service.h,v 1.1 2005/04/18 14:16:38 jdurand Exp $
 */

#ifndef __stager_error_service_h
#define __stager_error_service_h

#include "osdep.h"

EXTERN_C int DLL_DECL stager_error_select _PROTO((void **));
EXTERN_C int DLL_DECL stager_error_process _PROTO((void *));

#endif /* __stager_error_service_h */
