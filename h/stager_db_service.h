/*
 * $Id: stager_db_service.h,v 1.1 2004/10/28 17:38:29 jdurand Exp $
 */

#ifndef __stager_db_service_h
#define __stager_db_service_h

#include "osdep.h"

EXTERN_C int DLL_DECL stager_db_select _PROTO((void **));
EXTERN_C int DLL_DECL stager_db_process _PROTO((void *));

#endif /* __stager_db_service_h */
