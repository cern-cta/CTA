/*
 * $Id: stager_job_service.h,v 1.1 2004/11/24 14:11:49 jdurand Exp $
 */

#ifndef __stager_job_service_h
#define __stager_job_service_h

#include "osdep.h"

EXTERN_C int DLL_DECL stager_job_select _PROTO((void **));
EXTERN_C int DLL_DECL stager_job_process _PROTO((void *));

#endif /* __stager_job_service_h */
