/*
 * $Id: stager_util.h,v 1.2 2004/12/09 09:38:07 jdurand Exp $
 */

#ifndef stager_util_h
#define stager_util_h

#include <sys/types.h>
#include <time.h>
#include <stdarg.h>
#include "osdep.h"
#include "Cns_api.h"

EXTERN_C void DLL_DECL stager_util_time _PROTO((time_t, char *));
EXTERN_C void DLL_DECL stager_log _PROTO((char *, char *, int, int, struct Cns_fileid *, ...));

#endif /* stager_api_h */
