/*
 * $Id: stager_util.h,v 1.3 2007/12/07 11:40:53 sponcec3 Exp $
 */

#ifndef stager_util_h
#define stager_util_h

#include <sys/types.h>
#include <time.h>
#include <stdarg.h>
#include "osdep.h"
#include "Cns_api.h"

EXTERN_C void DLL_DECL stager_util_time _PROTO((time_t, char *));
EXTERN_C void DLL_DECL stager_log _PROTO((const char *, const char *, int, int, struct Cns_fileid *, ...));

#endif /* stager_api_h */
