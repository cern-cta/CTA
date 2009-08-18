/*
 * $Id: stager_util.h,v 1.4 2009/08/18 09:43:00 waldron Exp $
 */

#ifndef stager_util_h
#define stager_util_h

#include <sys/types.h>
#include <time.h>
#include <stdarg.h>
#include "osdep.h"
#include "Cns_api.h"

EXTERN_C void DLL_DECL stager_log _PROTO((const char *, const char *, int, int, struct Cns_fileid *, ...));

#endif /* stager_api_h */
