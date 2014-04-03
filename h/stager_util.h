/*
 * $Id: stager_util.h,v 1.4 2009/08/18 09:43:00 waldron Exp $
 */

#pragma once

#include <sys/types.h>
#include <time.h>
#include <stdarg.h>
#include "osdep.h"
#include "Cns_api.h"

EXTERN_C void stager_log (const char *, const char *, int, int, struct Cns_fileid *, ...);

