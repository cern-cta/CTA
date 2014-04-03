/*
 * $Id: stager_extern_globals.h,v 1.2 2009/07/13 06:22:08 waldron Exp $
 */

/* Stager extern variables needed definitions because they are used in the global library */

#pragma once

#include "osdep.h"
#include "Cuuid.h"

extern int     stagerTrace;
extern int     stagerDebug;
extern char    *stagerLog;
extern Cuuid_t stagerUuid;

