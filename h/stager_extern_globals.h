/*
 * $Id: stager_extern_globals.h,v 1.1 2004/11/30 15:43:55 jdurand Exp $
 */

/* Stager extern variables needed definitions because they are used in the global library */

#ifndef __stager_extern_globals_h
#define __stager_extern_globals_h

#include "osdep.h"
#include "Cuuid.h"

extern int     stagerNoDlf;
extern int     stagerTrace;
extern int     stagerDebug;
extern char    *stagerLog;
extern Cuuid_t stagerUuid;

#endif /* __stager_extern_globals_h */
