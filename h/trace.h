/*
 * @(#)trace.h,v 1.7 2000/09/03 07:28:50 CERN IT-PDP/DM Frederic Hemmer
 */

/*
 * Copyright (C) 1990-2000 by CERN/CN/SW/DC
 * All rights reserved
 */

/* trace.h      tracing routines header                                 */

#ifndef _TRACE_H_INCLUDED_
#define _TRACE_H_INCLUDED_
#ifndef _SHIFT_H_INCLUDED_
#include <osdep.h>
#endif

EXTERN_C void DLL_DECL print_trace _PROTO((int, char *, char *, ...));
EXTERN_C void DLL_DECL init_trace _PROTO((char *));
EXTERN_C void DLL_DECL end_trace _PROTO((void));
EXTERN_C void DLL_DECL print_trace_r _PROTO((void *, int, char *, char *, ...));
EXTERN_C void DLL_DECL init_trace_r _PROTO((void **, char *));
EXTERN_C void DLL_DECL end_trace_r _PROTO((void *));

EXTERN_C int  DLL_DECL notrace;

#define TRACE           if ( !notrace ) print_trace
#define INIT_TRACE      if ( !notrace ) init_trace
#define END_TRACE       if ( !notrace ) end_trace
#define NOTRACE         {notrace = 1;}

#endif /* _TRACE_H_INCLUDED_  */
