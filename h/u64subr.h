/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/*
 * @(#)$RCSfile: u64subr.h,v $ $Revision: 1.1 $ $Date: 2004/10/05 13:19:21 $ CERN/IT/ADC/CA Jean-Damien Durand
 */

#ifndef __u64subr_h
#define __u64subr_h

#include "osdep.h"

EXTERN_C char * DLL_DECL i64tostr _PROTO((HYPER, char *, int));
EXTERN_C char * DLL_DECL i64tohexstr _PROTO((HYPER, char *, int));
EXTERN_C U_HYPER DLL_DECL strtou64 _PROTO((CONST char *));
EXTERN_C U_HYPER DLL_DECL hexstrtou64 _PROTO((CONST char *));
EXTERN_C char * DLL_DECL u64tostr _PROTO((U_HYPER, char *, int));
EXTERN_C char * DLL_DECL u64tohexstr _PROTO((U_HYPER, char *, int));
EXTERN_C U_HYPER DLL_DECL strutou64 _PROTO((CONST char *));
EXTERN_C U_HYPER DLL_DECL hexstrutou64 _PROTO((CONST char *));
EXTERN_C char * DLL_DECL u64tostru _PROTO((U_HYPER, char *, int));

#endif /* __u64subr_h */
