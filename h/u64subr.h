/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/*
 * @(#)$RCSfile: u64subr.h,v $ $Revision: 1.2 $ $Date: 2008/05/05 08:51:59 $ CERN/IT/ADC/CA Jean-Damien Durand
 */

#ifndef __u64subr_h
#define __u64subr_h

#include "osdep.h"

EXTERN_C char * i64tostr _PROTO((HYPER, char *, int));
EXTERN_C char * i64tohexstr _PROTO((HYPER, char *, int));
EXTERN_C U_HYPER strtou64 _PROTO((CONST char *));
EXTERN_C U_HYPER hexstrtou64 _PROTO((CONST char *));
EXTERN_C char * u64tostr _PROTO((U_HYPER, char *, int));
EXTERN_C char * u64tohexstr _PROTO((U_HYPER, char *, int));
EXTERN_C U_HYPER strutou64 _PROTO((CONST char *));
EXTERN_C U_HYPER hexstrutou64 _PROTO((CONST char *));
EXTERN_C char * u64tostru _PROTO((U_HYPER, char *, int));
EXTERN_C char * u64tostrsi _PROTO((U_HYPER, char *, int));

#endif /* __u64subr_h */
