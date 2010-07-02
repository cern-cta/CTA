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

EXTERN_C char * i64tostr (HYPER, char *, int);
EXTERN_C char * i64tohexstr (HYPER, char *, int);
EXTERN_C U_HYPER strtou64 (CONST char *);
EXTERN_C U_HYPER hexstrtou64 (CONST char *);
EXTERN_C char * u64tostr (U_HYPER, char *, int);
EXTERN_C char * u64tohexstr (U_HYPER, char *, int);
EXTERN_C U_HYPER strutou64 (CONST char *);
EXTERN_C U_HYPER hexstrutou64 (CONST char *);
EXTERN_C char * u64tostru (U_HYPER, char *, int);
EXTERN_C char * u64tostrsi (U_HYPER, char *, int);

#endif /* __u64subr_h */
