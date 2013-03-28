/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/*
 */

#ifndef __u64subr_h
#define __u64subr_h

#include "osdep.h"

EXTERN_C char * i64tostr (HYPER, char *, int);
EXTERN_C char * i64tohexstr (HYPER, char *, int);
EXTERN_C U_HYPER strtou64 (const char *);
EXTERN_C U_HYPER hexstrtou64 (const char *);
EXTERN_C char * u64tostr (U_HYPER, char *, int);
EXTERN_C char * u64tohexstr (U_HYPER, char *, int);
EXTERN_C U_HYPER strutou64 (const char *);
EXTERN_C U_HYPER hexstrutou64 (const char *);
EXTERN_C char * u64tostru (U_HYPER, char *, int);
EXTERN_C char * u64tostrsi (U_HYPER, char *, int);

#endif /* __u64subr_h */
