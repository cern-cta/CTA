/*
 * $Id: Cglobals.h,v 1.5 2000/03/14 09:49:06 jdurand Exp $
 * $Log: Cglobals.h,v $
 * Revision 1.5  2000/03/14 09:49:06  jdurand
 * Fixes for _WIN32 compilation
 *
 * Revision 1.4  1999-11-21 15:19:51+01  obarring
 * Include osdep.h to get DLL_DECL/EXPORT_C/_PROTO macros
 *
 * Revision 1.3  1999/07/30 16:00:12  obarring
 * Correct the copyright notice ....
 *
 * Revision 1.2  1999/07/30 15:59:10  obarring
 * Add copyright notice
 *
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _CASTOR_GLOBALS_H
#define _CASTOR_GLOBALS_H    

#include <osdep.h>

EXTERN_C void DLL_DECL Cglobals_init _PROTO((int (*) _PROTO((int *, void **)),
                                             int (*) _PROTO((int *, void *)),
                                             int (*) _PROTO(())));
EXTERN_C void DLL_DECL Cglobals_get _PROTO((int *, void **, size_t size));
EXTERN_C void DLL_DECL Cglobals_getTid _PROTO((int *));

#endif /* _CASTOR_GLOBALS_H */
