/*
 * $Id: Cglobals.h,v 1.9 2001/06/29 05:04:23 baud Exp $
 */

/*
 * Copyright (C) 1999-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _CASTOR_GLOBALS_H
#define _CASTOR_GLOBALS_H    

#include <osdep.h>
#include <stddef.h>                 /* For size_t                    */

EXTERN_C void DLL_DECL Cglobals_init _PROTO((int (*) _PROTO((int *, void **)),
                                             int (*) _PROTO((int *, void *)),
                                             int (*) _PROTO((void))));
EXTERN_C int DLL_DECL Cglobals_get _PROTO((int *, void **, size_t size));
EXTERN_C void DLL_DECL Cglobals_getTid _PROTO((int *));

#endif /* _CASTOR_GLOBALS_H */
