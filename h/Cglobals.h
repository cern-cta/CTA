/*
 * $Id: Cglobals.h,v 1.7 2000/07/07 10:56:27 jdurand Exp $
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
                                             int (*) _PROTO((void))));
EXTERN_C void DLL_DECL Cglobals_get _PROTO((int *, void **, size_t size));
EXTERN_C void DLL_DECL Cglobals_getTid _PROTO((int *));

#endif /* _CASTOR_GLOBALS_H */
