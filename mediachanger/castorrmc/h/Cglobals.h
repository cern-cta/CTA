/*
 * $Id: Cglobals.h,v 1.8 2001/06/14 10:38:54 jdurand Exp $
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
EXTERN_C int DLL_DECL Cglobals_get _PROTO((int *, void **, size_t size));
EXTERN_C void DLL_DECL Cglobals_getTid _PROTO((int *));

#endif /* _CASTOR_GLOBALS_H */
