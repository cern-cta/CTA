/*
 * $Id: Cmutex.h,v 1.1 2000/07/07 11:06:25 jdurand Exp $
 */

/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _CASTOR_MUTEX_H
#define _CASTOR_MUTEX_H    

#include <osdep.h>

EXTERN_C void DLL_DECL Cmutex_init _PROTO((int (*) _PROTO((void *, int)),
                                           int (*) _PROTO((void *))));
EXTERN_C int DLL_DECL Cmutex_lock _PROTO((void *, int));
EXTERN_C int DLL_DECL Cmutex_unlock _PROTO((void *));

#define Cmutex_trylock(addr) Cmutex_lock(addr,0)

#endif /* _CASTOR_MUTEX_H */

