/*
 * $Id: Cmutex.h,v 1.1 2000/07/07 11:06:25 jdurand Exp $
 */

/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _CASTOR_MUTEX_H
#define _CASTOR_MUTEX_H    

#include "osdep_trunk_r21843.h"

EXTERN_C void Cmutex_init (int (*) (void *, int),
			   int (*) (void *));
EXTERN_C int Cmutex_lock (void *, int);
EXTERN_C int Cmutex_unlock (void *);

#define Cmutex_trylock(addr) Cmutex_lock(addr,0)

#endif /* _CASTOR_MUTEX_H */

