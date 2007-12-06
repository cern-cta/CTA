/*
 * $Id: Cmutex.c,v 1.2 2007/12/06 14:24:46 sponcec3 Exp $
 */

/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * Cmutex.c - central entry to get and release all Castor mutexes
 */

#include <stdlib.h>
#include <Cmutex.h>

int (*local_mutex_lock) _PROTO((void *, int));
int (*local_mutex_unlock) _PROTO((void *));

void DLL_DECL Cmutex_init(mutex_lock,mutex_unlock)
    int (*mutex_lock) _PROTO((void *, int));
    int (*mutex_unlock) _PROTO((void *));
{
    if ( mutex_lock != NULL && local_mutex_lock == NULL ) local_mutex_lock = mutex_lock;
    if ( mutex_unlock != NULL && local_mutex_unlock == NULL ) local_mutex_unlock = mutex_unlock;
    return;    
}    

int DLL_DECL Cmutex_lock(addr,timeout)
     void *addr;
     int timeout;
{
  return(local_mutex_lock != NULL ? local_mutex_lock(addr,timeout) : 0);
}

int DLL_DECL Cmutex_unlock(addr)
     void *addr;
{
  return(local_mutex_unlock != NULL ? local_mutex_unlock(addr) : 0);
}
