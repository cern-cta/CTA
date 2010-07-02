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

int (*local_mutex_lock) (void *, int);
int (*local_mutex_unlock) (void *);

void Cmutex_init(int (*mutex_lock) (void *, int),
		 int (*mutex_unlock) (void *))
{
    if ( mutex_lock != NULL && local_mutex_lock == NULL ) local_mutex_lock = mutex_lock;
    if ( mutex_unlock != NULL && local_mutex_unlock == NULL ) local_mutex_unlock = mutex_unlock;
    return;    
}    

int Cmutex_lock(addr,timeout)
     void *addr;
     int timeout;
{
  return(local_mutex_lock != NULL ? local_mutex_lock(addr,timeout) : 0);
}

int Cmutex_unlock(addr)
     void *addr;
{
  return(local_mutex_unlock != NULL ? local_mutex_unlock(addr) : 0);
}
