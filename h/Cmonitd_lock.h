/*
 * $id$
 * Copyright (C) 2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* 
 * Cmonitd_lock.h - Header for the monitoring locking module  
 */
#include "semaphore.h"

#ifndef _CMONITD_LOCK_H_INCLUDED_
#define _CMONITD_LOCK_H_INCLUDED_

/* ---------------------------------------
 * Function declarations
 * --------------------------------------- */

/* Struct holding all the vars for the readers/writers locking mechanism */
struct Cthread_rwlock {
  Semaphore mutex1;
  Semaphore mutex2;
  Semaphore mutex3;
  Semaphore db;
  Semaphore r;
  int rcount;
  int wcount;
};

typedef struct Cthread_rwlock Cthread_rwlock_t;

/* ---------------------------------------
 * Function declarations
 * --------------------------------------- */

int Cthread_rwlock_init(Cthread_rwlock_t *lock);
int Cthread_rwlock_rdlock(Cthread_rwlock_t *lock);
int Cthread_rwlock_rdunlock(Cthread_rwlock_t *lock);
int Cthread_rwlock_wrlock(Cthread_rwlock_t *lock);
int Cthread_rwlock_wrunlock(Cthread_rwlock_t *lock);

#endif /*  _CMONITD_LOCK_H_INCLUDED_ */





