/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdio.h>
#include <Cthread_api.h>

#ifndef SEMAPHORES
#define SEMAPHORES

typedef struct Semaphore
{
    int         v;
}
Semaphore;


int         semaphore_down (Semaphore * s);
int         semaphore_up (Semaphore * s);
void        semaphore_destroy (Semaphore * s);
void        semaphore_init (Semaphore * s,int v);
int         semaphore_value (Semaphore * s);
int         tw_pthread_cond_signal (void * c);
int         tw_pthread_cond_wait (void * c);
int         tw_pthread_mutex_unlock (void * m);
int         tw_pthread_mutex_lock (void * m);
void        do_error (char *msg);

#endif
