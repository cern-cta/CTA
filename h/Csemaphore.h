/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdio.h>
#include <Cthread_api.h>

#ifndef CSEMAPHORES
#define CSEMAPHORES

typedef struct CSemaphore
{
    int         v;
}
CSemaphore;


int         Csemaphore_down (CSemaphore * s);
int         Csemaphore_up (CSemaphore * s);
void        Csemaphore_destroy (CSemaphore * s);
void        Csemaphore_init (CSemaphore * s,int v);
int         Csemaphore_value (CSemaphore * s);
int         tw_pthread_cond_signal (void * c);
int         tw_pthread_cond_wait (void * c);
int         tw_pthread_mutex_unlock (void * m);
int         tw_pthread_mutex_lock (void * m);
void        do_error (char *msg);

#endif /* CSEMAPHORES */
