/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "$RCSfile: Csemaphore.c,v $ $Revision: 1.2 $ $Date: 2004/02/16 13:32:41 $ CERN/IT/PDP/DM";
#endif /* not lint */

/*//////////////////////////////////////////////////////////////////////
// File: Csemaphore.c
// --------------------------------------------------------------------
// This is a POSIX threads semaphore implementation.  See the docs
// at http://dis.cs.umass.edu/~wagner/threads_html/tutorial.html
// for information on POSIX threads and how to use this lib.
// --------------------------------------------------------------------
// Written by: Tom A. Wagner
//             wagner@cs.umass.edu
//             Multi-Agent Systems Lab
//             Department of Computer and Information Science
//             University of Massachusetts
//             Amherst, Massachusetts 01003.
//
// This code was written at the Multi-Agent Systems Lab. at the 
// Department of Computer Science, University of Massachusetts, 
// Amherst, MA 01003.
//
// Copyright (c) 1996 UMASS CS Dept. All rights are reserved.
//
// Development of this code was partially supported by:
//    ONR grant N00014-92-J-1450
//    NSF grant IRI-9523419
//    DARPA grant, RaDEO program, 70NANB6H0074 as subcontractor for 
//          Boeing Helicoptor
////////////////////////////////////
//
// */

#include "Csemaphore.h"


/*
 * function must be called prior to semaphore use.
 *
 */
void
Csemaphore_init (CSemaphore * s,int value)
{
    s->v = value;
}

/*
 * function should be called when there is no longer a need for
 * the semaphore.
 *
 */
void
Csemaphore_destroy (CSemaphore * s)
{
    if (Cthread_mutex_destroy ((s)) == -1)
     do_error ("Error destroying Csemaphore mutex");
}

/*
 * function increments the semaphore and signals any threads that
 * are blocked waiting a change in the semaphore.
 *
 */
int
Csemaphore_up (CSemaphore * s)
{
    int         value_after_op;

    tw_pthread_mutex_lock ((void *)(s));

    (s->v)++;
    value_after_op = s->v;

    tw_pthread_mutex_unlock ((void *)(s));
    tw_pthread_cond_signal ((void *)(s));

    return( value_after_op );
}

/*
 * function decrements the semaphore and blocks if the semaphore is
 * <= 0 until another thread signals a change.
 *
 */
int
Csemaphore_down (CSemaphore * s)
{
    int         value_after_op;

    tw_pthread_mutex_lock ((void *)(s));
    while (s->v <= 0)
    {
     tw_pthread_cond_wait ((void *)(s));
    }

    (s->v)--;
    value_after_op = s->v;

    tw_pthread_mutex_unlock ((void *)(s));

    return (value_after_op);
}

/*
 * function does NOT block but simply decrements the semaphore.
 * should not be used instead of down -- only for programs where
 * multiple threads must up on a semaphore before another thread
 * can go down, i.e., allows programmer to set the semaphore to
 * a negative value prior to using it for synchronization.
 *
 */
int
Csemaphore_decrement (CSemaphore * s)
{
    int         value_after_op;

    tw_pthread_mutex_lock ((void *)(s));
    s->v--;
    value_after_op = s->v;
    tw_pthread_mutex_unlock ((void *)(s));

    return (value_after_op);
}

/*
 * function returns the value of the semaphore at the time the
 * critical section is accessed.  obviously the value is not guarenteed
 * after the function unlocks the critical section.  provided only
 * for casual debugging, a better approach is for the programmar to
 * protect one semaphore with another and then check its value.
 * an alternative is to simply record the value returned by semaphore_up
 * or semaphore_down.
 *
 */
int
Csemaphore_value (CSemaphore * s)
{
    /* not for sync */
    int         value_after_op;

    tw_pthread_mutex_lock ((void *)(s));
    value_after_op = s->v;
    tw_pthread_mutex_unlock ((void *)(s));

    return (value_after_op);
}



/* -------------------------------------------------------------------- */
/* The following functions replace standard library functions in that   */
/* they exit on any error returned from the system calls.  Saves us     */
/* from having to check each and every call above.                      */
/* -------------------------------------------------------------------- */


int
tw_pthread_mutex_unlock(void *m)
{
    int         return_value;

    if ((return_value = Cthread_mutex_unlock (m)) == -1)
     do_error ("Cthread_mutex_unlock");

    return (return_value);
}

int
tw_pthread_mutex_lock (void * m)
{
    int         return_value;

    if ((return_value = Cthread_mutex_lock (m)) == -1)
     do_error ("Cthread_mutex_lock");

    return (return_value);
}

int
tw_pthread_cond_wait (void * c)
{
    int         return_value;

    if ((return_value = Cthread_cond_wait (c)) == -1)
     do_error ("Cthread_cond_wait");

    return (return_value);
}

int
tw_pthread_cond_signal (void * c)
{
    int         return_value;

    if ((return_value = Cthread_cond_signal (c)) == -1)
     do_error ("pthread_cond_signal");

    return (return_value);
}


/*
 * function just prints an error message and exits 
 *
 */
void
do_error (char *msg)
{
    perror (msg);
    exit (1);
}
