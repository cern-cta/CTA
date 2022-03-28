/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1990-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

/*
 * Castor_globals.c - central entry to maintain all Castor globals
 */

/*
 * All static and global variables in the Castor client part, i.e.
 * routines which can be called from within independent parts of
 * Castor itself as well as routines which are externalized to the
 * users, must be declared through this routine in order to
 * guarantee thread-safeness. The schema works if the calling
 * application is entirely based on Cthread, in which case the
 * Cglobals_init() routine is called from Cthread_init(). If
 * other thread packages are used, the application developer must
 * provide two routines similar to Cthread_getspecific() and
 * Cthread_setspecific() to be passed in a call to Cglobals_init().
 */

#include <stdlib.h>
#include <Cglobals.h>

/*
 * Define a structure to maintain global data in a single-threaded
 * environment.
 */
typedef struct Cglobals {
    void *addr;
    int *key;
} Cglobals_t;

/*
 * Internal static storage. Thread-safeness is guaranteed if
 * Cglobals_init() is called from Cthread_init() or at process
 * startup before any threads (except main) have been created.
 */
extern int (*local_getspec)(int *, void **);
extern int (*local_setspec)(int *, void *);
int (*local_getspec)(int *, void **) = NULL;
int (*local_setspec)(int *, void *) = NULL;
static int (*local_getTid)(void) = NULL;

static Cglobals_t **single_thread_globals = NULL;
static int nb_globals = 0;
/*
 * Single thread globals table allocation size.
 */
const int alloc_size = 1000;

/*
 * Castor errno externals (used as keys in multi-thread env.)
 * Note that we cannot include serrno.h to get the definitions
 * since it re-defines serrno if _REENTRANT.
 */
EXTERN_C int serrno;
int serrno;
EXTERN_C int rfio_errno;
int rfio_errno;
EXTERN_C int Copterr;
int Copterr;
EXTERN_C int Coptind;
int Coptind;
EXTERN_C int Coptopt;
int Coptopt;
EXTERN_C int Coptreset;
int Coptreset;
EXTERN_C char *Coptarg;
char *Coptarg;
EXTERN_C int Coptarg_key;
int Coptarg_key;
/*
 * Function prototypes for multi-thread env. version of errno externals
 */
int *C__serrno (void);
int *C__rfio_errno (void);
int *C__Copterr (void);
int *C__Coptind (void);
int *C__Coptopt (void);
int *C__Coptreset (void);
char **C__Coptarg (void);

/*
 * Cglobals_init() - assing routines to provide thread local storage.
 * Globals that existed prior to this call are all re-assigned as
 * thread-specific to calling thread (normally the main thread).
 */
void Cglobals_init(int (*getspec) (int *, void **),
		   int (*setspec) (int *, void *),
		   int (*getTid) (void))
{
    int i,rc;
    int *key;
    void *addr;

    if ( getspec != NULL && local_getspec == NULL ) local_getspec = getspec;
    if ( setspec != NULL && local_setspec == NULL ) local_setspec = setspec;
    if ( getTid  != NULL && local_getTid  == NULL ) local_getTid = getTid;
    if ( local_getspec != NULL  && local_setspec != NULL ) {
        if ( single_thread_globals != NULL ) {
            /*
             * Re-assign all old single-thread globals to thread
             * specific data for the calling thread.
             */
            for ( i=0; i<nb_globals; i++) {
                key = single_thread_globals[i]->key;
                rc = local_getspec(key,&addr);
                if ( rc == -1 || addr == NULL ) {
                    addr = single_thread_globals[i]->addr;
                    rc = local_setspec(key,addr);
                }
                free(single_thread_globals[i]);
                single_thread_globals[i] = NULL;
            }
            free(single_thread_globals);
        }
        /*
         * Inherit the current values of the errno globals.
         */
        *C__serrno() = serrno;
        *C__rfio_errno() = rfio_errno;
        *C__Copterr() = Copterr;
        *C__Coptind() = Coptind;
        *C__Coptopt() = Coptopt;
        *C__Coptreset() = Coptreset;
        *C__Coptarg() = Coptarg;

        single_thread_globals = NULL;
    }
    return;
}

/*
 * Cglobals_get() - simulate keyed accessed global storage in
 * single-thread environment and transparently call TLS routines
 * in multi-thread environment. This routine should be called for
 * every internal static storage in the Castor library.
 */
/*
 * Returns:       -1            on error
 *                 0            on success and not first call
 *                 1            on success and first call (e.g. alloc)
 */
int Cglobals_get(int *key,
                 void **addr,
                 size_t size)
{
    if ( key == NULL || addr == NULL || size<= (size_t)0 ) {
        return(-1);
    }
    if ( local_setspec == NULL ) {
        if ( *key <= 0 ) {
            Cglobals_t *tmp;

            if ((*addr = calloc(1,size)) == NULL) {
              return(-1);
            }
            if ( single_thread_globals == NULL ) {
                if ((single_thread_globals = (Cglobals_t **) malloc(sizeof(Cglobals_t *)*alloc_size)) == NULL) {
                  free(*addr);
                  *addr = NULL;
                  return(-1);
                }
            } else if ( nb_globals == alloc_size ) {
                Cglobals_t **dummy;

                if ((dummy =  (Cglobals_t **) realloc(single_thread_globals, sizeof(Cglobals_t *)*(nb_globals+alloc_size))) == NULL) {
                  free(*addr);
                  *addr = NULL;
                  return(-1);
                }
                single_thread_globals = dummy;
            }
            if ((tmp = (Cglobals_t *) malloc(sizeof(Cglobals_t))) == NULL) {
              free(*addr);
              *addr = NULL;
              return(-1);
            }
            tmp->addr = *addr;
            tmp->key = key;
            single_thread_globals[nb_globals] = tmp;
            nb_globals++;
            *key = nb_globals;
            return(1);
        } else {
            *addr = (void *) single_thread_globals[(*key) - 1]->addr;
            return(0);
        }
    } else {
        int rc;

        rc = local_getspec(key,addr);
        if ( (rc == -1) || (*addr == NULL) ) {
            if ((*addr = calloc(1,size)) == NULL) {
              return(-1);
            }
            if ((rc = local_setspec(key,*addr)) != 0) {
              free(*addr);
              *addr = NULL;
              return(-1);
            } else {
              return(1);
            }
        } else {
          return(0);
        }
    }
}

/*
 * Cglobals_getTid() - get current thread Id. -1 is returned
 * if Cglobals_init() has not been called. Note that in a Cthread
 * environment Tid = -1 will always refer to main thread since
 * Cthread_self() returns an error for the main thread (the main
 * thread has *NOT* been created with a Cthread_create()).
 *
 */
void Cglobals_getTid(int *Tid)
{
    if ( Tid == NULL ) return;
    if ( local_getTid == NULL ) *Tid = -1;
    else *Tid = local_getTid();
    return;
}

int *C__serrno() {
    int rc;
    void *addr;

    if ( local_setspec == NULL ) {
        return(&serrno);
    } else {
        addr = NULL;
        /*
         * We re-use the old single thread serrno as key
         */
        rc = local_getspec(&serrno,&addr);
        if ( rc == -1 || addr == NULL ) {
            addr = (int *)calloc(1,sizeof(int));
            rc = local_setspec(&serrno,addr);
        }
        /*
         * If memory allocation failed, we can still
         * return the external. This is obviously not
         * thread-safe but at least the application
         * will not die with a strange coredump in a
         * hidden de-reference of C__serrno().
         */
        if ( addr == NULL ) return(&serrno);
        else return(addr);
    }
}

int *C__rfio_errno() {
    int rc;
    void *addr;

    if ( local_setspec == NULL ) {
        return(&rfio_errno);
    } else {
        addr = NULL;
        /*
         * We re-use the old single thread rfio_errno as key
         */
        rc = local_getspec(&rfio_errno,&addr);
        if ( rc == -1 || addr == NULL ) {
            addr = (int *)calloc(1,sizeof(int));
            rc = local_setspec(&rfio_errno,addr);
        }
        /*
         * If memory allocation failed, we can still
         * return the external. This is obviously not
         * thread-safe but at least the application
         * will not die with a strange coredump in a
         * hidden de-reference of C__rfio_errno().
         */
        if ( addr == NULL ) return(&rfio_errno);
        else return(addr);
    }
}

int *C__Copterr() {
    int rc;
    void *addr;

    if ( local_setspec == NULL ) {
        return(&Copterr);
    } else {
        addr = NULL;
        /*
         * We re-use the old single thread Copterr as key
         */
        rc = local_getspec(&Copterr,&addr);
        if ( rc == -1 || addr == NULL ) {
            addr = (int *)calloc(1,sizeof(int));
            rc = local_setspec(&Copterr,addr);
        }
        /*
         * If memory allocation failed, we can still
         * return the external. This is obviously not
         * thread-safe but at least the application
         * will not die with a strange coredump in a
         * hidden de-reference of C_Copterr.
         */
        if ( addr == NULL ) return(&Copterr);
        else return(addr);
    }
}

int *C__Coptind() {
    int rc;
    void *addr;

    if ( local_setspec == NULL ) {
        return(&Coptind);
    } else {
        addr = NULL;
        /*
         * We re-use the old single thread rfio_errno as key
         */
        rc = local_getspec(&Coptind,&addr);
        if ( rc == -1 || addr == NULL ) {
            addr = (int *)calloc(1,sizeof(int));
            rc = local_setspec(&Coptind,addr);
        }
        /*
         * If memory allocation failed, we can still
         * return the external. This is obviously not
         * thread-safe but at least the application
         * will not die with a strange coredump in a
         * hidden de-reference of C__Coptind.
         */
        if ( addr == NULL ) return(&Coptind);
        else return(addr);
    }
}

int *C__Coptopt() {
    int rc;
    void *addr;

    if ( local_setspec == NULL ) {
        return(&Coptopt);
    } else {
        addr = NULL;
        /*
         * We re-use the old single thread rfio_errno as key
         */
        rc = local_getspec(&Coptopt,&addr);
        if ( rc == -1 || addr == NULL ) {
            addr = (int *)calloc(1,sizeof(int));
            rc = local_setspec(&Coptopt,addr);
        }
        /*
         * If memory allocation failed, we can still
         * return the external. This is obviously not
         * thread-safe but at least the application
         * will not die with a strange coredump in a
         * hidden de-reference of C__Coptopt.
         */
        if ( addr == NULL ) return(&Coptopt);
        else return(addr);
    }
}

int *C__Coptreset() {
    int rc;
    void *addr;

    if ( local_setspec == NULL ) {
        return(&Coptreset);
    } else {
        addr = NULL;
        /*
         * We re-use the old single thread rfio_errno as key
         */
        rc = local_getspec(&Coptreset,&addr);
        if ( rc == -1 || addr == NULL ) {
            addr = (int *)calloc(1,sizeof(int));
            rc = local_setspec(&Coptreset,addr);
        }
        /*
         * If memory allocation failed, we can still
         * return the external. This is obviously not
         * thread-safe but at least the application
         * will not die with a strange coredump in a
         * hidden de-reference of C__Coptreset.
         */
        if ( addr == NULL ) return(&Coptreset);
        else return(addr);
    }
}

char **C__Coptarg() {
    int rc;
    void *addr;

    if ( local_setspec == NULL ) {
        return(&Coptarg);
    } else {
        addr = NULL;
        /*
         * We re-use the old single thread rfio_errno as key
         */
        rc = local_getspec(&Coptarg_key,&addr);
        if ( rc == -1 || addr == NULL ) {
            addr = (char **)calloc(1,sizeof(char *));
            rc = local_setspec(&Coptarg_key,addr);
        }
        /*
         * If memory allocation failed, we can still
         * return the external. This is obviously not
         * thread-safe but at least the application
         * will not die with a strange coredump in a
         * hidden de-reference of C__Coptarg.
         */
        if ( addr == NULL ) return(&Coptarg);
        else return(addr);
    }
}
