/*
 * $Id: Cglobals.c,v 1.2 1999/07/20 15:09:33 obarring Exp $
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
static int (*local_getspec)(int *, void **) = NULL;
static int (*local_setspec)(int *, void *) = NULL;
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
extern int serrno;
int serrno;
extern int rfio_errno;
int rfio_errno;
/*
 * Function prototypes for multi-thread env. version of errno externals
 */
#if defined(__STDC__)
int *__serrno(void);
int *__rfio_errno(void);
#else /* __STDC__ */
int *__serrno();
int *__rfio_errno();
#endif /* __STDC__ */

/*
 * Cglobals_init() - assing routines to provide thread local storage. 
 * Globals that existed prior to this call are all re-assigned as
 * thread-specific to calling thread (normally the main thread).
 */
void Cglobals_init(int (*getspec)(int *, void **),
                            int (*setspec)(int *, void *)) {
    int i,rc;
    int *key;
    void *addr;

    if ( getspec != NULL && local_getspec == NULL ) local_getspec = getspec;
    if ( setspec != NULL && local_setspec == NULL ) local_setspec = setspec;
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
        *__serrno() = serrno;
        *__rfio_errno() = rfio_errno;
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
void Cglobals_get(int *key, void **addr, size_t size) {
    int rc;
    Cglobals_t *tmp;

    if ( key == NULL || addr == NULL || size<= (size_t)0 ) return;
    if ( local_setspec == NULL ) {
        if ( *key <= 0 ) {
            *addr = calloc(1,size);
            if ( *addr == NULL ) return;
            if ( single_thread_globals == NULL ) {
                single_thread_globals = 
                    (Cglobals_t **)malloc(sizeof(Cglobals_t *)*alloc_size);
            } else if ( nb_globals == alloc_size ) {
                single_thread_globals = 
                    (Cglobals_t **)realloc(single_thread_globals,
                    sizeof(Cglobals_t *)*(nb_globals+alloc_size));
            }
            if ( single_thread_globals == NULL ) {
                free(*addr);
                *addr = NULL;
                return;
            }
            tmp = (Cglobals_t *)malloc(sizeof(Cglobals_t));
            tmp->addr = *addr;
            tmp->key = key;
            single_thread_globals[nb_globals] = tmp;
            nb_globals++;
            *key = nb_globals;
        } else *addr = (void *)single_thread_globals[*key-1]->addr;
    } else {
        rc = local_getspec(key,addr);
        if ( rc == -1 || *addr == NULL ) {
            *addr = calloc(1,size);
            rc = local_setspec(key,*addr);
        }
    }
    return;
}

int *__serrno() {
    int rc;
    int *addr;

    if ( local_setspec == NULL ) {
        return(&serrno);
    } else {
        addr = NULL;
        rc = local_getspec(&serrno,(void **)&addr);
        if ( rc == -1 || addr == NULL ) {
            addr = (int *)calloc(1,sizeof(int));
            rc = local_setspec(&serrno,(void *)addr);
        }
        /*
         * If memory allocation failed, we can still
         * return the external. This is obviously not
         * thread-safe but at least the application
         * will not die with a strange coredump in a
         * hidden de-reference of __serrno().
         */
        if ( addr == NULL ) return(&serrno);
        else return(addr);
    }
}

int *__rfio_errno() {
    int rc;
    int *addr;

    if ( local_setspec == NULL ) {
        return(&rfio_errno);
    } else {
        addr = NULL;
        rc = local_getspec(&rfio_errno,(void **)&addr);
        if ( rc == -1 || addr == NULL ) {
            addr = (int *)calloc(1,sizeof(int));
            rc = local_setspec(&rfio_errno,(void *)addr);
        }
        /*
         * If memory allocation failed, we can still
         * return the external. This is obviously not
         * thread-safe but at least the application
         * will not die with a strange coredump in a
         * hidden de-reference of __rfio_errno().
         */
        if ( addr == NULL ) return(&rfio_errno);
        else return(addr);
    }
}
