/*
 * $Id: Cthread.c,v 1.13 1999/09/10 15:49:42 jdurand Exp $
 *
 * $Log: Cthread.c,v $
 * Revision 1.13  1999/09/10 15:49:42  jdurand
 * Made non __STDC__ compliant
 *
 * Revision 1.12  1999-09-08 18:19:49+02  jdurand
 * Bug fixes (missing return value for some timeouted routines)
 *
 * Revision 1.11  1999-09-07 15:46:48+02  jdurand
 * Fixed typo (ECHTREADERROR -> SECTHREADERR)
 *
 * Revision 1.10  1999/09/02 08:39:39  jdurand
 * Added serrno error values
 *
 * Revision 1.9  1999/08/24 15:50:03  jdurand
 * Changed debug printout for exported functions to have source:line information
 *
 * Revision 1.8  1999/07/26 07:20:27  obarring
 * Add Cthread_self in argument list of the new Cglobals_init()
 *
 * Revision 1.7  1999/07/20 20:05:11  jdurand
 * Changed _Cglobals_init call to Cglobals_init
 *
 * Revision 1.6  1999/07/20 13:49:49  obarring
 * Remove references to Cthread_errno.h
 *
 * Revision 1.5  1999/07/20 13:45:37  jdurand
 * *** empty log message ***
 *
 * Revision 1.4  1999/07/20 13:38:11  obarring
 * Add call to Cglobals_init()
 *
 * Revision 1.3  1999/07/20 13:26:52  obarring
 * empty line for test
 *
 * Revision 1.2  1999/07/20 08:49:20  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Added missing Id and Log CVS's directives
 *
 */

#include <Cthread_api.h>
#include <Cglobals.h>
#include <serrno.h>
#include <errno.h>

#ifdef DEBUG
#ifndef CTHREAD_DEBUG
#define CTHREAD_DEBUG
#endif
#endif

#ifdef CTHREAD_DEBUG
#include <stdio.h>
#endif


/* ============================================ */
/* CASTOR Thread Interface (Cthread)            */
/* ============================================ */
/*                                              */
/* Dependency flag: CTHREAD                     */
/* - If this flag is activated at the compile   */
/*   time, then the following API will use      */
/*   the local thread implementation.           */
/*   The default is then the POSIX 1003.4a      */
/*   (Draft 4) prototype. The prototypes used   */
/*   can be altered with the following flags    */
/*   at compile time:                           */
/*   CTHREAD_POSIX : POSIX 1003.4a Draft 4      */
/*   CTHREAD_DCE   : DCE Threads                */
/*   CTHREAD_LINUX : LinuxThreads               */
/*   CTHREAD_WIN32 : WIN32 threads              */
/*                                              */
/* Examples:                                    */
/* - To use POSIX thread compile with:          */
/*   -DCTHREAD            or                    */
/*   -DCTHREAD -DCTHREAD_POSIX                  */
/* - To use DCE thread compile with:            */
/*   -DCTHREAD -DCTHREAD_DCE                    */
/* - To use LinuxThread compile with:           */
/*   -DCTHREAD -DCTHREAD_LINUX                  */
/*                                              */
/* Please note that use of CTHREAD_POSIX,       */
/* CTHREAD_DCE, or CTHREAD_LINUX                */
/* without the CTHREAD flag is useless          */
/* on Unix-like systems.                        */
/*                                              */
/* The exception is on Windows/NT, on which the */
/* thread behaviour will always apply,          */
/* regardless of any cthread compile flag option*/
/* This means that on Windows/NT, the following */
/* flags will always be enforced:               */
/* CTHREAD                                      */
/* CTHREAD_WIN32                                */
/*                                              */
/* - To not use threads, compile with:          */
/*   -DNOCTHREAD    or nothing                  */
/*                                              */
/* Again, -DNOCTHREAD is useless on             */
/* on Windows/NT, where the following flags are */
/* enforced:                                    */
/* CTHREAD                                      */
/* CTHREAD_WIN32                                */
/*                                              */
/* Finally please notice that the use of        */
/* CNOCTHREAD has higher level than the         */
/* CTHREAD. CNOCTHREAD is the default           */
/* except on Windows/NT where CTHREAD is        */
/* the only possibility.                        */
/*                                              */
/* In the same way, CTHREAD_POSIX is higher     */
/* than CTHREAD_DCE, itself higher than         */
/* CTHREAD_LINUX and CTHREAD_POSIX is           */
/* the default.                                 */
/*                                              */
/* For Windows, default is CTHREAD_WIN32        */
/*                                              */
/* ============================================ */
/*                                              */
/* History:                                     */
/* 06-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* 21-APR-1999       Windows support            */
/*                   Olof.Barring@cern.ch       */
/*                                              */
/* ============================================ */

/* -------------------------------------------- */
/* Initialization:                              */
/* Let's analyse the compile flags              */
/* -------------------------------------------- */
#include <Cthread_flags.h>
#include <Cglobals.h>

/* ------------------------------------ */
/* For the what command                 */
/* ------------------------------------ */
static char sccsid[] = "@(#)$RCSfile: Cthread.c,v $ $Revision: 1.13 $ $Date: 1999/09/10 15:49:42 $ CERN IT-PDP/DM Olof Barring, Jean-Damien Durand";

/* ============================================ */
/* Typedefs                                     */
/* ============================================ */
/* -------------------------------------------- */
/* Uniform definition of a thread/process ID    */
/* -------------------------------------------- */
#ifdef _CTHREAD
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
typedef unsigned long Cth_pid_t;
#else /* _CTHREAD_PROTO_WIN32 */
typedef pthread_t Cth_pid_t;
#endif /* CTHREAD_PROTO_WIN32 */
#else
typedef pid_t Cth_pid_t;
#endif /* _CTHREAD */
/* -------------------------------------------- */
/* Uniform definition of a lock variable        */
/* -------------------------------------------- */
#ifdef _CTHREAD
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
/* 
 * This is a WIN32 mutex.
 * Use handles as an option. Default is critical
 * section which should be more efficient in normal
 * working conditions. Try to set
 * _CTHREAD_WIN32MTXIf if perf. is low on
 * on multi-CPU platforms
 */
#  ifdef _CTHREAD_WIN32MTX
typedef HANDLE Cth_mtx_t
#  else /* _CTHREAD_WIN32MTX */
typedef struct {
    CRITICAL_SECTION mtx;
    int nb_locks;
} Cth_mtx_t;
#  endif /* _CHTREAD_WIN32MTX */

#else /* _CTHREAD_PROTO_WIN32 */
/* This is a pthread mutex */
typedef pthread_mutex_t Cth_mtx_t;
#endif /* _CTHREAD_PROTO_WIN32 */
#else
/* This is semaphore, e.g. just a counter */
typedef int Cth_mtx_t;
#endif
/* -------------------------------------------- */
/* Uniform definition of a cond variable        */
/* -------------------------------------------- */
#ifdef _CTHREAD
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
/*
 * The Cthread condition implementation is taken
 * from a proposal by Douglas C. Schmidt and
 * Irfan Pyarali
 * (ref. http://www.cs.wustl.edu/~schmidt/win32-cv-1.html)
 * The section 3.2 implementation is choosen to
 * assure minimal overhead.
 */
#define CTHREAD_WIN32_SIGNAL  0
#define CTHREAD_WIN32_BROADCAST 1
#define CTHREAD_WIN32_MAX_EVENTS 2
typedef struct {
    /* 
     * Count + lock to serialize access to count
     * on the number of threads waiting for
     * the condition.
     */
    unsigned int nb_waiters;
    CRITICAL_SECTION nb_waiters_lock;
    /*
     * Signal (auto-reset) and
     * broadcast (manual reset) events
     */
    HANDLE cond_events[CTHREAD_WIN32_MAX_EVENTS];
    HANDLE last_waiter_done;
} Cth_cond_t;
#else /* _CTHREAD_PROTO_WIN32 */
typedef pthread_cond_t Cth_cond_t;
#endif /* _CTHREAD_PROT_WIN32 */
#else
typedef int Cth_cond_t;
#endif
/* -------------------------------------------- */
/* Uniform definition of a specific variable    */
/* -------------------------------------------- */
#ifdef _CTHREAD
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
typedef DWORD Cth_spec_t;
#else
typedef pthread_key_t Cth_spec_t;
#endif
#else
typedef int Cth_spec_t;
#endif

/* -------------------------------------------- */
/* Thread starter                               */
/* -------------------------------------------- */
#ifdef _CTHREAD
typedef struct Cthread_start_params {
#ifdef __STDC__
  void *(*_thread_routine)(void *);
#else
  void *(*_thread_routine)();
#endif
  void *_thread_arg;
  int   detached;
} Cthread_start_params_t;
#endif /* _CTHREAD_PROTO_WIN32 */

/* -------------------------------------------- */
/* Uniform declaration of a _once_t typedef and */
/* of a _once_init value                        */
/* -------------------------------------------- */
#ifndef _CTHREAD
typedef  int Cth_once_t;
#define CTHREAD_ONCE_INIT 0
#else
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
/*
 * For Windows there is no good possibility to
 * serialize the initialization. We use a flag and
 * rely on the fact that the process starts with one
 * single thread which will call Cthread_... routines
 * for all thread operations. Thus, the first Cthread_..
 * call is automatically serialized.
 */
typedef int Cth_once_t;
#define CTHREAD_ONCE_INIT 0
#else
#ifdef PTHREAD_ONCE_INIT
typedef pthread_once_t Cth_once_t;
#define CTHREAD_ONCE_INIT PTHREAD_ONCE_INIT
#else
#ifdef pthread_once_init
typedef pthread_once_t Cth_once_t;
#define CTHREAD_ONCE_INIT pthread_once_init
#else
typedef pthread_once_t Cth_once_t;
/* Let's assume a (really honnest) value of 0 */
#define CTHREAD_ONCE_INIT 0
#endif /* pthread_once_init */
#endif /* PTHREAD_ONCE_INIT */
#endif /* _WIN32 */
#endif /* _CTHREAD */

/* ============================================ */
/* Structures                                   */
/* ============================================ */
/* -------------------------------------------- */
/* Map between Cthread ID and Thread/Process ID */
/* -------------------------------------------- */
/* This structure is a definition of a thread   */
/* from the Cthread point of view:              */
/* - The Cthread ID                             */
/* - The Thread/Process ID                      */
/* - The startup routine                        */
/* - The pointer to the next element            */
/* -------------------------------------------- */
struct Cid_element_t {
  int       cid;            /* Cthread ID       */
  Cth_pid_t pid;            /* Thread/Proc. ID  */
  unsigned thID;            /* WIN32 thread ID  */
#ifdef __STDC__
  void   *(*addr)(void *);  /* Start-up         */
#else
  void   *(*addr)();        /* Start-up         */
#endif
  int       detached;       /* Is it detached ? */
  struct Cid_element_t *next; /* Next element   */
};

/* -------------------------------------------- */
/* Map between address to lock and a variable   */
/* Please note that we always SYSTEMATICALLY    */
/* associate a conditionnal variable with a     */
/* mutex for an eventual timeout on getting     */
/* the mutex's lock.                            */
/* -------------------------------------------- */
/* This structure is a definition of a mutex    */
/* from the Cthread point of view:              */
/* - The address to lock                        */
/* - The mutex address                          */
/* - The associated conditionnal variable       */
/* - The pointer to the next element            */
/* -------------------------------------------- */
struct Cmtx_element_t {
  void                *addr; /* Adress to lock  */
  Cth_mtx_t            mtx; /* Associated mutex */
  Cth_cond_t           cond; /* Associate cond  */
  struct Cmtx_element_t *next; /* Next element  */
  int                  nwait; /* cond_wait nb   */
};

/* -------------------------------------------- */
/* Map between specific variable and Cthread's  */
/* specific variable ID                         */
/* -------------------------------------------- */
/* This structure is a definition of a specific */
/* variable from the Cthread point of view:     */
/* - The address of the specific variable       */
/* - A once value for initialization            */
/* - The associated key                         */
/* - The associated Cthread ID                  */
/* - The pointer to next element                */
/* -------------------------------------------- */
struct Cspec_element_t {
  int          *global_key; /* Client Glob. Key */
  Cth_spec_t          key;  /* Key              */
  struct Cspec_element_t *next; /* Next element */
#ifndef _CTHREAD
  void         *addr;       /* malloced address */
#endif
};

/* -------------------------------------------- */
/* Internal structure used by Cthread. To make  */
/* that in thread environment the access to     */
/* vital variables like the Cmtx_* and Cid_*    */
/* elements is not touched by more than one     */
/* thread at a time, Cthread uses an internal   */
/* mutex.                                       */
/* This has NO effect in a non-thread           */
/* environment as of date of this library       */
/* -------------------------------------------- */
/* This structure is a definition of an         */
/* internal structure useful from the Cthread   */
/* point of view when it work in a thread       */
/* environment:                                 */
/* - The mutex address                          */
/* -------------------------------------------- */
struct Cthread_protect_t {
  Cth_mtx_t            mtx; /* Associated mutex */
};

/* ============================================ */
/* Internal Prototypes                          */
/* ============================================ */
/* Add a Cthread ID */
#ifdef __STDC__
int   _Cthread_addcid(char *, int, char *, int, Cth_pid_t *, unsigned, void *(*)(void *), int);
#else
int   _Cthread_addcid();
#endif
/* Add a mutex */
#ifdef __STDC__
int   _Cthread_addmtx(char *, int, struct Cmtx_element_t *);
#else
int   _Cthread_addmtx();
#endif
/* Add a TSD */
#ifdef __STDC__
int   _Cthread_addspec(char *, int, struct Cspec_element_t *);
#else
int   _Cthread_addspec();
#endif
/* Obain a mutex lock */
#ifdef __STDC__
int   _Cthread_obtain_mtx(char *, int, Cth_mtx_t *, int);
#else
int   _Cthread_obtain_mtx();
#endif
/* Release a mutex lock */
#ifdef __STDC__
int   _Cthread_release_mtx(char *, int, Cth_mtx_t *);
#else
int   _Cthread_release_mtx();
#endif
/* Methods to create a mutex to protect Cthread */
/* internal tables and linked lists             */
/* This is useful only in thread environment    */
static int _Cthread_once_status = -1;
#ifdef __STDC__
void  _Cthread_once();
#else
void  _Cthread_once();
#endif
#ifdef __STDC__
int   _Cthread_init();
#else
int   _Cthread_init();
#endif
/* Release of a thread-specific variable        */
#ifdef __STDC__
void  _Cthread_keydestructor(void *);
#else
void  _Cthread_keydestructor();
#endif
/* Release of a thread                          */
#ifdef __STDC__
int   _Cthread_destroy(char *, int, int);
#else
int   _Cthread_destroy();
#endif
#ifdef _CTHREAD
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
unsigned __stdcall _Cthread_start_threadex(void *);
void     __cdecl   _Cthread_start_thread(void *);
static int _Cthread_win32_cond_init(Cth_cond_t *);
static int _Cthread_win32_cond_wait(Cth_cond_t *, Cth_mtx_t *, int);
static int _Cthread_win32_cond_broadcast(Cth_cond_t *);
static int _Cthread_win32_cond_signal(Cth_cond_t *);
static int _Cthread_win32_cond_destroy(Cth_cond_t *);
#else
void *_Cthread_start_pthread(void *);
#endif
#else
/* Signal handler - Simplify the POSIX sigaction calls */
#ifdef __STDC__
typedef void    Sigfunc(int);
#else
typedef void    Sigfunc();
#endif
#ifdef __STDC__
Sigfunc *_Cthread_signal(char *, int, int, Sigfunc *);
#else
Sigfunc *_Cthread_signal();
#endif
#ifdef __STDC__
void     _Cthread_sigchld(int);
#else
void     _Cthread_sigchld();
#endif
#ifdef __STDC__
void    *_Cthread_start_nothread(void *(*)(void *), void *, Sigfunc *);
#else
void    *_Cthread_start_nothread();
#endif
#endif
/* Find a global key in Cspec structure         */
#ifdef __STDC__
struct Cspec_element_t *_Cthread_findglobalkey(char *, int, int *);
#else
struct Cspec_element_t *_Cthread_findglobalkey();
#endif

/* ============================================ */
/* Static variables                             */
/* ============================================ */
/* Cid             => Processes lists           */
/* Cmtx            => Mutex lists               */
/* Cthread           => For internal use        */
/* Cspec           => TSD lists                 */
/* once            => For pthread_once() in a   */
/*                    thread environment        */
/* ============================================ */
/* -------------------------------------------- */
/* Cid is the starting point of ALL processes   */
/* from the Cthread point of view.              */
/* -------------------------------------------- */
static struct Cid_element_t   Cid;
#ifdef _CTHREAD
static struct Cmtx_element_t  Cmtx;
#endif
static struct Cthread_protect_t Cthread;
static struct Cspec_element_t Cspec;
static Cth_once_t             once = CTHREAD_ONCE_INIT;

/* ============================================ */
/* Routine  : Cthread_init                      */
/* Arguments:                                   */
/* -------------------------------------------- */
/* Output   : 0 == OK, -1 == not OK             */
/* -------------------------------------------- */
/* History:                                     */
/* 07-APR-1999       First implementation       */
/*                   Olof.Barring@cern.ch       */
/* ============================================ */
/* Notes:                                       */
/* This routine is to make sure that, in a      */
/* a thread environment at least, some internal */
/* Cthread structure are correctly initialized  */
/* ============================================ */
CTHREAD_DECL Cthread_init() {
  return(_Cthread_init());
}

/* ============================================ */
/* Routine  : _Cthread_init                     */
/* Arguments:                                   */
/* -------------------------------------------- */
/* Output   : 0 == OK, -1 == not OK             */
/* -------------------------------------------- */
/* History:                                     */
/* 07-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine is to make sure that, in a      */
/* a thread environment at least, some internal */
/* Cthread structure are correctly initialized  */
/* ============================================ */
/* This routine should be called internally by  */
/* any Cthread function.                        */
/* ============================================ */
int _Cthread_init() {
  int status = 0;
  
#ifndef _CTHREAD
  if ( !once ) (void)_Cthread_once();
#else
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  /* In WIN32 there is nothing corresponding to        */
  /* pthread_once(). Instead we rely on that Cthread_..*/
  /* is the unique thread interface so that the first  */
  /* Cthread_.. call is serialized. This will not work */
  /* if the application uses non-Cthread routines to   */
  /* first create some threads.                        */
  if ( !once ) (void)_Cthread_once();
#else /* _CTHREAD_PROTO_WIN32 */
  /* In a thread environment we need to have our own */
  /* mutex. So we create it right now - we do it     */
  /* dynamically, since we don't rely on the fact    */
  /* that the macro PTHREAD_MUTEX_INITIALIZER exists */
  /* everywhere                                      */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  int n;

  if ((n = pthread_once(&once,_Cthread_once)) != 0) {
    errno = n;
    _Cthread_once_status = -1;
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_once(&once,_Cthread_once) != 0)
    _Cthread_once_status = -1;
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* On linux, pthread_once always returns 0... */
  pthread_once(&once,_Cthread_once);
#  else
  _Cthread_once_status = -1;
  serrno = SEOPNOTSUP;
  return(-1);
#  endif
#endif /* _CTHREAD_PROTO_WIN32 */
#endif /* _CTHREAD */
  if ((status = _Cthread_once_status) != 0)
    serrno = SECTHREADINIT;

  return(status);
}

#ifdef _CTHREAD
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
/* ============================================ */
/* Routine  : _Cthread_start_threadex           */
/* Arguments: thread startup parameters         */
/* -------------------------------------------- */
/* Output   : pointer to thread exit status     */
/*            location.                         */
/* -------------------------------------------- */
/* History:                                     */
/* 22-APR-1999       First implementation       */
/*                   Olof.Barring@cern.ch       */
/* 29-APR-1999       Add _Cthread_addcid        */
/*                   Jean-Damien.Durand@cern.ch */
/* 30-APR-1999       Change addcid call to pass */
/*                   NULL for thread handle     */
/*                   since it is not available  */
/*                   to the thread.             */
/*                   (Olof)                     */
/* ============================================ */
/* Notes:                                       */
/* Thread startup wrapper (Windows only!)       */
/* Start routine for non-detached threads.      */
/* This routine should be passed to             */
/* _beginthreadex(). Note that the thread handle*/
/* should be closed with CloseHandle() when the */
/* thread has exit (see _Cthread_destroy()).    */
/* Profit from wrapper to do some cleanup at end*/
/* of thread.                                   */
/* ============================================ */
unsigned __stdcall _Cthread_start_threadex(void *arg) {
	Cthread_start_params_t *start_params;
	void                 *status;
    int                   thID;
    void               *(*routine)(void *);
    void               *routineargs;

    thID = GetCurrentThreadId();
	if ( arg == NULL ) {
      serrno = EINVAL;
      return(-1);
    }
	start_params = (Cthread_start_params_t *)arg;
    if (_Cthread_addcid(__FILE__,__LINE__,NULL,0,thID,start_params->_thread_routine,start_params->detached) < 0) {
      free(arg);
      return(-1);
    }
    routine = start_params->_thread_routine;
    routineargs = start_params->_thread_arg;
    free(arg);
    /* status = start_params->_thread_routine(start_params->_thread_arg); */
    status = routine(routineargs);

    /* Destroy the entry in Cthread internal linked list */
    _Cthread_destroy(Cthread_self());
	return((unsigned)status);
}
/* ============================================ */
/* Routine  : _Cthread_start_thread             */
/* Arguments: thread startup parameters         */
/* -------------------------------------------- */
/* Output   :                                   */
/* -------------------------------------------- */
/* History:                                     */
/* 22-APR-1999       First implementation       */
/*                   Olof.Barring@cern.ch       */
/* 29-APR-1999       Add _Cthread_addcid        */
/*                   Jean-Damien.Durand@cern.ch */
/* 30-APR-1999       Change addcid call to pass */
/*                   NULL for thread handle     */
/*                   since it is not available  */
/*                   to the thread.             */
/*                   (Olof)                     */
/* ============================================ */
/* Notes:                                       */
/* Thread startup wrapper (Windows only!)       */
/* Start routine for attached threads.          */
/* This routine should be passed to             */
/* _beginthread(). No system resource cleanup is*/
/* needed for these threads because             */
/* CloseHandle() is automatically called by     */
/* system at thread exit).                      */
/* Profit from wrapper to do some cleanup at end*/
/* of thread.                                   */
/* ============================================ */
void __cdecl _Cthread_start_thread(void *arg) {
	Cthread_start_params_t *start_params;
	void                 *status;
    int                   thID;
    void               *(*routine)(void *);
    void               *routineargs;


    thID = GetCurrentThreadId();
	if ( arg == NULL ) {
      serrno = EINVAL;
      return;
    }
	start_params = (Cthread_start_params_t *)arg;
    if (_Cthread_addcid(__FILE__,__LINE__,NULL,0,thID,start_params->_thread_routine,start_params->detached) < 0) {
      free(arg);
      return;
    }
    routine = start_params->_thread_routine;
    routineargs = start_params->_thread_arg;
    free(arg);
    /* status = start_params->_thread_routine(start_params->_thread_arg); */
    status = routine(routineargs);

    /* Destroy the entry in Cthread internal linked list */
    _Cthread_destroy(Cthread_self());
	return;
}
#else /* _CTHREAD_PROTO_WIN32 */
/* ============================================ */
/* Routine  : _Cthread_start_pthread            */
/* Arguments: thread startup parameters         */
/* -------------------------------------------- */
/* Output   : thread output location status     */
/* -------------------------------------------- */
/* History:                                     */
/* 29-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
void *_Cthread_start_pthread(void *arg) {
  Cthread_start_params_t *start_params; /* params         */
  void                 *status;    /* Thread status     */
  Cth_pid_t             pid;       /* pthread_t         */
  unsigned              thID = 0;  /* Thread ID (WIN32) */
  void               *(*routine)(void *);
  void               *routineargs;

#ifdef CTHREAD_DEBUG
  fprintf(stderr,"[Cthread    [%2d]] In _Cthread_start_pthread(0x%x)\n",
          _Cthread_self(),(unsigned long) arg);
#endif

  if (arg == NULL) {
    serrno = EINVAL;
    return(NULL);
  }
  start_params = (Cthread_start_params_t *) arg;


  pid = pthread_self();
  if (_Cthread_addcid(__FILE__,__LINE__,NULL,0,&pid,thID,start_params->_thread_routine,start_params->detached) < 0) {
    free(arg);
    return(NULL);
  }

  routine = start_params->_thread_routine;
  routineargs = start_params->_thread_arg;
  free(arg);
  /* status = start_params->_thread_routine(start_params->_thread_arg); */
  status = routine(routineargs);
  
  /* Destroy the entry in Cthread internal linked list */
  _Cthread_destroy("Cthread.c(_Cthread_start_pthread)",__LINE__,Cthread_self());
  return(status);
}
#endif /* _CTHREAD_PROTO_WIN32 */
#else /* _CTHREAD */
/* ============================================ */
/* Routine  : _Cthread_signal                   */
/* Arguments: signal to trap, signal routine    */
/* -------------------------------------------- */
/* Output   : status (SIG_ERR if error)         */
/* -------------------------------------------- */
/* History:                                     */
/* 11-MAY-1999       First implementation       */
/*                   [with R.W.Stevens example] */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
Sigfunc *_Cthread_signal(file, line, signo, func)
     char *file;
     int line;
     int signo;
     Sigfunc *func;
{
  struct sigaction	act, oact;
  
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,"[Cthread    [%2d]] In _Cthread_signal(%d,0x%x) called at/behind %s:%d\n",
            _Cthread_self(),signo,(unsigned long) func,file, line);
  }
#endif

  act.sa_handler = func;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  if (signo == SIGALRM) {
#ifdef	SA_INTERRUPT
    act.sa_flags |= SA_INTERRUPT;	/* SunOS 4.x */
#endif
  } else {
#ifdef	SA_RESTART
    act.sa_flags |= SA_RESTART;		/* SVR4, 44BSD */
#endif
  }
  if (sigaction(signo, &act, &oact) < 0) {
    serrno = SEINTERNAL;
    return(SIG_ERR);
  }
  return(oact.sa_handler);
}

/* ============================================ */
/* Routine  : _Cthread_sigchld                  */
/* Arguments: error code                        */
/* -------------------------------------------- */
/* Output   :                                   */
/* -------------------------------------------- */
/* History:                                     */
/* 11-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
void _Cthread_sigchld(errcode)
     int errcode;
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  int                   n;                /* Status           */
  Cth_pid_t             pid;              /* Process pid      */
  int                   status;

  if ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
    /* The child "pid" has exited normally :   */
    /* We remove it from the list of processes */
    
#ifdef CTHREAD_DEBUG
    fprintf(stderr,"[Cthread    [%2d]] In _Cthread_sigchld(%d) for child PID=%d (exit status = %d)\n",
            _Cthread_self(),
            errcode,pid,status);
#endif
    /* We check that this pid correspond to a known */
    /* Cthread ID                                   */
    if (! _Cthread_obtain_mtx(NULL,0,&(Cthread.mtx),-1)) {
      n = 1;
      while (current->next != NULL) {
        current = current->next;
        
#ifdef CTHREAD_DEBUG
        fprintf(stderr,"[Cthread    [%2d]] In _Cthread_sigchld(%d) for child PID=%d ... Current scanned PID=%d\n",
                _Cthread_self(),
                errcode,pid,current->pid);
#endif
        
        if (current->pid == pid) {
          n = 0;
        } else {
          /* We check other processes, like those who terminated BEFORE they entered */
          /* in the list...                                                          */
          if ( (kill(current->pid,0) == -1) && (errno == ESRCH) ) {
#ifdef CTHREAD_DEBUG
            fprintf(stderr,"[Cthread    [%2d]] In _Cthread_sigchld(%d) : Calling _Cthread_destroy(%d) for yet finished PID=%d\n",
                    _Cthread_self(),
                    errcode,current->cid,current->pid);
#endif
            _Cthread_destroy(NULL,0,current->cid);
          }
        }
      }
      _Cthread_release_mtx(NULL,0,&(Cthread.mtx));
      
      if (n == 0) {
#ifdef CTHREAD_DEBUG
        fprintf(stderr,"[Cthread    [%2d]] In _Cthread_sigchld(%d) : Calling _Cthread_destroy(%d) for PID=%d\n",
                _Cthread_self(),
                errcode,current->cid,pid);
#endif
        _Cthread_destroy(NULL,0,current->cid);
      } else {
#ifdef CTHREAD_DEBUG
        fprintf(stderr,"[Cthread    [%2d]] In _Cthread_sigchld(%d) : Can't find corresponding Cthread ID for PID=%d !\n",
                _Cthread_self(),
                errcode,pid);
#endif
      }
    }
  }

  /* Under UNIX SVR4, the handler for signals returns back */
  /* to the default action (which is, for the case of SIGCHLD,*/
  /* to ignore the signal) before the user specified signal */
  /* handler gets called. Therefore, we must establish ourselves */
  /* as the signal handler each time we're called. Is this the */
  /* proper place? Are there any race conditions under POSIX */
  /* (primary UNIX standardization) semantics for signals? */
  
  
  /* _Cthread_signal(__FILE__,__LINE__,SIGCHLD, _Cthread_sigchld); */
}

/* ============================================ */
/* Routine  : _Cthread_start_nothread           */
/* Arguments: Process startup parameters        */
/* -------------------------------------------- */
/* Output   : process output location status    */
/* -------------------------------------------- */
/* History:                                     */
/* 10-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
void *_Cthread_start_nothread(startroutine, arg, old_sigchld)
#ifdef __STDC__
     void *(*startroutine)(void *);
#else
     void *(*startroutine)();
#endif
     void *arg;
     Sigfunc *old_sigchld;
{
  void                 *status;    /* Thread status     */
  Cth_pid_t             pid;       /* pthread_t         */
  unsigned              thID = 0;  /* Thread ID (WIN32) */
  struct Cspec_element_t *current = &Cspec;   /* Curr Cspec_element */
  struct Cspec_element_t *previous = NULL;    /* Curr Cspec_element */

  if (startroutine == NULL) {
    serrno = EINVAL;
    return(NULL);
  }

  pid = getpid();

  /* If we want Cthread_self() to work... */
  if (_Cthread_addcid(__FILE__,__LINE__,NULL,0,&pid,thID,startroutine,0) < 0) {
    return(NULL);
  }

  /* We restore parents normal handler */
  if (_Cthread_signal(__FILE__,__LINE__,SIGCHLD, old_sigchld) == SIG_ERR) {
    return(NULL);
  }

  status = startroutine(arg);

  /* The process should be garbage collected by the kernel */
  /* Neverthless, we clean-up ourself all the eventual     */
  /* Process-Specific Data                                 */
  while (1) {
    current = &Cspec;
    if (current->next != NULL) {
      while (current->next != NULL) {
        previous = current;
        current = current->next;
      }
      if (current->addr != NULL) {
        free(current->addr);
      }
      free(current);
      previous->next = NULL;
    } else {
      break;
    }
  }

  /* Destroy the entry in Cthread internal linked list */
  _Cthread_destroy(__FILE__,__LINE__,Cthread_self());
  return(status);
}
#endif /* _CTHREAD */

/* ============================================ */
/* Routine  : Cthread_Create                    */
/* Arguments: caller source file                */
/*            caller source line                */
/*            start routine address             */
/*            start routine arguments address   */
/* -------------------------------------------- */
/* Output   : integer >= 0 - Cthread ID         */
/*                    <  0 - Error              */
/* -------------------------------------------- */
/* History:                                     */
/* 06-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine is allowed to be called ONLY    */
/* by the main process. There are two reasons   */
/* for this limitation:                         */
/*   1) disallow recursive creation             */
/*   2) Insure synchronization                  */
/* ============================================ */
CTHREAD_DECL Cthread_Create(file, line, startroutine, arg)
     char *file;
     int line;
#ifdef __STDC__
     void *(*startroutine)(void *);
#else
     void *(*startroutine)();
#endif
     void *arg;
{
  Cth_pid_t pid;                        /* Thread/Process ID */
  unsigned thID = 0;                    /* Thread ID (WIN32) */
#ifdef _CTHREAD
  Cthread_start_params_t *starter;      /* Starter           */
#if _CTHREAD_PROTO != _CTHREAD_PROTO_WIN32
  int            n;                     /* status            */
  pthread_attr_t attr;                  /* Thread attribute  */
#endif /* _CTHREAD_PROTO_WIN32 */
#else
  Sigfunc *old_sigchld = SIG_IGN;
#endif /* _CTHREAD */
  
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,
            "[Cthread    [%2d]] In Cthread_create(0x%x,0x%x) called at/behind %s:%d\n",
            _Cthread_self(),
            (unsigned long) startroutine, (unsigned long) arg,
            file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Verify the arguments */
  if (startroutine == NULL) {
    /* startroutine not specified */
    serrno = EINVAL;
    return(-1);
  }

#ifdef _NOCTHREAD
  /* Not a thread implementation */
  /* We don't want to exit with the child */
  if ((old_sigchld = _Cthread_signal(__FILE__,__LINE__,SIGCHLD, _Cthread_sigchld)) == SIG_ERR)
    return(-1);

  /* Fork a new process */
  if ((pid = fork()) < 0) {
    /* Error at process creation */
#ifdef CTHREAD_DEBUG
    if (file != NULL) {
      fprintf(stderr,
              "[Cthread    [%2d]] In Cthread_create(0x%x,0x%x) called at/behind %s:%d : Error at fork\n",
              _Cthread_self(),
              (unsigned long) startroutine,(unsigned long) arg,
              file,line);
    }
#endif
    serrno = SEINTERNAL;
    return(-1);
  }
  if (pid == 0) {
    int *status;
    /* We are in the child    */
    /*... Execute the routine */
    status = _Cthread_start_nothread(startroutine,arg,old_sigchld);
    if (status != NULL) {
      exit(*status);
    } else {
      exit(EXIT_SUCCESS);
    }
  }
  /* We are in the parent                     */
  /* We will cleanup in the SIGCLHD handler   */
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,
            "[Cthread    [%2d]] In Cthread_create(0x%x,0x%x) called at/behind %s:%d : Created PID=%d\n",
            _Cthread_self(),
            (unsigned long) startroutine,(unsigned long) arg,
            file,line,
            pid);
  }
#endif  
  return(_Cthread_addcid(__FILE__,__LINE__,file,line,&pid,thID,startroutine,0));
#else /* ifdef _NOCTHREAD */
  /* This is a thread implementation */

  /* Create the starter              */
  if ((starter = (Cthread_start_params_t *) malloc(sizeof(Cthread_start_params_t))) == NULL) {
    serrno = SEINTERNAL;
    return(-1);
  }
  starter->_thread_routine = startroutine;
  starter->_thread_arg     = arg;
  starter->detached        = 0;

  /* Create the thread               */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  
  /* a-la POSIX */
  /* Create a thread default attr    */
  if ((n = pthread_attr_init(&attr)) != 0) {
    free(starter);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  if ((n = pthread_create(&pid,&attr,_Cthread_start_pthread, (void *) starter)) != 0) {
    pthread_attr_destroy(&attr);
    free(starter);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  /* Clear the thread default attr */
  if ((n = pthread_attr_destroy(&attr)) != 0) {
    free(starter);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  /* a-la DCE */
  /* Create a thread default attr    */
  if (pthread_attr_create(&attr) != 0) {
    serrno = SECTHREADERR;
    free(starter);
    return(-1);
  }
  if (pthread_create(&pid,attr,_Cthread_start_pthread, (void *) starter) != 0) {
    serrno = SECTHREADERR;
    pthread_attr_delete(&attr);
    free(starter);
    return(-1);
  }
  /* Clear the thread default attr */
  if (pthread_attr_delete(&attr) != 0) {
    serrno = SECTHREADERR;
    free(starter);
    return(-1);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* a-la LinuxThreads */
  /* Create a thread default attr    */
  if ((n = pthread_attr_init(&attr))) {
    free(starter);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  if ((n = pthread_create(&pid,&attr,_Cthread_start_pthread, (void *) starter))) {
    /* Creation error */
    free(starter);
    pthread_attr_destroy(&attr);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  /* Clear the thread default attr */
  if ((n = pthread_attr_destroy(&attr)) != 0) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  if ( (pid = _beginthreadex(NULL,0,_Cthread_start_threadex,starter,0,&thID)) == 0 ) {
    free(starter);
    serrno = SECTHREADERR;
	return(-1);
  }
#else
  serrno = SEOPNOTSUP;
  return(-1);
#endif
  /* Add it               */
  return(_Cthread_addcid(__FILE__,__LINE__,file,line,&pid,thID,startroutine,0));
#endif /* ifdef _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Create_Detached           */
/* Arguments: caller source file                */
/*            caller source line                */
/*            start routine address             */
/*            start routine arguments address   */
/* -------------------------------------------- */
/* Output   : integer >= 0 - Cthread ID         */
/*                    <  0 - Error              */
/* -------------------------------------------- */
/* History:                                     */
/* 06-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine is allowed to be called ONLY    */
/* by the main process. There are two reasons   */
/* for this limitation:                         */
/*   1) disallow recursive creation             */
/*   2) Insure synchronization                  */
/* ============================================ */
CTHREAD_DECL Cthread_Create_Detached(file, line, startroutine, arg)
     char *file;
     int line;
#ifdef __STDC__
     void *(*startroutine)(void *);
#else
     void *(*startroutine)();
#endif
     void *arg;
{
#ifdef _CTHREAD
  Cth_pid_t pid;                        /* Thread/Process ID */
  unsigned thID = 0;                       /* Thread ID (WIN32) */
  Cthread_start_params_t *starter;
#if _CTHREAD_PROTO != _CTHREAD_PROTO_WIN32
  int            n;                   /* status            */
  pthread_attr_t attr;                /* Thread attribute  */
#endif /* _CTHREAD_PROTO_WIN32 */
#endif
  
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,
            "[Cthread    [%2d]] In Cthread_create_detached(0x%x,0x%x) called at/behind %s:%d\n",
            _Cthread_self(),
            (unsigned long) startroutine, (unsigned long) arg,
            file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Verify the arguments */
  if (startroutine == NULL) {
    /* startroutine not specified */
    serrno = EINVAL;
    return(-1);
  }

#ifdef _NOCTHREAD
  /* Not a thread implementation */
  return(Cthread_create(startroutine,arg));
#else /* ifdef _NOCTHREAD */
  /* This is a thread implementation */
  /* Create the starter              */
  if ((starter = (Cthread_start_params_t *) malloc(sizeof(Cthread_start_params_t))) == NULL) {
    serrno = SEINTERNAL;
    return(-1);
  }
  starter->_thread_routine = startroutine;
  starter->_thread_arg     = arg;
  starter->detached        = 1;
  /* Create the thread               */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  /* a-la POSIX */
  /* Create a thread default attr    */
  if ((n = pthread_attr_init(&attr))) {
    free(starter);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  /* And make it detach as a default */
  if ((n = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED))) {
    free(starter);
    pthread_attr_destroy(&attr);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  if ((n = pthread_create(&pid,&attr,_Cthread_start_pthread, (void *) starter))) {
    /* Creation error */
    free(starter);
    pthread_attr_destroy(&attr);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  /* Clear the thread default attr */
  if ((n = pthread_attr_destroy(&attr))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  /* a-la DCE : impossible, there is no detach atribute */
  return(Cthread_create(startroutine,arg));
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* a-la LinuxThreads */
  /* Create a thread default attr    */
  if ((n = pthread_attr_init(&attr))) {
    free(starter);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  /* And make it detach as a default */
  if ((n = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED))) {
    free(starter);
    pthread_attr_destroy(&attr);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  if ((n = pthread_create(&pid,&attr,_Cthread_start_pthread, (void *) starter))) {
    /* Creation error */
    free(starter);
    pthread_attr_destroy(&attr);
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  /* Clear the thread default attr */
  if ((n = pthread_attr_destroy(&attr))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  starter = (Cthread_start_params_t *)malloc(sizeof(Cthread_start_params_t));
  starter->_thread_routine = startroutine;
  starter->_thread_arg = arg;
  if ( (pid = _beginthread(_Cthread_start_thread,0,starter)) == -1 ) {
      free(starter);
      serrno = SECTHREADERR;
      return(-1);
  }
#else
  serrno = EOPNOTSUP;
  return(-1);
#endif
  return(_Cthread_addcid(__FILE__,__LINE__,file,line,&pid,thID,startroutine,1));
#endif /* ifdef _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Join                      */
/* Arguments: caller source file                */
/*            caller source line                */
/*            Cthread ID                        */
/*            status pointer to pointer         */
/* -------------------------------------------- */
/* Output   : integer  0 - OK                   */
/*                   < 0 - Error                */
/* -------------------------------------------- */
/* History:                                     */
/* 06-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
CTHREAD_DECL Cthread_Join(file, line, cid, status)
     char *file;
     int line;
     int cid;
     int **status;
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  int                 n;                /* Status           */

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,
            "[Cthread    [%2d]] In Cthread_join(%d,0x%x) called at/behind %s:%d\n",
            _Cthread_self(),
            (int) cid, (unsigned long) status,
            file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* We check that this cid correspond to a known */
  /* process                                      */
  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);

  n = 1;
  while (current->next != NULL) {
    current = current->next;
    if (current->cid == cid) {
      n = 0;
      break;
    }
  }
  _Cthread_release_mtx(file,line,&(Cthread.mtx));

  if (n) {
    serrno = EINVAL;
    return(-1);
  }

#ifdef _NOCTHREAD
  n = (waitpid(current->pid,*status,WNOHANG) == current->pid ? 0 : -1);
  _Cthread_destroy(file,line,current->cid);
  if (n != 0)
    serrno = SEINTERNAL;
  return(n);
#else /* ifdef _NOCTHREAD */
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  n = -1;
  if ( !current->detached ) {
	for (;;) {
	  if ( (n = WaitForSingleObject((HANDLE)current->pid,INFINITE)) == 
	   	        WAIT_FAILED ) {
        serrno = SECTHREADERR;
        n = -1;
		break;
	  }
      if ( status != NULL ) {
        if ( (n = GetExitCodeThread((HANDLE)current->pid,
	  	                          (LPDWORD)status)) == STILL_ACTIVE ) {
          continue;
		}
        n = 0;
	  } else {
        serrno = SECTHREADERR;
        n = -1;
      }
      break;
	}
    _Cthread_destroy(cid);
  }
  return(n);
#else /* _CTHREAD_PROTO_WIN32 */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if ((n = pthread_join(current->pid,(void **) status))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  return(0);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_join(current->pid,(void **) status)) {
    serrno = SECTHREADERR;
    return(-1);
  }
  return(0);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  if ((n = pthread_join(current->pid,(void **) status))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  return(0);
#  else
  serrno = EOPNOTSUP;
  return(-1);
#  endif
#endif /* _CTHREAD_PROTO_WIN32 */
#endif /* ifdef _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Detach                    */
/* Arguments: caller source file                */
/*            caller source line                */
/*            Cthread ID                        */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 08-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
CTHREAD_DECL Cthread_Detach(file, line, cid)
     char *file;
     int line;
     int cid;
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  int                 n;                /* Status           */

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,
            "[Cthread    [%2d]] In Cthread_detach(%d) called at/behind %s:%d\n",
            _Cthread_self(),cid,file,line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* We check that this cid correspond to a known */
  /* process                                      */
  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);

  n = 1;
  while (current->next != NULL) {
    current = current->next;
    if (current->cid == cid) {
      n = 0;
      break;
    }
  }
  _Cthread_release_mtx(file,line,&(Cthread.mtx));

  if (n) {
    serrno = EINVAL;
    return(-1);
  }

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  return(0);
#else /* ifdef _NOCTHREAD */
  /* This is a thread implementation */
  /* Is it already detached ? */
  if (current->detached) {
    /* Yes. It is OK for Cthread point of view */
    return(0);
  }
  /* Not yet detached: let's do it */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if ((n = pthread_detach(current->pid))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  current->detached = 1;
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  /* Bloody DCE is changing pthread_t structure */
  {
    int status;
    Cth_pid_t thispid;

    memcpy(&thispid,&(current->pid),sizeof(Cth_pid_t));
    if (pthread_detach(&thispid)) {
      serrno = SECTHREADERR;
      return(-1);
    }
  }
  current->detached = 1;
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  if ((n = pthread_detach(current->pid))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  current->detached = 1;
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  /* This is a noop. Windows doesn't make any real distinction */
  current->detached = 1;
#else /* _CTHREAD_PROTO */
  serrno = EOPNOTSUP;
  return(-1);
#endif
  return(0);
#endif /* ifdef _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Self                      */
/* Arguments: caller source file                */
/*            caller source line                */
/* -------------------------------------------- */
/* Output   : >= 0    - Cthread-ID              */
/*            <  0    - ERROR                   */
/* -------------------------------------------- */
/* History:                                     */
/* 08-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
CTHREAD_DECL Cthread_Self0() {
  return(Cthread_Self("Cthread.c(Cthread_Self0)",__LINE__));
}
CTHREAD_DECL Cthread_Self(file, line)
     char *file;
     int line;
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  unsigned            thID;             /* WIN32 thread ID */
#else /* _CTHREAD_PROTO_WIN32 */
  Cth_pid_t             pid;              /* Current PID      */
#endif /* _CTHREAD_PROTO_WIN32 */
  int                 n;                /* Return value     */

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,"[Cthread    [%2d]] In Cthread_self() called at/behind %s:%d\n",
            _Cthread_self(),file,line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Get current pid */
#ifdef _NOCTHREAD
  pid = getpid();
#else
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  thID = GetCurrentThreadId();
#else /* _CTHREAD_PROTO_WIN32 */
  pid = pthread_self();
#endif /* _CTHREAD_PROTO_WIN32 */
#endif

  /* We check that this pid correspond to a known */
  /* process                                      */
  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);

  n = -1;
  while (current->next != NULL) {
    current = current->next;
#ifdef _NOCTHREAD
    if (current->pid == pid) {
      n = current->cid;
      break;
    }
#else
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_POSIX
#ifdef CTHREAD_DEBUG
    if (file != NULL) {
      fprintf(stderr,"[Cthread    [%2d]] In Cthread_self() called at/behind %s:%d : Comparing current->pid=%d and pid=%d\n",_Cthread_self(),
              file,line,
              (int) current->pid, (int) pid);
    }
#endif
    if (pthread_equal(current->pid,pid)) {
      n = current->cid;
      break;
    }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_DCE
    if (pthread_equal(current->pid,pid)) {
      n = current->cid;
      break;
    }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_LINUX
    if (pthread_equal(current->pid,pid)) {
      n = current->cid;
      break;
    }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
    if ( current->thID == thID ) {
      n = current->cid;
	  break;
	}
#endif
#endif
  }
  _Cthread_release_mtx(file,line,&(Cthread.mtx));

  if (n < 0)
    serrno = EINVAL;
  return(n);
}

/* ============================================ */
/* Routine  : Cthread_destroy                   */
/* Arguments: Cthread ID                        */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 08-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int _Cthread_destroy(file, line, cid)
     char *file;
     int line;
     int cid;
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  struct Cid_element_t *previous = NULL;  /* Curr Cid_element */
  int                 n;                /* Status           */

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,"[Cthread    [%2d]] In _Cthread_destroy(%d) called at/behind %s:%d\n",
            _Cthread_self(),cid,file,line);
  }
#endif

  /* We check that this cid correspond to a known */
  /* process                                      */
  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);

  n = 1;
  previous = NULL;
  while (current->next != NULL) {
    previous = current;
    current = current->next;
    if (current->cid == cid) {
      n = 0;
      break;
    }
  }

  if (n) {
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
    serrno = EINVAL;
    return(-1);
  }

  /* It is a known process - We delete the entry */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  /* For threads created with _beginthreadex() we must do cleanup */
  if ( !current->detached ) CloseHandle((HANDLE)current->pid);
#endif /* _CTHREAD_PROTO_WIN32 */
  if (previous != NULL) 
    previous->next = current->next;
  free(current);
  _Cthread_release_mtx(file,line,&(Cthread.mtx));

  return(0);
}

/* ============================================ */
/* Routine  : Cthread_Cond_Broadcast            */
/* Arguments: caller source file                */
/*            caller source line                */
/*            address to get a condition signal */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (NOT OK)                */
/* -------------------------------------------- */
/* History:                                     */
/* 07-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
CTHREAD_DECL Cthread_Cond_Broadcast(file, line, addr)
     char *file;
     int line;
     void *addr;
{
#ifdef _CTHREAD
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  int                  n;                 /* Status            */
  int                  rc;
#endif

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,
            "[Cthread    [%2d]] In Cthread_cond_broadcast(0x%x) called at/behind %s:%d\n",
            _Cthread_self(),(unsigned long) addr,
            file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  return(0);
#else
  /* This is a thread implementation            */
  /* We search to see if the mutex is already   */
  /* implemented                                */
  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);
  n = 1;
  while (current->next != NULL) {
    current = current->next;
    if (current->addr == addr) {
      n = 0;
      break;
    }
  }
  if (n) {
    /* There is NO already associated mutex     */
    /* Cthread consider it as a progamming logic  */
    /* error, since in thread programming, the  */
    /* algorithm should ALWAYS be:              */
    /* 1  - create a mutex                      */
    /* 1b - create a conditionnal variable      */
    /* (this is done automatically) by Cthread    */
    /* 2 - Loop on a predicate                  */
    /* (already created by Cthread)               */
    /* 3 - Wait inside the loop                 */
    /* (this automatically release the mutex)   */
    /* 4 - Release the mutex, gained again at   */
    /* the return of the wait condition         */
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
    serrno = EINVAL;
    return(-1);
  }
  rc = 0;
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if (current->nwait > 1) {
    if ((rc = pthread_cond_broadcast(&(current->cond)))) {
      errno = rc;
      serrno = SECTHREADERR;
      rc = -1;
    }
  } else {
    if ((rc = pthread_cond_signal(&(current->cond)))) {
      errno = rc;
      serrno = SECTHREADERR;
      rc = -1;
    }
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (current->nwait > 1) {
    if (pthread_cond_broadcast(&(current->cond))) {
      serrno = SECTHREADERR;
      rc = -1;
    }
  } else {
    if (pthread_cond_signal(&(current->cond))) {
      serrno = SECTHREADERR;
      rc = -1;
    }
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* On linux pthread_cond_broadcast and pthread_cond_signal */
  /* never returns an error code...                          */
  if (current->nwait > 1)
    pthread_cond_broadcast(&(current->cond));
  else
    pthread_cond_signal(&(current->cond));
  rc = 0;
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  if (current->nwait > 1) {
    rc = _Cthread_win32_cond_broadcast(&(current->cond));
  } else {
    rc = _Cthread_win32_cond_signal(&(current->cond));
  }
#else
  serrno = EOPNOTSUP;
  return(-1);
#endif /* _CTHREAD_PROTO */
  _Cthread_release_mtx(file,line,&(Cthread.mtx));
  return(rc);
#endif /* ifdef _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Wait_Condition            */
/* Arguments: caller source file                */
/*            caller source line                */
/*            address to get a condition on     */
/*            integer eventual timeout          */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (NOT OK)                */
/* -------------------------------------------- */
/* History:                                     */
/* 07-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
CTHREAD_DECL Cthread_Wait_Condition(file, line, addr, timeout)
     char *file;
     int line;
     void *addr;
     int timeout;
{
#ifdef _CTHREAD
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  int                    n;                 /* Status            */
  int                    rc;
#endif
  
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,
            "[Cthread    [%2d]] In Cthread_wait_condition(0x%x,%d) called at/behind %s:%d\n",
            _Cthread_self(),
            (unsigned long) addr,(int) timeout,
            file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }
  
#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  return(0);
#else

  /* This is a thread implementation            */
  /* We search to see if the mutex is already   */
  /* implemented                                */
  
  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);
  n = 1;
  while (current->next != NULL) {
    current = current->next;
    if (current->addr == addr) {
      n = 0;
      break;
    }
  }
  if (n != 0) {
    /* There is NO already associated mutex     */
    /* Cthread consider it as a progamming logic*/
    /* error, since in thread programming, the  */
    /* algorithm should ALWAYS be:              */
    /* 1  - create a mutex                      */
    /* 1b - create a conditionnal variable      */
    /* (this is done automatically) by Cthread  */
    /* 2 - Loop on a predicate                  */
    /* (already created by Cthread)             */
    /* 3 - Wait inside the loop                 */
    /* (this automatically release the mutex)   */
    /* 4 - Release the mutex, gained again at   */
    /* the return of the wait condition         */

    _Cthread_release_mtx(file,line,&(Cthread.mtx));
    serrno = EINVAL;
    return(-1);
  }

  /* Flag the number of threads waiting on addr */

  ++current->nwait;

  _Cthread_release_mtx(file,line,&(Cthread.mtx));
                    
  if (timeout <= 0) {
    /* No timeout : pthread_cond_wait */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_cond_wait(&(current->cond),&(current->mtx)))) {
      errno = n;
      serrno = SECTHREADERR;
      rc = -1;
    } else {
      rc = 0;
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_cond_wait(&(current->cond),&(current->mtx))) {
      serrno = SECTHREADERR;
      rc = -1;
    } else {
      rc = 0;
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    /* On linux pthread_cond_wait never returns an error code... */
    pthread_cond_wait(&(current->cond),&(current->mtx));
    rc = 0;
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32 
    rc = _Cthread_win32_cond_wait(&(current->cond),&(current->mtx),-1);
#else
    serrno = SEOPNOTSUP;
    rc = -1;
#endif /* _CTHREAD_PROTO */
  } else {
#if _CTHREAD_PROTO !=  _CTHREAD_PROTO_WIN32
	  /* timeout > 0 : pthread_cond_timedwait */
    struct timeval          tv;
    struct timespec         ts;
    /* Get current time */
    if (gettimeofday(&tv, (void *) NULL) < 0) {
      serrno = SEINTERNAL;
      rc = -1;
    } else {
      /* timeout seconds in the future */
      ts.tv_sec = tv.tv_sec + timeout;
      /* microsec to nanosec */
      ts.tv_nsec = tv.tv_usec * 1000;
#endif /* _CTHREAD_PROTO !=  _CTHREAD_PROTO_WIN32 */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
      if ((n = pthread_cond_timedwait(&(current->cond),
                                      &(current->mtx),&ts))) {
        errno = n;
        serrno = ( errno = ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
      if (pthread_cond_timedwait(&(current->cond),
                                      &(current->mtx),&ts)) {
        serrno = ( errno = ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
      if ((n = pthread_cond_timedwait(&(current->cond),
                                      &(current->mtx),&ts))) {
        errno = n;
        serrno = ( errno = ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32 
      rc = _Cthread_win32_cond_wait(&(current->cond),&(current->mtx),timeout);
#else
      serrno = SEOPNOTSUP;
      rc = -1;
#endif
    }
    /* OK */
    /* [jdurand] why is it there ??? rc = 0; */
  }

  /* This end of the routine will decrease the number */
  /* of wait doing a Wait_Condition on addr           */

  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);
  while (current->next != NULL) {
    current = current->next;
    if (current->addr == addr) {
      /* Decrease the number of waiting threads */
      --current->nwait;
      break;
    }
  }
  _Cthread_release_mtx(file,line,&(Cthread.mtx));
  return(rc);
#endif /* ifdef _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Lock_Mtx                  */
/* Arguments: caller source file                */
/*            caller source line                */
/*            address to get a lock on          */
/*            integer eventual timeout          */
/* (see _Cthread_obtain_mtx)                    */
/* -------------------------------------------- */
/* Output   : integer >= 0 - Cthread ID         */
/*                                              */
/* -------------------------------------------- */
/* History:                                     */
/* 06-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
CTHREAD_DECL Cthread_Lock_Mtx(file, line, addr, timeout)
     char *file;
     int line;
     void *addr;
     int timeout;
{
#ifdef _CTHREAD
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  int                  n;                 /* Status            */
#endif

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,
            "[Cthread    [%2d]] In Cthread_lock_mtx(0x%x,%d) called at/behind %s:%d\n",
            _Cthread_self(),
            (unsigned long) addr,(int) timeout,
            file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  return(0);
#else
  /* This is a thread implementation            */
  /* We search to see if the mutex is already   */
  /* implemented                                */

  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);

  n = 1;
  while (current->next != NULL) {
    current = current->next;
    if (current->addr == addr) {
      n = 0;
      break;
    }
  }

  _Cthread_release_mtx(file,line,&(Cthread.mtx));

  if (n) {
    /* Not yet existing mutex : we have to create one */
    struct Cmtx_element_t *Cmtx_new;

    if ((Cmtx_new = (struct Cmtx_element_t *) malloc(sizeof(struct Cmtx_element_t)))
        == NULL) {
      serrno = SEINTERNAL;
      return(-1);
    }

    /* Fill the addr element */
    Cmtx_new->addr = addr;
    /* Make sure it points to nothing after */
    Cmtx_new->next = NULL;
    /* Counter to know the number of thread waiting on addr */
    Cmtx_new->nwait = 0;
    /* Create the mutex and cond. var. associated */
    {
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
      /* Create the mutex a-la POSIX */
      pthread_mutexattr_t  mattr;
      pthread_condattr_t  cattr;
      /* Create the mutex attribute */
      if ((n = pthread_mutexattr_init(&mattr))) {
        /* Release Cmtx element */
        free(Cmtx_new);
        errno = n;
        serrno = SECTHREADERR;
        return(-1);
      }
      if ((n = pthread_mutex_init(&(Cmtx_new->mtx),&mattr))) {
        /* Release mutex attribute resources */
        pthread_mutexattr_destroy(&mattr);
        /* Release Cmtx element */
        free(Cmtx_new);
        errno = n;
        serrno = SECTHREADERR;
        return(-1);
      }
      /* Create the associate conditionnal variable attribute */
      if ((n = pthread_condattr_init(&cattr))) {
        /* Release mutex attribute resources */
        pthread_mutexattr_destroy(&mattr);
        /* Release mutexes */
        pthread_mutex_destroy(&(Cmtx_new->mtx));
        /* Release Cmtx element */
        free(Cmtx_new);
        errno = n;
        serrno = SECTHREADERR;
        return(-1);
      }
      if ((n = pthread_cond_init(&(Cmtx_new->cond),&cattr))) {
        /* Release cond attribute resources */
        pthread_condattr_destroy(&cattr);
        /* Release mutex attribute resources */
        pthread_mutexattr_destroy(&mattr);
        /* Release mutexes */
        pthread_mutex_destroy(&(Cmtx_new->mtx));
        /* Release Cmtx element */
        free(Cmtx_new);
        errno = n;
        serrno = SECTHREADERR;
        return(-1);
      }
      /* Release cond attribute resources */
      pthread_condattr_destroy(&cattr);
      /* Release mutex attribute resources */
      pthread_mutexattr_destroy(&mattr);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
      /* Create the mutex a-la DCE */
      if (pthread_mutex_init(&(Cmtx_new->mtx),pthread_mutexattr_default)) {
        /* Release Cmtx element */
        free(Cmtx_new);
        serrno = SECTHREADERR;
        return(-1);
      }
      if (pthread_cond_init(&(Cmtx_new->cond),pthread_condattr_default)) {
        /* Release mutexes */
        pthread_mutex_destroy(&(Cmtx_new->mtx));
        /* Release Cmtx element */
        free(Cmtx_new);
        serrno = SECTHREADERR;
        return(-1);
      }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
      /* Create the mutex a-la POSIX */
      pthread_mutexattr_t  mattr;
      pthread_condattr_t  cattr;
      /* Create the mutex attribute */
      if ((n = pthread_mutexattr_init(&mattr))) {
        /* Release Cmtx element */
        free(Cmtx_new);
        errno = n;
        serrno = SECTHREADERR;
        return(-1);
      }
      if ((n = pthread_mutex_init(&(Cmtx_new->mtx),&mattr))) {
        /* Release mutex attribute resources */
        pthread_mutexattr_destroy(&mattr);
        /* Release Cmtx element */
        free(Cmtx_new);
        errno = n;
        serrno = SECTHREADERR;
        return(-1);
      }
      /* Create the associate conditionnal variable attribute */
      if ((n = pthread_condattr_init(&cattr))) {
        /* Release mutex attribute resources */
        pthread_mutexattr_destroy(&mattr);
        /* Release mutexes */
        pthread_mutex_destroy(&(Cmtx_new->mtx));
        /* Release Cmtx element */
        free(Cmtx_new);
        errno = n;
        serrno = SECTHREADERR;
        return(-1);
      }
      if ((n = pthread_cond_init(&(Cmtx_new->cond),&cattr))) {
        /* Release cond attribute resources */
        pthread_condattr_destroy(&cattr);
        /* Release mutex attribute resources */
        pthread_mutexattr_destroy(&mattr);
        /* Release mutexes */
        pthread_mutex_destroy(&(Cmtx_new->mtx));
        /* Release Cmtx element */
        free(Cmtx_new);
        errno = n;
        serrno = SECTHREADERR;
        return(-1);
      }
      /* Release cond attribute resources */
      pthread_condattr_destroy(&cattr);
      /* Release mutex attribute resources */
      pthread_mutexattr_destroy(&mattr);
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
#  ifdef _CTHREAD_WIN32MTX
      if ( (Cmtx_new->mtx = CreateMutex(NULL,FALSE,NULL)) == NULL ) {
        /* Release Cmtx element */
        free(Cmtx_new);
        serrno = SECTHREADERR;
        return(-1);
	  }
#  else /* _CTHREAD_WIN32MTX */
      /*
       * Default is to use critical section.
       * The InitializeCriticalSection does not return a 
       * value so we simply assume it was successful.
       */
      InitializeCriticalSection(&(Cmtx_new->mtx.mtx));
      Cmtx_new->mtx.nb_locks = 0;
#  endif /* _CTHREAD_WIN32MTX */
      if ( (n = _Cthread_win32_cond_init(&(Cmtx_new->cond))) ) {
        /* Release mutex */
#  ifdef _CTHREAD_WIN32MTX
        CloseHandle(Cmtx_new->mtx);
#  else /* _CTHREAD_WIN32MTX */
        DeleteCriticalSection(&(Cmtx_new->mtx.mtx));
#  endif /* _CTHREAD_WIN32MTX */
		/* Release Cmtx element */
        free(Cmtx_new);
        serrno = SECTHREADERR;
        return(-1);
      }
#else
      serrno = SEOPNOTSUP;
      return(-1);
#endif
    }
    /* Cmtx_new->mtx contains now a valid mutex   */
    /* AND a valid contionnal variable            */
    if (_Cthread_addmtx(file,line,Cmtx_new)) {
      /* Error ! Destroy what we have created */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
      pthread_mutex_destroy(&(current->mtx));
      pthread_cond_destroy(&(current->cond));
      free(Cmtx_new);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
      pthread_mutex_destroy(&(current->mtx));
      pthread_cond_destroy(&(current->cond));
      free(Cmtx_new);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
      pthread_mutex_destroy(&(current->mtx));
      pthread_cond_destroy(&(current->cond));
      free(Cmtx_new);
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
#  ifdef _CTHREAD_WIN32MTX
      CloseHandle(Cmtx_new->mtx);
#  else /* _CTHREAD_WIN32MTX */
      DeleteCriticalSection(&(Cmtx_new->mtx.mtx));
#  endif /* _CTHREAD_WIN32MTX */
      _Cthread_win32_cond_destroy(&(Cmtx_new->cond));
      free(Cmtx_new);
#else
      /* Nothing to clear there */
#endif
      return(-1);
    }
    /* We now have to obtain the lock on this mutex */
    return(_Cthread_obtain_mtx(file,line,&(Cmtx_new->mtx),timeout));
  } else {
    /* We have to obtain the lock on this mutex */
    return(_Cthread_obtain_mtx(file,line,&(current->mtx),timeout));
  }
#endif /* _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Mutex_Unlock              */
/* Arguments: caller source file                */
/*            caller source line                */
/*            address to get a lock off         */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 06-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
CTHREAD_DECL Cthread_Mutex_Unlock(file, line, addr)
     char *file;
     int line;
     void *addr;
{
#ifdef _CTHREAD
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  int                  n;                 /* Status            */
#endif

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,
            "[Cthread    [%2d]] In Cthread_unlock_mtx(0x%x) called at/behind %s:%d\n",
            _Cthread_self(),(unsigned long) addr,
            file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  return(0);
#else
  /* This is a thread implementation            */
  /* We search to see if the mutex is already   */
  /* implemented                                */

  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);

  n = 1;
  while (current->next != NULL) {
    current = current->next;
    if (current->addr == addr) {
      n = 0;
      break;
    }
  }

  _Cthread_release_mtx(file,line,&(Cthread.mtx));

  if (n) {
    /* Not yet existing mutex */
    serrno = EINVAL;
    return(-1);
  } else {
    return(_Cthread_release_mtx(file,line,&(current->mtx)));
  }
#endif /* ifdef _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Mutex_Destroy             */
/* Arguments: caller source file                */
/*            caller source line                */
/*            address to get a lock deleted     */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 08-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
CTHREAD_DECL Cthread_Mutex_Destroy(file, line, addr)
     char *file;
     int line;
     void *addr;
{
#ifdef _CTHREAD
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  struct Cmtx_element_t *previous = NULL;   /* Prev Cmtx_element */
  int                    n;                 /* Status            */
#endif

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,
            "[Cthread    [%2d]] In Cthread_mutex_destroy(0x%x) called at/behind %s:%d\n",
            _Cthread_self(),(unsigned long) addr,file,line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  return(0);
#else
  /* This is a thread implementation            */
  /* We search to see if the mutex is already   */
  /* implemented                                */

  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);

  n = 1;
  previous = NULL;
  while (current->next != NULL) {
    previous = current;
    current = current->next;
    if (current->addr == addr) {
      n = 0;
      break;
    }
  }

  _Cthread_release_mtx(file,line,&(Cthread.mtx));

  if (n) {
    /* Not yet existing mutex */
    serrno = EINVAL;
    return(-1);
  } else {
    /* Destroy the mutex */
    if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
      return(-1);
    /* By: - changing the previous next pointer */
    if (previous != NULL)
      previous->next = current->next;
    /*     - Releasing the mutex and cond       */
    n = 0;
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    n += pthread_mutex_destroy(&(current->mtx));
    n += pthread_cond_destroy(&(current->cond));
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    n += pthread_mutex_destroy(&(current->mtx));
    n += pthread_cond_destroy(&(current->cond));
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    n += pthread_mutex_destroy(&(current->mtx));
    n += pthread_cond_destroy(&(current->cond));
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
#  ifdef _CTHREAD_WIN32MTX
    CloseHandle(current->mtx);
#  else /* _CTHREAD_WIN32MTX */
    DeleteCriticalSection(&(current->mtx.mtx));
#  endif /* _CTHREAD_WIN32MTX */
    _Cthread_win32_cond_destroy(&(current->cond));
#else
    serrno = SEOPNOTSUP;
    return(-1);
#endif
    free(current);
    if (n != 0) {
      serrno = SECTHREADERR;
      n = -1;
    }
    return(n);
  }
#endif /* ifdef _NOCTHREAD */
}

/* ============================================ */
/* Routine  : _Cthread_release_mtx              */
/* Arguments: address of a mutex                */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 07-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* ============================================ */
int _Cthread_release_mtx(file, line, mtx)
     char *file;
     int line;
     Cth_mtx_t *mtx;
{
#ifdef _CTHREAD
   int               n;
#endif

#ifdef CTHREAD_DEBUG
   if (file != NULL) {
     fprintf(stderr,"[Cthread    [%2d]] In _Cthread_release_mtx(0x%x) called at/behind %s:%d\n",
             _Cthread_self(),(unsigned long) mtx, file, line);
   }
#endif

#ifndef _CTHREAD
  /* This is not a thread environment */
  return(0);
#else
  /* This is a thread environment */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if ((n = pthread_mutex_unlock(mtx))) {
    serrno = SECTHREADERR;
    errno = n;
    return(-1);
  } else {
    return(0);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_mutex_unlock(mtx)) {
    serrno = SECTHREADERR;
    return(-1);
  } else {
    return(0);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  if ((n = pthread_mutex_unlock(mtx))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  } else {
    return(0);
  }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
#  ifdef _CTHREAD_WIN32MTX
  if ( ( n = ReleaseMutex((HANDLE)*mtx) ) ) {
    return(0);
  } else {
    serrno = SECTHREADERR;
    return(-1);
  }
#  else /* _CTHREAD_WIN32MTX */
  /*
   * We must make sure that we own the critical section
   * before releasing it, otherwise there is a risk for
   * dead-lock. Note that critical sections are recursive
   * so there must be one "leave" for every "enter".
   */
  if ( TryEnterCriticalSection(&(mtx->mtx)) == TRUE ) {
      n=mtx->nb_locks + 1;
      /*
       * Reset counter before release to avoid race condition.
       */
      mtx->nb_locks = 0;
      while ( n-- > 0 ) LeaveCriticalSection(&(mtx->mtx));
  } else {
    serrno = SECTHREADERR;
    n = -1;
  }
  return(n);
#  endif /* _CTHREAD_WIN32MTX */
#else
  serrno = SEOPNOTSUP;
  return(-1);
#endif /* _CTHREAD_PROTO */
#endif
}

/* ============================================ */
/* Routine  : _Cthread_obtain_mtx               */
/* Arguments: address of a mutex                */
/*            integer - timeout                 */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 07-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine tries to get a mutex lock, with */
/* the use of a conditionnal variable if there  */
/* is one (the argument can be NULL), and an    */
/* eventual timeout:                            */
/* timeout < 0          - mutex_lock            */
/* timeout = 0          - mutex_trylock         */
/* timeout > 0          - cond_timedwait        */
/*                     or lock_mutex            */
/* The use of cond_timedwait is caution to the  */
/* non-NULL value to the second argument.       */
/* ============================================ */
int _Cthread_obtain_mtx(file, line, mtx, timeout)
     char *file;
     int line;
     Cth_mtx_t *mtx;
     int timeout;
{
#ifdef _CTHREAD
  int               n;
#endif

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,"[Cthread    [%2d]] In _Cthread_obtain_mtx(0x%x,%d) called at/behind %s:%d\n",_Cthread_self(),
            (unsigned long) mtx,(int) timeout, file, line);
  }
#endif

#ifndef _CTHREAD
  /* This is not a thread implementation */
  return(0);
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32 && defined(_CTHREAD_WIN32MTX)
  /*
   * Use Win32 mutex handles
   */
  if ( timeout < 0 ) {
	if ( (n = WaitForSingleObject((HANDLE)*mtx,INFINITE)) == WAIT_FAILED ) {
      serrno = SECTHREADERR;
      return(-1);
	} else {
      return(0);
    }
  } else {
    if ( (n=WaitForSingleObject((HANDLE)*mtx,timeout * 1000)) == WAIT_FAILED ) {
      serrno = SECTHREADERR;
      return(-1);
	} else if ( n == WAIT_TIMEOUT ) {
      serrno = SETIMEDOUT;
      return(-1);
	} else {
      return(0);
    }
  }
#else 
  /* This is a thread implementation */
  if (timeout < 0) {
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 && !defined(_CTHREAD_WIN32MTX)
    /*
     * Use Win32 critical section
     */
#ifdef CTHREAD_DEBUG
    if (file != NULL) {
      fprintf(stderr,"[Cthread    [%2d]] in _Cthread_obtain_mtx called at %s:%d : Enter Critical section 0x%x\n",
              _Cthread_self(),file,line,mtx);
    }
#endif /* CTHREAD_DEBUG */
    EnterCriticalSection(&(mtx->mtx));
    mtx->nb_locks++;
    return(0);
#  else /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 && _CTHREAD_WIN32MTX */
    /* Try to get the lock */
#    if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_mutex_lock(mtx))) {
      serrno = SECTHREADERR;
      errno = n;
      return(-1);
    }
    return(0);
#    elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_mutex_lock(mtx)) {
      serrno = SECTHREADERR;
      return(-1);
    }
    return(0);
#    elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    if ((n = pthread_mutex_lock(mtx))) {
      serrno = SECTHREADERR;
      errno = n;
      return(-1);
    }
    return(0);
#    else
    serrno = SEOPNOTSUP;
    return(-1);
#    endif /* _CTHREAD_PROTO */
#  endif /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 && _CTHREAD_WIN32MTX */
  } else if (timeout == 0) {
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_mutex_trylock(mtx))) {
      /* EBUSY or EINVAL */
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    } else {
      return(0);
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_mutex_trylock(mtx) != 1) {
      serrno = SECTHREADERR;
      return(-1);
    } else {
      return(0);
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    if ((n = pthread_mutex_trylock(mtx))) {
      /* EBUSY or EINVAL */
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    } else {
      return(0);
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 && defined(_CTHREAD_WIN32MTX)
    if ( TryEnterCriticalSection(&(mtx->mtx)) == TRUE ) {
        mtx->nb_locks++;
        return(0);
    } else {
      serrno = SECTHREADERR;
      return(-1);
    }
#else
    serrno = SEOPNOTSUP;
    return(-1);
#endif /* _CTHREAD_PROTO */
  } else {
    /* This code has been kept from comp.programming.threads */
    /* timeout > 0 */
    int gotmutex   = 0;
    unsigned long timewaited = 0;
    unsigned long Timeout = 0;

    /* Convert timeout in milliseconds */
    Timeout = timeout * 1000;

    while (timewaited < Timeout && ! gotmutex) {
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
      if ((n = pthread_mutex_trylock(mtx)))
        errno = n;
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
      n = pthread_mutex_trylock(mtx);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
      if ((n = pthread_mutex_trylock(mtx)))
        errno = n;
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 && defined(_CTHREAD_WIN32MTX)
      n = -1;
      if ( TryEnterCriticalSection(&(mtx->mtx)) == TRUE ) {
          n = 0;
          mtx->nb_locks++;
      } else {
        errno = EBUSY;
      }
#else
      serrno = SEOPNOTSUP;
      return(-1);
#endif /* _CTHREAD_PROTO */
      if (errno == EDEADLK || n == 0) {
        gotmutex = 1;
        return(0);
      }
      if (errno == EBUSY) {
        timewaited += Timeout / 20;
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
        /* Win32 Sleep is in milliseconds */
        Sleep(Timeout/20);
#  else /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */
        /* usleep is in micro-seconds, not milli seconds... */
        usleep((Timeout * 1000)/20);
#endif /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */
      }
    }
    serrno = ETIMEDOUT;
    return(-1);
  }
#endif /* ifndef _CTHREAD */
}

/* ============================================ */
/* Routine  : _Cthread_addmtx                   */
/* Arguments: Cmtx_new - New Cmtx_element       */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 07-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine is adding a new mtx element     */
/* ============================================ */
int _Cthread_addmtx(file, line, Cmtx_new)
     char *file;
     int line;
     struct Cmtx_element_t *Cmtx_new;
{
#ifdef _CTHREAD
  struct Cmtx_element_t *current = &Cmtx;    /* Curr Cmtx_element */
#endif
  
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,"[Cthread    [%2d]] In _Cthread_addmtx(0x%x) called at/behind %s:%d\n",
            _Cthread_self(),(unsigned long) Cmtx_new, file, line);
  }
#endif

#ifndef _CTHREAD
  /* Not a thread implementation */
  return(0);
#else
  if ( _Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1) ) return(-1);
  /* First found the last element */
  while (current->next != NULL) {
    current = current->next;
  }

  /* Change pointer of last element to its next one */
  current->next = Cmtx_new;

  /* Make sure that new structure points to no else */
  /* element                                        */
  ((struct Cmtx_element_t *) current->next)->next = NULL;
  
  _Cthread_release_mtx(file,line,&(Cthread.mtx));

  return(0);
#endif
}

/* ============================================ */
/* Routine  : _Cthread_addcid                   */
/* Arguments: pid   * - Thread/Process ID       */
/*            thID    - Thread ID (WIN32 only)  */
/*       startroutine - Thread/Process start-up */
/*            flag    - Detached flag           */
/* -------------------------------------------- */
/* Output   : >= 0 (OK) -1 (ERROR)              */
/* -------------------------------------------- */
/* History:                                     */
/* 06-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* 30-APR-1999       Take into account that the */
/*                   call may come from the     */
/*                   thread startup where the   */
/*                   thread handle (Windows) is */
/*                   unknown.                   */
/*                   Olof.Barring@cern.ch       */
/* ============================================ */
/* Notes:                                       */
/* This routine is adding a new cid element     */
/* If the element happens to already exist then */
/* its parameters are changed and the element   */
/* cid is returned                              */
/* ============================================ */
int _Cthread_addcid(Cthread_file, Cthread_line, file, line, pid, thID, startroutine, detached)
     char *Cthread_file;
     int Cthread_line;
     char *file;
     int line;
     Cth_pid_t *pid;
     unsigned thID;
#ifdef __STDC__
     void *(*startroutine)(void *);
#else
     void *(*startroutine)();
#endif
     int detached;
{
  struct Cid_element_t *current = &Cid;    /* Curr Cid_element */
  int                 current_cid = -1;    /* Curr Cthread ID    */

#ifdef CTHREAD_DEBUG
  fprintf(stderr,"[Cthread    [%2d]] In _Cthread_addcid(0x%x,%d,0x%x,%d) called at %s:%d and behind %s:%d\n",
          _Cthread_self(),
          (unsigned long) pid,(int) thID, (unsigned long) startroutine, (int) detached,
          Cthread_file, Cthread_line,
          file != NULL ? file : "(disabled)",
          file != NULL ? line : -1);
#endif
  
  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);
  
  /* First found the last element */
  while (current->next != NULL) {
    current = current->next;
#ifndef _CTHREAD
    if (current->pid == *pid) {
      current->detached = detached;
#ifdef CTHREAD_DEBUG
      fprintf(stderr,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d and behind %s:%d : Already existing cid = %d\n",_Cthread_self(),
              Cthread_file,Cthread_line,
              file != NULL ? file : "(disabled)",
              file != NULL ? line : -1,
              current->cid);
#endif
      _Cthread_release_mtx(file,line,&(Cthread.mtx));
      return(current->cid);
    }
#else /* _CTHREAD */
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
    if (current->thID == thID) {
      current->detached = detached;
      if ( pid != NULL ) current->pid = *pid;
#ifdef CTHREAD_DEBUG
      fprintf(stderr,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d and behind %s:%d : Already existing cid = %d\n",_Cthread_self(),
              Cthread_file,Cthread_line,
              file != NULL ? file : "(disabled)",
              file != NULL ? line : -1,
              current->cid);
#endif
      _Cthread_release_mtx(&(Cthread.mtx));
      return(current->cid);
    }
#else /* _CTHREAD_PROTO_WIN32 */
    if (pthread_equal(current->pid,*pid)) {
      current->detached = detached;
#ifdef CTHREAD_DEBUG
      fprintf(stderr,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d and behind %s:%d : Already existing cid = %d\n",_Cthread_self(),
              Cthread_file,Cthread_line,
              file != NULL ? file : "(disabled)",
              file != NULL ? line : -1,
              current->cid);
#endif
      _Cthread_release_mtx(file,line,&(Cthread.mtx));
      return(current->cid);
    }
#endif /* _CTHREAD_PROTO_WIN32 */
#endif /* _CTHREAD */
    current_cid = current->cid;
  } /* End of while ( current->next != NULL ) */
  
  /* Not found                         */
  
  /* Make sure next cid is positive... */
  if (++current_cid < 0) {
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
    serrno = SEINTERNAL;
    return(-1);
  }
  
  /* Change pointer of last element to its next one */
  if ((current->next = malloc(sizeof(struct Cid_element_t))) == NULL) {
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
    serrno = SEINTERNAL;
    return(-1);
  }
  
  /* Fill structure */
  /* 
   * For Windows pid may be == NULL because this routine may first
   * be called by the thread startup where the thread handle is
   * not accessible (only the thread ID is available within the
   * thre thread itself).
   */
  if ( pid != NULL ) {
    /* On some systems (like DCE) pthread_t is a structure */
    /*
    memcpy(&(((struct Cid_element_t *) current->next)->pid),
           pid,
           sizeof(Cth_pid_t));
    */
    /* 
	 * Normal assignment should work as well! even if *pid is
     * a structure (Olof).
	 */
    /*
     * Olof is right. Let's do normal C assigment, though
     */
    ((struct Cid_element_t *) current->next)->pid = *pid;
  }
  /* ((struct Cid_element_t *) current->next)->pid      = pid; */
  ((struct Cid_element_t *) current->next)->thID     = thID;
  ((struct Cid_element_t *) current->next)->addr     = startroutine;
  ((struct Cid_element_t *) current->next)->detached = detached;
  ((struct Cid_element_t *) current->next)->next     = NULL;
  ((struct Cid_element_t *) current->next)->cid      = current_cid;
  
  _Cthread_release_mtx(file,line,&(Cthread.mtx));
  
  /* Returns cid */
#ifdef CTHREAD_DEBUG
  fprintf(stderr,"[Cthread    [%2d]] _Cthread_addcid() called at %s:%d and behind %s:%d: Returning cid=%d\n",
          _Cthread_self(),
          Cthread_file,Cthread_line,
          file != NULL ? file : "(disabled)",
          file != NULL ? line : -1,
          current_cid);
#endif
  /* In a non-thread implementation is SHOULD return zero ! */
  return(current_cid);
}

/* ============================================ */
/* Routine  : _Cthread_once                     */
/* Arguments:                                   */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 07-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine is calling pthread_once and     */
/* thus initialize some internal vital internal */
/* Cthread variables.                           */
/* ============================================ */
void _Cthread_once() {
#ifdef _CTHREAD
#if _CTHREAD_PROTO !=  _CTHREAD_PROTO_WIN32
  pthread_mutexattr_t  mattr;
  int                  n;
#endif /* _CTHREAD_PROTO != _CTHREAD_PROTO_WIN32 */
#endif

#ifndef _CTHREAD
  /* In a non-thread environment, initialize the */
  /* pointers                                    */
  Cid.next     = NULL;
  Cspec.next   = NULL;
  _Cthread_once_status = 0;
  return;
#else
  /* In a thread environment, initialize the */
  /* Cthread internal structure                */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  /* Create the mutex a-la POSIX */
  /* Create the mutex attribute */
  if ((n = pthread_mutexattr_init(&mattr)) != 0) {
    errno = n;
	return;
  }
  if ((n = pthread_mutex_init(&(Cthread.mtx),&mattr)) != 0) {
    errno = n;
	return;
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    /* Create the mutex a-la DCE */
  if (pthread_mutex_init(&(Cthread.mtx),pthread_mutexattr_default) != 0) {
    return;
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* Create the mutex a-la LinuxThreads      */
  /* On this OS, pthread_mutexattr_init and  */
  /* pthread_mutex_init always returns 0 ... */
  pthread_mutexattr_init(&mattr);
  pthread_mutex_init(&(Cthread.mtx),&mattr);
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
#  ifdef _CTHREAD_WIN32MTX
  if ( !once ) Cthread.mtx = CreateMutex(NULL,FALSE,NULL);
  once = 1;
  if ( Cthread.mtx == NULL ) return;
#  else /* _CTHREAD_WIN32MTX */
  InitializeCriticalSection(&(Cthread.mtx.mtx));
  Cthread.mtx.nb_locks = 0;
  once = 1;
#  endif /* _CTHREAD_WIN32MTX */
#else
  /* Nothing to do ! */
#endif
  /* Initialize the structures */
  Cid.next   = NULL;
  Cmtx.next  = NULL;
  Cspec.next = NULL;
  _Cthread_once_status = 0;
  /* Initialize thread specific globals environment*/
  Cglobals_init(Cthread_Getspecific_init,Cthread_Setspecific0,Cthread_Self0); 
  return;
#endif /* ifndef _CTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_keydestructor             */
/* Arguments: void *addr                        */
/* -------------------------------------------- */
/* History:                                     */
/* 28-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
void _Cthread_keydestructor(addr)
     void *addr;
{
  /* Release the Thread-Specific key */
#ifdef CTHREAD_DEBUG
  fprintf(stderr,"[Cthread    [%2d]] In _Cthread_keydestructor(0x%x)\n",_Cthread_self(),(unsigned long) addr);
#endif
  free(addr);
}

/* ============================================ */
/* Routine  : Cthread_findglobalkey             */
/* Arguments: int *global_key                   */
/* -------------------------------------------- */
/* Output   : != NULL (OK) NULL (ERROR)         */
/* -------------------------------------------- */
/* History:                                     */
/* 29-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
struct Cspec_element_t *_Cthread_findglobalkey(file, line, global_key)
     char *file;
     int line;
     int *global_key;
{
  struct Cspec_element_t *current = &Cspec;   /* Curr Cspec_element */
  int                  n;                 /* Status            */

  /* Verify the arguments */
  if (global_key == NULL) {
    serrno = EINVAL;
    return(NULL);
  }

  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(NULL);

  n = 1;
  while (current->next != NULL) {
    current = current->next;
    if (current->global_key == global_key) {
      n = 0;
      break;
    }
  }

  _Cthread_release_mtx(file,line,&(Cthread.mtx));
  if (n) {
    if (file != NULL) {
      /* If file it is initialization, then we will have */
      /* recursive call because of serrno.               */
      serrno = EINVAL;
    }
    return(NULL);
  } else {
    return(current);
  }

}

/* ============================================ */
/* Routine  : Cthread_Setspecific               */
/* Arguments: caller source file                */
/*            caller source line                */
/*            int *global_key                   */
/* Arguments: void *addr                        */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 28-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* 30-APR-1999       Windows support            */
/*                   Olof.Barring@cern.ch       */
/* ============================================ */
CTHREAD_DECL Cthread_Setspecific0(global_key, addr)
     int *global_key;
     void *addr;
{
  return(Cthread_Setspecific("Cthread.c(Cthread_Setspecfic0)",__LINE__,global_key,addr));
}

CTHREAD_DECL Cthread_Setspecific(file, line, global_key, addr)
     char *file;
     int line;
     int *global_key;
     void *addr;
{
  struct Cspec_element_t *current = &Cspec;   /* Curr Cspec_element */
#ifdef _CTHREAD
  int                  n;                 /* Status            */
#endif

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,"[Cthread    [%2d]] In Cthread_setspecific(0x%x,0x%x) called at/behind %s:%d\n",
            _Cthread_self(),
            (unsigned long) global_key, (unsigned long) addr,
            file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Verify the arguments */
  if (global_key == NULL) {
    serrno = EINVAL;
    return(-1);
  }
#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  if ((current = _Cthread_findglobalkey(file,line,global_key)) == NULL) {
    /* Nothing yet */
    void *tsd = NULL;
    if (Cthread_getspecific(global_key, &tsd)) {
      return(-1);
    }
    /* And we check again the global structure    */
    if ((current = _Cthread_findglobalkey(file,line,global_key)) == NULL) {
      /* This should not happen */
      return(-1);
    }
  }
  current->addr = addr;
  return(0);
#else
  /* This is a thread implementation            */
  /* We search to see if the mutex is already   */
  /* implemented                                */

  if ((current = _Cthread_findglobalkey(file,line,global_key)) == NULL) {
    void *tsd = NULL;
    /* There is no global key equal to global_key */
    /* Let's create a new one                     */
    if (Cthread_getspecific(global_key, &tsd)) {
      return(-1);
    }
    /* And we check again the global structure    */
    if ((current = _Cthread_findglobalkey(file,line,global_key)) == NULL) {
      return(-1);
    }
  }
  
  /* We associate the "current" structure key that is */
  /* within with the addr given as argument           */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if ((n = pthread_setspecific(current->key, addr))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_setspecific(current->key, addr)) {
    serrno = SECTHREADERR;
    return(-1);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  if ((n = pthread_setspecific(current->key, addr))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  if ( !(n = TlsSetValue(current->key,addr)) ) {
    serrno = SECTHREADERR;
    return(-1);
  }
#else
  serrno = SEOPNOTSUP;
  return(-1);
#endif
  /* OK */
  return(0);
#endif
}

/* ============================================ */
/* Routine  : Cthread_Getspecific               */
/* Arguments: caller source file                */
/*            caller source line                */
/*            int *global_key                   */
/*            void **addr                       */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 29-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* 30-APR-1999       Windows support            */
/*                   Olof.Barring@cern.ch       */
/* ============================================ */
CTHREAD_DECL Cthread_Getspecific0(global_key, addr)
     int *global_key;
     void **addr;
{
  return(Cthread_Getspecific("Cthread.c(Cthread_Getspecific0)",__LINE__,global_key,addr));
}

CTHREAD_DECL Cthread_Getspecific_init(global_key, addr)
     int *global_key;
     void **addr;
{
  /* Just to avoid debug printing recursion from Cglobals_init() */
  return(Cthread_Getspecific(NULL,__LINE__,global_key,addr));
}

CTHREAD_DECL Cthread_Getspecific(file, line, global_key, addr)
     char *file;
     int line;
     int *global_key;
     void **addr;
{
  struct Cspec_element_t *current = &Cspec;   /* Curr Cspec_element */
#ifdef _CTHREAD
  int                     n;                  /* Status             */
  void                   *tsd = NULL;         /* TSD address        */
#endif

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    /* to avoid recursion */
    fprintf(stderr,"[Cthread    [%2d]] In Cthread_getspecific(0x%x) called at/behind %s:%d\n",
            _Cthread_self(),
            (unsigned long) global_key,
            file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Verify the arguments */
  if (global_key == NULL || addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  if ((current = _Cthread_findglobalkey(file,line,global_key)) == NULL) {
    /* Nothing yet */
    struct Cspec_element_t *Cspec_new;
    
    if ((Cspec_new = (struct Cspec_element_t *) malloc(sizeof(struct Cspec_element_t)))
        == NULL) {
      serrno = SEINTERNAL;
      return(-1);
    }
    
    /* Initialize global_key */
    Cspec_new->global_key = global_key;
    /* Make sure it points to nothing after */
    Cspec_new->next = NULL;
    /* Cspec_new->key contains now a valid key   */
    
    if (_Cthread_addspec(file,line,Cspec_new)) {
      /* Error ! Destroy what we have created */
      free(Cspec_new);
      return(-1);
    }

    *addr = NULL;
    return(0);
  } else {
    *addr = current->addr;
    return(0);
  }
#else
  /* This is a thread implementation            */
  /* We search to see if the mutex is already   */
  /* implemented                                */

  if ((current = _Cthread_findglobalkey(file,line,global_key)) == NULL) {
    /* Not yet existing TSD : we have to create a     */
    /* Thread-Specific key for it                     */
    struct Cspec_element_t *Cspec_new;

    if ((Cspec_new = (struct Cspec_element_t *) malloc(sizeof(struct Cspec_element_t)))
        == NULL) {
      serrno = SEINTERNAL;
      return(-1);
    }

#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    /* Create the specific variable a-la POSIX */
    if ((n = pthread_key_create(&(Cspec_new->key), _Cthread_keydestructor))) {
      errno = n;
      serrno = SECTHREADERR;
      free(Cspec_new);
      return(-1);
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    /* Create the specific variable a-la DCE */
    if (pthread_keycreate(&(Cspec_new->key), _Cthread_keydestructor)) {
      serrno = SECTHREADERR;
      free(Cspec_new);
      return(-1);
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    /* Create the specific variable a-la LinuxThreads */
    if ((n = pthread_key_create(&(Cspec_new->key), _Cthread_keydestructor))) {
      errno = n;
      serrno = SECTHREADERR;
      free(Cspec_new);
      return(-1);
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
    if ( (n = TlsAlloc()) == 0xFFFFFFFF ) {
      free(Cspec_new);
      serrno = SECTHREADERR;
      return(-1);
	} else Cspec_new->key = n;
#else
    serrno = SEOPNOTSUP;
    return(-1);
#endif /* _CTHREAD_PROTO */
    /* Initialize global_key */
    Cspec_new->global_key = global_key;
    /* Make sure it points to nothing after */
    Cspec_new->next = NULL;
    /* Cspec_new->key contains now a valid key   */

    if (_Cthread_addspec(file,line,Cspec_new)) {
      /* Error ! Destroy what we have created */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
      pthread_key_delete(Cspec_new->key);
      free(Cspec_new);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
      /* No such routine in DCE ! */
      free(Cspec_new);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
      pthread_key_delete(Cspec_new->key);
      free(Cspec_new);
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
	  TlsFree(Cspec_new->key);
	  free(Cspec_new);
#else
      /* Nothing to clear there */
#endif
      return(-1);
    }
    /* We set the address of the TSD to NULL */
    *addr = NULL;
    /* We return OK */
    return(0);
  }
  /* We want to know the TSD address */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  tsd = pthread_getspecific(current->key);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_getspecific(current->key,&tsd)) {
    tsd = NULL;
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  tsd = pthread_getspecific(current->key);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  tsd = TlsGetValue(current->key);
#else
  serrno = SEOPNOTSUP;
  return(-1);
#endif
  /* We change the address location */
  *addr = tsd;
  /* We return OK */
  return(0);
#endif /* _NOCTHREAD */
}

/* ============================================ */
/* Routine  : _Cthread_addspec                  */
/* Arguments: Cspec_new - New Cspec_element     */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 07-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine is adding a new mtx element     */
/* ============================================ */
int _Cthread_addspec(file, line, Cspec_new)
     char *file;
     int line;
     struct Cspec_element_t *Cspec_new;
{
  struct Cspec_element_t *current = &Cspec;    /* Curr Cmtx_element */
  
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    fprintf(stderr,"[Cthread    [%2d]] In _Cthread_addspec(0x%x) called at/behind %s:%d\n",_Cthread_self(),
            (unsigned long) Cspec_new,file,line);
  }
#endif

  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);

  /* First found the last element */
  while (current->next != NULL) {
    current = current->next;
  }

  /* Change pointer of last element to its next one */
  current->next = Cspec_new;

  /* Make sure that new structure points to no else */
  /* element                                        */
  ((struct Cspec_element_t *) current->next)->next = NULL;
  
  _Cthread_release_mtx(file,line,&(Cthread.mtx));

  return(0);
}
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
/* ============================================ */
/* Routine  : _Cthread_win32_cond_init          */
/* Arguments: Cth_cond_t *cv                    */
/* -------------------------------------------- */
/* Output   : 0 (OK) or -1 (ERROR)              */
/* -------------------------------------------- */
/* History:                                     */
/* 29-JUN-1999       First implementation       */
/*                   Olof.Barring@cern.ch       */
/* ============================================ */
/* Notes:                                       */
/* Win32 implementation of pthread_cond_init.   */
/* The implementation is based on section 3.2 of*/
/* Douglas C. Schmidt and Irfan Pyarali's       */
/* article                                      */
/* http://www.cs.wustl.edu/~schmidt/win32-cv-1.html*/
/* ============================================ */
static int _Cthread_win32_cond_init(Cth_cond_t *cv) {
    /*
     * Initialize the lock on nb_waiters counter ...
     */
    InitializeCriticalSection(&cv->nb_waiters_lock);
    /*
     * .. and initialize the counter itself.
     */
    cv->nb_waiters = 0;
    /*
     * Create an auto-reset event for condition signal
     */
    if ( (cv->cond_events[CTHREAD_WIN32_SIGNAL] = 
        CreateEvent(NULL,FALSE,FALSE,NULL)) == NULL ) {
        serrno = SECTHREADERR;
        return(-1);
    }
    /*
     * Create a manual-reset event for condition broadcast
     */
    if ( (cv->cond_events[CTHREAD_WIN32_BROADCAST] = 
        CreateEvent(NULL,TRUE,FALSE,NULL)) == NULL ) {
        serrno = SECTHREADERR;
        return(-1);
    }
    /*
     * Create a manual-reset event to assure faireness at
     * broadcast
     */
    if ( (cv->last_waiter_done = 
        CreateEvent(NULL,TRUE,FALSE,NULL)) == NULL ) {
        serrno = SECTHREADERR;
        return(-1);
    }
    return(0);
}
/* ============================================ */
/* Routine  : _Cthread_win32_cond_wait          */
/* Arguments: Cth_cond_t *cv                    */
/*            Cth_mtx_t *mtx                    */
/*            int timeout                       */
/* -------------------------------------------- */
/* Output   : 0 (OK) or -1 (ERROR)              */
/* -------------------------------------------- */
/* History:                                     */
/* 29-JUN-1999       First implementation       */
/*                   Olof.Barring@cern.ch       */
/* ============================================ */
/* Notes:                                       */
/* Win32 implementation of pthread_cond_wait.   */
/* The implementation is based on section 3.2 of*/
/* Douglas C. Schmidt and Irfan Pyarali's       */
/* article.                                     */
/* http://www.cs.wustl.edu/~schmidt/win32-cv-1.html*/
/* An important modification is the use of      */
/* a last_waiter_done event. This prevents a    */
/* single thread to enter a loop on cond_wait() */
/* when a broadcast was set. With this mechanism*/
/* each thread is guaranteed to wake up exactly */
/* once on a broadcast. One could also envisage */
/* to add a first_waiter_done event which it    */
/* reset(!) when first waiter exits from wait.  */
/* Such event together with a wait for it in the*/
/* beginning of this routine could prevent any  */
/* new thread that called cond_wait to wake up  */
/* on same broadcast. However, POSIX doesn't    */
/* specify anything like that and it is rather a*/
/* philisophical matter what timing should      */
/* rule the behaviour when a thread calls this  */
/* routine while it is processing a broadcast as*/
/* long as it is not lost! (i.e. hangs forever).*/
/* The present implementation guarantees that   */
/* if a thread calls this routine during the    */
/* processing of a broadcast, it will either    */
/* be released by the present broadcast or by   */
/* the next one depending on whether the last   */
/* waiter grabbed the counter mutex after (in   */
/* which case it is no longer the last waiter)  */
/* or before the incomming thread (was that     */
/* clear...?).                                  */
/* ============================================ */
static int _Cthread_win32_cond_wait(Cth_cond_t *cv,
                                    Cth_mtx_t *mtx,
                                    int timeout) {
    int timeout_msec,rc,last_waiter,broadcast;
    if ( timeout > 0 ) timeout_msec = timeout * 1000;
    else timeout_msec = INFINITE;
    /*
     * Avoid race on nb_waiters counter. We don't
     * use _Cthread_obtain_mtx() etc. to reduce
     * overhead from internal logic.
     */
    EnterCriticalSection(&(cv->nb_waiters_lock));
    cv->nb_waiters++;
    LeaveCriticalSection(&(cv->nb_waiters_lock));
    /*
     * Relase the external mutex here.
     */
    _Cthread_release_mtx(mtx);
    /* Wait for either event to become signaled, 
     * due to _Cthread_win32_cond_signal being called or 
     * _Cthread_win32_cond_broadcast being called.
     */
    rc = WaitForMultipleObjects(CTHREAD_WIN32_MAX_EVENTS,
        cv->cond_events,FALSE,timeout_msec);
    EnterCriticalSection(&(cv->nb_waiters_lock));
    cv->nb_waiters--;
    broadcast = (rc == (WAIT_OBJECT_0 + CTHREAD_WIN32_BROADCAST));
    last_waiter = ((broadcast) && (cv->nb_waiters == 0));
    if ( last_waiter || rc == WAIT_FAILED || rc == WAIT_TIMEOUT ) {
        /*
         * This is the last waiter to be notified or to stop
         * waiting, so reset the manual event (broadcast only).
         * Keep the counter mutex until the broadcast event
         * is reset in order to prevent incomming threads to
         * wake up on the preset broadcast.
         */
        if ( broadcast ) {
            ResetEvent(cv->cond_events[CTHREAD_WIN32_BROADCAST]);
            PulseEvent(cv->last_waiter_done);
        }
        LeaveCriticalSection(&(cv->nb_waiters_lock));
        if ( rc == WAIT_FAILED || rc == WAIT_TIMEOUT ) {
          serrno = (rc == WAIT_TIMEOUT ? SETIMEDOUT : SECTHREADERR);
          return(-1);
        }
    } else {
        /*
         * Release counter mutex to next waiter or new
         * incomming waiters.
         */
        LeaveCriticalSection(&(cv->nb_waiters_lock));
        /*
         * Prevent that one thread is signaled twice for
         * same condition at broadcast.
         */
        if ( broadcast) {
            rc = WaitForSingleObject(cv->last_waiter_done,timeout_msec);
            if ( rc == WAIT_FAILED || rc == WAIT_TIMEOUT ) {
              serrno = (rc == WAIT_TIMEOUT ? SETIMEDOUT : SECTHREADERR);
              return(-1);
            }

        }
    }
    /*
     * Reacquire the mutex before returning
     */
    return(_Cthread_obtain_mtx(mtx,timeout));
}
/* ============================================ */
/* Routine  : _Cthread_win32_cond_broadcast     */
/* Arguments: Cth_cond_t *cv                    */
/* -------------------------------------------- */
/* Output   : 0 (OK) or -1 (ERROR)              */
/* -------------------------------------------- */
/* History:                                     */
/* 29-JUN-1999       First implementation       */
/*                   Olof.Barring@cern.ch       */
/* ============================================ */
/* Notes:                                       */
/* Win32 implementation of pthread_cond_broadcast*/
/* The implementation is based on section 3.2 of*/
/* Douglas C. Schmidt and Irfan Pyarali's       */
/* article                                      */
/* http://www.cs.wustl.edu/~schmidt/win32-cv-1.html*/
/* ============================================ */
static int _Cthread_win32_cond_broadcast(Cth_cond_t *cv) {
    int have_waiters;
    /*
     * Avoid race on nb_waiters counter. We don't
     * use _Cthread_obtain_mtx() etc. to reduce
     * overhead from internal logic.
     */
    EnterCriticalSection(&(cv->nb_waiters_lock));
    have_waiters = (cv->nb_waiters > 0);
    LeaveCriticalSection(&(cv->nb_waiters_lock));
    if ( have_waiters ) 
        return(SetEvent(cv->cond_events[CTHREAD_WIN32_BROADCAST]));
    else return(0);
}
/* ============================================ */
/* Routine  : _Cthread_win32_cond_signal        */
/* Arguments: Cth_cond_t *cv                    */
/* -------------------------------------------- */
/* Output   : 0 (OK) or -1 (ERROR)              */
/* -------------------------------------------- */
/* History:                                     */
/* 29-JUN-1999       First implementation       */
/*                   Olof.Barring@cern.ch       */
/* ============================================ */
/* Notes:                                       */
/* Win32 implementation of pthread_cond_signal  */
/* The implementation is based on section 3.2 of*/
/* Douglas C. Schmidt and Irfan Pyarali's       */
/* article                                      */
/* http://www.cs.wustl.edu/~schmidt/win32-cv-1.html*/
/* ============================================ */
static int _Cthread_win32_cond_signal(Cth_cond_t *cv) {
    int have_waiters;
    /*
     * Avoid race on nb_waiters counter. We don't
     * use _Cthread_obtain_mtx() etc. to reduce
     * overhead from internal logic.
     */
    EnterCriticalSection(&(cv->nb_waiters_lock));
    have_waiters = (cv->nb_waiters > 0);
    LeaveCriticalSection(&(cv->nb_waiters_lock));
    if ( have_waiters ) 
        return(SetEvent(cv->cond_events[CTHREAD_WIN32_SIGNAL]));
    else return(0);
}
/* ============================================ */
/* Routine  : _Cthread_win32_cond_destroy       */
/* Arguments: Cth_cond_t *cv                    */
/* -------------------------------------------- */
/* Output   : 0 (OK) or -1 (ERROR)              */
/* -------------------------------------------- */
/* History:                                     */
/* 29-JUN-1999       First implementation       */
/*                   Olof.Barring@cern.ch       */
/* ============================================ */
/* Notes:                                       */
/* Win32 implementation of pthread_cond_destroy */
/* ============================================ */
static int _Cthread_win32_cond_destroy(Cth_cond_t *cv) {
    if ( cv == NULL ) return(-1);
    DeleteCriticalSection(&(cv->nb_waiters_lock));
    CloseHandle(cv->cond_events[CTHREAD_WIN32_SIGNAL]);
    CloseHandle(cv->cond_events[CTHREAD_WIN32_BROADCAST]);
    CloseHandle(cv->last_waiter_done);
    return(0);
}
#endif /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */

/* ============================================ */
/* Routine  : Cthread_proto                     */
/* Arguments:                                   */
/* -------------------------------------------- */
/* Output   : 0 (ERROR) or Thread Protocol      */
/* -------------------------------------------- */
/* History:                                     */
/* 02-JUN-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* Thread Protocol is, for the moment:          */
/* 1         POSIX                              */
/* 2         DCE                                */
/* 3         LinuxThreads                       */
/* 4         WIN32                              */
/* ============================================ */
CTHREAD_DECL Cthread_proto() {
#ifdef _CTHREAD
  return(_CTHREAD_PROTO);
#else
  return(0);
#endif
}

/* ============================================ */
/* Routine  : Cthread_isproto                   */
/* Arguments:                                   */
/* -------------------------------------------- */
/* Output   : 0 (OK) or 1 (ERROR)               */
/* -------------------------------------------- */
/* History:                                     */
/* 02-JUN-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* Thread Protocol is, for the moment:          */
/* 1         POSIX                              */
/* 2         DCE                                */
/* 3         LinuxThreads                       */
/* 4         WIN32                              */
/* ============================================ */
CTHREAD_DECL Cthread_isproto(proto)
     char *proto;
{
  if (proto == NULL) {
    serrno = EINVAL;
    return(1);
  }
#ifdef _CTHREAD
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if (strcmp(proto,"POSIX") != 0) {
    serrno = EINVAL;
    return(-1);
  }
  return(0);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (strcmp(proto,"DCE") != 0) {
    serrno = EINVAL;
    return(-1);
  }
  return(0);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  if (strcmp(proto,"LINUX") != 0) {
    serrno = EINVAL;
    return(-1);
  }
  return(0);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  if (strcmp(proto,"WIN32") != 0) {
    serrno = EINVAL;
    return(-1);
  }
  return(0);
#else
  serrno = SEOPNOTSUP;
  return(1);
#endif
#else
  serrno = EINVAL;
  return(1);
#endif
}

/* ============================================ */
/* Routine  : Cthread_environment               */
/* Arguments:                                   */
/* -------------------------------------------- */
/* Output   : CTHREAD_TRUE_THREAD or            */
/*            CTHREAD_MULTI_PROCESS             */
/* -------------------------------------------- */
/* History:                                     */
/* 02-JUN-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* Says if the implementation is singlethreaded */
/* or not.                                      */
/* ============================================ */
CTHREAD_DECL Cthread_environment() {
#ifdef _CTHREAD
  return(CTHREAD_TRUE_THREAD);
#else
  return(CTHREAD_MULTI_PROCESS);
#endif
}

/* ============================================ */
/* Routine  : _Cthread_self                     */
/* Arguments:                                   */
/* -------------------------------------------- */
/* Output   : >= 0    - Cthread-ID              */
/*            <  0    - ERROR                   */
/* -------------------------------------------- */
/* History:                                     */
/* 08-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
CTHREAD_DECL _Cthread_self() {
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  unsigned            thID;             /* WIN32 thread ID */
#else /* _CTHREAD_PROTO_WIN32 */
  Cth_pid_t             pid;              /* Current PID      */
#endif /* _CTHREAD_PROTO_WIN32 */
  int                 n;                /* Return value     */

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Get current pid */
#ifdef _NOCTHREAD
  pid = getpid();
#else
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  thID = GetCurrentThreadId();
#else /* _CTHREAD_PROTO_WIN32 */
  pid = pthread_self();
#endif /* _CTHREAD_PROTO_WIN32 */
#endif

  /* We check that this pid correspond to a known */
  /* process                                      */

  n = -1;
  while (current->next != NULL) {
    current = current->next;
#ifdef _NOCTHREAD
    if (current->pid == pid) {
      n = current->cid;
      break;
    }
#else
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_POSIX
    if (pthread_equal(current->pid,pid)) {
      n = current->cid;
      break;
    }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_DCE
    if (pthread_equal(current->pid,pid)) {
      n = current->cid;
      break;
    }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_LINUX
    if (pthread_equal(current->pid,pid)) {
      n = current->cid;
      break;
    }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
    if ( current->thID == thID ) {
      n = current->cid;
	  break;
	}
#endif
#endif
  }

  return(n);
}


