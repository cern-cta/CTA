/*
 * $Id: Cthread.c,v 1.50 2007/02/13 07:52:24 waldron Exp $
 */

/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cthread.c,v $ $Revision: 1.50 $ $Date: 2007/02/13 07:52:24 $ CERN IT-PDP/DM Olof Barring, Jean-Damien Durand";
#endif /* not lint */

#include <Cthread_api.h>
#include <Cthread_typedef.h>
#include <Cglobals.h>
#include <Cmutex.h>
#include <serrno.h>
#include <errno.h>
#include <osdep.h>
#include <signal.h>

#ifdef DEBUG
#ifndef CTHREAD_DEBUG
#define CTHREAD_DEBUG
#endif
#endif

#ifdef CTHREAD_DEBUG
#include <log.h>
#endif

/* ============================================ */
/* Internal Prototypes                          */
/* ============================================ */
/* Add a Cthread ID */
int   _Cthread_addcid _PROTO((char *, int, char *, int, Cth_pid_t *, unsigned, void *(*)(void *), int));
/* Add a mutex */
int   _Cthread_addmtx _PROTO((char *, int, struct Cmtx_element_t *));
/* Add a TSD */
int   _Cthread_addspec _PROTO((char *, int, struct Cspec_element_t *));
/* Obain a mutex lock */
#ifndef CTHREAD_DEBUG
int   _Cthread_obtain_mtx _PROTO((char *, int, Cth_mtx_t *, int));
#else
#define _Cthread_obtain_mtx(a,b,c,d) _Cthread_obtain_mtx_debug(__FILE__,__LINE__,a,b,c,d)
int   _Cthread_obtain_mtx_debug _PROTO((char *, int, char *, int, Cth_mtx_t *, int));
#endif
/* Release a mutex lock */
int   _Cthread_release_mtx _PROTO((char *, int, Cth_mtx_t *));
/* Methods to create a mutex to protect Cthread */
/* internal tables and linked lists             */
/* This is useful only in thread environment    */
void  _Cthread_once _PROTO((void));
int   _Cthread_init _PROTO((void));
/* Release of a thread-specific variable        */
void  _Cthread_keydestructor _PROTO((void *));
/* Release of a thread                          */
int   _Cthread_destroy _PROTO((char *, int, int));
#ifdef _CTHREAD
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
unsigned __stdcall _Cthread_start_threadex _PROTO((void *));
void     __cdecl   _Cthread_start_thread _PROTO((void *));
int _Cthread_win32_cond_init _PROTO((Cth_cond_t *));
int _Cthread_win32_cond_wait _PROTO((Cth_cond_t *, Cth_mtx_t *, int));
int _Cthread_win32_cond_broadcast _PROTO((Cth_cond_t *));
int _Cthread_win32_cond_signal _PROTO((Cth_cond_t *));
int _Cthread_win32_cond_destroy _PROTO((Cth_cond_t *));
#else
void *_Cthread_start_pthread _PROTO((void *));
#endif
#else
/* Signal handler - Simplify the POSIX sigaction calls */
typedef void    Sigfunc _PROTO((int));
Sigfunc *_Cthread_signal _PROTO((char *, int, int, Sigfunc *));
void     _Cthread_sigchld _PROTO((int));
void    *_Cthread_start_nothread _PROTO((void *(*)(void *), void *, Sigfunc *));
#endif /* _CTHREAD */

/* Find a global key in Cspec structure         */
struct Cspec_element_t *_Cthread_findglobalkey _PROTO((char *, int, int *));
/* Cthread-ID (as a Thread-Specific variable) destructor */
void _Cthread_cid_destructor _PROTO((void *));
/* Cthread-ID once for all... */
void _Cthread_cid_once _PROTO((void));

/* ------------------------------------ */
/* For the Cthread_self() command       */
/* (Thread-Specific Variable)           */
/* ------------------------------------ */
#ifdef _CTHREAD
Cth_spec_t cid_key;
#else
int cid_key = 0;
#endif
Cth_once_t cid_once = CTHREAD_ONCE_INIT;

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
struct Cid_element_t   Cid;
#ifdef _CTHREAD
struct Cmtx_element_t  Cmtx;
#endif
struct Cthread_protect_t Cthread;
struct Cspec_element_t Cspec;
Cth_once_t             once = CTHREAD_ONCE_INIT;
int _Cthread_unprotect = 0;
int Cthread_debug = 0;
int _Cthread_once_status = -1;

/* ============================================ */
/* Routine  : _Cthread_cid_destructor           */
/* Arguments: address to destroy.               */
/* -------------------------------------------- */
/* Output   : <none>                            */
/* -------------------------------------------- */
/* History:                                     */
/* 17-SEP-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes: See Cthread_Self().                   */
/* ============================================ */
void _Cthread_cid_destructor(ptr)
     void *ptr;
{
  if (ptr != NULL)
    free(ptr);
}

/* ============================================ */
/* Routine  : _Cthread_cid_once                 */
/* Arguments: <none>                            */
/* -------------------------------------------- */
/* Output   : <none>                            */
/* -------------------------------------------- */
/* History:                                     */
/* 17-SEP-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes: See Cthread_Self().                   */
/* ============================================ */
void _Cthread_cid_once()
{
  int n;

  /* Create the key... */
#ifndef _CTHREAD
  /* Oooopps... impossible */
  serrno = SEOPNOTSUP;
#else
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  /* Create the specific variable a-la POSIX */
  if ((n = pthread_key_create(&cid_key,&_Cthread_cid_destructor))) {
    errno = n;
    serrno = SECTHREADERR;
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  /* Create the specific variable a-la DCE */
  if (pthread_keycreate(&cid_key,&_Cthread_cid_destructor)) {
    serrno = SECTHREADERR;
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* Create the specific variable a-la LinuxThreads */
  if ((n = pthread_key_create(&cid_key,&_Cthread_cid_destructor))) {
    errno = n;
    serrno = SECTHREADERR;
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  if ( (n = TlsAlloc()) == 0xFFFFFFFF ) {
    serrno = SECTHREADERR;
  } else {
    cid_key = n;
  }
#else
  serrno = SEOPNOTSUP;
#endif /* _CTHREAD_PROTO */
#endif
}

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
int DLL_DECL Cthread_init() {
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
  if ( !once ) {
      (void)_Cthread_once();
      (void)_Cthread_cid_once();
  }
#else /* _CTHREAD_PROTO_WIN32 */
  /* In a thread environment we need to have our own */
  /* mutex. So we create it right now - we do it     */
  /* dynamically, since we don't rely on the fact    */
  /* that the macro PTHREAD_MUTEX_INITIALIZER exists */
  /* everywhere                                      */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  int n;

  if ((n = pthread_once(&once,&_Cthread_once)) != 0) {
    errno = n;
    _Cthread_once_status = -1;
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_once(&once,&_Cthread_once) != 0)
    _Cthread_once_status = -1;
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* On linux, pthread_once always returns 0... */
  pthread_once(&once,&_Cthread_once);
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
/* ============================================ */
unsigned __stdcall _Cthread_start_threadex(void *arg) {
    Cthread_start_params_t *start_params;
    unsigned               status;
    int                    thID;
    void                   *(*routine)(void *);
    void                   *routineargs;

    if ( arg == NULL ) {
        serrno = EINVAL;
        return(status);
    }
    start_params = (Cthread_start_params_t *)arg;
    thID = GetCurrentThreadId();
    if (_Cthread_addcid(__FILE__,__LINE__,NULL,0,NULL,thID,start_params->_thread_routine,start_params->detached) < 0) {
        free(arg);
        return(status);
    }
    routine = start_params->_thread_routine;
    routineargs = start_params->_thread_arg;
    free(start_params);
    start_params = NULL;
    arg = NULL;
    status = (unsigned)routine(routineargs);
    /* 
     * Destroy the entry in Cthread internal linked list. Note that
     * if we are non-detached (i.e. Cthread_detached has not
     * been called) the _Cthread_destroy has no effect. The thread
     * element in linked list will remain until somebody calls
     * Cthread_join()
     */
    _Cthread_destroy(__FILE__,__LINE__,Cthread_self());

    return(status);
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
  int                detached;

#ifdef CTHREAD_DEBUG
  if (Cthread_debug != 0)
    log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_start_pthread(0x%lx)\n",
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
  detached = start_params->detached;
  free(arg);
  /* status = start_params->_thread_routine(start_params->_thread_arg); */
  status = routine(routineargs);
  
  /* 
   * Destroy the entry in Cthread internal linked list if it is a detached
   * thread (otherwise it is destroyed in Cthread_join()).
   */
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
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_signal(%d,0x%x) called at/behind %s:%d\n",
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
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_sigchld(%d) for child PID=%d (exit status = %d)\n",
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
        if (Cthread_debug != 0)
          log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_sigchld(%d) for child PID=%d ... Current scanned PID=%d\n",
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
            if (Cthread_debug != 0)
              log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_sigchld(%d) : Calling _Cthread_destroy(%d) for yet finished PID=%d\n",
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
        if (Cthread_debug != 0)
          log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_sigchld(%d) : Calling _Cthread_destroy(%d) for PID=%d\n",
              _Cthread_self(),
              errcode,current->cid,pid);
#endif
        _Cthread_destroy(NULL,0,current->cid);
      } else {
#ifdef CTHREAD_DEBUG
        if (Cthread_debug != 0)
          log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_sigchld(%d) : Can't find corresponding Cthread ID for PID=%d !\n",
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
     void *(*startroutine) _PROTO((void *));
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

  pid = (Cth_pid_t) getpid();

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
int DLL_DECL Cthread_Create(file, line, startroutine, arg)
     char *file;
     int line;
     void *(*startroutine) _PROTO((void *));
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
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_create(0x%lx,0x%lx) called at/behind %s:%d\n",
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
      if (Cthread_debug != 0)
        log(LOG_INFO,
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
    if (Cthread_debug != 0)
      log(LOG_INFO,
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
#if ((defined(__osf__) && defined(__alpha)) || (defined(IRIX5) || defined(IRIX6) || defined(IRIX64)))
  /*
   * Need to increase the thread stack on DUNIX and IRIX
   */
#if (defined(__osf__) && defined(__alpha))
  n = 256*1024;
#elif (defined(IRIX5) || defined(IRIX6) || defined(IRIX64))
  n = 512*1024;
#else
  /* Any default that can the #define's did not catched ? */
  n = 256*1024;
#endif
  if ((n = pthread_attr_setstacksize(&attr,n)) != 0) {
      pthread_attr_destroy(&attr);
      free(starter);
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
  }
#endif /* __osf__ && __alpha */
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
#if defined(__osf__) && defined(__alpha)
  /*
   * Need to increase the thread stack on DUNIX
   */
  n = 256*1024;
  if ( (n = pthread_attr_setstacksize(&attr,n)) ) {
      pthread_attr_destroy(&attr);
      free(starter);
      serrno = SECTHREADERR;
      return(-1);
  }
#endif /* __osf__ && __alpha */
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
int DLL_DECL Cthread_Create_Detached(file, line, startroutine, arg)
     char *file;
     int line;
     void *(*startroutine) _PROTO((void *));
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
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_create_detached(0x%lx,0x%lx) called at/behind %s:%d\n",
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
  if ( (pid = _beginthreadex(NULL,0,_Cthread_start_threadex,starter,0,&thID)) ==
 0 ) {
      free(starter);
      serrno = SECTHREADERR;
      return(-1);
  }
#else
  serrno = SEOPNOTSUP;
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
int DLL_DECL Cthread_Join(file, line, cid, status)
     char *file;
     int line;
     int cid;
     int **status;
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  int                   n;                /* Status           */

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_join(%d,0x%lx) called at/behind %s:%d\n",
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
  current->joined = 1;
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
               /*
                * GetExitCodeThread() returns boolean: non-zero == OK, 0 == not OK.
                */
               if ( n == 0 ) n = -1;
               else n = 0;
           } else {
               serrno = SECTHREADERR;
               n = -1;
           }
           break;
       }
       current->joined = 1;
       _Cthread_destroy(__FILE__,__LINE__,current->cid);
  }
  return(n);
#else /* _CTHREAD_PROTO_WIN32 */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if ((n = pthread_join(current->pid,(void **) status))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  /*
   * We can safely destroy here since the thread has exit.
   * If it was detached it has done the cleanup itself.
   * There cannot be any concurrent access to current->joined
   * since the thread has exit.
   */
  current->joined = 1;
  if ( !current->detached ) 
     _Cthread_destroy(__FILE__,__LINE__,current->cid);
  return(0);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_join(current->pid,(void **) status)) {
    serrno = SECTHREADERR;
    return(-1);
  }
  /*
   * We can safely destroy here since the thread has exit.
   * If it was detached it has done the cleanup itself.
   * There cannot be any concurrent access to current->joined
   * since the thread has exit.
   */
  current->joined = 1;
  if ( !current->detached ) 
     _Cthread_destroy(__FILE__,__LINE__,current->cid);
  return(0);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  if ((n = pthread_join(current->pid,(void **) status))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  /*
   * We can safely destroy here since the thread has exit.
   * If it was detached it has done the cleanup itself.
   * There cannot be any concurrent access to current->joined
   * since the thread has exit.
   */
  current->joined = 1;
  if ( !current->detached ) 
     _Cthread_destroy(__FILE__,__LINE__,current->cid);
  return(0);
#  else
  serrno = SEOPNOTSUP;
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
int DLL_DECL Cthread_Detach(file, line, cid)
     char *file;
     int line;
     int cid;
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  int                 n;                  /* Status           */
  int detached = 0;                       /* Local store      */

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,
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
      detached = current->detached;
      current->detached = 1;
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
  if (detached) {
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
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  if ((n = pthread_detach(current->pid))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  /* This is a noop. Windows doesn't make any real distinction */
#else /* _CTHREAD_PROTO */
  serrno = SEOPNOTSUP;
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
int DLL_DECL Cthread_Self0() {
  return(_Cthread_self());
}
int DLL_DECL Cthread_Self(file, line)
     char *file;
     int line;
{
#ifdef _CTHREAD
  void               *tsd = NULL;       /* Thread-Specific Variable */
#else
  struct Cid_element_t *current = &Cid; /* Curr Cid_element */
  Cth_pid_t             pid;            /* Current PID      */
#endif
  int                   n;              /* Return value     */

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In Cthread_self() called at/behind %s:%d\n",
          _Cthread_self(),file,line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);


#ifdef _NOCTHREAD
  /* In a non-threaded environment, we just check */
  /* the list.                                    */

  /* We check that this pid correspond to a known */
  /* process                                      */
  pid = getpid();
  n = -1;
  while (current->next != NULL) {
    current = current->next;
    if (current->pid == pid) {
      n = current->cid;
      break;
    }
  }

  /* We do not set serrno from Cthread_Self() */
  /* if (n < 0)                               */
  /*   serrno = EINVAL;                       */
  return(n);

#else /* _NOCTHREAD */

  /* In a threaded environment, we get Thread Specific   */
  /* Variable, directly with the thread interface        */
  /* Yes, Because we want the less overhead as possible. */
  /* A previous version of Cthread was always looking to */
  /* the internal linked list. Generated a lot of        */
  /* _Cthread_obtain_mtx()/_Cthread_release_mtx()        */
  /* overhead. Now those two internal methods are called */
  /* only once, to get the cid. After it is stored in a  */
  /* Thread-Specific variable, whose associated key is   */
  /* cid_key.                                            */

  /* We makes sure that a key is associated once for all */
  /* with what we want.                                  */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  /* In WIN32 there is nothing corresponding to        */
  /* pthread_once(). Instead we rely on that Cthread_..*/
  /* is the unique thread interface.                   */
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if ((n = pthread_once(&cid_once,&_Cthread_cid_once)) != 0) {
    errno = n;
    /* serrno = SECTHREADERR; */
    return(-1);
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_once(&cid_once,&_Cthread_cid_once) != 0) {
    /* serrno = SECTHREADERR; */
    return(-1);
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* On linux, pthread_once always returns 0... */
  pthread_once(&cid_once,&_Cthread_cid_once);
#  else
  /* serrno = SEOPNOTSUP; */
  return(-1);
#  endif

  /* We look if there is already a Thread-Specific       */
  /* Variable.                                           */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  tsd = pthread_getspecific(cid_key);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_getspecific(cid_key,&tsd)) {
    tsd = NULL;
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  tsd = pthread_getspecific(cid_key);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  tsd = TlsGetValue(cid_key);
#  else
  /* serrno = SEOPNOTSUP; */
  return(-1);
#  endif

  if (tsd == NULL) {

    /* We try to create the key-value */
    if ((tsd = (void *) malloc(sizeof(int))) == NULL) {
      /* serrno = SEINTERNAL; */
      return(-1);
    }

    /* We associate the key-value with the key   */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_setspecific(cid_key, tsd))) {
      errno = n;
      /* serrno = SECTHREADERR; */
      return(-1);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_setspecific(cid_key, tsd)) {
      /* serrno = SECTHREADERR; */
      return(-1);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    if ((n = pthread_setspecific(cid_key, tsd))) {
      errno = n;
      /* serrno = SECTHREADERR; */
      return(-1);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
    if ( !(n = TlsSetValue(cid_key, tsd)) ) {
      /* serrno = SECTHREADERR; */
      return(-1);
    }
#  else
    /* serrno = SEOPNOTSUP; */
    return(-1);
#  endif
    
    /* And we don't yet know the cid... So we set it to -2 */

    * (int *) tsd = -2;
    
    return(-2);

  } else {

    /* Thread-Specific variable already exist ! */

    return((int) * (int *) tsd);

  }
#endif /* _NOCTHREAD */
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
/* 22-NOV-1999       Make sure that non-detached*/
/*                   threads has been joined    */
/*                   before removing the element*/
/*                   (Olof.Barring@cern.ch)     */
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
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_destroy(%d) called at/behind %s:%d\n",
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

  /*
   * We can only destroy detached threads or non-detached
   * threads that have been joined. Non-detached threads for
   * which there hasn't yet been any join must remain in list
   * even if they have exit. The reason is that a potential
   * "joiner" should still be able to access the threads exit
   * status.
   */
  if ( current->detached || current->joined ) {
#ifdef CTHREAD_DEBUG
  if (Cthread_debug != 0)
    log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_destroy(%d), detached=%d, joined=%d\n",
        _Cthread_self(),cid,current->detached,current->joined);
#endif

    /* It is a known process - We delete the entry */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
    /* For threads created with _beginthreadex() we must do cleanup */
    if ( !current->detached ) CloseHandle((HANDLE)current->pid);
#endif /* _CTHREAD_PROTO_WIN32 */
    if (previous != NULL) {
      previous->next = current->next;
    } else {
      /* No more entry... */
      Cid.next = NULL;
    }
    free(current);
  }
  _Cthread_release_mtx(file,line,&(Cthread.mtx));

  return(0);
}

/* ============================================ */
/* Routine  : Cthread_Cond_Broadcast_ext        */
/* Arguments: caller source file                */
/*            caller source line                */
/*            address of a Cthread structure    */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (NOT OK)                */
/* -------------------------------------------- */
/* History:                                     */
/* 13-OCT-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cthread_Cond_Broadcast_ext(file, line, addr)
     char *file;
     int line;
     void *addr;
{
#ifdef _CTHREAD
  struct Cmtx_element_t *current = addr;  /* Curr Cmtx_element */
  int                  rc;
#endif

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_cond_broadcast_ext(0x%lx) called at/behind %s:%d\n",
          _Cthread_self(),(unsigned long) addr,
          file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  return(0);
#else
  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

  /* This is a thread implementation            */
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
  if ( rc != -1 ) rc = 0;
#else
  serrno = SEOPNOTSUP;
  return(-1);
#endif /* _CTHREAD_PROTO */
  return(rc);
#endif /* ifdef _NOCTHREAD */
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
int DLL_DECL Cthread_Cond_Broadcast(file, line, addr)
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
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_cond_broadcast(0x%lx) called at/behind %s:%d\n",
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
  if ( rc != -1 ) rc = 0;
#else
  serrno = SEOPNOTSUP;
  return(-1);
#endif /* _CTHREAD_PROTO */
  _Cthread_release_mtx(file,line,&(Cthread.mtx));
  return(rc);
#endif /* ifdef _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Wait_Condition_ext        */
/* Arguments: caller source file                */
/*            caller source line                */
/*            address of a Cthread structure    */
/*             describing the address to get a  */
/*             condition on                     */
/*            integer eventual timeout          */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (NOT OK)                */
/* -------------------------------------------- */
/* History:                                     */
/* 13-OCT-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cthread_Wait_Condition_ext(file, line, addr, timeout)
     char *file;
     int line;
     void *addr;
     int timeout;
{
#ifdef _CTHREAD
  struct Cmtx_element_t *current = addr;    /* Curr Cmtx_element */
  int                    n;                 /* Status            */
  int                    rc;
#endif
  
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_wait_condition_ext(0x%lx,%d) called at/behind %s:%d\n",
          _Cthread_self(),
          (unsigned long) addr,(int) timeout,
          file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  if (timeout > 0) {
    Sigfunc *old_alrm;
    if ((old_alrm = _Cthread_signal(__FILE__,__LINE__,SIGALRM,SIG_DFL)) != SIG_ERR) {
      sleep(timeout);
      _Cthread_signal(__FILE__,__LINE__,SIGALRM,old_alrm);
    }
  }
  return(0);
#else
  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }
  
  /* This is a thread implementation            */
  /* Flag the number of threads waiting on addr */

  ++current->nwait;

  if (timeout <= 0) {
    /* No timeout : pthread_cond_wait */
#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_cond_wait(&(current->cond),&(current->mtx))) != 0) {
      errno = n;
      serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
      rc = -1;
    } else {
      rc = 0;
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_cond_wait(&(current->cond),&(current->mtx)) != 0) {
      serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
      rc = -1;
    } else {
      rc = 0;
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    /* On linux pthread_cond_wait never returns an error code... */
    if ((n = pthread_cond_wait(&(current->cond),&(current->mtx))) != 0) {
      errno = n;
      serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
      rc = -1;
    } else {
      rc = 0;
    }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32 
    if ((rc = _Cthread_win32_cond_wait(&(current->cond),&(current->mtx),-1)) != 0) {
      /*
       * Note that _Cthread_win32_cond_wait() does not set errno, it sets
       * serrno directly. ETIMEDOUT is undefined on _WIN32
       */
      serrno = ( serrno == SETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
    }
#else
    serrno = SEOPNOTSUP;
    rc = -1;
#endif /* _CTHREAD_PROTO */
  } else {
#if _CTHREAD_PROTO != _CTHREAD_PROTO_WIN32
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
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
      if ((n = pthread_cond_timedwait(&(current->cond),&(current->mtx),&ts)) != 0) {
        errno = n;
        serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
      if (pthread_cond_timedwait(&(current->cond),&(current->mtx),&ts) != 0) {
        serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
      if ((n = pthread_cond_timedwait(&(current->cond),&(current->mtx),&ts)) != 0) {
        errno = n;
        serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
#  else
      serrno = SEOPNOTSUP;
	  rc = -1;
#  endif
	}
#else /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */ 
    if ((rc = _Cthread_win32_cond_wait(&(current->cond),&(current->mtx),timeout)) != 0) {
      /*
       * Note that _Cthread_win32_cond_wait() does not set errno, it sets
       * serrno directly. ETIMEDOUT is undefined on _WIN32
       */
      serrno = ( serrno == SETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
    }
#endif
  }

  /* This end of the routine will decrease the number */
  /* of wait doing a Wait_Condition on addr           */

  /* Decrease the number of waiting threads */
   --current->nwait;

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
int DLL_DECL Cthread_Wait_Condition(file, line, addr, timeout)
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
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_wait_condition(0x%lx,%d) called at/behind %s:%d\n",
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
  if (timeout > 0) {
    Sigfunc *old_alrm;
    if ((old_alrm = _Cthread_signal(__FILE__,__LINE__,SIGALRM,SIG_DFL)) != SIG_ERR) {
      sleep(timeout);
      _Cthread_signal(__FILE__,__LINE__,SIGALRM,old_alrm);
    }
  }
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
    if ((n = pthread_cond_wait(&(current->cond),&(current->mtx))) != 0) {
      errno = n;
      serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
      rc = -1;
    } else {
      rc = 0;
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_cond_wait(&(current->cond),&(current->mtx)) != 0) {
      serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
      rc = -1;
    } else {
      rc = 0;
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    /* On linux pthread_cond_wait never returns an error code... */
    if ((n = pthread_cond_wait(&(current->cond),&(current->mtx))) != 0) {
      errno = n;
      serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
      rc = -1;
    } else {
      rc = 0;
    }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32 
    if ((rc = _Cthread_win32_cond_wait(&(current->cond),&(current->mtx),-1)) != 0) {
      /*
       * Note that _Cthread_win32_cond_wait() does not set errno, it sets
       * serrno directly. ETIMEDOUT is undefined on _WIN32
       */
      serrno = ( serrno == SETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
    }
#else
    serrno = SEOPNOTSUP;
    rc = -1;
#endif /* _CTHREAD_PROTO */
  } else {
#if _CTHREAD_PROTO != _CTHREAD_PROTO_WIN32
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
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
      if ((n = pthread_cond_timedwait(&(current->cond),&(current->mtx),&ts)) != 0) {
        errno = n;
        serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
      if (pthread_cond_timedwait(&(current->cond),&(current->mtx),&ts)) {
        serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
      if ((n = pthread_cond_timedwait(&(current->cond),&(current->mtx),&ts))) {
        errno = n;
        serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
#  else
      serrno = SEOPNOTSUP;
	  rc = -1;
#  endif
	}
#else /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */ 
    if ((rc = _Cthread_win32_cond_wait(&(current->cond),&(current->mtx),timeout)) != 0) {
      /*
       * Note that _Cthread_win32_cond_wait() does not set errno, it sets
       * serrno directly. ETIMEDOUT is undefined on _WIN32
       */
      serrno = ( serrno == SETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
    }
#endif
  }

  /* This end of the routine will decrease the number */
  /* of wait doing a Wait_Condition on addr           */

  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);
  current = &Cmtx;
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
/* Output   : 0 (OK) or -1 (ERROR)              */
/* -------------------------------------------- */
/* History:                                     */
/* 06-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cthread_Lock_Mtx(file, line, addr, timeout)
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
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_lock_mtx(0x%lx,%d) called at/behind %s:%d\n",
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

  /*
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
  */

  if (n) {
    /* Not yet existing mutex : we have to create one */
    struct Cmtx_element_t *Cmtx_new;

    if ((Cmtx_new = (struct Cmtx_element_t *) malloc(sizeof(struct Cmtx_element_t)))
        == NULL) {
      serrno = SEINTERNAL;
      _Cthread_release_mtx(file,line,&(Cthread.mtx));
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
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
        return(-1);
      }
      if ((n = pthread_mutex_init(&(Cmtx_new->mtx),&mattr))) {
        /* Release mutex attribute resources */
        pthread_mutexattr_destroy(&mattr);
        /* Release Cmtx element */
        free(Cmtx_new);
        errno = n;
        serrno = SECTHREADERR;
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
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
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
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
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
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
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
        return(-1);
      }
      if (pthread_cond_init(&(Cmtx_new->cond),pthread_condattr_default)) {
        /* Release mutexes */
        pthread_mutex_destroy(&(Cmtx_new->mtx));
        /* Release Cmtx element */
        free(Cmtx_new);
        serrno = SECTHREADERR;
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
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
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
        return(-1);
      }
      if ((n = pthread_mutex_init(&(Cmtx_new->mtx),&mattr))) {
        /* Release mutex attribute resources */
        pthread_mutexattr_destroy(&mattr);
        /* Release Cmtx element */
        free(Cmtx_new);
        errno = n;
        serrno = SECTHREADERR;
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
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
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
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
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
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
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
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
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
        return(-1);
      }
#else
      serrno = SEOPNOTSUP;
      _Cthread_release_mtx(file,line,&(Cthread.mtx));
      return(-1);
#endif
    }
    /* Cmtx_new->mtx contains now a valid mutex   */
    /* AND a valid conditional variable           */
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
      _Cthread_release_mtx(file,line,&(Cthread.mtx));
      return(-1);
    }
    /* We now have to obtain the lock on this mutex */
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
    return(_Cthread_obtain_mtx(file,line,&(Cmtx_new->mtx),timeout));
  } else {
    /* We have to obtain the lock on this mutex */
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
    return(_Cthread_obtain_mtx(file,line,&(current->mtx),timeout));
  }
#endif /* _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Lock_Mtx_ext              */
/* Arguments: caller source file                */
/*            caller source line                */
/*            address of structure describing   */
/*            lock to get a mutex on            */
/*            integer eventual timeout          */
/* (see _Cthread_obtain_mtx)                    */
/* -------------------------------------------- */
/* Output   : 0 (OK) or -1 (ERROR)              */
/* -------------------------------------------- */
/* History:                                     */
/* 13-OCT-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cthread_Lock_Mtx_ext(file, line, addr, timeout)
     char *file;
     int line;
     void *addr;
     int timeout;
{
#ifdef _CTHREAD
  struct Cmtx_element_t *current = addr;  /* Curr Cmtx_element */
#endif

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_lock_mtx_ext(0x%lx,%d) called at/behind %s:%d\n",
          _Cthread_self(),
          (unsigned long) addr,(int) timeout,
          file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  return(0);
#else
  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

  /* This is a thread implementation            */
  return(_Cthread_obtain_mtx(file,line,&(current->mtx),timeout));
#endif /* _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Lock_Mtx_addr             */
/* Arguments: caller source file                */
/*            caller source line                */
/*            address of a lock variable        */
/* -------------------------------------------- */
/* Output   : (void *) NULL or != NULL          */
/*                                              */
/* -------------------------------------------- */
/* History:                                     */
/* 13-OCT-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Note: This routine returns the internal      */
/*       address of the structure used by       */
/*       Cthread corresponding to address       */
/*       given in parameter.                    */
/* ============================================ */
void DLL_DECL *Cthread_Lock_Mtx_addr(file, line, addr)
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
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_lock_mtx_addr(0x%lx) called at/behind %s:%d\n",
          _Cthread_self(),
          (unsigned long) addr,
          file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(NULL);

  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(NULL);
  }

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  serrno = SEOPNOTSUP;
  return(NULL);
#else
  /* This is a thread implementation            */
  /* We search to see if the mutex is already   */
  /* implemented                                */

  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(NULL);

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
    serrno = SEENTRYNFND;
    return(NULL);
  }

  return((void *) current);
#endif /* _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Mutex_Unlock_ext          */
/* Arguments: caller source file                */
/*            caller source line                */
/*            address of a Cthread structure    */
/*            describing address on which to    */
/*            get a lock off                    */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 06-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cthread_Mutex_Unlock_ext(file, line, addr)
     char *file;
     int line;
     void *addr;
{
#ifdef _CTHREAD
  struct Cmtx_element_t *current = addr;   /* Curr Cmtx_element */
#endif

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_unlock_mtx_ext(0x%lx) called at/behind %s:%d\n",
          _Cthread_self(),(unsigned long) addr,
          file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  return(0);
#else
  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

  /* This is a thread implementation            */
  return(_Cthread_release_mtx(file,line,&(current->mtx)));
#endif /* ifdef _NOCTHREAD */
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
int DLL_DECL Cthread_Mutex_Unlock(file, line, addr)
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
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_unlock_mtx(0x%lx) called at/behind %s:%d\n",
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
int DLL_DECL Cthread_Mutex_Destroy(file, line, addr)
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
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_mutex_destroy(0x%lx) called at/behind %s:%d\n",
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

  if (n) {
    /* Not yet existing mutex */
    serrno = EINVAL;
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
    return(-1);
  } else {
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
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
    return(-1);
#endif
    free(current);
    if (n != 0) {
      serrno = SECTHREADERR;
      n = -1;
    }
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
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
/* ==================================================================================== */
/* If file == NULL then it it called from Cglobals.c, and we do not to overwrite serrno */
/* ==================================================================================== */

int _Cthread_release_mtx(file, line, mtx)
     char *file;
     int line;
     Cth_mtx_t *mtx;
{
#ifdef _CTHREAD
   int               n;
#endif

   if (_Cthread_unprotect != 0) {
     if (mtx == &(Cthread.mtx)) {
       /* Unprotect mode */
       return(0);
     }
   }

#ifdef CTHREAD_DEBUG
   if (file != NULL) {
     if (Cthread_debug != 0)
       log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_release_mtx(0x%lx) called at/behind %s:%d\n",
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
    if (file != NULL) {
      serrno = SECTHREADERR;
    }
    errno = n;
    return(-1);
  } else {
    return(0);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_mutex_unlock(mtx)) {
    if (file != NULL) {
      serrno = SECTHREADERR;
    }
    return(-1);
  } else {
    return(0);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  if ((n = pthread_mutex_unlock(mtx))) {
    errno = n;
    if (file != NULL) {
      serrno = SECTHREADERR;
    }
    return(-1);
  } else {
    return(0);
  }
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
#  ifdef _CTHREAD_WIN32MTX
  if ( ( n = ReleaseMutex((HANDLE)*mtx) ) ) {
    return(0);
  } else {
    if (file != NULL) {
      serrno = SECTHREADERR;
    }
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
      while ( n > 0 ) {
        n--;
        LeaveCriticalSection(&(mtx->mtx));
      }
  } else {
    if (file != NULL) {
      serrno = SECTHREADERR;
    }
    n = -1;
  }
  return(n);
#  endif /* _CTHREAD_WIN32MTX */
#else
  if (file != NULL) {
    serrno = SEOPNOTSUP;
  }
  return(-1);
#endif /* _CTHREAD_PROTO */
#endif
}

#ifndef CTHREAD_DEBUG
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
/* ==================================================================================== */
/* If file == NULL then it it called from Cglobals.c, and we do not to overwrite serrno */
/* ==================================================================================== */

int _Cthread_obtain_mtx(file, line, mtx, timeout)
     char *file;
     int line;
     Cth_mtx_t *mtx;
     int timeout;
{
#ifdef _CTHREAD
  int               n;
#endif

  if (_Cthread_unprotect != 0) {
    if (mtx == &(Cthread.mtx)) {
      /* Unprotect mode */
      return(0);
    }
  }

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_obtain_mtx(0x%lx,%d) called at/behind %s:%d\n",_Cthread_self(),
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
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    } else {
      return(0);
    }
  } else {
    if ( (n=WaitForSingleObject((HANDLE)*mtx,timeout * 1000)) == WAIT_FAILED ) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    } else if ( n == WAIT_TIMEOUT ) {
      if (file != NULL) {
        serrno = SETIMEDOUT;
      }
      return(-1);
    } else {
      return(0);
    }
  }
#else 
  /* This is a thread implementation */
  if (timeout < 0) {
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
    /*
     * Use Win32 critical section
     */
    EnterCriticalSection(&(mtx->mtx));
    mtx->nb_locks++;
    return(0);
#  else /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */
    /* Try to get the lock */
#    if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_mutex_lock(mtx))) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      errno = n;
      return(-1);
    }
    return(0);
#    elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_mutex_lock(mtx)) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    }
    return(0);
#    elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    if ((n = pthread_mutex_lock(mtx))) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      errno = n;
      return(-1);
    }
    return(0);
#    else
    if (file != NULL) {
      serrno = SEOPNOTSUP;
    }
    return(-1);
#    endif /* _CTHREAD_PROTO */
#  endif /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */
  } else if (timeout == 0) {
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_mutex_trylock(mtx))) {
      /* EBUSY or EINVAL */
      errno = n;
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    } else {
      return(0);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_mutex_trylock(mtx) != 1) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    } else {
      return(0);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    if ((n = pthread_mutex_trylock(mtx))) {
      /* EBUSY or EINVAL */
      errno = n;
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    } else {
      return(0);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
    if ( TryEnterCriticalSection(&(mtx->mtx)) == TRUE ) {
        mtx->nb_locks++;
        return(0);
    } else {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    }
#  else
    if (file != NULL) {
      serrno = SEOPNOTSUP;
    }
    return(-1);
#  endif /* _CTHREAD_PROTO */
  } else {
    /* This code has been kept from comp.programming.threads */
    /* timeout > 0 */
    int gotmutex   = 0;
    unsigned long timewaited = 0;
    unsigned long Timeout = 0;
#if !(defined(IRIX5) || defined(IRIX6) || defined(IRIX64))
    struct timeval ts;
#endif

    /* Convert timeout in milliseconds */
    Timeout = timeout * 1000;

    while (timewaited < Timeout && ! gotmutex) {
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
      if ((n = pthread_mutex_trylock(mtx)))
        errno = n;
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
      n = pthread_mutex_trylock(mtx);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
      if ((n = pthread_mutex_trylock(mtx)))
        errno = n;
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
      n = -1;
      if ( TryEnterCriticalSection(&(mtx->mtx)) == TRUE ) {
          n = 0;
          mtx->nb_locks++;
      } else {
        errno = EBUSY;
      }
#  else
      if (file != NULL) {
        serrno = SEOPNOTSUP;
      }
      return(-1);
#  endif /* _CTHREAD_PROTO */
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
#if (defined(IRIX5) || defined(IRIX6) || defined(IRIX64) || defined(linux))
        /* usleep is in micro-seconds, not milli seconds... */
        usleep((Timeout * 1000)/20);
#else
        /* This method deadlocks on IRIX or linux (poll() via select() bug)*/
        ts.tv_sec = Timeout;
        ts.tv_usec = 0;
        select(0,NULL,NULL,NULL,&ts);
#endif
#  endif /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */
      }
    }
    if (file != NULL) {
      serrno = ETIMEDOUT;
    }
    return(-1);
  }
#endif /* ifndef _CTHREAD */
}

#else /* CTHREAD_DEBUG */

/* ============================================ */
/* Routine  : _Cthread_obtain_mtx_debug         */
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
/* ==================================================================================== */
/* If file == NULL then it it called from Cglobals.c, and we do not to overwrite serrno */
/* ==================================================================================== */

int _Cthread_obtain_mtx_debug(Cthread_file, Cthread_line, file, line, mtx, timeout)
     char *Cthread_file;
     int Cthread_line;
     char *file;
     int line;
     Cth_mtx_t *mtx;
     int timeout;
{
#ifdef _CTHREAD
  int               n;
#endif

  if (_Cthread_unprotect != 0) {
    if (mtx == &(Cthread.mtx)) {
      /* Unprotect mode */
      return(0);
    }
  }

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_obtain_mtx_debug(0x%lx,%d) called at %s:%d and behind %s:%d\n",
          _Cthread_self(),
          (unsigned long) mtx,(int) timeout, 
          Cthread_file, Cthread_line,
          file, line);
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
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
	} else {
      return(0);
    }
  } else {
    if ( (n=WaitForSingleObject((HANDLE)*mtx,timeout * 1000)) == WAIT_FAILED ) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
	} else if ( n == WAIT_TIMEOUT ) {
      if (file != NULL) {
        serrno = SETIMEDOUT;
      }
      return(-1);
	} else {
      return(0);
    }
  }
#else 
  /* This is a thread implementation */
  if (timeout < 0) {
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
    /*
     * Use Win32 critical section
     */
    EnterCriticalSection(&(mtx->mtx));
    mtx->nb_locks++;
    return(0);
#  else /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */
    /* Try to get the lock */
#    if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_mutex_lock(mtx))) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      errno = n;
      return(-1);
    }
    return(0);
#    elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_mutex_lock(mtx)) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    }
    return(0);
#    elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    if ((n = pthread_mutex_lock(mtx))) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      errno = n;
      return(-1);
    }
    return(0);
#    else
    if (file != NULL) {
      serrno = SEOPNOTSUP;
    }
    return(-1);
#    endif /* _CTHREAD_PROTO */
#  endif /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */
  } else if (timeout == 0) {
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_mutex_trylock(mtx))) {
      /* EBUSY or EINVAL */
      errno = n;
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    } else {
      return(0);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_mutex_trylock(mtx) != 1) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    } else {
      return(0);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    if ((n = pthread_mutex_trylock(mtx))) {
      /* EBUSY or EINVAL */
      errno = n;
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    } else {
      return(0);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
    if ( TryEnterCriticalSection(&(mtx->mtx)) == TRUE ) {
        mtx->nb_locks++;
        return(0);
    } else {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
    }
#  else
    if (file != NULL) {
      serrno = SEOPNOTSUP;
    }
    return(-1);
#  endif /* _CTHREAD_PROTO */
  } else {
    /* This code has been kept from comp.programming.threads */
    /* timeout > 0 */
    int gotmutex   = 0;
    unsigned long timewaited = 0;
    unsigned long Timeout = 0;
#if !(defined(IRIX5) || defined(IRIX6) || defined(IRIX64) || defined(linux))
    struct timeval ts;
#endif

    /* Convert timeout in milliseconds */
    Timeout = timeout * 1000;

    while (timewaited < Timeout && ! gotmutex) {
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
      if ((n = pthread_mutex_trylock(mtx)))
        errno = n;
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
      n = pthread_mutex_trylock(mtx);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
      if ((n = pthread_mutex_trylock(mtx)))
        errno = n;
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
      n = -1;
      if ( TryEnterCriticalSection(&(mtx->mtx)) == TRUE ) {
          n = 0;
          mtx->nb_locks++;
      } else {
        errno = EBUSY;
      }
#  else
      if (file != NULL) {
        serrno = SEOPNOTSUP;
      }
      return(-1);
#  endif /* _CTHREAD_PROTO */
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
#if (defined(IRIX5) || defined(IRIX6) || defined(IRIX64) || defined(linux))
        /* usleep is in micro-seconds, not milli seconds... */
        usleep((Timeout * 1000)/20);
#else
        /* This method deadlocks on IRIX or linux (poll() via select() bug)*/
        ts.tv_sec = Timeout;
        ts.tv_usec = 0;
        select(0,NULL,NULL,NULL,&ts);
#endif
#  endif /* _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32 */
      }
    }
    if (file != NULL) {
      serrno = SETIMEDOUT;
    }
    return(-1);
  }
#endif /* ifndef _CTHREAD */
}

#endif /* CTHREAD_DEBUG */

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
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addmtx(0x%lx) called at/behind %s:%d\n",
          _Cthread_self(),(unsigned long) Cmtx_new, file, line);
  }
#endif

#ifndef _CTHREAD
  /* Not a thread implementation */
  return(0);
#else
  /*
    if ( _Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1) ) 
      return(-1);
  */

  /* First found the last element */
  while (current->next != NULL) {
    current = current->next;
  }

  /* Change pointer of last element to its next one */
  current->next = Cmtx_new;

  /* Make sure that new structure points to no else */
  /* element                                        */
  ((struct Cmtx_element_t *) current->next)->next = NULL;
  
  /* 
     _Cthread_release_mtx(file,line,&(Cthread.mtx));
  */

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
     void *(*startroutine) _PROTO((void *));
     int detached;
{
  struct Cid_element_t *current = &Cid;    /* Curr Cid_element */
  int                 current_cid = -1;    /* Curr Cthread ID    */
  void               *tsd = NULL;          /* Thread-Specific Variable */
#ifdef _CTHREAD
  int n;
#if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  int                 ourthID;
#endif
#endif
  Cth_pid_t           ourpid;             /* We will need to identify ourself */

#ifdef CTHREAD_DEBUG
  if (Cthread_file != NULL) {
    /* To avoid recursion */
    if (file == NULL) {
      if (Cthread_debug != 0)
        log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid(0x%lx,%d,0x%lx,%d) called at %s:%d\n",
            _Cthread_self(),
            (unsigned long) pid,(int) thID, (unsigned long) startroutine, (int) detached,
            Cthread_file, Cthread_line);
    } else {
      if (Cthread_debug != 0)
        log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid(0x%lx,%d,0x%lx,%d) called at %s:%d and behind %s:%d\n",
            _Cthread_self(),
            (unsigned long) pid,(int) thID, (unsigned long) startroutine, (int) detached,
            Cthread_file, Cthread_line,
            file, line);
    }
  }
#endif
  
#ifdef _CTHREAD

  /* In the case of threaded environment, and if the entry we wanted */
  /* to insert is... us, then we check the Thread Specific Variable  */
  /* right now.                                                      */
  
  /* In a threaded environment, we get Thread Specific   */
  /* Variable, directly with the thread interface        */
  /* Yes, Because we want the less overhead as possible. */
  /* A previous version of Cthread was always looking to */
  /* the internal linked list. Generated a lot of        */
  /* _Cthread_obtain_mtx()/_Cthread_release_mtx()        */
  /* overhead. Now those two internal methods are called */
  /* only once, to get the cid. After it is stored in a  */
  /* Thread-Specific variable, whose associated key is   */
  /* cid_key.                                            */
  
  /* We makes sure that a key is associated once for all */
  /* with what we want.                                  */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  /* In WIN32 there is nothing corresponding to        */
  /* pthread_once(). Instead we rely on that Cthread_..*/
  /* is the unique thread interface.                   */
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if ((n = pthread_once(&cid_once,&_Cthread_cid_once)) != 0) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_once(&cid_once,&_Cthread_cid_once) != 0) {
    serrno = SECTHREADERR;
    return(-1);
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* On linux, pthread_once always returns 0... */
  pthread_once(&cid_once,&_Cthread_cid_once);
#  else
  serrno = SEOPNOTSUP;
  return(-1);
#  endif
  
  /* We look if there is already a Thread-Specific       */
  /* Variable.                                           */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  tsd = pthread_getspecific(cid_key);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_getspecific(cid_key,&tsd)) {
    tsd = NULL;
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  tsd = pthread_getspecific(cid_key);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  tsd = TlsGetValue(cid_key);
#  else
  serrno = SEOPNOTSUP;
  return(-1);
#  endif
  
  if (tsd == NULL) {
    /* We try to create the key-value */
    if ((tsd = (void *) malloc(sizeof(int))) == NULL) {
      serrno = SEINTERNAL;
      return(-1);
    }
    
    /* We associate the key-value with the key   */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_setspecific(cid_key, tsd))) {
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_setspecific(cid_key, tsd)) {
      serrno = SECTHREADERR;
      return(-1);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    if ((n = pthread_setspecific(cid_key, tsd))) {
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
    if ( !(n = TlsSetValue(cid_key, tsd)) ) {
      serrno = SECTHREADERR;
        return(-1);
    }
#  else
    serrno = SEOPNOTSUP;
    return(-1);
#  endif /* _CTHREAD_PROTO */

    /* And we initialize it */
    * (int *) tsd = -2;
  }
#else
  /* We use standard Cthread functions that simulates TSD in a fork model... */
  if (Cthread_getspecific(&cid_key,(void **) &tsd) != 0) {
    return(-1);
  }
  if (tsd == NULL) {
    if ((tsd = (void *) malloc(sizeof(Cth_pid_t))) == NULL) {
      return(-1);
    }
    if (Cthread_setspecific(&cid_key,tsd) != 0) {
      return(-1);
    }
  }
#endif /* CTHREAD */

#ifdef _CTHREAD
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  ourthID = GetCurrentThreadId();
#  else
  ourpid = pthread_self();
#  endif
#else
  ourpid = getpid();
#endif

  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);

  /* First find the last element */
  while (current->next != NULL) {
    current = current->next;

#ifndef _CTHREAD
    if (current->pid == *pid) {
      current->detached = detached;
      current->joined = 0;
#  ifdef CTHREAD_DEBUG
      if (Cthread_file != NULL) {
        /* To avoid recursion */
        if (file == NULL) {
          if (Cthread_debug != 0)
            log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d : Already existing cid = %d\n",_Cthread_self(),
                Cthread_file,Cthread_line,
                current->cid);
        } else {
          if (Cthread_debug != 0)
            log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d and behind %s:%d : Already existing cid = %d\n",_Cthread_self(),
                Cthread_file,Cthread_line,
                file,line,
                current->cid);
        }
        current_cid = current->cid;
        break;
      }
#  endif
    }
#else /* _CTHREAD */
#  if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
    if (current->thID == thID) {
      current->detached = detached;
      current->joined = 0;
      if ( pid != NULL ) current->pid = *pid;
#    ifdef CTHREAD_DEBUG
      if (Cthread_file != NULL) {
        /* To avoid recursion */
        if (file == NULL) {
          if (Cthread_debug != 0)
            log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d : Already existing cid = %d\n",_Cthread_self(),
                Cthread_file,Cthread_line,
                current->cid);
        } else {
          if (Cthread_debug != 0)
            log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d and behind %s:%d : Already existing cid = %d\n",_Cthread_self(),
                Cthread_file,Cthread_line,
                file, line,
                current->cid);
        }
      }
#    endif
      current_cid = current->cid;
      break;
    }
#  else /* _CTHREAD_PROTO_WIN32 */
    if (pthread_equal(current->pid,*pid)) {
      current->detached = detached;
      current->joined = 0;
#    ifdef CTHREAD_DEBUG
      if (Cthread_file != NULL) {
        /* To avoid recursion */
        if (file == NULL) {
          if (Cthread_debug != 0)
            log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d : Already existing cid=%d (current pid=%d)\n",_Cthread_self(),
                Cthread_file,Cthread_line,
                current->cid,getpid());
        } else {
          if (Cthread_debug != 0)
            log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d and behind %s:%d : Already existing cid=%d (current pid=%d)\n",_Cthread_self(),
                Cthread_file,Cthread_line,
                file, line,
                current->cid,getpid());
        }
      }
#    endif
      current_cid = current->cid;
      break;
    }
#  endif /* _CTHREAD_PROTO_WIN32 */
#endif /* _CTHREAD */
  }
  
  /* Not found                         */
  
  if (current_cid < 0) {
#ifdef CTHREAD_DEBUG
  if (Cthread_file != NULL) {
    /* To avoid recursion */
    if (file == NULL) {
      if (Cthread_debug != 0)
        log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d : cid not found. Will process a new one.\n",
            _Cthread_self(),
            Cthread_file,Cthread_line);
    } else {
      if (Cthread_debug != 0)
        log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d and behind %s:%d : cid not found. Will process a new one.\n",
            _Cthread_self(),
            Cthread_file,Cthread_line,
            file, line);
    }
  }
#endif
    /* Not found */
    if (startroutine == NULL) {
      /* This is the special case of initialization */
      current_cid = -1;
      * (int *) tsd = -1;
    } else {
      /* Make sure next cid is positive... */
      if ((current_cid = current->cid + 1) < 0) {
        _Cthread_release_mtx(file,line,&(Cthread.mtx));
        serrno = SEINTERNAL;
        return(-1);
      }
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
     * thread itself).
     */
    if ( pid != NULL ) {
      ((struct Cid_element_t *) current->next)->pid = *pid;
    }
    /* ((struct Cid_element_t *) current->next)->pid      = pid; */
    ((struct Cid_element_t *) current->next)->thID     = thID;
    ((struct Cid_element_t *) current->next)->addr     = startroutine;
    ((struct Cid_element_t *) current->next)->detached = detached;
    ((struct Cid_element_t *) current->next)->joined   = 0;
    ((struct Cid_element_t *) current->next)->cid      = current_cid;
    ((struct Cid_element_t *) current->next)->next     = NULL;
    
#ifdef CTHREAD_DEBUG
    if (Cthread_file != NULL) {
      /* To avoid recursion */
      if (file == NULL) {
        if (Cthread_debug != 0)
          log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d : created a new cid element with CthreadID=%d.\n",
              _Cthread_self(),
              Cthread_file,Cthread_line,
              current_cid);
      } else {
        if (Cthread_debug != 0)
          log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d and behind %s:%d : created a new cid element with CthreadID=%d.\n",
              _Cthread_self(),
              Cthread_file,Cthread_line,
              file, line,
              current_cid);
      }
    }
#endif

    current = current->next;

  }

  if (
#ifdef _CTHREAD
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
      ourthID == current->thID
#  else
      pthread_equal(ourpid,current->pid)
#  endif
#else
      ourpid == current->pid
#endif
      ) {
    /* We, the calling thread, is the same that is asking for a new cid */
    /* We update our tsd.                                               */
    * (int *) tsd = current_cid;
#    ifdef CTHREAD_DEBUG
    if (Cthread_file != NULL) {
      /* To avoid recursion */
      if (file == NULL) {
        if (Cthread_debug != 0)
          log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d : We are the same thread that own found CthreadID=%d. Now our output of _Cthread_self() should be equal to %d, please verify: _Cthread_self() = %d\n",
              _Cthread_self(),
              Cthread_file,Cthread_line,
              current_cid,
              current_cid,_Cthread_self());
      } else {
        if (Cthread_debug != 0)
          log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d and behind %s:%d : We are the same thread that own found CthreadID=%d. Now our output of _Cthread_self() should be equal to %d, please verify: _Cthread_self() = %d\n",
              _Cthread_self(),
              Cthread_file,Cthread_line,
              file, line,
              current_cid,
              current_cid,_Cthread_self());
      }
    }
#    endif
  }

  _Cthread_release_mtx(file,line,&(Cthread.mtx));
  
  /* Returns cid */
#ifdef CTHREAD_DEBUG
  if (Cthread_file != NULL) {
    /* To avoid recursion */
    if (file == NULL) {
      if (Cthread_debug != 0)
        log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d : Returning cid=%d (current pid=%d)\n",
            _Cthread_self(),
            Cthread_file,Cthread_line,
            current_cid,getpid());
    } else {
      if (Cthread_debug != 0)
        log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addcid() called at %s:%d and behind %s:%d : Returning cid=%d (current pid=%d)\n",
            _Cthread_self(),
            Cthread_file,Cthread_line,
            file, line,
            current_cid,getpid());
    }
  }
#endif
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
  Cth_pid_t pid;
  unsigned thID = 0;       /* WIN32 thread ID  */
#ifdef _CTHREAD
#if _CTHREAD_PROTO != _CTHREAD_PROTO_WIN32
  pthread_mutexattr_t  mattr;
#if _CTHREAD_PROTO != _CTHREAD_PROTO_LINUX
  int                  n;
#endif
#endif /* _CTHREAD_PROTO != _CTHREAD_PROTO_WIN32 */
#endif

#ifndef _CTHREAD
  /* In a non-thread environment, initialize the */
  /* pointers                                    */
  Cid.cid      = -1;
  Cid.pid      = -1;
  Cid.thID     = 0;
  Cid.addr     = NULL;
  Cid.detached = 0;
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
    pthread_mutexattr_destroy(&mattr);
    errno = n;
	return;
  }
  pthread_mutexattr_destroy(&mattr);
  pid = (Cth_pid_t) pthread_self();
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    /* Create the mutex a-la DCE */
  if (pthread_mutex_init(&(Cthread.mtx),pthread_mutexattr_default) != 0) {
    return;
  }
  pid = pthread_self();
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* Create the mutex a-la LinuxThreads      */
  /* On this OS, pthread_mutexattr_init and  */
  /* pthread_mutex_init always returns 0 ... */
  pthread_mutexattr_init(&mattr);
  pthread_mutex_init(&(Cthread.mtx),&mattr);
  pthread_mutexattr_destroy(&mattr);
  pid = (Cth_pid_t) pthread_self();
#elif _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  thID = GetCurrentThreadId();
  pid = getpid();
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
  Cid.cid      = -1;
  Cid.pid      = pid;
  Cid.thID     = thID;
  Cid.addr     = NULL;
  Cid.detached = 0;
  Cid.next     = NULL;
  Cspec.next   = NULL;
  Cmtx.next  = NULL;
  Cspec.next = NULL;
  _Cthread_once_status = 0;
  /* We now check if the caller has a Thread-Specific variable  */
  /* If it has not such one, yet, then _Cthread_self() will     */
  /* return -2, and by the way will have created the TSD for us */
  if (_Cthread_self() == -2) {
    /* Cthread do not know, yet, of this thread. This can occur   */
    /* only in the main thread. We, so, just have to create an    */
    /* entry in the Cthread linked list, so that subsequent calls */
    /* will recognize the main thread.                            */
    _Cthread_addcid(NULL,0,NULL,0,&pid,thID,NULL,0);
  }
  /* Initialize thread specific globals environment*/

  Cglobals_init(Cthread_Getspecific_init,Cthread_Setspecific0,(int (*) _PROTO((void))) Cthread_Self0); 
  Cmutex_init(Cthread_Lock_Mtx_init,Cthread_Mutex_Unlock_init);
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
  if (Cthread_debug != 0)
    log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_keydestructor(0x%lx)\n",_Cthread_self(),(unsigned long) addr);
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
/* ==================================================================================== */
/* If file == NULL then it it called from Cglobals.c, and we do not to overwrite serrno */
/* ==================================================================================== */

struct Cspec_element_t *_Cthread_findglobalkey(file, line, global_key)
     char *file;
     int line;
     int *global_key;
{
  struct Cspec_element_t *current = &Cspec;   /* Curr Cspec_element */
  int                  n;                 /* Status            */

  /* Verify the arguments */
  if (global_key == NULL) {
    if (file != NULL) {
      serrno = EINVAL;
    }
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
    /* It is not formally an error if the key is not yet known to Cthread, so */
    /* we do not overwrite serrno                                             */
    /*
      if (file != NULL) {
        serrno = EINVAL;
      }
    */
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
int DLL_DECL Cthread_Setspecific0(global_key, addr)
     int *global_key;
     void *addr;
{
  return(Cthread_Setspecific(NULL,0,global_key,addr));
}

int DLL_DECL Cthread_Setspecific(file, line, global_key, addr)
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
    /* To avoid recursion */
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In Cthread_setspecific(0x%lx,0x%lx) called at/behind %s:%d\n",
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
int DLL_DECL Cthread_Getspecific0(global_key, addr)
     int *global_key;
     void **addr;
{
  return(Cthread_Getspecific("Cthread.c(Cthread_Getspecific0)",__LINE__,global_key,addr));
}

int DLL_DECL Cthread_Getspecific_init(global_key, addr)
     int *global_key;
     void **addr;
{
  /* Just to avoid debug printing recursion from Cglobals_init() */
  return(Cthread_Getspecific(NULL,__LINE__,global_key,addr));
}

int DLL_DECL Cthread_Lock_Mtx_init(addr,timeout)
     void *addr;
     int timeout;
{
  return(Cthread_Lock_Mtx(__FILE__,__LINE__,addr,timeout));
}

int DLL_DECL Cthread_Mutex_Unlock_init(addr)
     void *addr;
{
  /* Just to avoid debug printing recursion from Cmutex_init() */
  return(Cthread_Mutex_Unlock(__FILE__,__LINE__,addr));
}

/* ==================================================================================== */
/* If file == NULL then it it called from Cglobals.c, and we do not to overwrite serrno */
/* ==================================================================================== */

int DLL_DECL Cthread_Getspecific(file, line, global_key, addr)
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
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In Cthread_getspecific(0x%lx) called at/behind %s:%d\n",
          _Cthread_self(),
          (unsigned long) global_key,
          file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  /* Verify the arguments */
  if (global_key == NULL || addr == NULL) {
    if (file != NULL) {
      serrno = EINVAL;
    }
    return(-1);
  }

#ifdef _NOCTHREAD
  /* This is not a thread implementation */
  if ((current = _Cthread_findglobalkey(file,line,global_key)) == NULL) {
    /* Nothing yet */
    struct Cspec_element_t *Cspec_new;
    
    if ((Cspec_new = (struct Cspec_element_t *) malloc(sizeof(struct Cspec_element_t)))
        == NULL) {
      if (file != NULL) {
        serrno = SEINTERNAL;
      }
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
      if (file != NULL) {
        serrno = SEINTERNAL;
      }
      return(-1);
    }

#if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    /* Create the specific variable a-la POSIX */
    if ((n = pthread_key_create(&(Cspec_new->key),&_Cthread_keydestructor))) {
      errno = n;
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      free(Cspec_new);
      return(-1);
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    /* Create the specific variable a-la DCE */
    if (pthread_keycreate(&(Cspec_new->key),&_Cthread_keydestructor)) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      free(Cspec_new);
      return(-1);
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    /* Create the specific variable a-la LinuxThreads */
    if ((n = pthread_key_create(&(Cspec_new->key),&_Cthread_keydestructor))) {
      errno = n;
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      free(Cspec_new);
      return(-1);
    }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
    if ( (n = TlsAlloc()) == 0xFFFFFFFF ) {
      free(Cspec_new);
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      return(-1);
	} else Cspec_new->key = n;
#else
    if (file != NULL) {
      serrno = SEOPNOTSUP;
    }
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
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addspec(0x%lx) called at/behind %s:%d\n",_Cthread_self(),
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
int _Cthread_win32_cond_init(Cth_cond_t *cv) {
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
int _Cthread_win32_cond_wait(Cth_cond_t *cv,
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
    _Cthread_release_mtx(NULL,0,mtx);
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
    return(_Cthread_obtain_mtx(NULL,0,mtx,timeout));
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
int _Cthread_win32_cond_broadcast(Cth_cond_t *cv) {
    int have_waiters;
    /*
     * Avoid race on nb_waiters counter. We don't
     * use _Cthread_obtain_mtx() etc. to reduce
     * overhead from internal logic.
     */
    EnterCriticalSection(&(cv->nb_waiters_lock));
    have_waiters = (cv->nb_waiters > 0);
    LeaveCriticalSection(&(cv->nb_waiters_lock));
    if ( have_waiters ) {
        if ( SetEvent(cv->cond_events[CTHREAD_WIN32_BROADCAST]) == 0 ) return(-1);
        else return(0);
    } else return(0);
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
int _Cthread_win32_cond_signal(Cth_cond_t *cv) {
    int have_waiters;
    /*
     * Avoid race on nb_waiters counter. We don't
     * use _Cthread_obtain_mtx() etc. to reduce
     * overhead from internal logic.
     */
    EnterCriticalSection(&(cv->nb_waiters_lock));
    have_waiters = (cv->nb_waiters > 0);
    LeaveCriticalSection(&(cv->nb_waiters_lock));
    if ( have_waiters ) {
        if ( SetEvent(cv->cond_events[CTHREAD_WIN32_SIGNAL]) == 0 ) return(-1);
        else return(0);
    } else return(0);
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
int _Cthread_win32_cond_destroy(Cth_cond_t *cv) {
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
int DLL_DECL Cthread_proto() {
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
int DLL_DECL Cthread_isproto(proto)
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
int DLL_DECL Cthread_environment() {
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
int DLL_DECL _Cthread_self() {
#ifdef _CTHREAD
  void               *tsd = NULL;       /* Thread-Specific Variable */
#else
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  Cth_pid_t             pid;            /* Current PID      */
#endif  
  int                   n;              /* Return value     */

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);


#ifdef _NOCTHREAD
  /* In a non-threaded environment, we just check */
  /* the list.                                    */

  /* We check that this pid correspond to a known */
  /* process                                      */
  pid = getpid();
  n = -1;
  while (current->next != NULL) {
    current = current->next;
    if (current->pid == pid) {
      n = current->cid;
      break;
    }
  }

  /* if (n < 0)         */
  /*   serrno = EINVAL; */
  return(n);

#else /* _NOCTHREAD */

  /* In a threaded environment, we get Thread Specific   */
  /* Variable, directly with the thread interface        */
  /* Yes, Because we want the less overhead as possible. */
  /* A previous version of Cthread was always looking to */
  /* the internal linked list. Generated a lot of        */
  /* _Cthread_obtain_mtx()/_Cthread_release_mtx()        */
  /* overhead. Now those two internal methods are called */
  /* only once, to get the cid. After it is stored in a  */
  /* Thread-Specific variable, whose associated key is   */
  /* cid_key.                                            */

  /* We makes sure that a key is associated once for all */
  /* with what we want.                                  */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  /* In WIN32 there is nothing corresponding to        */
  /* pthread_once(). Instead we rely on that Cthread_..*/
  /* is the unique thread interface.                   */
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if ((n = pthread_once(&cid_once,&_Cthread_cid_once)) != 0) {
    errno = n;
    /* serrno = SECTHREADERR; */
    return(-1);
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_once(&cid_once,&_Cthread_cid_once) != 0) {
    /* serrno = SECTHREADERR; */
    return(-1);
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  /* On linux, pthread_once always returns 0... */
  pthread_once(&cid_once,&_Cthread_cid_once);
#  else
  /* serrno = SEOPNOTSUP; */
  return(-1);
#  endif

  /* We look if there is already a Thread-Specific       */
  /* Variable.                                           */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  tsd = pthread_getspecific(cid_key);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if (pthread_getspecific(cid_key,&tsd)) {
    tsd = NULL;
  }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  tsd = pthread_getspecific(cid_key);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
  tsd = TlsGetValue(cid_key);
#  else
  /* serrno = SEOPNOTSUP; */
  return(-1);
#  endif

  if (tsd == NULL) {

    /* We try to create the key-value */
    if ((tsd = (void *) malloc(sizeof(int))) == NULL) {
      /* serrno = SEINTERNAL; */
      return(-1);
    }

    /* We associate the key-value with the key   */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
    if ((n = pthread_setspecific(cid_key, tsd))) {
      errno = n;
      /* serrno = SECTHREADERR; */
      return(-1);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
    if (pthread_setspecific(cid_key, tsd)) {
      /* serrno = SECTHREADERR; */
      return(-1);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
    if ((n = pthread_setspecific(cid_key, tsd))) {
      errno = n;
      /* serrno = SECTHREADERR; */
      return(-1);
    }
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_WIN32
    if ( !(n = TlsSetValue(cid_key, tsd)) ) {
      /* serrno = SECTHREADERR; */
      return(-1);
    }
#  else
    /* serrno = SEOPNOTSUP; */
    return(-1);
#  endif
    
    /* And we don't yet know the cid... So we set it to -2 */

    * (int *) tsd = -2;
    
    return(-2);

  } else {

    /* Thread-Specific variable already exist ! */

    return((int) * (int *) tsd);

  }
#endif /* _NOCTHREAD */
}

/* =============================================== */
/* Routine  : Cthread_unprotect                    */
/* Arguments:                                      */
/* ----------------------------------------------- */
/* Output   :                                      */
/* ----------------------------------------------- */
/* History:                                        */
/* 13-OCT-1999       First implementation          */
/*                   Jean-Damien.Durand@cern.ch    */
/* =============================================== */
/* Notes:                                          */
/* Sets, without lock, the variable                */
/*           _Cthread_unprotect                    */
/* to 1, disabling the internal mutex lock/unlock  */
/* to access Cthread internal linked lists.        */
/* =============================================== */
void DLL_DECL Cthread_unprotect() {
  _Cthread_unprotect = 1;
}

/* ============================================ */
/* Routine  : Cthread_Kill                      */
/* Arguments: caller source file                */
/*            caller source line                */
/*            Cthread ID                        */
/*            signal number                     */
/* -------------------------------------------- */
/* Output   : integer  0 - OK                   */
/*                   < 0 - Error                */
/* -------------------------------------------- */
/* History:                                     */
/* 27-NOV-2001       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cthread_Kill(file, line, cid, signo)
     char *file;
     int line;
     int cid;
     int signo;
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  int                   n;                /* Status           */

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_kill(%d,%d) called at/behind %s:%d\n",
          _Cthread_self(),
          cid, signo,
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
  n = kill(current->pid,signo);
  if (n != 0)
    serrno = SEINTERNAL;
  return(n != 0 ? -1 : 0);
#else /* ifdef _NOCTHREAD */
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  if (TerminateThread((HANDLE)current->pid,(DWORD)signo) == 0) { /* Failure */
    return(-1);
  }
  /* Memory Leak/Unpredictable behaviour possible from now on - cross your finger */
  /* No other safe AND simple method to kill another thread with _WIN32 ? */
  return(0);
#else /* _CTHREAD_PROTO_WIN32 */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  if ((n = pthread_kill(current->pid,signo))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  return(0);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  serrno = SEOPNOTSUP; /* Does not exist on DCE */
  return(-1);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  if ((n = pthread_kill(current->pid,signo))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  return(0);
#  else
  serrno = SEOPNOTSUP;
  return(-1);
#  endif
#endif /* _CTHREAD_PROTO_WIN32 */
#endif /* ifdef _NOCTHREAD */
}

/* ============================================ */
/* Routine  : Cthread_Exit                      */
/* Arguments: caller source file                */
/*            caller source line                */
/*            status                            */
/* -------------------------------------------- */
/* Note: WE ASSUME status is an int * in non-   */
/* multithreaded applications and on _WIN32     */
/* -------------------------------------------- */
/* Output   : integer  0 - OK                   */
/*                   < 0 - Error                */
/* -------------------------------------------- */
/* History:                                     */
/* 27-NOV-2001       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
void DLL_DECL Cthread_Exit(file, line, status)
     char *file;
     int line;
     void *status;
{

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,
          "[Cthread    [%2d]] In Cthread_exit(0x%lx) called at/behind %s:%d\n",
          _Cthread_self(),
          (unsigned long) status,
          file, line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return;

#ifdef _NOCTHREAD
  exit(status != NULL ? (int) * (int *) status : 0);
#else /* ifdef _NOCTHREAD */
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  _endthreadex(status != NULL ? (unsigned) * (int *) status : 0); /* Assume an int *, changed to unsigned afterwards */
  serrno = SEOPNOTSUP; /* Don't know how to do it yet */
  return;
#else /* _CTHREAD_PROTO_WIN32 */
#  if _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  pthread_exit(status);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  pthread_exit(status);
#  elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  pthread_exit(status);
#  else
  serrno = SEOPNOTSUP;
  return;
#  endif
#endif /* _CTHREAD_PROTO_WIN32 */
#endif /* ifdef _NOCTHREAD */
}

