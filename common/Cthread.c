/*
 * $Id: Cthread.c,v 1.52 2007/12/07 16:09:40 sponcec3 Exp $
 */

/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <Cthread_api.h>
#include <Cthread_typedef.h>
#include <Cglobals.h>
#include <Cmutex.h>
#include <serrno.h>
#include <errno.h>
#include <osdep.h>
#include <signal.h>
#include <string.h>

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
int   _Cthread_addcid (const char *, int, const char *, int, Cth_pid_t *, unsigned, void *(*)(void *), int);
/* Add a mutex */
int   _Cthread_addmtx (const char *, int, struct Cmtx_element_t *);
/* Add a TSD */
int   _Cthread_addspec (const char *, int, struct Cspec_element_t *);
/* Obain a mutex lock */
#ifndef CTHREAD_DEBUG
int   _Cthread_obtain_mtx (const char *, int, Cth_mtx_t *, int);
#else
#define _Cthread_obtain_mtx(a,b,c,d) _Cthread_obtain_mtx_debug(__FILE__,__LINE__,a,b,c,d)
int   _Cthread_obtain_mtx_debug (const char *, int, const char *, int, Cth_mtx_t *, int);
#endif
/* Release a mutex lock */
int   _Cthread_release_mtx (const char *, int, Cth_mtx_t *);
/* Methods to create a mutex to protect Cthread */
/* internal tables and linked lists             */
/* This is useful only in thread environment    */
void  _Cthread_once (void);
int   _Cthread_init (void);
/* Release of a thread-specific variable        */
void  _Cthread_keydestructor (void *);
/* Release of a thread                          */
int   _Cthread_destroy (const char *, int, int);
void *_Cthread_start_pthread (void *);

/* Find a global key in Cspec structure         */
struct Cspec_element_t *_Cthread_findglobalkey (const char *, int, int *);
/* Cthread-ID (as a Thread-Specific variable) destructor */
void _Cthread_cid_destructor (void *);
/* Cthread-ID once for all... */
void _Cthread_cid_once (void);

/* ------------------------------------ */
/* For the Cthread_self() command       */
/* (Thread-Specific Variable)           */
/* ------------------------------------ */
Cth_spec_t cid_key;
Cth_once_t cid_once = 0;

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
struct Cmtx_element_t  Cmtx;
struct Cthread_protect_t Cthread;
struct Cspec_element_t Cspec;
Cth_once_t             once = 0;
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
void _Cthread_cid_destructor(void *ptr)
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
  if ((n = pthread_key_create(&cid_key,&_Cthread_cid_destructor))) {
    errno = n;
    serrno = SECTHREADERR;
  }
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
int Cthread_init() {
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
  /* In a thread environment we need to have our own */
  /* mutex. So we create it right now - we do it     */
  /* dynamically, since we don't rely on the fact    */
  /* that the macro PTHREAD_MUTEX_INITIALIZER exists */
  /* everywhere                                      */
  int n;
  if ((n = pthread_once(&once,&_Cthread_once)) != 0) {
    errno = n;
    _Cthread_once_status = -1;
  }
  if ((status = _Cthread_once_status) != 0)
    serrno = SECTHREADINIT;

  return(status);
}

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
int Cthread_Create(const char *file,
                   int line,
                   void *(*startroutine) (void *),
                   void *arg)
{
  Cth_pid_t pid;                        /* Thread/Process ID */
  unsigned thID = 0;                    /* Thread ID (WIN32) */
  Cthread_start_params_t *starter;      /* Starter           */
  int            n;                     /* status            */
  pthread_attr_t attr;                  /* Thread attribute  */
  
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

  /* Create the starter              */
  if ((starter = (Cthread_start_params_t *) malloc(sizeof(Cthread_start_params_t))) == NULL) {
    serrno = SEINTERNAL;
    return(-1);
  }
  starter->_thread_routine = startroutine;
  starter->_thread_arg     = arg;
  starter->detached        = 0;

  /* Create the thread               */
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
  /* Add it               */
  return(_Cthread_addcid(__FILE__,__LINE__,file,line,&pid,thID,startroutine,0));
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
int Cthread_Create_Detached(const char *file,
                            int line,
                            void *(*startroutine) (void *),
                            void *arg)
{
  Cth_pid_t pid;                        /* Thread/Process ID */
  unsigned thID = 0;                       /* Thread ID (WIN32) */
  Cthread_start_params_t *starter;
  int            n;                   /* status            */
  pthread_attr_t attr;                /* Thread attribute  */
  
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
  return(_Cthread_addcid(__FILE__,__LINE__,file,line,&pid,thID,startroutine,1));
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
int Cthread_Join(const char *file,
                 int line,
                 int cid,
                 int **status)
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
int Cthread_Detach(const char *file,
                   int line,
                   int cid)
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

  /* This is a thread implementation */
  /* Is it already detached ? */
  if (detached) {
    /* Yes. It is OK for Cthread point of view */
    return(0);
  }
  /* Not yet detached: let's do it */
  if ((n = pthread_detach(current->pid))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  return(0);
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
int Cthread_Self0() {
  return(_Cthread_self());
}
int Cthread_Self(const char *file,
                 int line)
{
  void               *tsd = NULL;       /* Thread-Specific Variable */
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
  if ((n = pthread_once(&cid_once,&_Cthread_cid_once)) != 0) {
    errno = n;
    /* serrno = SECTHREADERR; */
    return(-1);
  }

  /* We look if there is already a Thread-Specific       */
  /* Variable.                                           */
  tsd = pthread_getspecific(cid_key);

  if (tsd == NULL) {

    /* We try to create the key-value */
    if ((tsd = (void *) malloc(sizeof(int))) == NULL) {
      /* serrno = SEINTERNAL; */
      return(-1);
    }

    /* We associate the key-value with the key   */
    if ((n = pthread_setspecific(cid_key, tsd))) {
      errno = n;
      /* serrno = SECTHREADERR; */
      return(-1);
    }
    
    /* And we don't yet know the cid... So we set it to -2 */
    * (int *) tsd = -2;
    return(-2);

  } else {

    /* Thread-Specific variable already exist ! */
    return((int) * (int *) tsd);
  }
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
int _Cthread_destroy(const char *file,
                     int line,
                     int cid)
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
int Cthread_Cond_Broadcast_ext(const char *file,
                               int line,
                               void *addr)
{
  struct Cmtx_element_t *current = addr;  /* Curr Cmtx_element */
  int                  rc;

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

  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

  /* This is a thread implementation            */
  rc = 0;
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
  return(rc);
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
int Cthread_Cond_Broadcast(const char *file,
                           int line,
                           void *addr)
{
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  int                  n;                 /* Status            */
  int                  rc;

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
  _Cthread_release_mtx(file,line,&(Cthread.mtx));
  return(rc);
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
int Cthread_Wait_Condition_ext(const char *file,
                               int line,
                               void *addr,
                               int timeout)
{
  struct Cmtx_element_t *current = addr;    /* Curr Cmtx_element */
  int                    n;                 /* Status            */
  int                    rc;
  
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
    if ((n = pthread_cond_wait(&(current->cond),&(current->mtx))) != 0) {
      errno = n;
      serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
      rc = -1;
    } else {
      rc = 0;
    }
  } else {
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
      if ((n = pthread_cond_timedwait(&(current->cond),&(current->mtx),&ts)) != 0) {
        errno = n;
        serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
	}
  }

  /* This end of the routine will decrease the number */
  /* of wait doing a Wait_Condition on addr           */

  /* Decrease the number of waiting threads */
   --current->nwait;

  return(rc);
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
int Cthread_Wait_Condition(const char *file,
                           int line,
                           void *addr,
                           int timeout)
{
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  int                    n;                 /* Status            */
  int                    rc;
  
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
    if ((n = pthread_cond_wait(&(current->cond),&(current->mtx))) != 0) {
      errno = n;
      serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
      rc = -1;
    } else {
      rc = 0;
    }
  } else {
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
      if ((n = pthread_cond_timedwait(&(current->cond),&(current->mtx),&ts)) != 0) {
        errno = n;
        serrno = ( errno == ETIMEDOUT ? SETIMEDOUT : SECTHREADERR );
        rc = -1;
      } else {
        rc = 0;
      }
	}
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
int Cthread_Lock_Mtx(const char *file,
                     int line,
                     void *addr,
                     int timeout)
{
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  int                  n;                 /* Status            */

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
    }
    /* Cmtx_new->mtx contains now a valid mutex   */
    /* AND a valid conditional variable           */
    if (_Cthread_addmtx(file,line,Cmtx_new)) {
      /* Error ! Destroy what we have created */
      pthread_mutex_destroy(&(current->mtx));
      pthread_cond_destroy(&(current->cond));
      free(Cmtx_new);
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
int Cthread_Lock_Mtx_ext(const char *file,
                         int line,
                         void *addr,
                         int timeout)
{
  struct Cmtx_element_t *current = addr;  /* Curr Cmtx_element */

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

  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

  /* This is a thread implementation            */
  return(_Cthread_obtain_mtx(file,line,&(current->mtx),timeout));
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
void *Cthread_Lock_Mtx_addr(const char *file,
                            int line,
                            void *addr)
{
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  int                  n;                 /* Status            */

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
int Cthread_Mutex_Unlock_ext(const char *file,
                             int line,
                             void *addr)
{
  struct Cmtx_element_t *current = addr;   /* Curr Cmtx_element */

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

  /* Verify the arguments */
  if (addr == NULL) {
    serrno = EINVAL;
    return(-1);
  }

  /* This is a thread implementation            */
  return(_Cthread_release_mtx(file,line,&(current->mtx)));
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
int Cthread_Mutex_Unlock(const char *file,
                         int line,
                         void *addr)
{
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  int                  n;                 /* Status            */

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
int Cthread_Mutex_Destroy(const char *file,
                          int line,
                          void *addr)
{
  struct Cmtx_element_t *current = &Cmtx;   /* Curr Cmtx_element */
  struct Cmtx_element_t *previous = NULL;   /* Prev Cmtx_element */
  int                    n;                 /* Status            */

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
    n += pthread_mutex_destroy(&(current->mtx));
    n += pthread_cond_destroy(&(current->cond));
    free(current);
    if (n != 0) {
      serrno = SECTHREADERR;
      n = -1;
    }
    _Cthread_release_mtx(file,line,&(Cthread.mtx));
    return(n);
  }
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

int _Cthread_release_mtx(const char *file,
                         int line,
                         Cth_mtx_t *mtx)
{
   int               n;

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

  /* This is a thread environment */
  if ((n = pthread_mutex_unlock(mtx))) {
    if (file != NULL) {
      serrno = SECTHREADERR;
    }
    errno = n;
    return(-1);
  } else {
    return(0);
  }
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

int _Cthread_obtain_mtx(const char *file,
                        int line,
                        Cth_mtx_t *mtx,
                        int timeout)
{
  int               n;

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

  /* This is a thread implementation */
  if (timeout < 0) {
    /* Try to get the lock */
    if ((n = pthread_mutex_lock(mtx))) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      errno = n;
      return(-1);
    }
    return(0);
  } else if (timeout == 0) {
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
  } else {
    /* This code has been kept from comp.programming.threads */
    /* timeout > 0 */
    int gotmutex   = 0;
    unsigned long timewaited = 0;
    unsigned long Timeout = 0;
    struct timeval ts;

    /* Convert timeout in milliseconds */
    Timeout = timeout * 1000;

    while (timewaited < Timeout && ! gotmutex) {
      if ((n = pthread_mutex_trylock(mtx)))
        errno = n;
      if (errno == EDEADLK || n == 0) {
        gotmutex = 1;
        return(0);
      }
      if (errno == EBUSY) {
        timewaited += Timeout / 20;
#if defined(linux)
        /* usleep is in micro-seconds, not milli seconds... */
        usleep((Timeout * 1000)/20);
#else
        /* This method deadlocks on IRIX or linux (poll() via select() bug)*/
        ts.tv_sec = Timeout;
        ts.tv_usec = 0;
        select(0,NULL,NULL,NULL,&ts);
#endif
      }
    }
    if (file != NULL) {
      serrno = ETIMEDOUT;
    }
    return(-1);
  }
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

int _Cthread_obtain_mtx_debug(const char *Cthread_file,
                              int Cthread_line,
                              const char *file,
                              int line,
                              Cth_mtx_t *mtx,
                              int timeout)
{
  int               n;

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

  /* This is a thread implementation */
  if (timeout < 0) {
    /* Try to get the lock */
    if ((n = pthread_mutex_lock(mtx))) {
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      errno = n;
      return(-1);
    }
    return(0);
  } else if (timeout == 0) {
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
  } else {
    /* This code has been kept from comp.programming.threads */
    /* timeout > 0 */
    int gotmutex   = 0;
    unsigned long timewaited = 0;
    unsigned long Timeout = 0;
#if !(defined(linux))
    struct timeval ts;
#endif
    /* Convert timeout in milliseconds */
    Timeout = timeout * 1000;

    while (timewaited < Timeout && ! gotmutex) {
      if ((n = pthread_mutex_trylock(mtx)))
        errno = n;
      if (errno == EDEADLK || n == 0) {
        gotmutex = 1;
        return(0);
      }
      if (errno == EBUSY) {
        timewaited += Timeout / 20;
#if defined(linux)
        /* usleep is in micro-seconds, not milli seconds... */
        usleep((Timeout * 1000)/20);
#else
        /* This method deadlocks on IRIX or linux (poll() via select() bug)*/
        ts.tv_sec = Timeout;
        ts.tv_usec = 0;
        select(0,NULL,NULL,NULL,&ts);
#endif
      }
    }
    if (file != NULL) {
      serrno = SETIMEDOUT;
    }
    return(-1);
  }
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
int _Cthread_addmtx(const char *file,
                    int line,
                    struct Cmtx_element_t *Cmtx_new)
{
  struct Cmtx_element_t *current = &Cmtx;    /* Curr Cmtx_element */
  
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_addmtx(0x%lx) called at/behind %s:%d\n",
          _Cthread_self(),(unsigned long) Cmtx_new, file, line);
  }
#endif

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
int _Cthread_addcid(const char *Cthread_file,
                    int Cthread_line,
                    const char *file,
                    int line,
                    Cth_pid_t *pid,
                    unsigned thID,
                    void *(*startroutine) (void *),
                    int detached)
{
  struct Cid_element_t *current = &Cid;    /* Curr Cid_element */
  int                 current_cid = -1;    /* Curr Cthread ID    */
  void               *tsd = NULL;          /* Thread-Specific Variable */
  int n;
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
  if ((n = pthread_once(&cid_once,&_Cthread_cid_once)) != 0) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  
  /* We look if there is already a Thread-Specific       */
  /* Variable.                                           */
  tsd = pthread_getspecific(cid_key);
  
  if (tsd == NULL) {
    /* We try to create the key-value */
    if ((tsd = (void *) malloc(sizeof(int))) == NULL) {
      serrno = SEINTERNAL;
      return(-1);
    }
    
    /* We associate the key-value with the key   */
    if ((n = pthread_setspecific(cid_key, tsd))) {
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    }

    /* And we initialize it */
    * (int *) tsd = -2;
  }

  ourpid = pthread_self();

  if (_Cthread_obtain_mtx(file,line,&(Cthread.mtx),-1))
    return(-1);

  /* First find the last element */
  while (current->next != NULL) {
    current = current->next;

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
      pthread_equal(ourpid,current->pid)
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
  pthread_mutexattr_t  mattr;
  int                  n;

  /* In a thread environment, initialize the */
  /* Cthread internal structure                */
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

  Cglobals_init(Cthread_Getspecific_init,Cthread_Setspecific0,(int (*) (void)) Cthread_Self0); 
  Cmutex_init(Cthread_Lock_Mtx_init,Cthread_Mutex_Unlock_init);
  return;
}

/* ============================================ */
/* Routine  : Cthread_keydestructor             */
/* Arguments: void *addr                        */
/* -------------------------------------------- */
/* History:                                     */
/* 28-APR-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
void _Cthread_keydestructor(void *addr)
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
     const char *file;
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
int Cthread_Setspecific0(int *global_key,
                         void *addr)
{
  return(Cthread_Setspecific(NULL,0,global_key,addr));
}

int Cthread_Setspecific(const char *file,
                        int line,
                        int *global_key,
                        void *addr)
{
  struct Cspec_element_t *current = &Cspec;   /* Curr Cspec_element */
  int                  n;                 /* Status            */

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
  if ((n = pthread_setspecific(current->key, addr))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  /* OK */
  return(0);
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
int Cthread_Getspecific0(int *global_key,
                         void **addr)
{
  return(Cthread_Getspecific("Cthread.c(Cthread_Getspecific0)",__LINE__,global_key,addr));
}

int Cthread_Getspecific_init(int *global_key,
                             void **addr)
{
  /* Just to avoid debug printing recursion from Cglobals_init() */
  return(Cthread_Getspecific(NULL,__LINE__,global_key,addr));
}

int Cthread_Lock_Mtx_init(void *addr,
                          int timeout)
{
  return(Cthread_Lock_Mtx(__FILE__,__LINE__,addr,timeout));
}

int Cthread_Mutex_Unlock_init(void *addr)
{
  /* Just to avoid debug printing recursion from Cmutex_init() */
  return(Cthread_Mutex_Unlock(__FILE__,__LINE__,addr));
}

/* ==================================================================================== */
/* If file == NULL then it it called from Cglobals.c, and we do not to overwrite serrno */
/* ==================================================================================== */

int Cthread_Getspecific(const char *file,
                        int line,
                        int *global_key,
                        void **addr)
{
  struct Cspec_element_t *current = &Cspec;   /* Curr Cspec_element */
  int                     n;                  /* Status             */
  void                   *tsd = NULL;         /* TSD address        */

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

    /* Create the specific variable a-la POSIX */
    if ((n = pthread_key_create(&(Cspec_new->key),&_Cthread_keydestructor))) {
      errno = n;
      if (file != NULL) {
        serrno = SECTHREADERR;
      }
      free(Cspec_new);
      return(-1);
    }
    /* Initialize global_key */
    Cspec_new->global_key = global_key;
    /* Make sure it points to nothing after */
    Cspec_new->next = NULL;
    /* Cspec_new->key contains now a valid key   */

    if (_Cthread_addspec(file,line,Cspec_new)) {
      /* Error ! Destroy what we have created */
      pthread_key_delete(Cspec_new->key);
      free(Cspec_new);
      return(-1);
    }
    /* We set the address of the TSD to NULL */
    *addr = NULL;
    /* We return OK */
    return(0);
  }
  /* We want to know the TSD address */
  tsd = pthread_getspecific(current->key);
  /* We change the address location */
  *addr = tsd;
  /* We return OK */
  return(0);
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
int _Cthread_addspec(const char *file,
                     int line,
                     struct Cspec_element_t *Cspec_new)
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
int Cthread_proto() {
  return(1);
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
int Cthread_isproto(char *proto)
{
  if (proto == NULL) {
    serrno = EINVAL;
    return(1);
  }
  if (strcmp(proto,"POSIX") != 0) {
    serrno = EINVAL;
    return(-1);
  }
  return(0);
}

/* ============================================ */
/* Routine  : Cthread_environment               */
/* Arguments:                                   */
/* -------------------------------------------- */
/* Output   : CTHREAD_TRUE_THREAD (1) or        */
/*            CTHREAD_MULTI_PROCESS (2)         */
/* -------------------------------------------- */
/* History:                                     */
/* 02-JUN-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* Says if the implementation is singlethreaded */
/* or not.                                      */
/* ============================================ */
int Cthread_environment() {
  return(1);
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
int _Cthread_self() {
  void               *tsd = NULL;       /* Thread-Specific Variable */
  int                   n;              /* Return value     */

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

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
  if ((n = pthread_once(&cid_once,&_Cthread_cid_once)) != 0) {
    errno = n;
    /* serrno = SECTHREADERR; */
    return(-1);
  }

  /* We look if there is already a Thread-Specific       */
  /* Variable.                                           */
  tsd = pthread_getspecific(cid_key);

  if (tsd == NULL) {

    /* We try to create the key-value */
    if ((tsd = (void *) malloc(sizeof(int))) == NULL) {
      /* serrno = SEINTERNAL; */
      return(-1);
    }

    /* We associate the key-value with the key   */
    if ((n = pthread_setspecific(cid_key, tsd))) {
      errno = n;
      /* serrno = SECTHREADERR; */
      return(-1);
    }
    
    /* And we don't yet know the cid... So we set it to -2 */

    * (int *) tsd = -2;
    
    return(-2);

  } else {

    /* Thread-Specific variable already exist ! */

    return((int) * (int *) tsd);

  }
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
void Cthread_unprotect() {
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
int Cthread_Kill(const char *file,
                 int line,
                 int cid,
                 int signo)
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

  if ((n = pthread_kill(current->pid,signo))) {
    errno = n;
    serrno = SECTHREADERR;
    return(-1);
  }
  return(0);
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
void Cthread_Exit(const char *file,
                  int line,
                  void *status)
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

  pthread_exit(status);
}

