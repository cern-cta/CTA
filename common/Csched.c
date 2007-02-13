/*
 * $Id: Csched.c,v 1.2 2007/02/13 07:52:24 waldron Exp $
 */

/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Csched.c,v $ $Revision: 1.2 $ $Date: 2007/02/13 07:52:24 $ CERN IT-PDP/DM Olof Barring, Jean-Damien Durand";
#endif /* not lint */

#include <Csched_api.h>
#include <Cthread_api.h>
#include <Cthread_typedef.h>
#include <Cglobals.h>
#include <serrno.h>
#include <errno.h>
#include <osdep.h>

#ifdef DEBUG
#ifndef CTHREAD_DEBUG
#define CTHREAD_DEBUG
#endif
#endif

#ifdef CTHREAD_DEBUG
#include <log.h>
#endif

/* ------------------------------------ */
/* For the Cthread_self() command       */
/* (Thread-Specific Variable)           */
/* ------------------------------------ */
#ifdef _CTHREAD
EXTERN_C Cth_spec_t cid_key;
#else
EXTERN_C int cid_key;
#endif
EXTERN_C Cth_once_t cid_once;

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
EXTERN_C struct Cid_element_t   Cid;
#ifdef _CTHREAD
EXTERN_C struct Cmtx_element_t  Cmtx;
#endif
EXTERN_C struct Cthread_protect_t Cthread;
EXTERN_C struct Cspec_element_t Cspec;
EXTERN_C Cth_once_t             once;
EXTERN_C int _Cthread_unprotect;

EXTERN_C int Cthread_debug;
EXTERN_C int _Cthread_once_status;

#ifdef CTHREAD_DEBUG
#define _Cthread_obtain_mtx(a,b,c,d) _Cthread_obtain_mtx_debug(__FILE__,__LINE__,a,b,c,d)
EXTERN_C int _Cthread_obtain_mtx_debug _PROTO((char *, int, char *, int, Cth_mtx_t *, int));
#endif
EXTERN_C int _Cthread_init _PROTO((void));
EXTERN_C int _Cthread_release_mtx _PROTO((char *, int, Cth_mtx_t *));

/* =============================================== */
/* Routine  : Csched_Getschedparam                 */
/* Arguments: Caller filename                      */
/*            Caller line number                   */
/*            CThread ID                           */
/*            Policy (output)                      */
/*            Parameters (output)                  */
/* ----------------------------------------------- */
/* Output   : 0 if (OK), -1 if not OK              */
/*            Policy, Parameters                   */
/* ----------------------------------------------- */
/* History:                                        */
/* 12-JUN-2000       First implementation          */
/*                   Jean-Damien.Durand@cern.ch    */
/* =============================================== */
/* Notes:                                          */
/* =============================================== */
int DLL_DECL Csched_Getschedparam(file,line,cid,policy,Cparam)
     char *file;
     int line;
     int cid;
     int *policy;
     Csched_param_t *Cparam;
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  int                   n;                /* Status           */

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_Getschedparam(%d,0x%lx,0x%lx) called at/behind %s:%d\n",
          _Cthread_self(),cid,(unsigned long) policy,(unsigned long) Cparam,file, line);
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
  /* Cannot be supported - wrong environment */
  serrno = SEOPNOTSUP;
  return(-1);
#else
  if (policy == NULL || Cparam == NULL) {
    /* policy and param are required to be non-NULL */
    serrno = EINVAL;
    return(-1);
  }
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  /* Not yet supported on _WIN32 */
  serrno = SEOPNOTSUP;
  return(-1);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  {
    struct sched_param param;

    if ((n = pthread_getschedparam(current->pid, policy, &param)) != 0) {
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    }

    Cparam->sched_priority = param.sched_priority;
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if ((n = pthread_getscheduler(current->pid)) < 0) {
    serrno = SECTHREADERR;
    return(-1);
  }
  *policy = n;
  if ((n = pthread_getprio(current->pid)) < 0) {
    serrno = SECTHREADERR;
    return(-1);
  }
  Cparam->sched_priority = n;
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  {
    struct sched_param param;

    if ((n = pthread_getschedparam(current->pid, policy, &param)) != 0) {
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    }

    Cparam->sched_priority = param.sched_priority;
  }
#endif /* _CTHREAD_PROTO */
#endif /* _NOCTHREAD */
  /* OK */
  return(0);
}

/* =============================================== */
/* Routine  : Csched_Setschedparam                 */
/* Arguments: Caller filename                      */
/*            Caller line number                   */
/*            CThread ID                           */
/*            Policy                               */
/*            Parameters                           */
/* ----------------------------------------------- */
/* Output   : 0 if (OK), -1 if not OK              */
/* ----------------------------------------------- */
/* History:                                        */
/* 12-JUN-2000       First implementation          */
/*                   Jean-Damien.Durand@cern.ch    */
/* =============================================== */
/* Notes:                                          */
/* =============================================== */
int DLL_DECL Csched_Setschedparam(file,line,cid,policy,Cparam)
     char *file;
     int line;
     int cid;
     int policy;
     Csched_param_t *Cparam;
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  int                   n;                /* Status           */

#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_Setschedparam(%d,%d,0x%lx) called at/behind %s:%d\n",
          _Cthread_self(),cid,policy,(unsigned long) Cparam,file, line);
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
  /* Cannot be supported - wrong environment */
  serrno = SEOPNOTSUP;
  return(-1);
#else
  if (policy == CSCHED_UNKNOWN) {
    /* Known to be unsupported */
    serrno = SEOPNOTSUP;
    return(-1);
  }
  if (Cparam == NULL) {
    /* param is required to be non-NULL */
    serrno = EINVAL;
    return(-1);
  }
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  /* Not yet supported on _WIN32 */
  serrno = SEOPNOTSUP;
  return(-1);
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_POSIX
  {
    struct sched_param param;

    param.sched_priority = Cparam->sched_priority;

    if ((n = pthread_setschedparam(current->pid, policy, &param)) != 0) {
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    }
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_DCE
  if ((n = pthread_setscheduler(current->pid, policy, Cparam->sched_priority)) < 0) {
    serrno = SECTHREADERR;
    return(-1);
  }
#elif _CTHREAD_PROTO == _CTHREAD_PROTO_LINUX
  {
    struct sched_param param;

    param.sched_priority = Cparam->sched_priority;

    if ((n = pthread_setschedparam(current->pid, policy, &param)) != 0) {
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    }
  }
#endif /* _CTHREAD_PROTO */
#endif /* _NOCTHREAD */
  /* OK */
  return(0);
}

/* =============================================== */
/* Routine  : Csched_Get_priority_min              */
/* Arguments: Caller filename                      */
/*            Caller line number                   */
/*            Policy                               */
/* ----------------------------------------------- */
/* Output   : 0 if (OK), -1 if not OK              */
/* ----------------------------------------------- */
/* History:                                        */
/* 12-JUN-2000       First implementation          */
/*                   Jean-Damien.Durand@cern.ch    */
/* =============================================== */
/* Notes:                                          */
/* =============================================== */
int DLL_DECL Csched_Get_priority_min(file,line,policy)
     char *file;
     int line;
     int policy;
{
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_Get_priority_min(%d) called at/behind %s:%d\n",
          _Cthread_self(),policy,file,line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

#ifdef _NOCTHREAD
  /* Cannot be supported - wrong environment */
  serrno = SEOPNOTSUP;
  return(-1);
#else
  if (policy == CSCHED_UNKNOWN) {
    /* Known to be unsupported */
    serrno = SEOPNOTSUP;
    return(-1);
  }
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  /* Not yet supported on _WIN32 */
  serrno = SEOPNOTSUP;
  return(-1);
#else
  {
    int min;

    if ((min = sched_get_priority_min(policy)) == -1) {
      errno = min;
      serrno = SECTHREADERR;
      return(-1);
    }

    return(min);
  }
#endif /* _CTHREAD_PROTO */
#endif /* _NOCTHREAD */
}

/* =============================================== */
/* Routine  : Csched_Get_priority_max              */
/* Arguments: Caller filename                      */
/*            Caller line number                   */
/*            Policy                               */
/* ----------------------------------------------- */
/* Output   : 0 if (OK), -1 if not OK              */
/* ----------------------------------------------- */
/* History:                                        */
/* 12-JUN-2000       First implementation          */
/*                   Jean-Damien.Durand@cern.ch    */
/* =============================================== */
/* Notes:                                          */
/* =============================================== */
int DLL_DECL Csched_Get_priority_max(file,line,policy)
     char *file;
     int line;
     int policy;
{
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_Get_priority_max(%d) called at/behind %s:%d\n",
          _Cthread_self(),policy,file,line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

#ifdef _NOCTHREAD
  /* Cannot be supported - wrong environment */
  serrno = SEOPNOTSUP;
  return(-1);
#else
  if (policy == CSCHED_UNKNOWN) {
    /* Known to be unsupported */
    serrno = SEOPNOTSUP;
    return(-1);
  }
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  /* Not yet supported on _WIN32 */
  serrno = SEOPNOTSUP;
  return(-1);
#else
  {
    int max;

    if ((max = sched_get_priority_max(policy)) == -1) {
      errno = max;
      serrno = SECTHREADERR;
      return(-1);
    }

    return(max);
  }
#endif /* _CTHREAD_PROTO */
#endif /* _NOCTHREAD */
}

/* =============================================== */
/* Routine  : Csched_Get_priority_mid              */
/* Arguments: Caller filename                      */
/*            Caller line number                   */
/*            Policy                               */
/* ----------------------------------------------- */
/* Output   : 0 if (OK), -1 if not OK              */
/* ----------------------------------------------- */
/* History:                                        */
/* 12-JUN-2000       First implementation          */
/*                   Jean-Damien.Durand@cern.ch    */
/* =============================================== */
/* Notes:                                          */
/* =============================================== */
int DLL_DECL Csched_Get_priority_mid(file,line,policy)
     char *file;
     int line;
     int policy;
{
#ifdef CTHREAD_DEBUG
  if (file != NULL) {
    if (Cthread_debug != 0)
      log(LOG_INFO,"[Cthread    [%2d]] In _Cthread_Get_priority_mid(%d) called at/behind %s:%d\n",
          _Cthread_self(),policy,file,line);
  }
#endif

  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

#ifdef _NOCTHREAD
  /* Cannot be supported - wrong environment */
  serrno = SEOPNOTSUP;
  return(-1);
#else
  if (policy == CSCHED_UNKNOWN) {
    /* Known to be unsupported */
    serrno = SEOPNOTSUP;
    return(-1);
  }
#if _CTHREAD_PROTO ==  _CTHREAD_PROTO_WIN32
  /* Not yet supported on _WIN32 */
  serrno = SEOPNOTSUP;
  return(-1);
#else
  {
    int min, max;

    if ((min = Csched_get_priority_min(policy)) < 0 ||
        (max = Csched_get_priority_max(policy)) < 0) {
      return(-1);
    }

    if ((min % 2) != (max % 2)) {
      /* One is even and one is odd : exact medium does not exist */
      return((max - min + 1) / 2);
    } else {
      /* Both are even or both are odd */
      return((max - min) / 2);
    }
  }
#endif /* _CTHREAD_PROTO */
#endif /* _NOCTHREAD */
}
