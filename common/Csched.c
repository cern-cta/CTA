/*
 * $Id: Csched.c,v 1.3 2007/12/06 14:24:47 sponcec3 Exp $
 */

/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <Csched_api.h>
#include <Cthread_api.h>
#include <Cthread_typedef.h>
#include <Cglobals.h>
#include <serrno.h>
#include <errno.h>
#include <osdep.h>

/* ------------------------------------ */
/* For the Cthread_self() command       */
/* (Thread-Specific Variable)           */
/* ------------------------------------ */
EXTERN_C pthread_key_t cid_key;
EXTERN_C pthread_once_t cid_once;

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
EXTERN_C struct Cmtx_element_t  Cmtx;
EXTERN_C struct Cthread_protect_t Cthread;
EXTERN_C struct Cspec_element_t Cspec;
EXTERN_C pthread_once_t         once;
EXTERN_C int _Cthread_unprotect;

EXTERN_C int _Cthread_once_status;
EXTERN_C int _Cthread_obtain_mtx (const char *, int, pthread_mutex_t *, int);
EXTERN_C int _Cthread_init (void);
EXTERN_C int _Cthread_release_mtx (char *, int, pthread_mutex_t *);

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
int Csched_Getschedparam(char *file,
                         int line,
                         int cid,
                         int *policy,
                         Csched_param_t *Cparam)
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  int                   n;                /* Status           */

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

  if (policy == NULL || Cparam == NULL) {
    /* policy and param are required to be non-NULL */
    serrno = EINVAL;
    return(-1);
  }
  {
    struct sched_param param;

    if ((n = pthread_getschedparam(current->pid, policy, &param)) != 0) {
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    }

    Cparam->sched_priority = param.sched_priority;
  }
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
int Csched_Setschedparam(char *file,
                         int line,
                         int cid,
                         int policy,
                         Csched_param_t *Cparam)
{
  struct Cid_element_t *current = &Cid;   /* Curr Cid_element */
  int                   n;                /* Status           */

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
  {
    struct sched_param param;

    param.sched_priority = Cparam->sched_priority;

    if ((n = pthread_setschedparam(current->pid, policy, &param)) != 0) {
      errno = n;
      serrno = SECTHREADERR;
      return(-1);
    }
  }
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
int Csched_Get_priority_min(char *file,
                            int line,
                            int policy)
{
  (void) file;
  (void) line;
  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  if (policy == CSCHED_UNKNOWN) {
    /* Known to be unsupported */
    serrno = SEOPNOTSUP;
    return(-1);
  }
  {
    int min;

    if ((min = sched_get_priority_min(policy)) == -1) {
      errno = min;
      serrno = SECTHREADERR;
      return(-1);
    }

    return(min);
  }
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
int Csched_Get_priority_max(char *file,
                            int line,
                            int policy)
{
  (void) file;
  (void) line;
  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  if (policy == CSCHED_UNKNOWN) {
    /* Known to be unsupported */
    serrno = SEOPNOTSUP;
    return(-1);
  }
  {
    int max;

    if ((max = sched_get_priority_max(policy)) == -1) {
      errno = max;
      serrno = SECTHREADERR;
      return(-1);
    }

    return(max);
  }
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
int Csched_Get_priority_mid(char *file,
                            int line,
                            int policy)
{
  (void) file;
  (void) line;
  /* Make sure initialization is/was done */
  if ( _Cthread_once_status && _Cthread_init() ) return(-1);

  if (policy == CSCHED_UNKNOWN) {
    /* Known to be unsupported */
    serrno = SEOPNOTSUP;
    return(-1);
  }
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
}
