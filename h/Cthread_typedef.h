/*
 * $Id: Cthread_typedef.h,v 1.2 2002/09/30 16:28:27 jdurand Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef __Cthread_typedef_h
#define __Cthread_typedef_h

#include <osdep.h>

/* -------------------------------------------- */
/* Initialization:                              */
/* Let's analyse the compile flags              */
/* -------------------------------------------- */
#include <Cthread_flags.h>
#include <Cglobals.h>

/* ============================================ */
/* Typedefs                                     */
/* ============================================ */
/* -------------------------------------------- */
/* Uniform definition of a thread/process ID    */
/* -------------------------------------------- */
typedef pthread_t Cth_pid_t;
/* -------------------------------------------- */
/* Uniform definition of a lock variable        */
/* -------------------------------------------- */
/* This is a pthread mutex */
typedef pthread_mutex_t Cth_mtx_t;

/* -------------------------------------------- */
/* Uniform definition of a cond variable        */
/* -------------------------------------------- */
typedef pthread_cond_t Cth_cond_t;

/* -------------------------------------------- */
/* Uniform definition of a specific variable    */
/* -------------------------------------------- */
typedef pthread_key_t Cth_spec_t;

/* -------------------------------------------- */
/* Thread starter                               */
/* -------------------------------------------- */
typedef struct Cthread_start_params {
  void *(*_thread_routine) (void *);
  void *_thread_arg;
  int   detached;
} Cthread_start_params_t;

typedef pthread_once_t Cth_once_t;
 
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
  void   *(*addr) (void *);  /* Start-up         */
  int       detached;       /* Is it detached ? */
  int       joined;         /* Has it been joined? */
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

#endif /* __Cthread_typedef_h */
