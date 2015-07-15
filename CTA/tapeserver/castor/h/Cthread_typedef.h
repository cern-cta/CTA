/*
 * $Id: Cthread_typedef.h,v 1.2 2002/09/30 16:28:27 jdurand Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#pragma once

#include <osdep.h>

/* -------------------------------------------- */
/* Initialization:                              */
/* Let's analyse the compile flags              */
/* -------------------------------------------- */
#include <Cthread_flags.h>
#include <Cglobals.h>

/* -------------------------------------------- */
/* Thread starter                               */
/* -------------------------------------------- */
typedef struct Cthread_start_params {
  void *(*_thread_routine) (void *);
  void *_thread_arg;
  int   detached;
} Cthread_start_params_t;
 
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
  pthread_t pid;            /* Thread/Proc. ID  */
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
  pthread_mutex_t            mtx; /* Associated mutex */
  pthread_cond_t           cond; /* Associate cond  */
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
  pthread_key_t          key;  /* Key              */
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
  pthread_mutex_t            mtx; /* Associated mutex */
};

