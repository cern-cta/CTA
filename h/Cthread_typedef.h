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
 * _CTHREAD_WIN32MTX if perf. is low on
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
  void *(*_thread_routine) _PROTO((void *));
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
  void   *(*addr) _PROTO((void *));  /* Start-up         */
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

#endif /* __Cthread_typedef_h */
