/*
 * $Id: Cthread_flags.h,v 1.6 2000/06/13 14:57:15 jdurand Exp $
 *
 * $Log: Cthread_flags.h,v $
 * Revision 1.6  2000/06/13 14:57:15  jdurand
 * CThread Scheduling constants
 *
 * Revision 1.5  2000/01/10 10:21:06  jdurand
 * Makes sure CTHREAD and NOCTHREAD are defined in conjunction with
 * _CTHREAD and _NOCTHREAD
 *
 * Revision 1.4  2000-01-10 11:11:21+01  jdurand
 * Force CTHREAD if (defined(_REENTRANT) || defined(_THREAD_SAFE)) on UNIX
 * (Previous test was on _REENTRANT only)
 *
 * Revision 1.3  1999-10-20 21:09:52+02  jdurand
 * Introduced a _CTHREAD_WIN32MTX sequence following imake's rule UseMtx.
 * If not present, default is Win32 CriticalSection way, then Cthread_flags.h
 * makes sure that _WIN32_WINNT version is >= 0x0400
 *
 * Revision 1.2  1999/07/26 15:18:25  jdurand
 * Some Raima interface
 *
 */

#ifndef __cthread_flags_h
#define __cthread_flags_h

/* -------------------------------------------- */
/* ... Flag to use threads                      */
/* -------------------------------------------- */
#ifdef _CTHREAD
#undef _CTHREAD
#endif

/* -------------------------------------------- */
/* ... Flag not to use threads                  */
/* -------------------------------------------- */
#ifdef _NOCTHREAD
#undef _NOCTHREAD
#endif

/* -------------------------------------------- */
/* ... Flag for thread prototypes               */
/* -------------------------------------------- */
#ifdef _CTHREAD_PROTO
#undef _CTHREAD_PROTO
#endif

#ifdef _CTHREAD_PROTO_POSIX
#undef _CTHREAD_PROTO_POSIX
#endif
#define _CTHREAD_PROTO_POSIX 1

#ifdef _CTHREAD_PROTO_DCE
#undef _CTHREAD_PROTO_DCE
#endif
#define _CTHREAD_PROTO_DCE 2

#ifdef _CTHREAD_PROTO_LINUX
#undef _CTHREAD_PROTO_LINUX
#endif
#define _CTHREAD_PROTO_LINUX 3

#ifdef _CTHREAD_PROTO_WIN32
#undef _CTHREAD_PROTO_WIN32
#endif
#define _CTHREAD_PROTO_WIN32 4

/* ----------------------------------------------- */
/* Analyse of compilation flags                    */
/* ----------------------------------------------- */

/* ----------------------------------------------- */
/* Windows : Force to always use threads on _WIN32 */
/* ----------------------------------------------- */
#ifdef _WIN32
#ifdef NOCTHREAD
#undef NOCTHREAD
#endif
#ifndef CTHREAD
#define CTHREAD
#endif
/* Makes sure that Windows/NT version is correct */
#ifndef _CTHREAD_WIN32MTX
#ifdef _WIN32_WINNT
#undef _WIN32_WINNT
#endif
#define _WIN32_WINNT 0x0400
#endif /* _CTHREAD_WIN32MTX */
#endif /* ifdef _WIN32 */

/* -------------------------------------------- */
/* Unix-like:                                   */
/* NOCTHREAD : Clearly the user do NOT          */
/* want threads                                 */
/* -------------------------------------------- */
#ifdef NOCTHREAD
#define _NOCTHREAD
#else

/* -------------------------------------------- */
/* CTHREAD undef : default is NOT to use        */
/* threads unless _REENTRANT or _THREAD_SAFE    */
/* is defined (UNIX)                            */
/* -------------------------------------------- */
#ifndef CTHREAD
#if (defined(_REENTRANT) || defined(_THREAD_SAFE))
#define CTHREAD
#define _CTHREAD
#else
#define NOCTHREAD
#define _NOCTHREAD
#endif
#else
#define _CTHREAD
#endif /* ifndef CTHREAD */
#endif /* ifdef NOCTHREAD */

/* -------------------------------------------- */
/* Let's determine the thread prototypes to     */
/* to use                                       */
/* For Windows we current don't have any choice */
/* -------------------------------------------- */
#ifdef _CTHREAD
#if _WIN32
#define _CTHREAD_PROTO _CTHREAD_PROTO_WIN32
#else
#ifdef CTHREAD_POSIX
#define _CTHREAD_PROTO _CTHREAD_PROTO_POSIX
#else
#ifdef CTHREAD_DCE
#define _CTHREAD_PROTO _CTHREAD_PROTO_DCE
#else
#ifdef CTHREAD_LINUX
#define _CTHREAD_PROTO _CTHREAD_PROTO_LINUX
#else
#define _CTHREAD_PROTO _CTHREAD_PROTO_POSIX
#endif /* ifdef CTHREAD_LINUX */
#endif /* ifdef CTHREAD_DCE */
#endif /* ifdef CTHREAD_POSIX */
#endif /* ifdef _WIN32 */
#endif /* ifdef CTHREAD */

/* -------------------------------------------- */
/* Flags to extern user who whishes to know the */
/* environment.                                 */
/* -------------------------------------------- */
/* This is really in a threaded environment */
#ifdef CTHREAD_TRUE_THREAD
#undef CTHREAD_TRUE_THREAD
#endif
#define CTHREAD_TRUE_THREAD 1
/* This is NOT a threaded environment */
#ifdef CTHREAD_MULTI_PROCESS
#undef CTHREAD_MULTI_PROCESS
#endif
#define CTHREAD_MULTI_PROCESS 2

/* ============================================ */
/* Includes                                     */
/* ============================================ */
#ifdef _CTHREAD
#ifdef _WIN32
#include <windows.h>
#include <winbase.h>
#include <process.h>
#else
#include <pthread.h>
#endif /* ifdfef _WIN32 */
#else  /* ifdef _CTHREAD */
#include <unistd.h>           /* getpid fork..  */
#include <sys/types.h>        /* waitpid        */
#include <sys/wait.h>         /* waitpid        */
#include <signal.h>           /* signal         */
#endif /* ifdef _CTHREAD */
#include <stdlib.h>           /* malloc, etc... */
#ifndef _WIN32
#include <unistd.h>           /* fork usleep... */
#include <sys/time.h>         /* gettimeofday   */
#endif /* ifndef _WIN32 */
#include <errno.h>            /* errno ...      */
#include <string.h>           /* strerror ...   */

/* ----------------------------------------------- */
/* Now that Thread Environment is loaded           */
/* (<pthread.h>) we can determine if at run        */
/* time we can support sort thread (very) specific */
/* functionnality.                                 */
/* ----------------------------------------------- */
/* - First In, First Out */
#ifdef CSCHED_FIFO
#undef CSCHED_FIFO
#endif
/* - Round Robin */
#ifdef CSCHED_RR
#undef CSCHED_RR
#endif
/* - Default (other) */
#ifdef CSCHED_OTHER
#undef CSCHED_OTHER
#endif
/* - Default (new primitive or non portable) */
#ifdef CSCHED_FG_NP
#undef CSCHED_FG_NP
#endif
/* - Background */
#ifdef CSCHED_BG_NP
#undef CSCHED_BG_NP
#endif
/* - Unknown */
#ifdef CSCHED_UNKNOWN
#undef CSCHED_UNKNOWN
#endif
#define CSCHED_UNKNOWN -1

#ifdef _CTHREAD
/* Thread Environment */
/* - First In, First Out */
#ifdef SCHED_FIFO
#define CSCHED_FIFO SCHED_FIFO
#else
#define CSCHED_FIFO CSCHED_UNKNOWN
#endif
/* - Round Robin */
#ifdef SCHED_RR
#define CSCHED_RR SCHED_RR
#else
#define CSCHED_RR CSCHED_UNKNOWN
#endif
/* - Default (other) */
#ifdef SCHED_OTHER
#define CSCHED_OTHER SCHED_OTHER
#else
#define CSCHED_OTHER CSCHED_UNKNOWN
#endif
/* - Default (new primitive or non portable) */
#ifdef SCHED_FG_NP
#define CSCHED_FG_NP SCHED_FG_NP
#else
#define CSCHED_FG_NP CSCHED_UNKNOWN
#endif
/* - Background */
#ifdef SCHED_BG_NP
#define CSCHED_BG_NP SCHED_BG_NP
#else
#define CSCHED_BG_NP CSCHED_UNKNOWN
#endif

#else /* _CTHREAD */

/* Not a Thread Environment */
/* - First In, First Out */
#define CSCHED_FIFO CSCHED_UNKNOWN
/* - Round Robin */
#define CSCHED_RR CSCHED_UNKNOWN
/* - Default (other) */
#define CSCHED_OTHER CSCHED_UNKNOWN
/* - Default (new primitive or non portable) */
#define CSCHED_FG_NP CSCHED_UNKNOWN
/* - Background */
#define CSCHED_BG_NP CSCHED_UNKNOWN

#endif /* _CTHREAD */

/* Force the standard definition of scheduling parameter structure      */
/* This also ensures that programs interfaced with Cthread will compile */
/* on non-MT environments                                               */
struct Csched_param {
  int sched_priority;
};
typedef struct Csched_param Csched_param_t;

#endif /* __cthread_flags_h */











