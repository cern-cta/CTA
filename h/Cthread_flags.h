/*
 * $Id: Cthread_flags.h,v 1.2 1999/07/26 15:18:25 jdurand Exp $
 *
 * $Log: Cthread_flags.h,v $
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

/* Analyse                                      */
/* -------------------------------------------- */
/* Windows : Force to always use threads        */
/* -------------------------------------------- */
#ifdef _WIN32
#ifdef NOCTHREAD
#undef NOCTHREAD
#endif
#ifndef CTHREAD
#define CTHREAD
#endif
#endif /* ifdef _WIN32 */

/* -------------------------------------------- */
/* Unix-like:                                   */
/* NOCTHREAD : Clearly the user do NOT     */
/* want threads                                 */
/* -------------------------------------------- */
#ifdef NOCTHREAD
#define _NOCTHREAD
#else

/* -------------------------------------------- */
/* No CTHREAD : default is NOT to use      */
/* threads unless _REENTRANT is defined         */
/* -------------------------------------------- */
#ifndef CTHREAD
#ifdef _REENTRANT
#define _CTHREAD
#else
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
/* We are really in a threaded environment */
#ifdef CTHREAD_TRUE_THREAD
#undef CTHREAD_TRUE_THREAD
#endif
#define CTHREAD_TRUE_THREAD 1
/* We are NOT in a threaded environment */
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

#endif /* __cthread_flags_h */











