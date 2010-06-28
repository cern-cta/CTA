/*
 * $Id: Cthread_flags.h,v 1.8 2002/09/30 16:28:27 jdurand Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef __Cthread_flags_h
#define __Cthread_flags_h

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

/* ----------------------------------------------- */
/* Analyse of compilation flags                    */
/* ----------------------------------------------- */

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
#include <pthread.h>
#else  /* ifdef _CTHREAD */
#include <unistd.h>           /* getpid fork..  */
#include <sys/types.h>        /* waitpid        */
#include <sys/wait.h>         /* waitpid        */
#include <signal.h>           /* signal         */
#endif /* ifdef _CTHREAD */
#include <stdlib.h>           /* malloc, etc... */
#include <unistd.h>           /* fork usleep... */
#include <sys/time.h>         /* gettimeofday   */
#include <errno.h>            /* errno ...      */
#include <string.h>           /* strerror ...   */

#endif /* __Cthread_flags_h */











