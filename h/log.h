/*
 * Copyright (C) 1990-2000 by CERN CN-PDP/CS
 * All rights reserved
 */

/*
 * @(#)$RCSfile: log.h,v $ $Revision: 1.7 $ $Date: 2000/06/13 12:58:55 $ CERN CN-PDP/CS F. Hemmer
 */

/* log.h        generalized logging facilities                          */

#ifndef _LOG_H_INCLUDED_
#define _LOG_H_INCLUDED_

#define LOG_NOLOG       -1      /* Don't log                            */
#include <osdep.h>

EXTERN_C void DLL_DECL (*logfunc) _PROTO((int, ...));  
                                /* logging function to use */
EXTERN_C void DLL_DECL initlog _PROTO((char *, int, char *));
#if defined(hpux)
EXTERN_C void DLL_DECL logit _PROTO(());
#else
EXTERN_C void DLL_DECL logit _PROTO((int, ...));
#endif
EXTERN_C int DLL_DECL getloglv _PROTO((void));

#ifdef log
#undef log
#endif
#define log (*logfunc)          /* logging function name                */

#include <syslog.h>             /* system logger definitions            */
#define LOG_EMERG       0       /* system is unusable                   */
#define LOG_ALERT       1       /* action must be taken immediately     */
#define LOG_CRIT        2       /* critical conditions                  */
#define LOG_ERR         3       /* error conditions                     */
#define LOG_WARNING     4       /* warning conditions                   */
#define LOG_NOTICE      5       /* normal but signification condition   */
#define LOG_INFO        6       /* informational                        */
#define LOG_DEBUG       7       /* debug-level messages                 */

#endif /* _LOG_H_INCLUDED_ */
