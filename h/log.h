/*
 * Copyright (C) 1990-2001 by CERN CN-PDP/CS
 * All rights reserved
 */

/*
 * @(#)$RCSfile: log.h,v $ $Revision: 1.10 $ $Date: 2001/05/21 11:07:18 $ CERN CN-PDP/CS F. Hemmer
 */

/* log.h        generalized logging facilities                          */

#ifndef _LOG_H_INCLUDED_
#define _LOG_H_INCLUDED_

#define LOG_NOLOG       -1      /* Don't log                            */
#ifndef _SHIFT_H_INCLUDED_
#include <osdep.h>
#endif

EXTERN_C void DLL_DECL (*logfunc) _PROTO((int, char *, ...));  
                                /* logging function to use */
EXTERN_C void DLL_DECL initlog _PROTO((char *, int, char *));
EXTERN_C void DLL_DECL logit _PROTO((int, char *, ...));
EXTERN_C int DLL_DECL getloglv _PROTO((void));

#ifdef log
#undef log
#endif
#define log (*logfunc)          /* logging function name                */

#ifndef _SYSLOG_WIN32_H
#include <syslog.h>             /* system logger definitions            */
#endif
#define LOG_EMERG       0       /* system is unusable                   */
#define LOG_ALERT       1       /* action must be taken immediately     */
#define LOG_CRIT        2       /* critical conditions                  */
#define LOG_ERR         3       /* error conditions                     */
#define LOG_WARNING     4       /* warning conditions                   */
#define LOG_NOTICE      5       /* normal but signification condition   */
#define LOG_INFO        6       /* informational                        */
#define LOG_DEBUG       7       /* debug-level messages                 */

#endif /* _LOG_H_INCLUDED_ */
