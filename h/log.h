/*
 * Copyright (C) 1990-1999 by CERN CN-PDP/CS
 * All rights reserved
 */

/*
 * @(#)$RCSfile: log.h,v $ $Revision: 1.3 $ $Date: 1999/10/15 06:35:23 $ CERN CN-PDP/CS F. Hemmer
 */

/* log.h        generalized logging facilities                          */

#ifndef _LOG_H_INCLUDED_
#define _LOG_H_INCLUDED_

#define LOG_NOLOG       -1      /* Don't log                            */

extern  void (*logfunc)();      /* logging function to use              */

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
