/*
 * $Id: stagerConstants.h,v 1.3 2004/10/22 20:09:47 jdurand Exp $
 */

#ifndef __stagerConstants_h
#define __stagerConstants_h

#define STAGER_DEFAULT_TIMEOUT      20                   /* Default send/recv timeout */
#define STAGER_DEFAULT_SECURE_PORT 5515                  /* Default secure port number */
#define STAGER_DEFAULT_PORT        5015                  /* Default port number */
#define STAGER_DEFAULT_DEBUG         0                   /* Default debug mode (!= 0 means yes) */
#define STAGER_DEFAULT_TRACE         0                   /* Default trace mode (!= 0 means yes) */
#define STAGER_DEFAULT_DB_NBTHREAD   5                   /* Default number of db threads */
#define STAGER_DEFAULT_USER_NBTHREAD 5                   /* Default number of user threads */
#define STAGER_DEFAULT_FACILITY   "stager"               /* Default Facility name */
#define STAGER_DEFAULT_IGNORECOMMANDLINE   0             /* Default ignore-comamnd-line mode (!= 0 means yes) */

#define STAGER_CLASS              "STAGER"               /* Label in config file (1st column) */

#define STAGER_CLASS_TIMEOUT      "TIMEOUT"              /* Sub-label in config file for timeout */
#define STAGER_CLASS_SECURE_PORT  "SPORT"                /* Sub-label in config file for secure port */
#define STAGER_CLASS_PORT         "PORT"                 /* Sub-label in config file for port */
#define STAGER_CLASS_DEBUG        "DEBUG"                /* Sub-label in config file for debug */
#define STAGER_CLASS_TRACE        "TRACE"                /* Sub-label in config file for trace */
#define STAGER_CLASS_DB_NBTHREAD  "DB_NBTHREAD"          /* Sub-label in config file for db_nbthread */
#define STAGER_CLASS_USER_NBTHREAD "USER_NBTHREAD"       /* Sub-label in config file for user_nbthread */
#define STAGER_CLASS_FACILITY     "FACILITY"             /* Sub-label in config file for facility */
#define STAGER_CLASS_IGNORECOMMANDLINE  "IGNORECOMMANDLINE" /* Sub-label in config file for ignore-command-line */

#define STAGER_ENV_TIMEOUT        "STAGER_TIMEOUT"       /* Environment variable for timeout */
#define STAGER_ENV_SECURE_PORT    "STAGER_SPORT"         /* Environment variable for secure port */
#define STAGER_ENV_PORT           "STAGER_PORT"          /* Environment variable for port */
#define STAGER_ENV_DEBUG          "STAGER_DEBUG"         /* Environment variable for debug */
#define STAGER_ENV_TRACE          "STAGER_TRACE"         /* Environment variable for trace */
#define STAGER_ENV_DB_NBTHREAD    "STAGER_DB_NBTHREAD"   /* Environment variable for db_nbthread */
#define STAGER_ENV_USER_NBTHREAD  "STAGER_USER_NBTHREAD" /* Environment variable for user_nbthread */
#define STAGER_ENV_FACILITY       "STAGER_FACILITY"      /* Environment variable for facility */
#define STAGER_ENV_IGNORECOMMANDLINE "STAGER_IGNORECOMMANDLINE" /* Environment variable for ignore-command-line */

#define STAGER_MUTEX_TIMEOUT      10                     /* Timeout on getting a mutex */
#define STAGER_COND_TIMEOUT       1                      /* Timeout on waiting on a condition variable */
#define STAGER_MUTEX_EMERGENCYTIMEOUT      60            /* Timeout on getting a mutex in the signal handler */

#endif /* __stagerConstants_h */
