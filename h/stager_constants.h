/*
 * $Id: stager_constants.h,v 1.3 2004/11/08 09:38:26 jdurand Exp $
 */

#ifndef __stager_constants_h
#define __stager_constants_h

#include "osdep.h"

#define STAGER_DEFAULT_TIMEOUT      20                   /* Default send/recv timeout */
#define STAGER_DEFAULT_SECURE_PORT 5515                  /* Default secure port number */
#define STAGER_DEFAULT_PORT        5015                  /* Default port number */
#define STAGER_DEFAULT_NOTIFY_PORT 55015                 /* Default notify port number */
#define STAGER_DEFAULT_SECURE        0                   /* Default secure mode - to be 1 in the future */
#define STAGER_DEFAULT_DEBUG         0                   /* Default debug mode (!= 0 means yes) */
#define STAGER_DEFAULT_TRACE         0                   /* Default trace mode (!= 0 means yes) */
#define STAGER_DEFAULT_DB_NBTHREAD   5                   /* Default number of db threads */
#define STAGER_DEFAULT_QUERY_NBTHREAD 5                  /* Default number of query threads */
#define STAGER_DEFAULT_UPDATE_NBTHREAD 5                 /* Default number of update threads */
#define STAGER_DEFAULT_GETNEXT_NBTHREAD 5                /* Default number of getnext threads */
#define STAGER_DEFAULT_FACILITY   "stager"               /* Default Facility name */
#define STAGER_DEFAULT_IGNORECOMMANDLINE   0             /* Default ignore-comamnd-line mode (!= 0 means yes) */
#define STAGER_DEFAULT_HOST       "localhost"            /* Default stager host */

#define STAGER_CLASS              "STAGER"               /* Label in config file (1st column) */

#define STAGER_CLASS_TIMEOUT      "TIMEOUT"              /* Sub-label in config file for timeout */
#define STAGER_CLASS_SECURE_PORT  "SPORT"                /* Sub-label in config file for secure port */
#define STAGER_CLASS_SECURE       "SECURE"               /* Sub-label in config file for secure mode */
#define STAGER_CLASS_NOTIFY_PORT  "NOTIFY_PORT"          /* Sub-label in config file for notification port */
#define STAGER_CLASS_PORT         "PORT"                 /* Sub-label in config file for port */
#define STAGER_CLASS_DEBUG        "DEBUG"                /* Sub-label in config file for debug */
#define STAGER_CLASS_TRACE        "TRACE"                /* Sub-label in config file for trace */
#define STAGER_CLASS_DB_NBTHREAD  "DB_NBTHREAD"          /* Sub-label in config file for db_nbthread */
#define STAGER_CLASS_QUERY_NBTHREAD "QUERY_NBTHREAD"     /* Sub-label in config file for query_nbthread */
#define STAGER_CLASS_UPDATE_NBTHREAD "UPDATE_NBTHREAD"   /* Sub-label in config file for update_nbthread */
#define STAGER_CLASS_GETNEXT_NBTHREAD "GETNEXT_NBTHREAD" /* Sub-label in config file for getnext_nbthread */
#define STAGER_CLASS_FACILITY     "FACILITY"             /* Sub-label in config file for facility */
#define STAGER_CLASS_IGNORECOMMANDLINE  "IGNORECOMMANDLINE" /* Sub-label in config file for ignore-command-line */
#define STAGER_CLASS_HOST         "HOST"                 /* Sub-label in config file for host */

#define STAGER_ENV_TIMEOUT        "STAGER_TIMEOUT"       /* Environment variable for timeout */
#define STAGER_ENV_SECURE_PORT    "STAGER_SPORT"         /* Environment variable for secure port */
#define STAGER_ENV_SECURE         "STAGER_SECURE"        /* Environment variable for secure stager */
#define STAGER_ENV_NOTIFY_PORT    "STAGER_NOTIFY_PORT"   /* Environment variable for notification port */
#define STAGER_ENV_PORT           "STAGER_PORT"          /* Environment variable for port */
#define STAGER_ENV_DEBUG          "STAGER_DEBUG"         /* Environment variable for debug */
#define STAGER_ENV_TRACE          "STAGER_TRACE"         /* Environment variable for trace */
#define STAGER_ENV_DB_NBTHREAD    "STAGER_DB_NBTHREAD"   /* Environment variable for db_nbthread */
#define STAGER_ENV_QUERY_NBTHREAD "STAGER_QUERY_NBTHREAD" /* Environment variable for query_nbthread */
#define STAGER_ENV_UPDATE_NBTHREAD "STAGER_UPDATE_NBTHREAD" /* Environment variable for update_nbthread */
#define STAGER_ENV_GETNEXT_NBTHREAD "STAGER_GETNEXT_NBTHREAD" /* Environment variable for getnext_nbthread */
#define STAGER_ENV_FACILITY       "STAGER_FACILITY"      /* Environment variable for facility */
#define STAGER_ENV_IGNORECOMMANDLINE "STAGER_IGNORECOMMANDLINE" /* Environment variable for ignore-command-line */
#define STAGER_ENV_HOST           "STAGER_HOST"          /* Environment variable for host */

#define STAGER_MUTEX_TIMEOUT      10                     /* Timeout on getting a mutex */
#define STAGER_COND_TIMEOUT       1                      /* Timeout on waiting on a condition variable */
#define STAGER_NOTIFICIATION_TIMEOUT   10                /* Timeout on waiting on a notifiy condition variable */
#define STAGER_MUTEX_EMERGENCYTIMEOUT      60            /* Timeout on getting a mutex in the signal handler */
#define STAGER_REPBUFSZ           1024                   /* Socket reply buffer size */
#define STAGER_MAGIC              0x24140701             /* Stager magic number */
#define STAGER_NOTIFY_MAGIC       0x34140701             /* Stager notification magic number */

#define STAGER_RC                 0                      /* Stager RC type */
#define STAGER_MSG_ERR            1                      /* Stager MSG_ERR type */
#define STAGER_MSG_OUT            2                      /* Stager MSG_OUT type */

#define STAGER_SERVICE_NAME        "stager"              /* Name in /etc/services if any */
#define STAGER_SECURE_SERVICE_NAME "sstager"             /* Name force sure service in /etc/services if any */
#define STAGER_SERVICE_PROTO       "tcp"                 /* Proto in /etc/services if any */
#define STAGER_SERVICE_NOTIFY_PROTO "udp"                /* Notify Proto in /etc/services if any */

#define STAGER_OPTION_ECHO     CONSTLL(0x00000000000001) /* --echo */
#define STAGER_OPTION_START    CONSTLL(0x00000000000002) /* --start */

#define STAGER_PRTBUFSZ           4096                   /* Buffer size in the client for printout */

#define STAGER_AUTOCOMMIT_TRUE    1                      /* Util macro for catalog interface : autocommit to 1 (== yes) */
#define STAGER_AUTOCOMMIT_FALSE   0                      /* Util macro for catalog interface : autocommit to 0 (== no)  */

#endif /* __stager_constants_h */
