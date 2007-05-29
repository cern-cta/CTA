/******************************************************************************************************
 *                                                                                                    *
 * lib.h - Castor Distribution Logging Facility                                                       *
 * Copyright (C) 2006 CERN IT/FIO (castor-dev@cern.ch)                                                *
 *                                                                                                    *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU *
 * General Public License as published by the Free Software Foundation; either version 2 of the       *
 * License, or (at your option) any later version.                                                    *
 *                                                                                                    *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without  *
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU     *
 * General Public License for more details.                                                           *
 *                                                                                                    *
 * You should have received a copy of the GNU General Public License along with this program; if not, *
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307,  *
 * USA.                                                                                               *
 *                                                                                                    *
 ******************************************************************************************************/

/**
 * @file  lib.h
 * @brief definitions and structures for api internals
 *
 * $Id: dlf_lib.h,v 1.5 2007/05/29 08:47:05 waldron Exp $
 */

#ifndef _API_H
#define _API_H

/* headers */
#include "dlf_api.h"
#include "queue.h"

/* definitions */
#define API_MAX_TARGETS          20         /**< the max number of logging targets/destinations    */
#define API_MAX_THREADS          4          /**< max number of threads                             */

#define API_QUEUE_SIZE           5000       /**< internal queue size for messages pending transport
                                                 to the server per thread                          */

/* socket related */
#define API_CONNECT_TIMEOUT      5          /**< number of seconds to wait for a connection        */
#define API_WRITE_TIMEOUT        20         /**< number of seconds to wait in netwrite_timeout     */
#define API_READ_TIMEOUT         20         /**< number of seconds to wait in netread_timeout      */


/**
 * opaque structures
 */

typedef struct target_t   target_t;


/**
 * structures
 */

struct target_t {
	char               path[1024];      /**< path to the log file                              */
	int                perm;            /**< access permissions for files                      */
	char               server[64];      /**< url to the server                                 */
	int                port;            /**< port to connect to on remote host                 */
	int                socket;          /**< file descriptor for socket communications         */
	int                fac_no;          /**< the facility number provided by a DLF_INIT call   */
	int                sevmask;         /**< the severity mask                                 */
	long               mode;            /**< the targets mode e.g. initialised, connected etc  */
	queue_t            *queue;          /**< internal fifo message queue                       */
	long               queue_size;      /**< the size of the queue                             */
	int                mutex;           /**< target specific mutex                             */
	int                shutdown;        /**< flag to indicate whether the thread should die    */
};


/* severity list */
static struct {
	char               *name;           /**< severity name                          */
	int                sevno;           /**< severity number                        */
	int                sevmask;         /**< severity mask                          */
	char               *confentname;    /**< name of severity environment variable  */
} severitylist[] = {
	{ "Emergency",  DLF_LVL_EMERGENCY,  0x000001,  "LOGEMERGENCY"  },
	{ "Alert",      DLF_LVL_ALERT,      0x000002,  "LOGALERT"      },
	{ "Error",      DLF_LVL_ERROR,      0x000004,  "LOGERROR"      },
	{ "Warning",    DLF_LVL_WARNING,    0x000008,  "LOGWARNING"    },
	{ "Auth",       DLF_LVL_AUTH,       0x000010,  "LOGAUTH"       },
	{ "Security",   DLF_LVL_SECURITY,   0x000020,  "LOGSECURITY"   },
	{ "Usage",      DLF_LVL_USAGE,      0x000040,  "LOGUSAGE"      },
	{ "System",     DLF_LVL_SYSTEM,     0x000080,  "LOGSYSTEM"     },
	{ "Monitoring", DLF_LVL_MONITORING, 0x000100,  "LOGMONITORING" },
	{ "Important",  DLF_LVL_IMPORTANT,  0x000200,  "LOGIMPORTANT"  },
	{ "Debug",      DLF_LVL_DEBUG,      0x000400,  "LOGDEBUG"      },
	{ "All",        -1,                 0x000800,  "LOGALL"        },
	{ "Standard",   -2,                 0x001000,  "LOGSTANDARD"   },
	{ NULL,         0,                  0,         NULL            }
};


#endif

/** End-of-File **/
