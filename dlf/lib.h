/******************************************************************************************************
 *                                                                                                    *
 * lib.h - Castor Distribution Logging Facility                                                       *
 * Copyright (C) 2005 CERN IT/FIO (castor-dev@cern.ch)                                                *
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
 * $Id: lib.h,v 1.4 2006/07/18 12:04:35 waldron Exp $
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
#define API_WRITE_TIMEOUT        120        /**< number of seconds to wait in netwrite_timeout     */
#define API_READ_TIMEOUT         120        /**< number of seconds to wait in netread_timeout      */


/**
 * opaque structures
 */

typedef struct target_t   target_t;


/**
 * structures
 */

struct target_t {
	char               path[1024];      /**< path to the log file                              */
	char               server[64];      /**< url to the server                                 */
	int                index;           /**< position of thread in target pool                 */
	int                port;            /**< port to connect to on remote host                 */
	int                socket;          /**< file descriptor for socket communications         */
	int                fac_no;          /**< the facility number provided by a DLF_INIT call   */
	int                sevmask;         /**< the severity mask                                 */
	long               mode;            /**< the targets mode e.g. initialised, connected etc  */
	time_t             pause;           /**< do nothing until pause time is in the past        */
	queue_t            *queue;          /**< internal fifo message queue                       */
	long               queue_size;      /**< the size of the queue                             */
	int                tid;             /**< thread id                                         */
	time_t             err_full;        /**< the last time a queue full error was reported     */
};


/* severity list */
static struct {
	char               *name;           /**< severity name                          */
	int                sevno;           /**< severity number                        */
	int                sevmask;         /**< severity mask                          */
	char               *confentname;    /**< name of severity environment variable  */
} severitylist[] = {
	{ "Emergency",  DLF_LVL_EMERGENCY,  0x000001,  "LOGEMERGENCY" },
	{ "Alert",      DLF_LVL_ALERT,      0x000002,  "LOGALERT"     },
	{ "Error",      DLF_LVL_ERROR,      0x000004,  "LOGERROR"     },
	{ "Warning",    DLF_LVL_WARNING,    0x000008,  "LOGWARNING"   },
	{ "Auth",       DLF_LVL_AUTH,       0x000010,  "LOGAUTH"      },
	{ "Security",   DLF_LVL_SECURITY,   0x000020,  "LOGSECURITY"  },
	{ "Usage",      DLF_LVL_USAGE,      0x000040,  "LOGUSAGE"     },
	{ "System",     DLF_LVL_SYSTEM,     0x000080,  "LOGSYSTEM"    },
	{ "Important",  DLF_LVL_IMPORTANT,  0x000100,  "LOGIMPORTANT" },
	{ "Debug",      DLF_LVL_DEBUG,      0x000200,  "LOGDEBUG"     },
	{ "All",        -1,                 0x000400,  "LOGALL"       },
	{ NULL,         0,                  0,         NULL           }
};


/* error list
 *   - we could use the serrno provided by castor commons or define the char *sys_dlferrlist[]. However,
 *     as errors aren't propagated back to the calling client there is no need to make the client aware
 *     of error codes, so we have the freedom to do as we want
 */
static struct {
	int     err_no;                     /**< error number returned by DLF_REP_ERR      */
	char    *err_str;                   /**< error string associated with error number */
} dlf_errorlist[] = {
	{ 1,  DLF_ERR_01 },
	{ 2,  DLF_ERR_02 },
	{ 3,  DLF_ERR_03 },
	{ 4,  DLF_ERR_04 },
	{ 5,  DLF_ERR_05 },
	{ 6,  DLF_ERR_06 },
	{ 7,  DLF_ERR_07 },
	{ 8,  DLF_ERR_08 },
	{ 0,  NULL       },
};


#endif

/** End-of-File **/
