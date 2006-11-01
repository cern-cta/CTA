/******************************************************************************************************
 *                                                                                                    *
 * common.h - Castor Distribution Logging Facility                                                    *
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
 * @file  dlf/common.h
 * @brief defines common definitions, prototypes and structures for use between all DLF applications
 *
 * $Id: common.h,v 1.5 2006/11/01 12:34:17 waldron Exp $
 */

#ifndef _COMMON_H
#define _COMMON_H

/* headers */
#include "osdep.h"
#include "common.h"
#include "server.h"

/* general return codes */
#define APP_SUCCESS           0             /**< return code of a successful function call        */
#define APP_FAILURE           -1            /**< return code of a failed function call            */

/* queue return codes */
#define APP_QUEUE_TERMINATED  40            /**< queue terminated no further operations allowed   */
#define APP_QUEUE_EMPTY       41            /**< queue empty, no data available                   */
#define APP_QUEUE_FULL        42            /**< queue full, no nelts/sockets available           */

/* modes
 *   - these modes are used by the server, dbi and client layers
 */
#define MODE_DEFAULT          0x0000000
#define MODE_INITIALISED      0x0000001
#define MODE_CONNECTED        0x0000002
#define MODE_ACTIVE           0x0000004
#define MODE_FORKING          0x0000008
#define MODE_SUSPENDED_QUEUE  0x0000010
#define MODE_SUSPENDED_DB     0x0000020
#define MODE_SERVER_PURGE     0x0000030
#define MODE_SHUTDOWN         0x0000040

#define MODE_SERVER           0x0001000
#define MODE_FILE             0x0002000


/**
 * mode macros
 */

#define IsConnected(x)       ((x) &   MODE_CONNECTED)
#define IsInitialised(x)     ((x) &   MODE_INITIALISED)
#define IsActive(x)          ((x) &   MODE_ACTIVE)
#define IsForking(x)         ((x) &   MODE_FORKING)
#define IsSuspendedQueue(x)  ((x) &   MODE_SUSPENDED_QUEUE)
#define IsSuspendedDB(x)     ((x) &   MODE_SUSPENDED_DB)
#define IsServerPurge(x)     ((x) &   MODE_SERVER_PURGE)
#define IsShutdown(x)        ((x) &   MODE_SHUTDOWN)
#define IsServer(x)          ((x) &   MODE_SERVER)
#define IsFile(x)            ((x) &   MODE_FILE)

#define SetConnected(x)      ((x) |=  MODE_CONNECTED)
#define SetInitialised(x)    ((x) |=  MODE_INITIALISED)
#define SetActive(x)         ((x) |=  MODE_ACTIVE)
#define SetForking(x)        ((x) |=  MODE_FORKING)
#define SetSuspendedQueue(x) ((x) |=  MODE_SUSPENDED_QUEUE)
#define SetSuspendedDB(x)    ((x) |=  MODE_SUSPENDED_DB)
#define SetServerPurge(x)    ((x) |=  MODE_SERVER_PURGE)
#define SetShutdown(x)       ((x) |=  MODE_SHUTDOWN)
#define SetServer(x)         ((x) |=  MODE_SERVER)
#define SetFile(x)           ((x) |=  MODE_FILE)

#define ClrConnected(x)      ((x) &= ~MODE_CONNECTED)
#define ClrInitialised(x)    ((x) &= ~MODE_INITIALISED)
#define ClrActive(x)         ((x) &= ~MODE_ACTIVE)
#define ClrForking(x)        ((x) &= ~MODE_FORKING)
#define ClrSuspendedQueue(x) ((x) &= ~MODE_SUSPENDED_QUEUE)
#define ClrSuspendedDB(x)    ((x) &= ~MODE_SUSPENDED_DB)
#define ClrServerPurge(x)    ((x) &= ~MODE_SERVER_PURGE)
#define ClrShutdown(x)       ((x) &= ~MODE_SHUTDOWN)
#define ClrFile(x)           ((x) &= ~MODE_FILE)


/**
 * opaque structures
 */

typedef struct message_t  message_t;
typedef struct param_t    param_t;
typedef struct msgtext_t  msgtext_t;


/**
 * structures
 */

struct message_t {
	uid_t              uid;             /**< user id                                          */
	gid_t              gid;             /**< group id                                         */
	pid_t              pid;             /**< process id                                       */
	char               timestamp[15];   /**< message timestamp in the format "%Y%m0%H%M%S"    */
	char               hostname[64];    /**< hostname which generated the message             */
	int                timeusec;        /**< timestamp in microseconds                        */
	int                tid;             /**< thread id                                        */
	char               reqid[37];       /**< request id                                       */
	size_t             size;            /**< size of the message used for socket transmission */
	param_t            *plist;          /**< linked list of parameters                        */

	U_BYTE             facility;        /**< facility number                                  */
	U_BYTE             severity;        /**< severity number                                  */
	U_SHORT            msg_no;          /**< unique message number                            */

	char               nshostname[64];  /**< name server hostname                             */
	char               nsfileid[38];    /**< name server file id                              */

	double             received;        /**< message received time in microseconds            */
};

struct param_t {
	char               name[21];        /**< parameter name                                   */
	int                type;            /**< type, one of DLF_PARAM_<TYPE>                    */
	char               *value;          /**< value to store                                   */
	param_t            *next;           /**< next parameter in linked list                    */
};

struct msgtext_t {
	U_SHORT            msg_no;          /**< unique message number                            */
	char               *msg_text;       /**< value associated with the message number         */
};


/**
 * Free a message_t structure reclaiming all the resources allocated during its construction including
 * the param_t linked list.
 *
 * @param arg   : pointer to the message_t structure to free
 * @returns     : void, this function is always successful.
 *
 * @warning the plist argument of the message must be initialised either to NULL or a linked list.
 * Failure to do this will cause a segmentation fault
 */

EXTERN_C void DLL_DECL free_message _PROTO((void *arg));


/**
 * Free an array of msgtext_t entries. The array must not contain more the DLF_MAX_MSGTEXTS entries as
 * an entries above this value will not be free'd
 *
 * @param texts  : a pointer to the array of message texts t be free'd
 * @returns      : void, this function is always successful.
 */

EXTERN_C void DLL_DECL free_msgtexts _PROTO((msgtext_t *texts[]));


/**
 * Free a param_t structure reclaiming all the resources allocated during its construction
 *
 * @param param : a pointer to the param_t structure to free
 * @returns     : void, this function is always successful.
 */

EXTERN_C void DLL_DECL free_param _PROTO((param_t *param));


/**
 * Small utility function to strip newlines '\n' characters from the end of a string
 * 
 * @param msg   : a pointer to the message to strip
 * @param len   : the length of the message
 * @returns     : void, this function is always successful.
 */
EXTERN_C void DLL_DECL stip_newline _PROTO((char *msg, int len));


#endif

/** End-of-File **/

