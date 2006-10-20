/******************************************************************************************************
 *                                                                                                    *
 * server.h - Castor Distribution Logging Facility                                                    *
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
 * @file  server.h
 * @brief definitions and structures associated with the server core
 *
 * $Id: server.h,v 1.5 2006/10/20 15:55:03 waldron Exp $
 */

#ifndef _SERVER_H
#define _SERVER_H

/* headers */
#include "osdep.h"

/* definitions */
#define MAX_THREADS              100        /**< maximum number of threads with a given thread pool  */
#define MAX_QUEUE_SIZE           250000     /**< the maximum size of the internal server queue       */
#define MAX_THROTTLE_TIME        5000       /**< maximum amount of time to throttle messages when
                                                 the queue becomes saturated in milliseconds         */
#define THROTTLE_THRESHOLD       98         /**< limit on the queue for when to begin throttling, set
                                                 this value to -1 to disable throttling              */
#define THROTTLE_PENALTIY        1000       /**< the throttle penalty in milliseconds for every 1000
                                                 messages over the threshold limit                   */

#define MAX_CLIENTS              300        /**< maximum amount of clients per thread                */
#define MAX_CLIENT_TIMEOUT       300        /**< the number of seconds to allow a client to be
                                                 connected without transmitting any data             */

/* thread related */
#define HANDLER_THREAD_POOLSIZE  2          /**< default thread pool size for handlers               */
#define HANDLER_THREAD_BALANCING 1          /**< the definition defines whether the server should
                                                 distribute connections across all threads in the
                                                 handler pool, prevent one thread from doing all
                                                 the server processing                               */

/* server related */
#define DEFAULT_SERVER_BACKLOG   25         /**< the number of connections to queue pending accept() */
#define DEFAULT_SERVER_PORT      5036       /**< the default port to listen on                       */

/* log helpers */
#define CLIENT_IDENT(client)     client->index, client->ip


/**
 * opaque structures
 */

typedef struct handler_t  handler_t;
typedef struct client_t   client_t;


/**
 * structures
 */

struct handler_t {
	int                tid;             /**< thread id                                           */
	int                index;           /**< position of thread in handler pool                  */
	int                listen;          /**< server listening socket                             */
	int                connected;       /**< number of clients connected through this thread     */
	client_t           *clients;        /**< array of clients                                    */
	int                mutex;           /**< thread mutex for accessing statistics               */

	long long          connections;     /**< number of connections accept()'d on this thread     */
	long long          clientpeak;      /**< total number of concurrent connections              */
	long long          messages;        /**< number of DLF_LOG requests                          */
	long long          inits;           /**< number of DLF_INIT requests                         */
	long long          errors;          /**< number of errors encountered during parsing         */
	long long          timeouts;        /**< the number of connections that have timed out       */
};

struct client_t {
	int                fd;              /**< file descriptor of the clients socket               */
	int                index;           /**< position of the client structure in the client pool */
	char               ip[16];          /**< the clients ip address in 255.255.255.255 format    */
	time_t             timestamp;       /**< the last time a message was received                */
	handler_t          *handler;        /**< a pointer to the handler associated with the client */
};


#endif

/** End-of-File **/
