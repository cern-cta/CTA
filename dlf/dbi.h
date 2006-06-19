/******************************************************************************************************
 *                                                                                                    *
 * dbi.h - Castor Distribution Logging Facility                                                       *
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
 * @file  dbi.h
 * @brief database interface layer
 *
 * $Id: dbi.h,v 1.2 2006/06/19 06:52:55 waldron Exp $
 */

#ifndef _DBI_H
#define _DBI_H

/* headers */
#include "common.h"
#include "osdep.h"
#include "server.h"

/* definitions */
#define DB_FLUSH_ROWS         49999         /**< maximum size of the database host arrays           */
#define DB_FLUSH_TIME         120           /**< the maximum time to wait before flushing data      */

/* oracle specific */
#define DB_ORA_ARRAY_SIZE     50000         /**< the number of nelts in an oracle host array        */

/* thread related */
#define DB_THREAD_POOLSIZE    2             /**< the default number of threads in the database pool */


/**
 * opaque structures
 */

typedef struct database_t database_t;


/**
 * Initialise the database interface layer. This call is database specific but should initialise the
 * global data structures including creation of the database threads.
 *
 * @param threads : the number of threads in the pool
 *
 * @returns     : APP_SUCCESS on success
 * @returns     : APP_FAILURE on failure
 *
 * @note under the ORACLE interface additional initialisation is performed to determine the initial
 * values for the primary keys used in the dlf_messages, dlf_host_map and dlf_nshost_map db tables
 */

EXTERN_C int DLL_DECL db_init _PROTO((int threads));


/**
 * Shutdown the database interface layer. All resources allocated and threads created in db_init()
 * should be terminated in a graceful way and all resources free'd
 *
 * @returns     : void, this function is always successful
 * @returns     : APP_SUCCESS on success
 */

EXTERN_C int DLL_DECL db_shutdown _PROTO((void));


/**
 * Open a connection to the database up to X retry attempts with a delay of 10 seconds between attempts
 *
 * @param db      : a pointer to the database handle
 * @param retries : the maximum number of times to try to connect to the database
 *
 * @returns       : APP_SUCCESS on success
 * @returns       : APP_FAILURE on failure
 */

EXTERN_C int DLL_DECL db_open _PROTO((database_t *db, unsigned int retries));


/**
 * This function is responsible for loop over the messages in the servers internal fifo queue and
 * translating them to database calls. This function will terminate database insertion when server_mode
 * = MODE_SUSPENDED
 *
 * @param db    : a pointer to the database handle
 * @returns     : void, exits on server shutdown
 */

EXTERN_C void DLL_DECL db_worker _PROTO((database_t *db));


/**
 * Initialise a facility by entering the message text into the database and returning the facility id.
 * If the message number and text already exists the interface should update the associated text
 *
 * @param facility : the name of the facility
 * @param texts    : an array of msgtext entries
 * @param fac_no   : the facility id
 *
 * @returns        : APP_SUCCESS on success
 * @returns        : APP_FAILURE on failure of the database or failed resolution of the facility to an
 *                   id. A fac_no = -1 indicates failed resolution.
 */

EXTERN_C int DLL_DECL db_initfac _PROTO((char *facility, msgtext_t *texts[], int *fac_no));


/**
 * Generate the database monitoring statistics about the number of operations performed by the server
 * For example, the number of commits, inserts, errors, connections etc...
 *
 * @param hpool    : a pointer to pointer to the handler pool of server.c
 * @param interval : how often the statistics should be generated in seconds
 *
 * @return         : APP_SUCCESS on success
 * @return         : APP_FAILURE on failure of the database
 */

EXTERN_C int DLL_DECL db_monitoring _PROTO((handler_t **hpool, unsigned int interval));


/**
 * Generate the castor2 job and request statistics. This computes the max, mim, standard deviation,
 * average time, incoming request and outgoing requests per sample interval for each job and request
 * types 'Get, PrepareToGet, Put, PrepareToPut etc...'
 *
 * @param interval : how often the statistics should be generated in seconds
 *
 * @returns        : APP_SUCCESS on success
 * @returns        : APP_FAILURE on failure of the database
 */

EXTERN_C int DLL_DECL db_stats _PROTO((unsigned int interval));


#endif

/** End-of-File **/
