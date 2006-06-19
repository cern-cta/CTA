/******************************************************************************************************
 *                                                                                                    *
 * queue.h - Castor Distribution Logging Facility                                                     *
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
 * @file  queue.h
 * @brief Thread safe FIFO bounded queue
 *
 * $Id: queue.h,v 1.2 2006/06/19 06:52:55 waldron Exp $
 */

#ifndef _QUEUE_H
#define _QUEUE_H

/* headers */
#include "osdep.h"

/**
 * opaque structure
 */

typedef struct queue_t queue_t;


/**
 * Create a FIFO queue
 *
 * @param q     : the new queue
 * @param size  : the bounded maximum size of the queue
 *
 * @returns     : ARP_SUCCESS on success
 * @returns     : APP_FAILURE on failure
 */

EXTERN_C int DLL_DECL queue_create _PROTO((queue_t **q, unsigned int size));


/**
 * Terminate a FIFO queue, sending an interrupt to all blocking threads
 *
 * @param q     : the queue handle
 *
 * @returns     : APP_SUCCESS on success
 * @returns     : APP_FAILURE on failure
 */

EXTERN_C int DLL_DECL queue_terminate _PROTO((queue_t *queue));


/**
 * Destroy a FIFO queue, this will suspend the queue using queue_terminate() as well as calling the
 * function provided for every nelt in the queue. If the queue is maxed out i.e. all nelts are occupied
 * it will not be posible to destroy the data within the queue if consumer threads are waiting to push
 * data in.
 *
 * @param q     : the queue handle
 * @param func  : a pointer to the function used to free the data within the queue
 *
 * @returns     : APP_SUCCESS on success
 * @returns     : APP_FAILURE on failure
 */

EXTERN_C int DLL_DECL queue_destroy _PROTO((queue_t *queue, void (*func)(void *)));


/**
 * Pop/get an object from the queue, returning immediately if the queue is empty
 *
 * @param queue : the queue handle
 * @param data  : the data in the queue will be returned to this variable
 * @param wait  : 1 = wait if queue is empty, 0 = no waiting return if empty
 *
 * @returns     : ARP_SUCCESS on success
 * @returns     : APP_FAILURE on failure and serrno is set to one of
 * @returns     : APP_QUEUE_TERMINATED on queue suspension, i.e no more push or pop'ing allowed
 * @returns     : APP_QUEUE_FEMPTY when the queue has no more data available
 * @returns     : APP_FAILURE on everything else
 */

EXTERN_C int DLL_DECL queue_pop _PROTO((queue_t *queue, void **data));


/**
 * Push/add an object to the queue, returning immediately if the queue is full
 *
 * @param queue : the queue handle
 * @param data  : a pointer to the data to add to the queue
 * @param wait  : 1 = wait if queue is full, 0 = no waiting return if full
 *
 * @returns     : ARP_SUCCESS on success
 * @returns     : APP_QUEUE_TERMINATED on queue suspension, i.e no more push or pop'ing allowed
 * @returns     : APP_QUEUE_FULL when queue has no more available nelts/sockets
 * @returns     : APP_FAILURE on everything else
 */

EXTERN_C int DLL_DECL queue_push _PROTO((queue_t *queue, void *data));


/**
 * Return the number of elements in the queue (its size)
 *
 * @param queue : the queue handle
 * @returns     : on success the number of elements in the queue
 *
 * @warning this function returns the number of elements in the fifo queue at the time of calling,
 * a lot of insertions and retrievals can occur on the queue. Meaning the value can quickly become
 * incorrect. Functions using this call should use some kind of delta +/- X
 */

EXTERN_C unsigned int DLL_DECL queue_size _PROTO((queue_t *queue));


#endif

/** End-of-File **/
