/******************************************************************************************************
 *                                                                                                    *
 * queue.c - Castor Distribution Logging Facility                                                     *
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
 * $Id
 */

/* headers */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "Cmutex.h"
#include "Cthread_api.h"
#include "common.h"
#include "errno.h"
#include "serrno.h"
#include "queue.h"
#include "log.h"


/* definitions */
#define DEFAULT_QUEUE_SIZE 100000;         /**< default size of a queue   */

/* structures */
struct queue_t {
	void             **data;           
 	unsigned int     size;             /**< size of the queue         */
	unsigned int     nelts;            /**< number of queue entries   */
	unsigned int     in;               /**< next empty table entry    */
	unsigned int     out;              /**< next filled table entry   */
	int              queue_mutex;     
	int              terminated;      
};


/*
 * Initialise the queue
 */

int DLL_DECL queue_create(queue_t **q, unsigned int size) {

	/* variables */
	queue_t *queue;

	/* use default queue size ? */
	if (size < 1) {
		size = DEFAULT_QUEUE_SIZE;
	}

	/* allocate memory */
	queue = (queue_t *) malloc(sizeof(queue_t));
	if (queue == NULL) {
		log(LOG_ERR, "queue_create() : %s\n", strerror(errno));
		return APP_FAILURE;
	}
	*q = queue;

	/* initialise data */
	queue->data = malloc(sizeof(void *) * size);
	if (queue->data == NULL) {
		log(LOG_ERR, "queue_create() : %s\n", strerror(errno));
		return APP_FAILURE;
	}
	queue->size       = size;
	queue->nelts      = 0;
	queue->in         = 0;
	queue->out        = 0;
	queue->terminated = 0;

	return APP_SUCCESS;
}


/*
 * Terminate the queue, suspend all push and pop operations
 */

int DLL_DECL queue_terminate(queue_t *queue) {
	
	/* variables */
	int     rv;

	if ((rv = Cthread_mutex_lock(&queue->queue_mutex)) != 0) {
		log(LOG_ERR, "queue_terminate() - failed to Cthread_mutex_lock() : %s\n", sstrerror(serrno));
		return APP_FAILURE;
	}
	
	/* we must hold one big mutex when setting this... otherwise we could cause a would-be popper
	 * or pusher to extract or insert data after termination
	 */
	queue->terminated = 1;

	if ((rv = Cthread_mutex_unlock(&queue->queue_mutex)) != APP_SUCCESS) {
		log(LOG_ERR, "queue_terminate() - failed to Cthread_mutex_unlock() : %s\n", sstrerror(serrno));
		return APP_FAILURE;
	}

	return APP_SUCCESS;
}


/*
 * Destroy the contents of the queue, interrupt any pop'ing or push'ing threads and free the contents
 * of the queue by passing a pointer to the function to free the data.
 */

int DLL_DECL queue_destroy(queue_t *queue, void (*func)(void *)) {
	
	/* variables */
	void    *data;
	int     rv;

	/* terminate the queue
	 *   - this will interrupt any threads trying to pop or push data
	 */
	rv = queue_terminate(queue);
	if (rv != APP_SUCCESS) {
		return APP_FAILURE;
	}

	/* function defined to free data ? */
	if (func == NULL) {
		return APP_SUCCESS;
	}

	/* obtain mutex lock */
	if ((rv = Cthread_mutex_lock(&queue->queue_mutex)) != APP_SUCCESS) {
		log(LOG_ERR, "queue_destroy() - failed to Cthread_mutex_lock() : %s\n", sstrerror(serrno));
		return APP_FAILURE;
	}

	/* loop over queue data */
	while (queue->nelts) {
		data = queue->data[queue->out];
		queue->nelts--;
		queue->out = (queue->out + 1) % queue->size;
		
		/* call function */
		(*func)(data);
	}
	free(queue->data);

	/* release mutex lock */
	Cthread_mutex_unlock(&queue->queue_mutex);
	
	return APP_SUCCESS;
}


/*
 * Push new data onto the queue. If the queue is full the thread will not block but return control to the
 * caller. It is up to the calling function to decide whether a retry is necessary.
 */

int DLL_DECL queue_push(queue_t *queue, void *data) {

	/* variables */
	int     rv;

	/* queue terminated ? */
	if (queue->terminated) {
		log(LOG_DEBUG, "queue_push() : queue terminated\n");
		return APP_QUEUE_TERMINATED;
	}

	/* obtain mutex lock */
	if ((rv = Cthread_mutex_lock(&queue->queue_mutex)) != APP_SUCCESS) {
		log(LOG_ERR, "queue_push() - failed to Cthread_mutex_lock() : %s\n", sstrerror(serrno));
		return APP_FAILURE;
	}

	/* queue full ? */
	if (queue->nelts == queue->size) {
		if ((rv = Cthread_mutex_unlock(&queue->queue_mutex)) != APP_SUCCESS) {
			log(LOG_ERR, "queue_push() - failed to Cthread_mutex_lock() : %s\n", sstrerror(serrno));
			return APP_FAILURE;
		}
		return APP_QUEUE_FULL;
	}

	queue->data[queue->in] = data;
	queue->in = (queue->in + 1) % queue->size;
	queue->nelts++;

	/* release mutex lock */
	Cthread_mutex_unlock(&queue->queue_mutex);	

	return APP_SUCCESS;
}


/*
 * Retrieve the next item from the queue. If there are no items available, the thread will not block but
 * return control to the caller. It is up to the calling function to decide when to retry. It is also
 * the calling function which must deal with memory cleanup of the data item.
 */

int DLL_DECL queue_pop(queue_t *queue, void **data) {

	/* variables */
	int     rv;

	/* queue terminated ? */
	if (queue->terminated) {
		log(LOG_DEBUG, "queue_pop() : queue terminated\n");
		return APP_QUEUE_TERMINATED;
	}

	/* obtain mutex lock */
	if ((rv = Cthread_mutex_lock(&queue->queue_mutex)) != APP_SUCCESS) {
		log(LOG_ERR, "queue_pop() - failed to Cthread_mutex_lock() : %s\n", sstrerror(serrno));
		return APP_FAILURE;
	}
	
	/* nothing available ? */
	if (queue->nelts == 0) {
		if ((rv = Cthread_mutex_unlock(&queue->queue_mutex)) != APP_SUCCESS) {
			log(LOG_ERR, "queue_pop() - failed to Cthread_mutex_unlock() : %s\n", sstrerror(serrno));
				return APP_FAILURE;
		}
		return APP_QUEUE_EMPTY;
	}

	*data = queue->data[queue->out];
	queue->nelts--;
	queue->out = (queue->out + 1) % queue->size;
	
	/* release mutex lock */
	Cthread_mutex_unlock(&queue->queue_mutex);	

	return APP_SUCCESS;
}


/*
 * Return the number of values in the 
 */

unsigned int DLL_DECL queue_size(queue_t *queue) {
	
	/* variables */
	unsigned int size = APP_FAILURE;

	/* determine current size */
	Cthread_mutex_lock(&queue->queue_mutex);
	size = queue->nelts;
	Cthread_mutex_unlock(&queue->queue_mutex);
	
	return size;
}


/** End-of-File **/
