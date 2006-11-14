/******************************************************************************************************
 *                                                                                                    *
 * hash.c - Castor Distribution Logging Facility                                                      *
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
 * $Id: hash.c,v 1.6 2006/11/14 17:02:17 waldron Exp $
 */

/* headers */
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "Cmutex.h"
#include "Cthread_api.h"
#include "errno.h"
#include "hash.h"
#include "log.h"


/* definitions */
#define DEFAULT_HASH_SIZE 25000;           /**< default size of a hash     */

/* structures */
struct hash_t {
 	unsigned int     size;             /**< size of the hash           */
	int              *mutex;           /**< mutex to protect hash data */
	entry_t          **table;          /**< table of hash entries      */
};

struct entry_t {
	char             *key;             /**< hash key                   */
	void             *value;           /**< hash value                 */
	entry_t          *next;            /**< next entry in linked list  */
};

/*
 * Initialise the hash
 */

int DLL_DECL hash_create(hash_t **h, unsigned int size) {

	/* variables */
	hash_t  *hash;
	int     i;

	/* use default hash size ? */
	if (size < 1) {
		size = DEFAULT_HASH_SIZE;
	}

	/* allocate memory */
	hash = (hash_t *) malloc(sizeof(hash_t));
	if (hash == NULL) {
		log(LOG_ERR, "hash_create() : %s\n", strerror(errno));
		return APP_FAILURE;
	}
	*h = hash;

	hash->table = (entry_t **) malloc(sizeof(entry_t) * size);
	hash->mutex = (int *) malloc(sizeof(int) * size);

	if ((hash->mutex == NULL) || (hash->table == NULL)) {
		log(LOG_ERR, "hash_create() : %s\n", strerror(errno));
		return APP_FAILURE;
	}

	/* initialise table */
	for (i = 0; i < size; i++) {
		hash->table[i] = NULL;
	}

	hash->size  = size;

	return APP_SUCCESS;
}


/*
 * Destroy hash
 */

int DLL_DECL hash_destroy(hash_t *h, void (*func)(void *)) {

	/* variables */
	entry_t *l;
	entry_t	*n;
	int     i;

	/* handle defined ? */
	if (h == NULL) {
		return APP_FAILURE;
	}

	/* free hash entries */
	for (i = 0; i < h->size; i++) {

		/* free hash entries */
		for (l = h->table[i]; l != NULL; l = n) {
			free(l->key);
			if ((func != NULL) && (l->value != NULL)) {
				(*func)(l->value);
			}
			n = l->next;
			free(l);
		}
	}

	/* free resources */
	free(h->table);
	free(h->mutex);
	free(h);

	return APP_SUCCESS;
}


/*
 * Generate hash index
 */

int DLL_DECL hash_index(hash_t *h, char *key) {

	/* variables */
	char *c;
	int   s = 0;

	/* handle and key defined ? */
	if ((h == NULL) || (key == NULL)) {
		return APP_FAILURE;
	}

	/* create hash key */
	for (c = key; *c != 0; c++) {
		if (isdigit(*c)) {
			s += atol(c);
			break;
		} else {
			s += *c;
		}
	}

	/* check for floating point exception */
	if (h->size == 0) {
		log(LOG_ERR, "hash_index() : floating point exception\n");
		return APP_FAILURE;
	}

	return s % h->size;
}


/*
 * Hash insertion
 */

int DLL_DECL hash_insert(hash_t *h, char *key, void *value) {

	/* variables */
	entry_t *l;
	entry_t	*n;
	int     i;

	/* handle and key defined ? */
	if ((h == NULL) || (key == NULL) || (!value)) {
		return APP_FAILURE;
	}

	/* get hashtable entry to use */
	i = hash_index(h, key);
	if (i == -1) {
		return APP_FAILURE;
	}

	Cthread_mutex_lock(&h->mutex[i]);

	/* key already in use ? */
	for (l = h->table[i]; l != NULL; l = l->next) {
		if (!strcmp(key, l->key)) {
			Cthread_mutex_unlock(&h->mutex[i]);
			return APP_FAILURE;
		}
	}

	/* insert new key into the beginning of the hash */
	n = (entry_t *) malloc(sizeof(entry_t));
	if (n == NULL) {
		Cthread_mutex_unlock(&h->mutex[i]);
		log(LOG_ERR, "hash_insert() : %s\n", strerror(errno));
		return APP_FAILURE;
	}

	n->key    = strdup(key);
	n->value  = value;
	n->next   = h->table[i];

	h->table[i] = n;
	Cthread_mutex_unlock(&h->mutex[i]);

	return APP_SUCCESS;
}


/*
 * Hash search
 */

int DLL_DECL hash_search(hash_t *h, char *key, void **value) {

	/* variables */
	entry_t *l;
	int     i;

	/* handle and key defined ? */
	if ((h == NULL) || (key == NULL)) {
		return APP_FAILURE;
	}

	/* get hashtable entry to use */
	i = hash_index(h, key);
	if (i == -1) {
		return APP_FAILURE;
	}

	/* lookup the index */
	Cthread_mutex_lock(&h->mutex[i]);
	l = h->table[i];
	Cthread_mutex_unlock(&h->mutex[i]);

	/* find key ? */
	for (; l != NULL; l = l->next) {
		if (!strcmp(key, l->key)) {
			*value = l->value;
			return APP_SUCCESS;
		}
	}

	return APP_FAILURE;
}


/*
 * Hash lock
 */

void DLL_DECL hash_lock(hash_t *h) {

	/* variables */
	int     i;

     	/* loop over hash entries and lock all mutexes */
	for (i = 0; (i < h->size) && (h != NULL); i++) {
		Cthread_mutex_lock(&h->mutex[i]);
	}
}


/*
 * Hash unlock
 */

void DLL_DECL hash_unlock(hash_t *h) {
	
	/* variable */
	int     i;

	/* loop over hash entries and unlock all the mutexes */
	for (i = 0; (i < h->size) && (h != NULL); i++) {
		Cthread_mutex_unlock(&h->mutex[i]);
	}
}


/*
 * Hash stats
 */

int DLL_DECL hash_stat(hash_t *h) {

	/* variables */
	entry_t *l;
	int     total, j, i;

	/* handle defined ? */
	if (h == NULL) {
		return APP_FAILURE;
	}

	/* loop over hash entries */
	for (i = 0, total = 0; i < h->size; i++) {
		for (j = 0, l = h->table[i]; l != NULL; l = l->next) {
			j++;
		}
		if (j > 1) {
			total += j;
		}
	}
	return total;
}


/** End-of-File **/
