/******************************************************************************************************
 *                                                                                                    *
 * hash.h - Castor Distribution Logging Facility                                                      *
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
 * @file  hash.h
 * @brief Thread safe hash implementation
 *
 * $Id: hash.h,v 1.3 2006/06/20 13:36:44 waldron Exp $
 */

#ifndef _HASH_H
#define _HASH_H

/* headers */
#include "osdep.h"

/**
 * opaque structures
 */

typedef struct hash_t  hash_t;
typedef struct entry_t entry_t;


/**
 * Create a hash for mapping strings to numbers. This implementation does not allow the storing of
 * void datatypes and is specifically designed and optimised to store numbers. For this very reason
 * there is also no need to remove the data from the hash prior to hash_destroy()
 *
 * @param h     : the new hash
 * @param size  : the size of the hash
 *
 * @returns     : APP_SUCCESS on success
 * @returns     : APP_FAILURE on failure. Normally caused by a memory allocation error.
 */

EXTERN_C int DLL_DECL hash_create _PROTO((hash_t **h, unsigned int size));


/**
 * Destroy a hash and all its entries. It is the responsible of the user to free the data associated
 * with the hash
 *
 * @param h     : the hash handle
 * @param func  : a pointer to the function used to free the data within the queue
 *
 * @returns     : APP_SUCCESS on success
 * @returns     : APP_FAILURE on null hash handle
 */

EXTERN_C int DLL_DECL hash_destroy _PROTO((hash_t *h, void (*func)(void *)));


/**
 * Insert a key with a given value into the hash
 *
 * @param h     : the hash handle
 * @param key   : the key to use within the hash
 * @param value : the value associated with the given key
 *
 * @returns     : APP_SUCCESS on success
 * @returns     : APP_FAILURE on failure (e.g. key already exists, insufficient parameters specified)
 */

EXTERN_C int DLL_DECL hash_insert _PROTO((hash_t *h, char *key, void *value));


/**
 * Search the hash for a given key which is returned by reference in variable 'value'
 *
 * @param h     : the hash handle
 * @param key   : the key to search for
 * @param value : the data in the hash will be returned to this variable
 *
 * @returns     : APP_SUCCESS on success
 * @returns     : APP_FAILURE on failure (e.g. key not found or insufficient parameters specified)
 */

EXTERN_C int DLL_DECL hash_search _PROTO((hash_t *h, char *key, void **value));


/**
 * Generate the performance statistic for the given hash
 * 
 * The performance value of the hash is the total number of entries where an index (nelt) has more then
 * one associated value.
 *
 * @param h     : the hash handle
 *
 * @returns     : APP_FAILURE on null hash handle
 * @returns     : The computed statistics value on success
 */

EXTERN_C int DLL_DECL hash_stats _PROTO((hash_t *h));


#endif

/** End-of-File **/
