/******************************************************************************************************
 *                                                                                                    *
 * process.h - Castor Distribution Logging Facility                                                   *
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
 * @file  process.h
 * @brief message processing prototypes for the server
 *
 * $Id: process.h,v 1.3 2006/07/18 12:04:35 waldron Exp $
 */

#ifndef _PROCESS_H
#define _PROCESS_H

/* headers */
#include "osdep.h"
#include "server.h"

/**
 * Process the incoming request from the client, parsing the security related information and passing
 * the data onto the corresponding data processor. It is the responsibility of the data processor
 * to communicate errors back to the client and dealing with the validation of the data
 *
 * @param client : a pointer to the client structure
 *
 * @returns      : APP_SUCCESS on success
 * @returns      : APP_FAILURE on processing failure
 *
 * @warning process errors of any kind result in the client being disconnected!
 */

EXTERN_C int DLL_DECL process_request _PROTO((client_t *client));


/**
 * Parse a message of type DLF_INIT. Initialisation of the facility's associated message text and
 * resolution of the facility name to an id
 *
 * @param client : a pointer to the client structure
 * @param data   : the data received from the client
 * @param len    : the length of the data
 *
 * @returns      : APP_SUCCESS on success and the facility it sent to the client
 * @returns      : APP_FAILURE on processing failure and an appropriate message sent to the client
 */

EXTERN_C int DLL_DECL parse_init _PROTO((client_t *client, char *data, int len));


/**
 * Parse a message of type DLF_LOG. Allocate memory for a message_t structure and push the data to
 * the servers internal fifo queue pending extraction and insertion by the database interface layer
 *
 * @param client : a pointer to the client structure
 * @param data   : the data received from the client
 * @param len    : the length of the data
 *
 * @returns      : APP_SUCCESS on success
 * @returns      : APP_FAILURE on processing failure and an appropriate message sent to the client
 */

EXTERN_C int DLL_DECL parse_log _PROTO((client_t *client, char *data, int len));


#endif

/** End-of-File **/
