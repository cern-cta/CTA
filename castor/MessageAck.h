/******************************************************************************
 *                      castor/MessageAck.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_MESSAGEACK_H
#define CASTOR_MESSAGEACK_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct C_MessageAck_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class MessageAck
// Acknowledgement message in the request handler protocol.
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int C_MessageAck_create(struct C_MessageAck_t** obj);

/**
 * Empty Destructor
 */
int C_MessageAck_delete(struct C_MessageAck_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* C_MessageAck_getIObject(struct C_MessageAck_t* obj);

/**
 * Dynamic cast from IObject
 */
struct C_MessageAck_t* C_MessageAck_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int C_MessageAck_print(struct C_MessageAck_t* instance);

/**
 * Gets the type of this kind of objects
 */
int C_MessageAck_TYPE(int* ret);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Sets the id of the object
 */
int C_MessageAck_setId(struct C_MessageAck_t* instance,
                       u_signed64 id);

/**
 * gets the id of the object
 */
int C_MessageAck_id(struct C_MessageAck_t* instance,
                    u_signed64* ret);

/**
 * Gets the type of the object
 */
int C_MessageAck_type(struct C_MessageAck_t* instance,
                      int* ret);

/**
 * virtual method to clone any object
 */
int C_MessageAck_clone(struct C_MessageAck_t* instance,
                       struct C_IObject_t* ret);

/**
 * Get the value of status
 */
int C_MessageAck_status(struct C_MessageAck_t* instance, int* var);

/**
 * Set the value of status
 */
int C_MessageAck_setStatus(struct C_MessageAck_t* instance, int new_var);

/**
 * Get the value of errorCode
 */
int C_MessageAck_errorCode(struct C_MessageAck_t* instance, int* var);

/**
 * Set the value of errorCode
 */
int C_MessageAck_setErrorCode(struct C_MessageAck_t* instance, int new_var);

/**
 * Get the value of errorMessage
 */
int C_MessageAck_errorMessage(struct C_MessageAck_t* instance, const char** var);

/**
 * Set the value of errorMessage
 */
int C_MessageAck_setErrorMessage(struct C_MessageAck_t* instance, const char* new_var);

#endif // CASTOR_MESSAGEACK_H
