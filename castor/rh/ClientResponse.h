/******************************************************************************
 *                      castor/rh/ClientResponse.h
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
 * @(#)$RCSfile: ClientResponse.h,v $ $Revision: 1.1 $ $Release$ $Date: 2004/12/02 17:56:04 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_RH_CLIENTRESPONSE_H
#define CASTOR_RH_CLIENTRESPONSE_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IClient_t;
struct C_IObject_t;
struct Crh_ClientResponse_t;
struct Crh_Response_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class ClientResponse
// A response dedicated to cases where an IClient is returned
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Crh_ClientResponse_create(struct Crh_ClientResponse_t** obj);

/**
 * Empty Destructor
 */
int Crh_ClientResponse_delete(struct Crh_ClientResponse_t* obj);

/**
 * Cast into Response
 */
struct Crh_Response_t* Crh_ClientResponse_getResponse(struct Crh_ClientResponse_t* obj);

/**
 * Dynamic cast from Response
 */
struct Crh_ClientResponse_t* Crh_ClientResponse_fromResponse(struct Crh_Response_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Crh_ClientResponse_getIObject(struct Crh_ClientResponse_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Crh_ClientResponse_t* Crh_ClientResponse_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Crh_ClientResponse_print(struct Crh_ClientResponse_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Crh_ClientResponse_TYPE(int* ret);

/*********************************************/
/* Implementation of Response abstract class */
/*********************************************/

/**
 * Get the value of errorCode
 * The error code in case of error
 */
int Crh_ClientResponse_errorCode(struct Crh_ClientResponse_t* instance, unsigned int* var);

/**
 * Set the value of errorCode
 * The error code in case of error
 */
int Crh_ClientResponse_setErrorCode(struct Crh_ClientResponse_t* instance, unsigned int new_var);

/**
 * Get the value of errorMessage
 * The error message in case of error
 */
int Crh_ClientResponse_errorMessage(struct Crh_ClientResponse_t* instance, const char** var);

/**
 * Set the value of errorMessage
 * The error message in case of error
 */
int Crh_ClientResponse_setErrorMessage(struct Crh_ClientResponse_t* instance, const char* new_var);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Gets the type of the object
 */
int Crh_ClientResponse_type(struct Crh_ClientResponse_t* instance,
                            int* ret);

/**
 * virtual method to clone any object
 */
int Crh_ClientResponse_clone(struct Crh_ClientResponse_t* instance,
                             struct C_IObject_t* ret);

/**
 * Get the value of id
 * The id of this object
 */
int Crh_ClientResponse_id(struct Crh_ClientResponse_t* instance, u_signed64* var);

/**
 * Set the value of id
 * The id of this object
 */
int Crh_ClientResponse_setId(struct Crh_ClientResponse_t* instance, u_signed64 new_var);

/**
 * Get the value of client
 */
int Crh_ClientResponse_client(struct Crh_ClientResponse_t* instance, struct C_IClient_t** var);

/**
 * Set the value of client
 */
int Crh_ClientResponse_setClient(struct Crh_ClientResponse_t* instance, struct C_IClient_t* new_var);

#endif // CASTOR_RH_CLIENTRESPONSE_H
