/******************************************************************************
 *                      castor/rh/StringResponse.h
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

#ifndef CASTOR_RH_STRINGRESPONSE_H
#define CASTOR_RH_STRINGRESPONSE_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct Crh_Response_t;
struct Crh_StringResponse_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StringResponse
// The most simple response : just a string
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Crh_StringResponse_create(struct Crh_StringResponse_t** obj);

/**
 * Empty Destructor
 */
int Crh_StringResponse_delete(struct Crh_StringResponse_t* obj);

/**
 * Cast into Response
 */
struct Crh_Response_t* Crh_StringResponse_getResponse(struct Crh_StringResponse_t* obj);

/**
 * Dynamic cast from Response
 */
struct Crh_StringResponse_t* Crh_StringResponse_fromResponse(struct Crh_Response_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Crh_StringResponse_getIObject(struct Crh_StringResponse_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Crh_StringResponse_t* Crh_StringResponse_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Crh_StringResponse_print(struct Crh_StringResponse_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Crh_StringResponse_TYPE(int* ret);

/*********************************************/
/* Implementation of Response abstract class */
/*********************************************/

/**
 * Get the value of errorCode
 * The error code in case of error
 */
int Crh_StringResponse_errorCode(struct Crh_StringResponse_t* instance, unsigned int* var);

/**
 * Set the value of errorCode
 * The error code in case of error
 */
int Crh_StringResponse_setErrorCode(struct Crh_StringResponse_t* instance, unsigned int new_var);

/**
 * Get the value of errorMessage
 * The error message in case of error
 */
int Crh_StringResponse_errorMessage(struct Crh_StringResponse_t* instance, const char** var);

/**
 * Set the value of errorMessage
 * The error message in case of error
 */
int Crh_StringResponse_setErrorMessage(struct Crh_StringResponse_t* instance, const char* new_var);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Gets the type of the object
 */
int Crh_StringResponse_type(struct Crh_StringResponse_t* instance,
                            int* ret);

/**
 * virtual method to clone any object
 */
int Crh_StringResponse_clone(struct Crh_StringResponse_t* instance,
                             struct C_IObject_t* ret);

/**
 * Get the value of content
 * Content of the response
 */
int Crh_StringResponse_content(struct Crh_StringResponse_t* instance, const char** var);

/**
 * Set the value of content
 * Content of the response
 */
int Crh_StringResponse_setContent(struct Crh_StringResponse_t* instance, const char* new_var);

/**
 * Get the value of id
 * The id of this object
 */
int Crh_StringResponse_id(struct Crh_StringResponse_t* instance, u_signed64* var);

/**
 * Set the value of id
 * The id of this object
 */
int Crh_StringResponse_setId(struct Crh_StringResponse_t* instance, u_signed64 new_var);

#endif // CASTOR_RH_STRINGRESPONSE_H
