/******************************************************************************
 *                      castor/rh/GetUpdateStartResponse.h
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

#ifndef CASTOR_RH_GETUPDATESTARTRESPONSE_H
#define CASTOR_RH_GETUPDATESTARTRESPONSE_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IClient_t;
struct C_IObject_t;
struct Crh_GetUpdateStartResponse_t;
struct Crh_Response_t;
struct Crh_StartResponse_t;
struct Cstager_DiskCopyForRecall_t;
struct Cstager_DiskCopy_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class GetUpdateStartResponse
// Adapted response to a GetUpdateStartRequest
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Crh_GetUpdateStartResponse_create(struct Crh_GetUpdateStartResponse_t** obj);

/**
 * Empty Destructor
 */
int Crh_GetUpdateStartResponse_delete(struct Crh_GetUpdateStartResponse_t* obj);

/**
 * Cast into StartResponse
 */
struct Crh_StartResponse_t* Crh_GetUpdateStartResponse_getStartResponse(struct Crh_GetUpdateStartResponse_t* obj);

/**
 * Dynamic cast from StartResponse
 */
struct Crh_GetUpdateStartResponse_t* Crh_GetUpdateStartResponse_fromStartResponse(struct Crh_StartResponse_t* obj);

/**
 * Cast into Response
 */
struct Crh_Response_t* Crh_GetUpdateStartResponse_getResponse(struct Crh_GetUpdateStartResponse_t* obj);

/**
 * Dynamic cast from Response
 */
struct Crh_GetUpdateStartResponse_t* Crh_GetUpdateStartResponse_fromResponse(struct Crh_Response_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Crh_GetUpdateStartResponse_getIObject(struct Crh_GetUpdateStartResponse_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Crh_GetUpdateStartResponse_t* Crh_GetUpdateStartResponse_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Crh_GetUpdateStartResponse_print(struct Crh_GetUpdateStartResponse_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Crh_GetUpdateStartResponse_TYPE(int* ret);

/*****************************************/
/* Implementation of StartResponse class */
/*****************************************/

/**
 * Get the value of id
 * The id of this object
 */
int Crh_GetUpdateStartResponse_id(struct Crh_GetUpdateStartResponse_t* instance, u_signed64* var);

/**
 * Set the value of id
 * The id of this object
 */
int Crh_GetUpdateStartResponse_setId(struct Crh_GetUpdateStartResponse_t* instance, u_signed64 new_var);

/**
 * Get the value of client
 */
int Crh_GetUpdateStartResponse_client(struct Crh_GetUpdateStartResponse_t* instance, struct C_IClient_t** var);

/**
 * Set the value of client
 */
int Crh_GetUpdateStartResponse_setClient(struct Crh_GetUpdateStartResponse_t* instance, struct C_IClient_t* new_var);

/**
 * Get the value of diskCopy
 */
int Crh_GetUpdateStartResponse_diskCopy(struct Crh_GetUpdateStartResponse_t* instance, struct Cstager_DiskCopy_t** var);

/**
 * Set the value of diskCopy
 */
int Crh_GetUpdateStartResponse_setDiskCopy(struct Crh_GetUpdateStartResponse_t* instance, struct Cstager_DiskCopy_t* new_var);

/*********************************************/
/* Implementation of Response abstract class */
/*********************************************/

/**
 * Get the value of errorCode
 * The error code in case of error
 */
int Crh_GetUpdateStartResponse_errorCode(struct Crh_GetUpdateStartResponse_t* instance, unsigned int* var);

/**
 * Set the value of errorCode
 * The error code in case of error
 */
int Crh_GetUpdateStartResponse_setErrorCode(struct Crh_GetUpdateStartResponse_t* instance, unsigned int new_var);

/**
 * Get the value of errorMessage
 * The error message in case of error
 */
int Crh_GetUpdateStartResponse_errorMessage(struct Crh_GetUpdateStartResponse_t* instance, const char** var);

/**
 * Set the value of errorMessage
 * The error message in case of error
 */
int Crh_GetUpdateStartResponse_setErrorMessage(struct Crh_GetUpdateStartResponse_t* instance, const char* new_var);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Sets the id of the object
 */
int Crh_GetUpdateStartResponse_setId(struct Crh_GetUpdateStartResponse_t* instance,
                                     u_signed64 id);

/**
 * gets the id of the object
 */
int Crh_GetUpdateStartResponse_id(struct Crh_GetUpdateStartResponse_t* instance,
                                  u_signed64* ret);

/**
 * Gets the type of the object
 */
int Crh_GetUpdateStartResponse_type(struct Crh_GetUpdateStartResponse_t* instance,
                                    int* ret);

/**
 * virtual method to clone any object
 */
int Crh_GetUpdateStartResponse_clone(struct Crh_GetUpdateStartResponse_t* instance,
                                     struct C_IObject_t* ret);

/**
 * Add a struct Cstager_DiskCopyForRecall_t* object to the sources list
 */
int Crh_GetUpdateStartResponse_addSources(struct Crh_GetUpdateStartResponse_t* instance, struct Cstager_DiskCopyForRecall_t* obj);

/**
 * Remove a struct Cstager_DiskCopyForRecall_t* object from sources
 */
int Crh_GetUpdateStartResponse_removeSources(struct Crh_GetUpdateStartResponse_t* instance, struct Cstager_DiskCopyForRecall_t* obj);

/**
 * Get the list of struct Cstager_DiskCopyForRecall_t* objects held by sources
 */
int Crh_GetUpdateStartResponse_sources(struct Crh_GetUpdateStartResponse_t* instance, struct Cstager_DiskCopyForRecall_t*** var, int* len);

#endif // CASTOR_RH_GETUPDATESTARTRESPONSE_H
