/******************************************************************************
 *                      castor/rh/GCFilesResponse.h
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

#ifndef CASTOR_RH_GCFILESRESPONSE_H
#define CASTOR_RH_GCFILESRESPONSE_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct Crh_GCFilesResponse_t;
struct Crh_Response_t;
struct Cstager_GCLocalFile_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class GCFilesResponse
// A list of files to be deleted on a diskServer. Answer to a Files2Delete Request
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Crh_GCFilesResponse_create(struct Crh_GCFilesResponse_t** obj);

/**
 * Empty Destructor
 */
int Crh_GCFilesResponse_delete(struct Crh_GCFilesResponse_t* obj);

/**
 * Cast into Response
 */
struct Crh_Response_t* Crh_GCFilesResponse_getResponse(struct Crh_GCFilesResponse_t* obj);

/**
 * Dynamic cast from Response
 */
struct Crh_GCFilesResponse_t* Crh_GCFilesResponse_fromResponse(struct Crh_Response_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Crh_GCFilesResponse_getIObject(struct Crh_GCFilesResponse_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Crh_GCFilesResponse_t* Crh_GCFilesResponse_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Crh_GCFilesResponse_print(struct Crh_GCFilesResponse_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Crh_GCFilesResponse_TYPE(int* ret);

/*********************************************/
/* Implementation of Response abstract class */
/*********************************************/

/**
 * Get the value of errorCode
 * The error code in case of error
 */
int Crh_GCFilesResponse_errorCode(struct Crh_GCFilesResponse_t* instance, unsigned int* var);

/**
 * Set the value of errorCode
 * The error code in case of error
 */
int Crh_GCFilesResponse_setErrorCode(struct Crh_GCFilesResponse_t* instance, unsigned int new_var);

/**
 * Get the value of errorMessage
 * The error message in case of error
 */
int Crh_GCFilesResponse_errorMessage(struct Crh_GCFilesResponse_t* instance, const char** var);

/**
 * Set the value of errorMessage
 * The error message in case of error
 */
int Crh_GCFilesResponse_setErrorMessage(struct Crh_GCFilesResponse_t* instance, const char* new_var);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Gets the type of the object
 */
int Crh_GCFilesResponse_type(struct Crh_GCFilesResponse_t* instance,
                             int* ret);

/**
 * virtual method to clone any object
 */
int Crh_GCFilesResponse_clone(struct Crh_GCFilesResponse_t* instance,
                              struct C_IObject_t* ret);

/**
 * Get the value of id
 * The id of this object
 */
int Crh_GCFilesResponse_id(struct Crh_GCFilesResponse_t* instance, u_signed64* var);

/**
 * Set the value of id
 * The id of this object
 */
int Crh_GCFilesResponse_setId(struct Crh_GCFilesResponse_t* instance, u_signed64 new_var);

/**
 * Add a struct Cstager_GCLocalFile_t* object to the files list
 */
int Crh_GCFilesResponse_addFiles(struct Crh_GCFilesResponse_t* instance, struct Cstager_GCLocalFile_t* obj);

/**
 * Remove a struct Cstager_GCLocalFile_t* object from files
 */
int Crh_GCFilesResponse_removeFiles(struct Crh_GCFilesResponse_t* instance, struct Cstager_GCLocalFile_t* obj);

/**
 * Get the list of struct Cstager_GCLocalFile_t* objects held by files
 */
int Crh_GCFilesResponse_files(struct Crh_GCFilesResponse_t* instance, struct Cstager_GCLocalFile_t*** var, int* len);

#endif // CASTOR_RH_GCFILESRESPONSE_H
