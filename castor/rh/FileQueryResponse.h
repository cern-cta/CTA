/******************************************************************************
 *                      castor/rh/FileQueryResponse.h
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

#ifndef CASTOR_RH_FILEQUERYRESPONSE_H
#define CASTOR_RH_FILEQUERYRESPONSE_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct Crh_FileQueryResponse_t;
struct Crh_Response_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class FileQueryResponse
// Response to the FileQueryRequest
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Crh_FileQueryResponse_create(struct Crh_FileQueryResponse_t** obj);

/**
 * Empty Destructor
 */
int Crh_FileQueryResponse_delete(struct Crh_FileQueryResponse_t* obj);

/**
 * Cast into Response
 */
struct Crh_Response_t* Crh_FileQueryResponse_getResponse(struct Crh_FileQueryResponse_t* obj);

/**
 * Dynamic cast from Response
 */
struct Crh_FileQueryResponse_t* Crh_FileQueryResponse_fromResponse(struct Crh_Response_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Crh_FileQueryResponse_getIObject(struct Crh_FileQueryResponse_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Crh_FileQueryResponse_t* Crh_FileQueryResponse_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Crh_FileQueryResponse_print(struct Crh_FileQueryResponse_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Crh_FileQueryResponse_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/

/**
 * Sets the id of the object
 */
int Crh_FileQueryResponse_setId(struct Crh_FileQueryResponse_t* instance,
                                u_signed64 id);

/**
 * gets the id of the object
 */
int Crh_FileQueryResponse_id(struct Crh_FileQueryResponse_t* instance,
                             u_signed64* ret);

/**
 * Gets the type of the object
 */
int Crh_FileQueryResponse_type(struct Crh_FileQueryResponse_t* instance,
                               int* ret);

/**
 * virtual method to clone any object
 */
int Crh_FileQueryResponse_clone(struct Crh_FileQueryResponse_t* instance,
                                struct C_IObject_t* ret);

/**
 * Get the value of fileName
 * Name of the file
 */
int Crh_FileQueryResponse_fileName(struct Crh_FileQueryResponse_t* instance, const char** var);

/**
 * Set the value of fileName
 * Name of the file
 */
int Crh_FileQueryResponse_setFileName(struct Crh_FileQueryResponse_t* instance, const char* new_var);

/**
 * Get the value of fileId
 * Castor FileId for this file
 */
int Crh_FileQueryResponse_fileId(struct Crh_FileQueryResponse_t* instance, u_signed64* var);

/**
 * Set the value of fileId
 * Castor FileId for this file
 */
int Crh_FileQueryResponse_setFileId(struct Crh_FileQueryResponse_t* instance, u_signed64 new_var);

/**
 * Get the value of status
 * Status of the file
 */
int Crh_FileQueryResponse_status(struct Crh_FileQueryResponse_t* instance, unsigned int* var);

/**
 * Set the value of status
 * Status of the file
 */
int Crh_FileQueryResponse_setStatus(struct Crh_FileQueryResponse_t* instance, unsigned int new_var);

/**
 * Get the value of size
 * Size of the file
 */
int Crh_FileQueryResponse_size(struct Crh_FileQueryResponse_t* instance, u_signed64* var);

/**
 * Set the value of size
 * Size of the file
 */
int Crh_FileQueryResponse_setSize(struct Crh_FileQueryResponse_t* instance, u_signed64 new_var);

/**
 * Get the value of poolName
 * Name of the pool containing the file
 */
int Crh_FileQueryResponse_poolName(struct Crh_FileQueryResponse_t* instance, const char** var);

/**
 * Set the value of poolName
 * Name of the pool containing the file
 */
int Crh_FileQueryResponse_setPoolName(struct Crh_FileQueryResponse_t* instance, const char* new_var);

/**
 * Get the value of creationTime
 * Time of the file creation
 */
int Crh_FileQueryResponse_creationTime(struct Crh_FileQueryResponse_t* instance, u_signed64* var);

/**
 * Set the value of creationTime
 * Time of the file creation
 */
int Crh_FileQueryResponse_setCreationTime(struct Crh_FileQueryResponse_t* instance, u_signed64 new_var);

/**
 * Get the value of accessTime
 * Time of the last access to the file
 */
int Crh_FileQueryResponse_accessTime(struct Crh_FileQueryResponse_t* instance, u_signed64* var);

/**
 * Set the value of accessTime
 * Time of the last access to the file
 */
int Crh_FileQueryResponse_setAccessTime(struct Crh_FileQueryResponse_t* instance, u_signed64 new_var);

/**
 * Get the value of nbAccesses
 * Number of accesses to the file
 */
int Crh_FileQueryResponse_nbAccesses(struct Crh_FileQueryResponse_t* instance, unsigned int* var);

/**
 * Set the value of nbAccesses
 * Number of accesses to the file
 */
int Crh_FileQueryResponse_setNbAccesses(struct Crh_FileQueryResponse_t* instance, unsigned int new_var);

#endif // CASTOR_RH_FILEQUERYRESPONSE_H
