/******************************************************************************
 *                      castor/rh/FileResponse.h
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

#ifndef CASTOR_RH_FILERESPONSE_H
#define CASTOR_RH_FILERESPONSE_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct Crh_FileResponse_t;
struct Crh_Response_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class FileResponse
// A response dealing with a castor file
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Crh_FileResponse_create(struct Crh_FileResponse_t** obj);

/**
 * Empty Destructor
 */
int Crh_FileResponse_delete(struct Crh_FileResponse_t* obj);

/**
 * Cast into Response
 */
struct Crh_Response_t* Crh_FileResponse_getResponse(struct Crh_FileResponse_t* obj);

/**
 * Dynamic cast from Response
 */
struct Crh_FileResponse_t* Crh_FileResponse_fromResponse(struct Crh_Response_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Crh_FileResponse_getIObject(struct Crh_FileResponse_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Crh_FileResponse_t* Crh_FileResponse_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Crh_FileResponse_print(struct Crh_FileResponse_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Crh_FileResponse_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/

/**
 * Sets the id of the object
 */
int Crh_FileResponse_setId(struct Crh_FileResponse_t* instance,
                           u_signed64 id);

/**
 * gets the id of the object
 */
int Crh_FileResponse_id(struct Crh_FileResponse_t* instance,
                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Crh_FileResponse_type(struct Crh_FileResponse_t* instance,
                          int* ret);

/**
 * Get the value of status
 * The status a the file we are considering
 */
int Crh_FileResponse_status(struct Crh_FileResponse_t* instance, unsigned int* var);

/**
 * Set the value of status
 * The status a the file we are considering
 */
int Crh_FileResponse_setStatus(struct Crh_FileResponse_t* instance, unsigned int new_var);

/**
 * Get the value of castorFileName
 * The name of the file considered
 */
int Crh_FileResponse_castorFileName(struct Crh_FileResponse_t* instance, const char** var);

/**
 * Set the value of castorFileName
 * The name of the file considered
 */
int Crh_FileResponse_setCastorFileName(struct Crh_FileResponse_t* instance, const char* new_var);

/**
 * Get the value of fileSize
 * The size of the file considered
 */
int Crh_FileResponse_fileSize(struct Crh_FileResponse_t* instance, u_signed64* var);

/**
 * Set the value of fileSize
 * The size of the file considered
 */
int Crh_FileResponse_setFileSize(struct Crh_FileResponse_t* instance, u_signed64 new_var);

/**
 * Get the value of errorCode
 * The error code in case of error
 */
int Crh_FileResponse_errorCode(struct Crh_FileResponse_t* instance, unsigned int* var);

/**
 * Set the value of errorCode
 * The error code in case of error
 */
int Crh_FileResponse_setErrorCode(struct Crh_FileResponse_t* instance, unsigned int new_var);

/**
 * Get the value of errorMessage
 * The error message in case of error
 */
int Crh_FileResponse_errorMessage(struct Crh_FileResponse_t* instance, const char** var);

/**
 * Set the value of errorMessage
 * The error message in case of error
 */
int Crh_FileResponse_setErrorMessage(struct Crh_FileResponse_t* instance, const char* new_var);

/**
 * Get the value of fileId
 * The castor file id identifying the file considered
 */
int Crh_FileResponse_fileId(struct Crh_FileResponse_t* instance, u_signed64* var);

/**
 * Set the value of fileId
 * The castor file id identifying the file considered
 */
int Crh_FileResponse_setFileId(struct Crh_FileResponse_t* instance, u_signed64 new_var);

/**
 * Get the value of subreqId
 * The Cuuid of the SubRequest that dealt with this file, given as a human readable
 * string
 */
int Crh_FileResponse_subreqId(struct Crh_FileResponse_t* instance, const char** var);

/**
 * Set the value of subreqId
 * The Cuuid of the SubRequest that dealt with this file, given as a human readable
 * string
 */
int Crh_FileResponse_setSubreqId(struct Crh_FileResponse_t* instance, const char* new_var);

#endif // CASTOR_RH_FILERESPONSE_H
