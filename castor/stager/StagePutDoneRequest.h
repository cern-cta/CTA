/******************************************************************************
 *                      castor/stager/StagePutDoneRequest.h
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

#ifndef CASTOR_STAGER_STAGEPUTDONEREQUEST_H
#define CASTOR_STAGER_STAGEPUTDONEREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IClient_t;
struct C_IObject_t;
struct Cstager_FileRequest_t;
struct Cstager_Request_t;
struct Cstager_StagePutDoneRequest_t;
struct Cstager_SubRequest_t;
struct Cstager_SvcClass_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StagePutDoneRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StagePutDoneRequest_create(struct Cstager_StagePutDoneRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StagePutDoneRequest_delete(struct Cstager_StagePutDoneRequest_t* obj);

/**
 * Cast into FileRequest
 */
struct Cstager_FileRequest_t* Cstager_StagePutDoneRequest_getFileRequest(struct Cstager_StagePutDoneRequest_t* obj);

/**
 * Dynamic cast from FileRequest
 */
struct Cstager_StagePutDoneRequest_t* Cstager_StagePutDoneRequest_fromFileRequest(struct Cstager_FileRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StagePutDoneRequest_getRequest(struct Cstager_StagePutDoneRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StagePutDoneRequest_t* Cstager_StagePutDoneRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_StagePutDoneRequest_getIObject(struct Cstager_StagePutDoneRequest_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_StagePutDoneRequest_t* Cstager_StagePutDoneRequest_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StagePutDoneRequest_print(struct Cstager_StagePutDoneRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StagePutDoneRequest_TYPE(int* ret);

/************************************************/
/* Implementation of FileRequest abstract class */
/************************************************/

/**
 * Add a struct Cstager_SubRequest_t* object to the subRequests list
 */
int Cstager_StagePutDoneRequest_addSubRequests(struct Cstager_StagePutDoneRequest_t* instance, struct Cstager_SubRequest_t* obj);

/**
 * Remove a struct Cstager_SubRequest_t* object from subRequests
 */
int Cstager_StagePutDoneRequest_removeSubRequests(struct Cstager_StagePutDoneRequest_t* instance, struct Cstager_SubRequest_t* obj);

/**
 * Get the list of struct Cstager_SubRequest_t* objects held by subRequests. Note
 * that the caller is responsible for the deletion of the returned vector.
 */
int Cstager_StagePutDoneRequest_subRequests(struct Cstager_StagePutDoneRequest_t* instance, struct Cstager_SubRequest_t*** var, int* len);

/********************************************/
/* Implementation of Request abstract class */
/********************************************/

/**
 * Get the value of flags
 */
int Cstager_StagePutDoneRequest_flags(struct Cstager_StagePutDoneRequest_t* instance, u_signed64* var);

/**
 * Set the value of flags
 */
int Cstager_StagePutDoneRequest_setFlags(struct Cstager_StagePutDoneRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of userName
 * Name of the user that submitted the request
 */
int Cstager_StagePutDoneRequest_userName(struct Cstager_StagePutDoneRequest_t* instance, const char** var);

/**
 * Set the value of userName
 * Name of the user that submitted the request
 */
int Cstager_StagePutDoneRequest_setUserName(struct Cstager_StagePutDoneRequest_t* instance, const char* new_var);

/**
 * Get the value of euid
 * Id of the user that submitted the request
 */
int Cstager_StagePutDoneRequest_euid(struct Cstager_StagePutDoneRequest_t* instance, unsigned long* var);

/**
 * Set the value of euid
 * Id of the user that submitted the request
 */
int Cstager_StagePutDoneRequest_setEuid(struct Cstager_StagePutDoneRequest_t* instance, unsigned long new_var);

/**
 * Get the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_StagePutDoneRequest_egid(struct Cstager_StagePutDoneRequest_t* instance, unsigned long* var);

/**
 * Set the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_StagePutDoneRequest_setEgid(struct Cstager_StagePutDoneRequest_t* instance, unsigned long new_var);

/**
 * Get the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_StagePutDoneRequest_mask(struct Cstager_StagePutDoneRequest_t* instance, unsigned long* var);

/**
 * Set the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_StagePutDoneRequest_setMask(struct Cstager_StagePutDoneRequest_t* instance, unsigned long new_var);

/**
 * Get the value of pid
 * Process id of the user process
 */
int Cstager_StagePutDoneRequest_pid(struct Cstager_StagePutDoneRequest_t* instance, unsigned long* var);

/**
 * Set the value of pid
 * Process id of the user process
 */
int Cstager_StagePutDoneRequest_setPid(struct Cstager_StagePutDoneRequest_t* instance, unsigned long new_var);

/**
 * Get the value of machine
 * The machine that submitted the request
 */
int Cstager_StagePutDoneRequest_machine(struct Cstager_StagePutDoneRequest_t* instance, const char** var);

/**
 * Set the value of machine
 * The machine that submitted the request
 */
int Cstager_StagePutDoneRequest_setMachine(struct Cstager_StagePutDoneRequest_t* instance, const char* new_var);

/**
 * Get the value of svcClassName
 */
int Cstager_StagePutDoneRequest_svcClassName(struct Cstager_StagePutDoneRequest_t* instance, const char** var);

/**
 * Set the value of svcClassName
 */
int Cstager_StagePutDoneRequest_setSvcClassName(struct Cstager_StagePutDoneRequest_t* instance, const char* new_var);

/**
 * Get the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_StagePutDoneRequest_userTag(struct Cstager_StagePutDoneRequest_t* instance, const char** var);

/**
 * Set the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_StagePutDoneRequest_setUserTag(struct Cstager_StagePutDoneRequest_t* instance, const char* new_var);

/**
 * Get the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_StagePutDoneRequest_reqId(struct Cstager_StagePutDoneRequest_t* instance, const char** var);

/**
 * Set the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_StagePutDoneRequest_setReqId(struct Cstager_StagePutDoneRequest_t* instance, const char* new_var);

/**
 * Get the value of creationTime
 * Time when the Request was created
 */
int Cstager_StagePutDoneRequest_creationTime(struct Cstager_StagePutDoneRequest_t* instance, u_signed64* var);

/**
 * Set the value of creationTime
 * Time when the Request was created
 */
int Cstager_StagePutDoneRequest_setCreationTime(struct Cstager_StagePutDoneRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of lastModificationTime
 * Time when the request was last modified
 */
int Cstager_StagePutDoneRequest_lastModificationTime(struct Cstager_StagePutDoneRequest_t* instance, u_signed64* var);

/**
 * Set the value of lastModificationTime
 * Time when the request was last modified
 */
int Cstager_StagePutDoneRequest_setLastModificationTime(struct Cstager_StagePutDoneRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of svcClass
 */
int Cstager_StagePutDoneRequest_svcClass(struct Cstager_StagePutDoneRequest_t* instance, struct Cstager_SvcClass_t** var);

/**
 * Set the value of svcClass
 */
int Cstager_StagePutDoneRequest_setSvcClass(struct Cstager_StagePutDoneRequest_t* instance, struct Cstager_SvcClass_t* new_var);

/**
 * Get the value of client
 */
int Cstager_StagePutDoneRequest_client(struct Cstager_StagePutDoneRequest_t* instance, struct C_IClient_t** var);

/**
 * Set the value of client
 */
int Cstager_StagePutDoneRequest_setClient(struct Cstager_StagePutDoneRequest_t* instance, struct C_IClient_t* new_var);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Gets the type of the object
 */
int Cstager_StagePutDoneRequest_type(struct Cstager_StagePutDoneRequest_t* instance,
                                     int* ret);

/**
 * virtual method to clone any object
 */
int Cstager_StagePutDoneRequest_clone(struct Cstager_StagePutDoneRequest_t* instance,
                                      struct C_IObject_t* ret);

/**
 * Get the value of parentUuid
 * The UUID of the parent request. This is used by clients to build the request and
 * is converted to a link to an existing FileRequest in the stager
 */
int Cstager_StagePutDoneRequest_parentUuid(struct Cstager_StagePutDoneRequest_t* instance, const char** var);

/**
 * Set the value of parentUuid
 * The UUID of the parent request. This is used by clients to build the request and
 * is converted to a link to an existing FileRequest in the stager
 */
int Cstager_StagePutDoneRequest_setParentUuid(struct Cstager_StagePutDoneRequest_t* instance, const char* new_var);

/**
 * Get the value of id
 * The id of this object
 */
int Cstager_StagePutDoneRequest_id(struct Cstager_StagePutDoneRequest_t* instance, u_signed64* var);

/**
 * Set the value of id
 * The id of this object
 */
int Cstager_StagePutDoneRequest_setId(struct Cstager_StagePutDoneRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of parent
 */
int Cstager_StagePutDoneRequest_parent(struct Cstager_StagePutDoneRequest_t* instance, struct Cstager_FileRequest_t** var);

/**
 * Set the value of parent
 */
int Cstager_StagePutDoneRequest_setParent(struct Cstager_StagePutDoneRequest_t* instance, struct Cstager_FileRequest_t* new_var);

#endif // CASTOR_STAGER_STAGEPUTDONEREQUEST_H
