/******************************************************************************
 *                      castor/stager/StagePrepareToPutRequest.h
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

#ifndef CASTOR_STAGER_STAGEPREPARETOPUTREQUEST_H
#define CASTOR_STAGER_STAGEPREPARETOPUTREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IClient_t;
struct C_IObject_t;
struct Cstager_FileRequest_t;
struct Cstager_Request_t;
struct Cstager_StagePrepareToPutRequest_t;
struct Cstager_SubRequest_t;
struct Cstager_SvcClass_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StagePrepareToPutRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StagePrepareToPutRequest_create(struct Cstager_StagePrepareToPutRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StagePrepareToPutRequest_delete(struct Cstager_StagePrepareToPutRequest_t* obj);

/**
 * Cast into FileRequest
 */
struct Cstager_FileRequest_t* Cstager_StagePrepareToPutRequest_getFileRequest(struct Cstager_StagePrepareToPutRequest_t* obj);

/**
 * Dynamic cast from FileRequest
 */
struct Cstager_StagePrepareToPutRequest_t* Cstager_StagePrepareToPutRequest_fromFileRequest(struct Cstager_FileRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StagePrepareToPutRequest_getRequest(struct Cstager_StagePrepareToPutRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StagePrepareToPutRequest_t* Cstager_StagePrepareToPutRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_StagePrepareToPutRequest_getIObject(struct Cstager_StagePrepareToPutRequest_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_StagePrepareToPutRequest_t* Cstager_StagePrepareToPutRequest_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StagePrepareToPutRequest_print(struct Cstager_StagePrepareToPutRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StagePrepareToPutRequest_TYPE(int* ret);

/************************************************/
/* Implementation of FileRequest abstract class */
/************************************************/

/**
 * Add a struct Cstager_SubRequest_t* object to the subRequests list
 */
int Cstager_StagePrepareToPutRequest_addSubRequests(struct Cstager_StagePrepareToPutRequest_t* instance, struct Cstager_SubRequest_t* obj);

/**
 * Remove a struct Cstager_SubRequest_t* object from subRequests
 */
int Cstager_StagePrepareToPutRequest_removeSubRequests(struct Cstager_StagePrepareToPutRequest_t* instance, struct Cstager_SubRequest_t* obj);

/**
 * Get the list of struct Cstager_SubRequest_t* objects held by subRequests
 */
int Cstager_StagePrepareToPutRequest_subRequests(struct Cstager_StagePrepareToPutRequest_t* instance, struct Cstager_SubRequest_t*** var, int* len);

/********************************************/
/* Implementation of Request abstract class */
/********************************************/

/**
 * Get the value of flags
 */
int Cstager_StagePrepareToPutRequest_flags(struct Cstager_StagePrepareToPutRequest_t* instance, u_signed64* var);

/**
 * Set the value of flags
 */
int Cstager_StagePrepareToPutRequest_setFlags(struct Cstager_StagePrepareToPutRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of userName
 * Name of the user that submitted the request
 */
int Cstager_StagePrepareToPutRequest_userName(struct Cstager_StagePrepareToPutRequest_t* instance, const char** var);

/**
 * Set the value of userName
 * Name of the user that submitted the request
 */
int Cstager_StagePrepareToPutRequest_setUserName(struct Cstager_StagePrepareToPutRequest_t* instance, const char* new_var);

/**
 * Get the value of euid
 * Id of the user that submitted the request
 */
int Cstager_StagePrepareToPutRequest_euid(struct Cstager_StagePrepareToPutRequest_t* instance, unsigned long* var);

/**
 * Set the value of euid
 * Id of the user that submitted the request
 */
int Cstager_StagePrepareToPutRequest_setEuid(struct Cstager_StagePrepareToPutRequest_t* instance, unsigned long new_var);

/**
 * Get the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_StagePrepareToPutRequest_egid(struct Cstager_StagePrepareToPutRequest_t* instance, unsigned long* var);

/**
 * Set the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_StagePrepareToPutRequest_setEgid(struct Cstager_StagePrepareToPutRequest_t* instance, unsigned long new_var);

/**
 * Get the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_StagePrepareToPutRequest_mask(struct Cstager_StagePrepareToPutRequest_t* instance, unsigned long* var);

/**
 * Set the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_StagePrepareToPutRequest_setMask(struct Cstager_StagePrepareToPutRequest_t* instance, unsigned long new_var);

/**
 * Get the value of pid
 * Process id of the user process
 */
int Cstager_StagePrepareToPutRequest_pid(struct Cstager_StagePrepareToPutRequest_t* instance, unsigned long* var);

/**
 * Set the value of pid
 * Process id of the user process
 */
int Cstager_StagePrepareToPutRequest_setPid(struct Cstager_StagePrepareToPutRequest_t* instance, unsigned long new_var);

/**
 * Get the value of machine
 * The machine that submitted the request
 */
int Cstager_StagePrepareToPutRequest_machine(struct Cstager_StagePrepareToPutRequest_t* instance, const char** var);

/**
 * Set the value of machine
 * The machine that submitted the request
 */
int Cstager_StagePrepareToPutRequest_setMachine(struct Cstager_StagePrepareToPutRequest_t* instance, const char* new_var);

/**
 * Get the value of svcClassName
 */
int Cstager_StagePrepareToPutRequest_svcClassName(struct Cstager_StagePrepareToPutRequest_t* instance, const char** var);

/**
 * Set the value of svcClassName
 */
int Cstager_StagePrepareToPutRequest_setSvcClassName(struct Cstager_StagePrepareToPutRequest_t* instance, const char* new_var);

/**
 * Get the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_StagePrepareToPutRequest_userTag(struct Cstager_StagePrepareToPutRequest_t* instance, const char** var);

/**
 * Set the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_StagePrepareToPutRequest_setUserTag(struct Cstager_StagePrepareToPutRequest_t* instance, const char* new_var);

/**
 * Get the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_StagePrepareToPutRequest_reqId(struct Cstager_StagePrepareToPutRequest_t* instance, const char** var);

/**
 * Set the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_StagePrepareToPutRequest_setReqId(struct Cstager_StagePrepareToPutRequest_t* instance, const char* new_var);

/**
 * Get the value of svcClass
 */
int Cstager_StagePrepareToPutRequest_svcClass(struct Cstager_StagePrepareToPutRequest_t* instance, struct Cstager_SvcClass_t** var);

/**
 * Set the value of svcClass
 */
int Cstager_StagePrepareToPutRequest_setSvcClass(struct Cstager_StagePrepareToPutRequest_t* instance, struct Cstager_SvcClass_t* new_var);

/**
 * Get the value of client
 */
int Cstager_StagePrepareToPutRequest_client(struct Cstager_StagePrepareToPutRequest_t* instance, struct C_IClient_t** var);

/**
 * Set the value of client
 */
int Cstager_StagePrepareToPutRequest_setClient(struct Cstager_StagePrepareToPutRequest_t* instance, struct C_IClient_t* new_var);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Sets the id of the object
 */
int Cstager_StagePrepareToPutRequest_setId(struct Cstager_StagePrepareToPutRequest_t* instance,
                                           u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StagePrepareToPutRequest_id(struct Cstager_StagePrepareToPutRequest_t* instance,
                                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StagePrepareToPutRequest_type(struct Cstager_StagePrepareToPutRequest_t* instance,
                                          int* ret);

/**
 * virtual method to clone any object
 */
int Cstager_StagePrepareToPutRequest_clone(struct Cstager_StagePrepareToPutRequest_t* instance,
                                           struct C_IObject_t* ret);

#endif // CASTOR_STAGER_STAGEPREPARETOPUTREQUEST_H
