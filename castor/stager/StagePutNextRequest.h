/******************************************************************************
 *                      castor/stager/StagePutNextRequest.h
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

#ifndef CASTOR_STAGER_STAGEPUTNEXTREQUEST_H
#define CASTOR_STAGER_STAGEPUTNEXTREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IClient_t;
struct C_IObject_t;
struct Cstager_FileRequest_t;
struct Cstager_ReqIdRequest_t;
struct Cstager_Request_t;
struct Cstager_StagePutNextRequest_t;
struct Cstager_SvcClass_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StagePutNextRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StagePutNextRequest_create(struct Cstager_StagePutNextRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StagePutNextRequest_delete(struct Cstager_StagePutNextRequest_t* obj);

/**
 * Cast into ReqIdRequest
 */
struct Cstager_ReqIdRequest_t* Cstager_StagePutNextRequest_getReqIdRequest(struct Cstager_StagePutNextRequest_t* obj);

/**
 * Dynamic cast from ReqIdRequest
 */
struct Cstager_StagePutNextRequest_t* Cstager_StagePutNextRequest_fromReqIdRequest(struct Cstager_ReqIdRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StagePutNextRequest_getRequest(struct Cstager_StagePutNextRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StagePutNextRequest_t* Cstager_StagePutNextRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_StagePutNextRequest_getIObject(struct Cstager_StagePutNextRequest_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_StagePutNextRequest_t* Cstager_StagePutNextRequest_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StagePutNextRequest_print(struct Cstager_StagePutNextRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StagePutNextRequest_TYPE(int* ret);

/*************************************************/
/* Implementation of ReqIdRequest abstract class */
/*************************************************/

/**
 * Get the value of parent
 */
int Cstager_StagePutNextRequest_parent(struct Cstager_StagePutNextRequest_t* instance, struct Cstager_FileRequest_t** var);

/**
 * Set the value of parent
 */
int Cstager_StagePutNextRequest_setParent(struct Cstager_StagePutNextRequest_t* instance, struct Cstager_FileRequest_t* new_var);

/********************************************/
/* Implementation of Request abstract class */
/********************************************/

/**
 * Get the value of flags
 */
int Cstager_StagePutNextRequest_flags(struct Cstager_StagePutNextRequest_t* instance, u_signed64* var);

/**
 * Set the value of flags
 */
int Cstager_StagePutNextRequest_setFlags(struct Cstager_StagePutNextRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of userName
 * Name of the user that submitted the request
 */
int Cstager_StagePutNextRequest_userName(struct Cstager_StagePutNextRequest_t* instance, const char** var);

/**
 * Set the value of userName
 * Name of the user that submitted the request
 */
int Cstager_StagePutNextRequest_setUserName(struct Cstager_StagePutNextRequest_t* instance, const char* new_var);

/**
 * Get the value of euid
 * Id of the user that submitted the request
 */
int Cstager_StagePutNextRequest_euid(struct Cstager_StagePutNextRequest_t* instance, unsigned long* var);

/**
 * Set the value of euid
 * Id of the user that submitted the request
 */
int Cstager_StagePutNextRequest_setEuid(struct Cstager_StagePutNextRequest_t* instance, unsigned long new_var);

/**
 * Get the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_StagePutNextRequest_egid(struct Cstager_StagePutNextRequest_t* instance, unsigned long* var);

/**
 * Set the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_StagePutNextRequest_setEgid(struct Cstager_StagePutNextRequest_t* instance, unsigned long new_var);

/**
 * Get the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_StagePutNextRequest_mask(struct Cstager_StagePutNextRequest_t* instance, unsigned long* var);

/**
 * Set the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_StagePutNextRequest_setMask(struct Cstager_StagePutNextRequest_t* instance, unsigned long new_var);

/**
 * Get the value of pid
 * Process id of the user process
 */
int Cstager_StagePutNextRequest_pid(struct Cstager_StagePutNextRequest_t* instance, unsigned long* var);

/**
 * Set the value of pid
 * Process id of the user process
 */
int Cstager_StagePutNextRequest_setPid(struct Cstager_StagePutNextRequest_t* instance, unsigned long new_var);

/**
 * Get the value of machine
 * The machine that submitted the request
 */
int Cstager_StagePutNextRequest_machine(struct Cstager_StagePutNextRequest_t* instance, const char** var);

/**
 * Set the value of machine
 * The machine that submitted the request
 */
int Cstager_StagePutNextRequest_setMachine(struct Cstager_StagePutNextRequest_t* instance, const char* new_var);

/**
 * Get the value of svcClassName
 */
int Cstager_StagePutNextRequest_svcClassName(struct Cstager_StagePutNextRequest_t* instance, const char** var);

/**
 * Set the value of svcClassName
 */
int Cstager_StagePutNextRequest_setSvcClassName(struct Cstager_StagePutNextRequest_t* instance, const char* new_var);

/**
 * Get the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_StagePutNextRequest_userTag(struct Cstager_StagePutNextRequest_t* instance, const char** var);

/**
 * Set the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_StagePutNextRequest_setUserTag(struct Cstager_StagePutNextRequest_t* instance, const char* new_var);

/**
 * Get the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_StagePutNextRequest_reqId(struct Cstager_StagePutNextRequest_t* instance, const char** var);

/**
 * Set the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_StagePutNextRequest_setReqId(struct Cstager_StagePutNextRequest_t* instance, const char* new_var);

/**
 * Get the value of svcClass
 */
int Cstager_StagePutNextRequest_svcClass(struct Cstager_StagePutNextRequest_t* instance, struct Cstager_SvcClass_t** var);

/**
 * Set the value of svcClass
 */
int Cstager_StagePutNextRequest_setSvcClass(struct Cstager_StagePutNextRequest_t* instance, struct Cstager_SvcClass_t* new_var);

/**
 * Get the value of client
 */
int Cstager_StagePutNextRequest_client(struct Cstager_StagePutNextRequest_t* instance, struct C_IClient_t** var);

/**
 * Set the value of client
 */
int Cstager_StagePutNextRequest_setClient(struct Cstager_StagePutNextRequest_t* instance, struct C_IClient_t* new_var);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Gets the type of the object
 */
int Cstager_StagePutNextRequest_type(struct Cstager_StagePutNextRequest_t* instance,
                                     int* ret);

/**
 * virtual method to clone any object
 */
int Cstager_StagePutNextRequest_clone(struct Cstager_StagePutNextRequest_t* instance,
                                      struct C_IObject_t* ret);

/**
 * Get the value of id
 * The id of this object
 */
int Cstager_StagePutNextRequest_id(struct Cstager_StagePutNextRequest_t* instance, u_signed64* var);

/**
 * Set the value of id
 * The id of this object
 */
int Cstager_StagePutNextRequest_setId(struct Cstager_StagePutNextRequest_t* instance, u_signed64 new_var);

#endif // CASTOR_STAGER_STAGEPUTNEXTREQUEST_H
