/******************************************************************************
 *                      castor/stager/StageRequestQueryRequest.h
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

#ifndef CASTOR_STAGER_STAGEREQUESTQUERYREQUEST_H
#define CASTOR_STAGER_STAGEREQUESTQUERYREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IClient_t;
struct C_IObject_t;
struct Cstager_QryRequest_t;
struct Cstager_Request_t;
struct Cstager_StageRequestQueryRequest_t;
struct Cstager_SvcClass_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageRequestQueryRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageRequestQueryRequest_create(struct Cstager_StageRequestQueryRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageRequestQueryRequest_delete(struct Cstager_StageRequestQueryRequest_t* obj);

/**
 * Cast into QryRequest
 */
struct Cstager_QryRequest_t* Cstager_StageRequestQueryRequest_getQryRequest(struct Cstager_StageRequestQueryRequest_t* obj);

/**
 * Dynamic cast from QryRequest
 */
struct Cstager_StageRequestQueryRequest_t* Cstager_StageRequestQueryRequest_fromQryRequest(struct Cstager_QryRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageRequestQueryRequest_getRequest(struct Cstager_StageRequestQueryRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageRequestQueryRequest_t* Cstager_StageRequestQueryRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_StageRequestQueryRequest_getIObject(struct Cstager_StageRequestQueryRequest_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_StageRequestQueryRequest_t* Cstager_StageRequestQueryRequest_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageRequestQueryRequest_print(struct Cstager_StageRequestQueryRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageRequestQueryRequest_TYPE(int* ret);

/********************************************/
/* Implementation of Request abstract class */
/********************************************/

/**
 * Get the value of flags
 */
int Cstager_StageRequestQueryRequest_flags(struct Cstager_StageRequestQueryRequest_t* instance, u_signed64* var);

/**
 * Set the value of flags
 */
int Cstager_StageRequestQueryRequest_setFlags(struct Cstager_StageRequestQueryRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of userName
 * Name of the user that submitted the request
 */
int Cstager_StageRequestQueryRequest_userName(struct Cstager_StageRequestQueryRequest_t* instance, const char** var);

/**
 * Set the value of userName
 * Name of the user that submitted the request
 */
int Cstager_StageRequestQueryRequest_setUserName(struct Cstager_StageRequestQueryRequest_t* instance, const char* new_var);

/**
 * Get the value of euid
 * Id of the user that submitted the request
 */
int Cstager_StageRequestQueryRequest_euid(struct Cstager_StageRequestQueryRequest_t* instance, unsigned long* var);

/**
 * Set the value of euid
 * Id of the user that submitted the request
 */
int Cstager_StageRequestQueryRequest_setEuid(struct Cstager_StageRequestQueryRequest_t* instance, unsigned long new_var);

/**
 * Get the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_StageRequestQueryRequest_egid(struct Cstager_StageRequestQueryRequest_t* instance, unsigned long* var);

/**
 * Set the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_StageRequestQueryRequest_setEgid(struct Cstager_StageRequestQueryRequest_t* instance, unsigned long new_var);

/**
 * Get the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_StageRequestQueryRequest_mask(struct Cstager_StageRequestQueryRequest_t* instance, unsigned long* var);

/**
 * Set the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_StageRequestQueryRequest_setMask(struct Cstager_StageRequestQueryRequest_t* instance, unsigned long new_var);

/**
 * Get the value of pid
 * Process id of the user process
 */
int Cstager_StageRequestQueryRequest_pid(struct Cstager_StageRequestQueryRequest_t* instance, unsigned long* var);

/**
 * Set the value of pid
 * Process id of the user process
 */
int Cstager_StageRequestQueryRequest_setPid(struct Cstager_StageRequestQueryRequest_t* instance, unsigned long new_var);

/**
 * Get the value of machine
 * The machine that submitted the request
 */
int Cstager_StageRequestQueryRequest_machine(struct Cstager_StageRequestQueryRequest_t* instance, const char** var);

/**
 * Set the value of machine
 * The machine that submitted the request
 */
int Cstager_StageRequestQueryRequest_setMachine(struct Cstager_StageRequestQueryRequest_t* instance, const char* new_var);

/**
 * Get the value of svcClassName
 */
int Cstager_StageRequestQueryRequest_svcClassName(struct Cstager_StageRequestQueryRequest_t* instance, const char** var);

/**
 * Set the value of svcClassName
 */
int Cstager_StageRequestQueryRequest_setSvcClassName(struct Cstager_StageRequestQueryRequest_t* instance, const char* new_var);

/**
 * Get the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_StageRequestQueryRequest_userTag(struct Cstager_StageRequestQueryRequest_t* instance, const char** var);

/**
 * Set the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_StageRequestQueryRequest_setUserTag(struct Cstager_StageRequestQueryRequest_t* instance, const char* new_var);

/**
 * Get the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_StageRequestQueryRequest_reqId(struct Cstager_StageRequestQueryRequest_t* instance, const char** var);

/**
 * Set the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_StageRequestQueryRequest_setReqId(struct Cstager_StageRequestQueryRequest_t* instance, const char* new_var);

/**
 * Get the value of svcClass
 */
int Cstager_StageRequestQueryRequest_svcClass(struct Cstager_StageRequestQueryRequest_t* instance, struct Cstager_SvcClass_t** var);

/**
 * Set the value of svcClass
 */
int Cstager_StageRequestQueryRequest_setSvcClass(struct Cstager_StageRequestQueryRequest_t* instance, struct Cstager_SvcClass_t* new_var);

/**
 * Get the value of client
 */
int Cstager_StageRequestQueryRequest_client(struct Cstager_StageRequestQueryRequest_t* instance, struct C_IClient_t** var);

/**
 * Set the value of client
 */
int Cstager_StageRequestQueryRequest_setClient(struct Cstager_StageRequestQueryRequest_t* instance, struct C_IClient_t* new_var);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/

/**
 * Sets the id of the object
 */
int Cstager_StageRequestQueryRequest_setId(struct Cstager_StageRequestQueryRequest_t* instance,
                                           u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageRequestQueryRequest_id(struct Cstager_StageRequestQueryRequest_t* instance,
                                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageRequestQueryRequest_type(struct Cstager_StageRequestQueryRequest_t* instance,
                                          int* ret);

/**
 * virtual method to clone any object
 */
int Cstager_StageRequestQueryRequest_clone(struct Cstager_StageRequestQueryRequest_t* instance,
                                           struct C_IObject_t* ret);

#endif // CASTOR_STAGER_STAGEREQUESTQUERYREQUEST_H
