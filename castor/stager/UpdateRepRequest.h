/******************************************************************************
 *                      castor/stager/UpdateRepRequest.h
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
 * @(#)$RCSfile: UpdateRepRequest.h,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/30 11:28:00 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_UPDATEREPREQUEST_H
#define CASTOR_STAGER_UPDATEREPREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IAddress_t;
struct C_IClient_t;
struct C_IObject_t;
struct Cstager_Request_t;
struct Cstager_SvcClass_t;
struct Cstager_UpdateRepRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class UpdateRepRequest
// Internal request for updating an object in the database. This request is there to
// avoid the jobs on the diskservers to handle a connection to the database.
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_UpdateRepRequest_create(struct Cstager_UpdateRepRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_UpdateRepRequest_delete(struct Cstager_UpdateRepRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_UpdateRepRequest_getRequest(struct Cstager_UpdateRepRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_UpdateRepRequest_t* Cstager_UpdateRepRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_UpdateRepRequest_getIObject(struct Cstager_UpdateRepRequest_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_UpdateRepRequest_t* Cstager_UpdateRepRequest_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_UpdateRepRequest_print(struct Cstager_UpdateRepRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_UpdateRepRequest_TYPE(int* ret);

/********************************************/
/* Implementation of Request abstract class */
/********************************************/

/**
 * Get the value of flags
 */
int Cstager_UpdateRepRequest_flags(struct Cstager_UpdateRepRequest_t* instance, u_signed64* var);

/**
 * Set the value of flags
 */
int Cstager_UpdateRepRequest_setFlags(struct Cstager_UpdateRepRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of userName
 * Name of the user that submitted the request
 */
int Cstager_UpdateRepRequest_userName(struct Cstager_UpdateRepRequest_t* instance, const char** var);

/**
 * Set the value of userName
 * Name of the user that submitted the request
 */
int Cstager_UpdateRepRequest_setUserName(struct Cstager_UpdateRepRequest_t* instance, const char* new_var);

/**
 * Get the value of euid
 * Id of the user that submitted the request
 */
int Cstager_UpdateRepRequest_euid(struct Cstager_UpdateRepRequest_t* instance, unsigned long* var);

/**
 * Set the value of euid
 * Id of the user that submitted the request
 */
int Cstager_UpdateRepRequest_setEuid(struct Cstager_UpdateRepRequest_t* instance, unsigned long new_var);

/**
 * Get the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_UpdateRepRequest_egid(struct Cstager_UpdateRepRequest_t* instance, unsigned long* var);

/**
 * Set the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_UpdateRepRequest_setEgid(struct Cstager_UpdateRepRequest_t* instance, unsigned long new_var);

/**
 * Get the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_UpdateRepRequest_mask(struct Cstager_UpdateRepRequest_t* instance, unsigned long* var);

/**
 * Set the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_UpdateRepRequest_setMask(struct Cstager_UpdateRepRequest_t* instance, unsigned long new_var);

/**
 * Get the value of pid
 * Process id of the user process
 */
int Cstager_UpdateRepRequest_pid(struct Cstager_UpdateRepRequest_t* instance, unsigned long* var);

/**
 * Set the value of pid
 * Process id of the user process
 */
int Cstager_UpdateRepRequest_setPid(struct Cstager_UpdateRepRequest_t* instance, unsigned long new_var);

/**
 * Get the value of machine
 * The machine that submitted the request
 */
int Cstager_UpdateRepRequest_machine(struct Cstager_UpdateRepRequest_t* instance, const char** var);

/**
 * Set the value of machine
 * The machine that submitted the request
 */
int Cstager_UpdateRepRequest_setMachine(struct Cstager_UpdateRepRequest_t* instance, const char* new_var);

/**
 * Get the value of svcClassName
 */
int Cstager_UpdateRepRequest_svcClassName(struct Cstager_UpdateRepRequest_t* instance, const char** var);

/**
 * Set the value of svcClassName
 */
int Cstager_UpdateRepRequest_setSvcClassName(struct Cstager_UpdateRepRequest_t* instance, const char* new_var);

/**
 * Get the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_UpdateRepRequest_userTag(struct Cstager_UpdateRepRequest_t* instance, const char** var);

/**
 * Set the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_UpdateRepRequest_setUserTag(struct Cstager_UpdateRepRequest_t* instance, const char* new_var);

/**
 * Get the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_UpdateRepRequest_reqId(struct Cstager_UpdateRepRequest_t* instance, const char** var);

/**
 * Set the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_UpdateRepRequest_setReqId(struct Cstager_UpdateRepRequest_t* instance, const char* new_var);

/**
 * Get the value of svcClass
 */
int Cstager_UpdateRepRequest_svcClass(struct Cstager_UpdateRepRequest_t* instance, struct Cstager_SvcClass_t** var);

/**
 * Set the value of svcClass
 */
int Cstager_UpdateRepRequest_setSvcClass(struct Cstager_UpdateRepRequest_t* instance, struct Cstager_SvcClass_t* new_var);

/**
 * Get the value of client
 */
int Cstager_UpdateRepRequest_client(struct Cstager_UpdateRepRequest_t* instance, struct C_IClient_t** var);

/**
 * Set the value of client
 */
int Cstager_UpdateRepRequest_setClient(struct Cstager_UpdateRepRequest_t* instance, struct C_IClient_t* new_var);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Gets the type of the object
 */
int Cstager_UpdateRepRequest_type(struct Cstager_UpdateRepRequest_t* instance,
                                  int* ret);

/**
 * virtual method to clone any object
 */
int Cstager_UpdateRepRequest_clone(struct Cstager_UpdateRepRequest_t* instance,
                                   struct C_IObject_t* ret);

/**
 * Get the value of id
 * The id of this object
 */
int Cstager_UpdateRepRequest_id(struct Cstager_UpdateRepRequest_t* instance, u_signed64* var);

/**
 * Set the value of id
 * The id of this object
 */
int Cstager_UpdateRepRequest_setId(struct Cstager_UpdateRepRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of object
 */
int Cstager_UpdateRepRequest_object(struct Cstager_UpdateRepRequest_t* instance, struct C_IObject_t** var);

/**
 * Set the value of object
 */
int Cstager_UpdateRepRequest_setObject(struct Cstager_UpdateRepRequest_t* instance, struct C_IObject_t* new_var);

/**
 * Get the value of address
 */
int Cstager_UpdateRepRequest_address(struct Cstager_UpdateRepRequest_t* instance, struct C_IAddress_t** var);

/**
 * Set the value of address
 */
int Cstager_UpdateRepRequest_setAddress(struct Cstager_UpdateRepRequest_t* instance, struct C_IAddress_t* new_var);

#endif // CASTOR_STAGER_UPDATEREPREQUEST_H
