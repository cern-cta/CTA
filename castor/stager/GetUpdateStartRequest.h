/******************************************************************************
 *                      castor/stager/GetUpdateStartRequest.h
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

#ifndef CASTOR_STAGER_GETUPDATESTARTREQUEST_H
#define CASTOR_STAGER_GETUPDATESTARTREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IClient_t;
struct C_IObject_t;
struct Cstager_GetUpdateStartRequest_t;
struct Cstager_Request_t;
struct Cstager_StartRequest_t;
struct Cstager_SvcClass_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class GetUpdateStartRequest
// Internal request used when a get or update job has just started. It does the
// scheduling of the given subrequest and returns the client information. This
// request exists to avoid the jobs on the diskservers to handle a connection to the
// database. 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_GetUpdateStartRequest_create(struct Cstager_GetUpdateStartRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_GetUpdateStartRequest_delete(struct Cstager_GetUpdateStartRequest_t* obj);

/**
 * Cast into StartRequest
 */
struct Cstager_StartRequest_t* Cstager_GetUpdateStartRequest_getStartRequest(struct Cstager_GetUpdateStartRequest_t* obj);

/**
 * Dynamic cast from StartRequest
 */
struct Cstager_GetUpdateStartRequest_t* Cstager_GetUpdateStartRequest_fromStartRequest(struct Cstager_StartRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_GetUpdateStartRequest_getRequest(struct Cstager_GetUpdateStartRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_GetUpdateStartRequest_t* Cstager_GetUpdateStartRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_GetUpdateStartRequest_getIObject(struct Cstager_GetUpdateStartRequest_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_GetUpdateStartRequest_t* Cstager_GetUpdateStartRequest_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_GetUpdateStartRequest_print(struct Cstager_GetUpdateStartRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_GetUpdateStartRequest_TYPE(int* ret);

/*************************************************/
/* Implementation of StartRequest abstract class */
/*************************************************/

/**
 * Get the value of subreqId
 * The id of the subRequest that should be scheduled
 */
int Cstager_GetUpdateStartRequest_subreqId(struct Cstager_GetUpdateStartRequest_t* instance, u_signed64* var);

/**
 * Set the value of subreqId
 * The id of the subRequest that should be scheduled
 */
int Cstager_GetUpdateStartRequest_setSubreqId(struct Cstager_GetUpdateStartRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of diskServer
 * The name of the diskserver on which the selected filesystem for the given
 * SubRequest resides
 */
int Cstager_GetUpdateStartRequest_diskServer(struct Cstager_GetUpdateStartRequest_t* instance, const char** var);

/**
 * Set the value of diskServer
 * The name of the diskserver on which the selected filesystem for the given
 * SubRequest resides
 */
int Cstager_GetUpdateStartRequest_setDiskServer(struct Cstager_GetUpdateStartRequest_t* instance, const char* new_var);

/**
 * Get the value of fileSystem
 * The mount point of the selected filesystem for the given SubRequest
 */
int Cstager_GetUpdateStartRequest_fileSystem(struct Cstager_GetUpdateStartRequest_t* instance, const char** var);

/**
 * Set the value of fileSystem
 * The mount point of the selected filesystem for the given SubRequest
 */
int Cstager_GetUpdateStartRequest_setFileSystem(struct Cstager_GetUpdateStartRequest_t* instance, const char* new_var);

/********************************************/
/* Implementation of Request abstract class */
/********************************************/

/**
 * Get the value of flags
 */
int Cstager_GetUpdateStartRequest_flags(struct Cstager_GetUpdateStartRequest_t* instance, u_signed64* var);

/**
 * Set the value of flags
 */
int Cstager_GetUpdateStartRequest_setFlags(struct Cstager_GetUpdateStartRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of userName
 * Name of the user that submitted the request
 */
int Cstager_GetUpdateStartRequest_userName(struct Cstager_GetUpdateStartRequest_t* instance, const char** var);

/**
 * Set the value of userName
 * Name of the user that submitted the request
 */
int Cstager_GetUpdateStartRequest_setUserName(struct Cstager_GetUpdateStartRequest_t* instance, const char* new_var);

/**
 * Get the value of euid
 * Id of the user that submitted the request
 */
int Cstager_GetUpdateStartRequest_euid(struct Cstager_GetUpdateStartRequest_t* instance, unsigned long* var);

/**
 * Set the value of euid
 * Id of the user that submitted the request
 */
int Cstager_GetUpdateStartRequest_setEuid(struct Cstager_GetUpdateStartRequest_t* instance, unsigned long new_var);

/**
 * Get the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_GetUpdateStartRequest_egid(struct Cstager_GetUpdateStartRequest_t* instance, unsigned long* var);

/**
 * Set the value of egid
 * Id of the group of the user that submitted the request
 */
int Cstager_GetUpdateStartRequest_setEgid(struct Cstager_GetUpdateStartRequest_t* instance, unsigned long new_var);

/**
 * Get the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_GetUpdateStartRequest_mask(struct Cstager_GetUpdateStartRequest_t* instance, unsigned long* var);

/**
 * Set the value of mask
 * Mask for accessing files in the user space
 */
int Cstager_GetUpdateStartRequest_setMask(struct Cstager_GetUpdateStartRequest_t* instance, unsigned long new_var);

/**
 * Get the value of pid
 * Process id of the user process
 */
int Cstager_GetUpdateStartRequest_pid(struct Cstager_GetUpdateStartRequest_t* instance, unsigned long* var);

/**
 * Set the value of pid
 * Process id of the user process
 */
int Cstager_GetUpdateStartRequest_setPid(struct Cstager_GetUpdateStartRequest_t* instance, unsigned long new_var);

/**
 * Get the value of machine
 * The machine that submitted the request
 */
int Cstager_GetUpdateStartRequest_machine(struct Cstager_GetUpdateStartRequest_t* instance, const char** var);

/**
 * Set the value of machine
 * The machine that submitted the request
 */
int Cstager_GetUpdateStartRequest_setMachine(struct Cstager_GetUpdateStartRequest_t* instance, const char* new_var);

/**
 * Get the value of svcClassName
 */
int Cstager_GetUpdateStartRequest_svcClassName(struct Cstager_GetUpdateStartRequest_t* instance, const char** var);

/**
 * Set the value of svcClassName
 */
int Cstager_GetUpdateStartRequest_setSvcClassName(struct Cstager_GetUpdateStartRequest_t* instance, const char* new_var);

/**
 * Get the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_GetUpdateStartRequest_userTag(struct Cstager_GetUpdateStartRequest_t* instance, const char** var);

/**
 * Set the value of userTag
 * This is a string that the user is free to use. It can be useful to classify and
 * select requests.
 */
int Cstager_GetUpdateStartRequest_setUserTag(struct Cstager_GetUpdateStartRequest_t* instance, const char* new_var);

/**
 * Get the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_GetUpdateStartRequest_reqId(struct Cstager_GetUpdateStartRequest_t* instance, const char** var);

/**
 * Set the value of reqId
 * The Cuuid identifying the Request, stored as a human readable string
 */
int Cstager_GetUpdateStartRequest_setReqId(struct Cstager_GetUpdateStartRequest_t* instance, const char* new_var);

/**
 * Get the value of creationTime
 * Time when the Request was created
 */
int Cstager_GetUpdateStartRequest_creationTime(struct Cstager_GetUpdateStartRequest_t* instance, u_signed64* var);

/**
 * Set the value of creationTime
 * Time when the Request was created
 */
int Cstager_GetUpdateStartRequest_setCreationTime(struct Cstager_GetUpdateStartRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of lastModificationTime
 * Time when the request was last modified
 */
int Cstager_GetUpdateStartRequest_lastModificationTime(struct Cstager_GetUpdateStartRequest_t* instance, u_signed64* var);

/**
 * Set the value of lastModificationTime
 * Time when the request was last modified
 */
int Cstager_GetUpdateStartRequest_setLastModificationTime(struct Cstager_GetUpdateStartRequest_t* instance, u_signed64 new_var);

/**
 * Get the value of svcClass
 */
int Cstager_GetUpdateStartRequest_svcClass(struct Cstager_GetUpdateStartRequest_t* instance, struct Cstager_SvcClass_t** var);

/**
 * Set the value of svcClass
 */
int Cstager_GetUpdateStartRequest_setSvcClass(struct Cstager_GetUpdateStartRequest_t* instance, struct Cstager_SvcClass_t* new_var);

/**
 * Get the value of client
 */
int Cstager_GetUpdateStartRequest_client(struct Cstager_GetUpdateStartRequest_t* instance, struct C_IClient_t** var);

/**
 * Set the value of client
 */
int Cstager_GetUpdateStartRequest_setClient(struct Cstager_GetUpdateStartRequest_t* instance, struct C_IClient_t* new_var);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Gets the type of the object
 */
int Cstager_GetUpdateStartRequest_type(struct Cstager_GetUpdateStartRequest_t* instance,
                                       int* ret);

/**
 * virtual method to clone any object
 */
int Cstager_GetUpdateStartRequest_clone(struct Cstager_GetUpdateStartRequest_t* instance,
                                        struct C_IObject_t* ret);

/**
 * Get the value of id
 * The id of this object
 */
int Cstager_GetUpdateStartRequest_id(struct Cstager_GetUpdateStartRequest_t* instance, u_signed64* var);

/**
 * Set the value of id
 * The id of this object
 */
int Cstager_GetUpdateStartRequest_setId(struct Cstager_GetUpdateStartRequest_t* instance, u_signed64 new_var);

#endif // CASTOR_STAGER_GETUPDATESTARTREQUEST_H
