/******************************************************************************
 *                      castor/stager/SubRequest.h
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
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_SUBREQUEST_H
#define CASTOR_STAGER_SUBREQUEST_H

// Include Files and Forward declarations for the C world
#include "castor/stager/SubRequestStatusCodes.h"
struct C_IObject_t;
struct C_int_t;
struct C_u_signed64_t;
struct C_uns_tigned int;
struct Cstager_Request_t;
struct Cstager_SubRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class SubRequest
// A subpart of a request delaing with a single castor file
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_SubRequest_create(struct Cstager_SubRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_SubRequest_delete(struct Cstager_SubRequest_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_SubRequest_getIObject(struct Cstager_SubRequest_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_SubRequest_t* Cstager_SubRequest_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_SubRequest_print(struct Cstager_SubRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_SubRequest_TYPE(struct C_int_t* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_SubRequest_setId(struct Cstager_SubRequest_t* instance,
                             unsigned long id);

/**
 * gets the id of the object
 */
int Cstager_SubRequest_id(struct Cstager_SubRequest_t* instance,
                          unsigned long* ret);

/**
 * Gets the type of the object
 */
int Cstager_SubRequest_type(struct Cstager_SubRequest_t* instance,
                            struct C_int_t* ret);

/**
 * Get the value of retryCounter
 */
int Cstager_SubRequest_retryCounter(struct Cstager_SubRequest_t* instance, struct C_uns_tigned int* var);

/**
 * Set the value of retryCounter
 */
int Cstager_SubRequest_setRetryCounter(struct Cstager_SubRequest_t* instance, struct C_uns_tigned int new_var);

/**
 * Get the value of fileName
 * Name of the file this SubRequest deals with. When stored in the catalog, this is
 * redundant with the link to the CastorFile table. However, this is needed in the
 * client
 */
int Cstager_SubRequest_fileName(struct Cstager_SubRequest_t* instance, const char** var);

/**
 * Set the value of fileName
 * Name of the file this SubRequest deals with. When stored in the catalog, this is
 * redundant with the link to the CastorFile table. However, this is needed in the
 * client
 */
int Cstager_SubRequest_setFileName(struct Cstager_SubRequest_t* instance, const char* new_var);

/**
 * Get the value of protocol
 * The protocol that will be used to access the file this SubRequest deals with
 */
int Cstager_SubRequest_protocol(struct Cstager_SubRequest_t* instance, const char** var);

/**
 * Set the value of protocol
 * The protocol that will be used to access the file this SubRequest deals with
 */
int Cstager_SubRequest_setProtocol(struct Cstager_SubRequest_t* instance, const char* new_var);

/**
 * Get the value of poolName
 * The name of the TapePool to use for the file this SubRequest deals with
 */
int Cstager_SubRequest_poolName(struct Cstager_SubRequest_t* instance, const char** var);

/**
 * Set the value of poolName
 * The name of the TapePool to use for the file this SubRequest deals with
 */
int Cstager_SubRequest_setPoolName(struct Cstager_SubRequest_t* instance, const char* new_var);

/**
 * Get the value of xsize
 * The size of the file. This gives how many bytes should be allocated rather than
 * the default.
 */
int Cstager_SubRequest_xsize(struct Cstager_SubRequest_t* instance, struct C_u_signed64_t* var);

/**
 * Set the value of xsize
 * The size of the file. This gives how many bytes should be allocated rather than
 * the default.
 */
int Cstager_SubRequest_setXsize(struct Cstager_SubRequest_t* instance, struct C_u_signed64_t new_var);

/**
 * Get the value of request
 */
int Cstager_SubRequest_request(struct Cstager_SubRequest_t* instance, struct Cstager_Request_t** var);

/**
 * Set the value of request
 */
int Cstager_SubRequest_setRequest(struct Cstager_SubRequest_t* instance, struct Cstager_Request_t* new_var);

/**
 * Get the value of status
 */
int Cstager_SubRequest_status(struct Cstager_SubRequest_t* instance, enum Cstager_SubRequestStatusCodes_t* var);

/**
 * Set the value of status
 */
int Cstager_SubRequest_setStatus(struct Cstager_SubRequest_t* instance, enum Cstager_SubRequestStatusCodes_t new_var);

#endif // CASTOR_STAGER_SUBREQUEST_H
