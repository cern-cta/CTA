/******************************************************************************
 *                      castor/stager/ReqIdRequest.h
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

#ifndef CASTOR_STAGER_REQIDREQUEST_H
#define CASTOR_STAGER_REQIDREQUEST_H

// Include Files and Forward declarations for the C world
struct Cstager_ReqIdRequest_t;
struct Cstager_ReqId_t;
struct Cstager_Request_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class ReqIdRequest
// A request dealing with a set of reqids
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_ReqIdRequest_create(struct Cstager_ReqIdRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_ReqIdRequest_delete(struct Cstager_ReqIdRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_ReqIdRequest_getRequest(struct Cstager_ReqIdRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_ReqIdRequest_t* Cstager_ReqIdRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_ReqIdRequest_print(struct Cstager_ReqIdRequest_t* instance);

/**
 * Add a struct Cstager_ReqId_t* object to the reqids list
 */
int Cstager_ReqIdRequest_addReqids(struct Cstager_ReqIdRequest_t* instance, struct Cstager_ReqId_t* obj);

/**
 * Remove a struct Cstager_ReqId_t* object from reqids
 */
int Cstager_ReqIdRequest_removeReqids(struct Cstager_ReqIdRequest_t* instance, struct Cstager_ReqId_t* obj);

/**
 * Get the list of struct Cstager_ReqId_t* objects held by reqids
 */
int Cstager_ReqIdRequest_reqids(struct Cstager_ReqIdRequest_t* instance, struct Cstager_ReqId_t*** var, int* len);

#endif // CASTOR_STAGER_REQIDREQUEST_H
