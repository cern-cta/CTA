/******************************************************************************
 *                      castor/stager/FileRequest.h
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

#ifndef CASTOR_STAGER_FILEREQUEST_H
#define CASTOR_STAGER_FILEREQUEST_H

// Include Files and Forward declarations for the C world
struct Cstager_FileRequest_t;
struct Cstager_Request_t;
struct Cstager_SubRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class FileRequest
// An abstract ancester for all file requests
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_FileRequest_create(struct Cstager_FileRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_FileRequest_delete(struct Cstager_FileRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_FileRequest_getRequest(struct Cstager_FileRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_FileRequest_t* Cstager_FileRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_FileRequest_print(struct Cstager_FileRequest_t* instance);

/**
 * Add a struct Cstager_SubRequest_t* object to the subRequests list
 */
int Cstager_FileRequest_addSubRequests(struct Cstager_FileRequest_t* instance, struct Cstager_SubRequest_t* obj);

/**
 * Remove a struct Cstager_SubRequest_t* object from subRequests
 */
int Cstager_FileRequest_removeSubRequests(struct Cstager_FileRequest_t* instance, struct Cstager_SubRequest_t* obj);

/**
 * Get the list of struct Cstager_SubRequest_t* objects held by subRequests
 */
int Cstager_FileRequest_subRequests(struct Cstager_FileRequest_t* instance, struct Cstager_SubRequest_t*** var, int* len);

#endif // CASTOR_STAGER_FILEREQUEST_H
