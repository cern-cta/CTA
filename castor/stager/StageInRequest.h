/******************************************************************************
 *                      castor/stager/StageInRequest.h
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
 * @(#)$RCSfile: StageInRequest.h,v $ $Revision: 1.3 $ $Release$ $Date: 2004/10/05 13:37:33 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_STAGEINREQUEST_H
#define CASTOR_STAGER_STAGEINREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StageInRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageInRequest
// A stagein request 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageInRequest_create(struct Cstager_StageInRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageInRequest_delete(struct Cstager_StageInRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageInRequest_getRequest(struct Cstager_StageInRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageInRequest_t* Cstager_StageInRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageInRequest_print(struct Cstager_StageInRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageInRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageInRequest_setId(struct Cstager_StageInRequest_t* instance,
                                 u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageInRequest_id(struct Cstager_StageInRequest_t* instance,
                              u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageInRequest_type(struct Cstager_StageInRequest_t* instance,
                                int* ret);

/**
 * Get the value of openflags
 * If it contains O_RDWR or O_WRONLY the file is considered to be opened by an
 * application that want to modify it.
 */
int Cstager_StageInRequest_openflags(struct Cstager_StageInRequest_t* instance, int* var);

/**
 * Set the value of openflags
 * If it contains O_RDWR or O_WRONLY the file is considered to be opened by an
 * application that want to modify it.
 */
int Cstager_StageInRequest_setOpenflags(struct Cstager_StageInRequest_t* instance, int new_var);

#endif // CASTOR_STAGER_STAGEINREQUEST_H
