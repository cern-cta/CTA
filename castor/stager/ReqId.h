/******************************************************************************
 *                      castor/stager/ReqId.h
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
 * @(#)$RCSfile: ReqId.h,v $ $Revision: 1.4 $ $Release$ $Date: 2004/10/07 14:34:03 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_REQID_H
#define CASTOR_STAGER_REQID_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct C_int_t;
struct Cstager_ReqIdRequest_t;
struct Cstager_ReqId_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class ReqId
// A Reqid, as defined in the new castor
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_ReqId_create(struct Cstager_ReqId_t** obj);

/**
 * Empty Destructor
 */
int Cstager_ReqId_delete(struct Cstager_ReqId_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_ReqId_getIObject(struct Cstager_ReqId_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_ReqId_t* Cstager_ReqId_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_ReqId_print(struct Cstager_ReqId_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_ReqId_TYPE(struct C_int_t* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_ReqId_setId(struct Cstager_ReqId_t* instance,
                        u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_ReqId_id(struct Cstager_ReqId_t* instance,
                     u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_ReqId_type(struct Cstager_ReqId_t* instance,
                       struct C_int_t* ret);

/**
 * Get the value of value
 * The reqid itself
 */
int Cstager_ReqId_value(struct Cstager_ReqId_t* instance, const char** var);

/**
 * Set the value of value
 * The reqid itself
 */
int Cstager_ReqId_setValue(struct Cstager_ReqId_t* instance, const char* new_var);

/**
 * Get the value of request
 */
int Cstager_ReqId_request(struct Cstager_ReqId_t* instance, struct Cstager_ReqIdRequest_t** var);

/**
 * Set the value of request
 */
int Cstager_ReqId_setRequest(struct Cstager_ReqId_t* instance, struct Cstager_ReqIdRequest_t* new_var);

#endif // CASTOR_STAGER_REQID_H
