/******************************************************************************
 *                      castor/stager/SvcClass.h
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

#ifndef CASTOR_STAGER_SVCCLASS_H
#define CASTOR_STAGER_SVCCLASS_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct C_int_t;
struct C_uns_tigned int;
struct Cstager_SvcClass_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class SvcClass
// A service, as seen by the user. A SvcClass is a container of resources and may be
// given as parameter of the request.
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_SvcClass_create(struct Cstager_SvcClass_t** obj);

/**
 * Empty Destructor
 */
int Cstager_SvcClass_delete(struct Cstager_SvcClass_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_SvcClass_getIObject(struct Cstager_SvcClass_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_SvcClass_t* Cstager_SvcClass_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_SvcClass_print(struct Cstager_SvcClass_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_SvcClass_TYPE(struct C_int_t* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_SvcClass_setId(struct Cstager_SvcClass_t* instance,
                           u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_SvcClass_id(struct Cstager_SvcClass_t* instance,
                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_SvcClass_type(struct Cstager_SvcClass_t* instance,
                          struct C_int_t* ret);

/**
 * Get the value of policy
 */
int Cstager_SvcClass_policy(struct Cstager_SvcClass_t* instance, const char** var);

/**
 * Set the value of policy
 */
int Cstager_SvcClass_setPolicy(struct Cstager_SvcClass_t* instance, const char* new_var);

/**
 * Get the value of nbDrives
 * Number of drives to use for this project. This is the default number, but it
 * could be that occasionnally more drives are used, if a resource is shared with
 * another project using more drives
 */
int Cstager_SvcClass_nbDrives(struct Cstager_SvcClass_t* instance, struct C_uns_tigned int* var);

/**
 * Set the value of nbDrives
 * Number of drives to use for this project. This is the default number, but it
 * could be that occasionnally more drives are used, if a resource is shared with
 * another project using more drives
 */
int Cstager_SvcClass_setNbDrives(struct Cstager_SvcClass_t* instance, struct C_uns_tigned int new_var);

#endif // CASTOR_STAGER_SVCCLASS_H
