/******************************************************************************
 *                      castor/stager/DiskPool.h
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

#ifndef CASTOR_STAGER_DISKPOOL_H
#define CASTOR_STAGER_DISKPOOL_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct Cstager_DiskPool_t;
struct Cstager_FileSystem_t;
struct Cstager_SvcClass_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class DiskPool
// A Resource as seen by the Scheduler. Resources can be allocated to one or many
// projects are are composed of a set of filesystems.
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_DiskPool_create(struct Cstager_DiskPool_t** obj);

/**
 * Empty Destructor
 */
int Cstager_DiskPool_delete(struct Cstager_DiskPool_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_DiskPool_getIObject(struct Cstager_DiskPool_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_DiskPool_t* Cstager_DiskPool_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_DiskPool_print(struct Cstager_DiskPool_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_DiskPool_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/

/**
 * Sets the id of the object
 */
int Cstager_DiskPool_setId(struct Cstager_DiskPool_t* instance,
                           u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_DiskPool_id(struct Cstager_DiskPool_t* instance,
                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_DiskPool_type(struct Cstager_DiskPool_t* instance,
                          int* ret);

/**
 * Get the value of name
 * Name of this pool
 */
int Cstager_DiskPool_name(struct Cstager_DiskPool_t* instance, const char** var);

/**
 * Set the value of name
 * Name of this pool
 */
int Cstager_DiskPool_setName(struct Cstager_DiskPool_t* instance, const char* new_var);

/**
 * Add a struct Cstager_FileSystem_t* object to the fileSystems list
 */
int Cstager_DiskPool_addFileSystems(struct Cstager_DiskPool_t* instance, struct Cstager_FileSystem_t* obj);

/**
 * Remove a struct Cstager_FileSystem_t* object from fileSystems
 */
int Cstager_DiskPool_removeFileSystems(struct Cstager_DiskPool_t* instance, struct Cstager_FileSystem_t* obj);

/**
 * Get the list of struct Cstager_FileSystem_t* objects held by fileSystems
 */
int Cstager_DiskPool_fileSystems(struct Cstager_DiskPool_t* instance, struct Cstager_FileSystem_t*** var, int* len);

/**
 * Add a struct Cstager_SvcClass_t* object to the svcClasses list
 */
int Cstager_DiskPool_addSvcClasses(struct Cstager_DiskPool_t* instance, struct Cstager_SvcClass_t* obj);

/**
 * Remove a struct Cstager_SvcClass_t* object from svcClasses
 */
int Cstager_DiskPool_removeSvcClasses(struct Cstager_DiskPool_t* instance, struct Cstager_SvcClass_t* obj);

/**
 * Get the list of struct Cstager_SvcClass_t* objects held by svcClasses
 */
int Cstager_DiskPool_svcClasses(struct Cstager_DiskPool_t* instance, struct Cstager_SvcClass_t*** var, int* len);

#endif // CASTOR_STAGER_DISKPOOL_H
