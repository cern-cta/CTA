/******************************************************************************
 *                      castor/stager/DiskServer.h
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

#ifndef CASTOR_STAGER_DISKSERVER_H
#define CASTOR_STAGER_DISKSERVER_H

// Include Files and Forward declarations for the C world
#include "castor/stager/DiskServerStatusCode.h"
#include "osdep.h"
struct C_IObject_t;
struct Cstager_DiskServer_t;
struct Cstager_FileSystem_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class DiskServer
// A disk server is a physical device handling some filesystems
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_DiskServer_create(struct Cstager_DiskServer_t** obj);

/**
 * Empty Destructor
 */
int Cstager_DiskServer_delete(struct Cstager_DiskServer_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_DiskServer_getIObject(struct Cstager_DiskServer_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_DiskServer_t* Cstager_DiskServer_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_DiskServer_print(struct Cstager_DiskServer_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_DiskServer_TYPE(int* ret);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Sets the id of the object
 */
int Cstager_DiskServer_setId(struct Cstager_DiskServer_t* instance,
                             u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_DiskServer_id(struct Cstager_DiskServer_t* instance,
                          u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_DiskServer_type(struct Cstager_DiskServer_t* instance,
                            int* ret);

/**
 * virtual method to clone any object
 */
int Cstager_DiskServer_clone(struct Cstager_DiskServer_t* instance,
                             struct C_IObject_t* ret);

/**
 * Get the value of name
 * Name of the DiskServer
 */
int Cstager_DiskServer_name(struct Cstager_DiskServer_t* instance, const char** var);

/**
 * Set the value of name
 * Name of the DiskServer
 */
int Cstager_DiskServer_setName(struct Cstager_DiskServer_t* instance, const char* new_var);

/**
 * Add a struct Cstager_FileSystem_t* object to the fileSystems list
 */
int Cstager_DiskServer_addFileSystems(struct Cstager_DiskServer_t* instance, struct Cstager_FileSystem_t* obj);

/**
 * Remove a struct Cstager_FileSystem_t* object from fileSystems
 */
int Cstager_DiskServer_removeFileSystems(struct Cstager_DiskServer_t* instance, struct Cstager_FileSystem_t* obj);

/**
 * Get the list of struct Cstager_FileSystem_t* objects held by fileSystems
 */
int Cstager_DiskServer_fileSystems(struct Cstager_DiskServer_t* instance, struct Cstager_FileSystem_t*** var, int* len);

/**
 * Get the value of status
 */
int Cstager_DiskServer_status(struct Cstager_DiskServer_t* instance, enum Cstager_DiskServerStatusCode_t* var);

/**
 * Set the value of status
 */
int Cstager_DiskServer_setStatus(struct Cstager_DiskServer_t* instance, enum Cstager_DiskServerStatusCode_t new_var);

#endif // CASTOR_STAGER_DISKSERVER_H
