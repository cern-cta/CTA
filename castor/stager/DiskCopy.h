/******************************************************************************
 *                      castor/stager/DiskCopy.h
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

#ifndef CASTOR_STAGER_DISKCOPY_H
#define CASTOR_STAGER_DISKCOPY_H

// Include Files and Forward declarations for the C world
#include "castor/stager/DiskCopyStatusCode.h"
#include "osdep.h"
struct C_IObject_t;
struct Cstager_CastorFile_t;
struct Cstager_DiskCopy_t;
struct Cstager_FileSystem_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class DiskCopy
// A copy of a file in the disk pool
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_DiskCopy_create(struct Cstager_DiskCopy_t** obj);

/**
 * Empty Destructor
 */
int Cstager_DiskCopy_delete(struct Cstager_DiskCopy_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_DiskCopy_getIObject(struct Cstager_DiskCopy_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_DiskCopy_t* Cstager_DiskCopy_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_DiskCopy_print(struct Cstager_DiskCopy_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_DiskCopy_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_DiskCopy_setId(struct Cstager_DiskCopy_t* instance,
                           u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_DiskCopy_id(struct Cstager_DiskCopy_t* instance,
                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_DiskCopy_type(struct Cstager_DiskCopy_t* instance,
                          int* ret);

/**
 * Get the value of path
 * path of this copy in the filesystem
 */
int Cstager_DiskCopy_path(struct Cstager_DiskCopy_t* instance, const char** var);

/**
 * Set the value of path
 * path of this copy in the filesystem
 */
int Cstager_DiskCopy_setPath(struct Cstager_DiskCopy_t* instance, const char* new_var);

/**
 * Get the value of fileSystem
 */
int Cstager_DiskCopy_fileSystem(struct Cstager_DiskCopy_t* instance, struct Cstager_FileSystem_t** var);

/**
 * Set the value of fileSystem
 */
int Cstager_DiskCopy_setFileSystem(struct Cstager_DiskCopy_t* instance, struct Cstager_FileSystem_t* new_var);

/**
 * Get the value of castorFile
 */
int Cstager_DiskCopy_castorFile(struct Cstager_DiskCopy_t* instance, struct Cstager_CastorFile_t** var);

/**
 * Set the value of castorFile
 */
int Cstager_DiskCopy_setCastorFile(struct Cstager_DiskCopy_t* instance, struct Cstager_CastorFile_t* new_var);

/**
 * Get the value of status
 */
int Cstager_DiskCopy_status(struct Cstager_DiskCopy_t* instance, enum Cstager_DiskCopyStatusCode_t* var);

/**
 * Set the value of status
 */
int Cstager_DiskCopy_setStatus(struct Cstager_DiskCopy_t* instance, enum Cstager_DiskCopyStatusCode_t new_var);

#endif // CASTOR_STAGER_DISKCOPY_H
