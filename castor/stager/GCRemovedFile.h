/******************************************************************************
 *                      castor/stager/GCRemovedFile.h
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
 * @(#)$RCSfile: GCRemovedFile.h,v $ $Revision: 1.1 $ $Release$ $Date: 2005/02/09 17:05:45 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_GCREMOVEDFILE_H
#define CASTOR_STAGER_GCREMOVEDFILE_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct Cstager_GCRemovedFile_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class GCRemovedFile
// Represents a file that was just deleted on a DiskServer
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_GCRemovedFile_create(struct Cstager_GCRemovedFile_t** obj);

/**
 * Empty Destructor
 */
int Cstager_GCRemovedFile_delete(struct Cstager_GCRemovedFile_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_GCRemovedFile_getIObject(struct Cstager_GCRemovedFile_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_GCRemovedFile_t* Cstager_GCRemovedFile_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_GCRemovedFile_print(struct Cstager_GCRemovedFile_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_GCRemovedFile_TYPE(int* ret);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Gets the type of the object
 */
int Cstager_GCRemovedFile_type(struct Cstager_GCRemovedFile_t* instance,
                               int* ret);

/**
 * virtual method to clone any object
 */
int Cstager_GCRemovedFile_clone(struct Cstager_GCRemovedFile_t* instance,
                                struct C_IObject_t* ret);

/**
 * Get the value of diskCopyId
 * Id of the DiskCopy reprensenting the deleted file in the catalog
 */
int Cstager_GCRemovedFile_diskCopyId(struct Cstager_GCRemovedFile_t* instance, u_signed64* var);

/**
 * Set the value of diskCopyId
 * Id of the DiskCopy reprensenting the deleted file in the catalog
 */
int Cstager_GCRemovedFile_setDiskCopyId(struct Cstager_GCRemovedFile_t* instance, u_signed64 new_var);

/**
 * Get the value of id
 * The id of this object
 */
int Cstager_GCRemovedFile_id(struct Cstager_GCRemovedFile_t* instance, u_signed64* var);

/**
 * Set the value of id
 * The id of this object
 */
int Cstager_GCRemovedFile_setId(struct Cstager_GCRemovedFile_t* instance, u_signed64 new_var);

#endif // CASTOR_STAGER_GCREMOVEDFILE_H
