/******************************************************************************
 *                      castor/stager/CastorFile.h
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

#ifndef CASTOR_STAGER_CASTORFILE_H
#define CASTOR_STAGER_CASTORFILE_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct Cstager_CastorFile_t;
struct Cstager_DiskCopy_t;
struct Cstager_FileClass_t;
struct Cstager_SvcClass_t;
struct Cstager_TapeCopy_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class CastorFile
// A castor file.
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_CastorFile_create(struct Cstager_CastorFile_t** obj);

/**
 * Empty Destructor
 */
int Cstager_CastorFile_delete(struct Cstager_CastorFile_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_CastorFile_getIObject(struct Cstager_CastorFile_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_CastorFile_t* Cstager_CastorFile_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_CastorFile_print(struct Cstager_CastorFile_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_CastorFile_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/

/**
 * Sets the id of the object
 */
int Cstager_CastorFile_setId(struct Cstager_CastorFile_t* instance,
                             u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_CastorFile_id(struct Cstager_CastorFile_t* instance,
                          u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_CastorFile_type(struct Cstager_CastorFile_t* instance,
                            int* ret);

/**
 * Get the value of fileId
 * The id of this castor file. This identifies it uniquely
 */
int Cstager_CastorFile_fileId(struct Cstager_CastorFile_t* instance, u_signed64* var);

/**
 * Set the value of fileId
 * The id of this castor file. This identifies it uniquely
 */
int Cstager_CastorFile_setFileId(struct Cstager_CastorFile_t* instance, u_signed64 new_var);

/**
 * Get the value of nsHost
 * The name server hosting this castor file
 */
int Cstager_CastorFile_nsHost(struct Cstager_CastorFile_t* instance, const char** var);

/**
 * Set the value of nsHost
 * The name server hosting this castor file
 */
int Cstager_CastorFile_setNsHost(struct Cstager_CastorFile_t* instance, const char* new_var);

/**
 * Get the value of fileSize
 * Size of the file
 */
int Cstager_CastorFile_fileSize(struct Cstager_CastorFile_t* instance, u_signed64* var);

/**
 * Set the value of fileSize
 * Size of the file
 */
int Cstager_CastorFile_setFileSize(struct Cstager_CastorFile_t* instance, u_signed64 new_var);

/**
 * Get the value of svcClass
 */
int Cstager_CastorFile_svcClass(struct Cstager_CastorFile_t* instance, struct Cstager_SvcClass_t** var);

/**
 * Set the value of svcClass
 */
int Cstager_CastorFile_setSvcClass(struct Cstager_CastorFile_t* instance, struct Cstager_SvcClass_t* new_var);

/**
 * Get the value of fileClass
 */
int Cstager_CastorFile_fileClass(struct Cstager_CastorFile_t* instance, struct Cstager_FileClass_t** var);

/**
 * Set the value of fileClass
 */
int Cstager_CastorFile_setFileClass(struct Cstager_CastorFile_t* instance, struct Cstager_FileClass_t* new_var);

/**
 * Add a struct Cstager_DiskCopy_t* object to the diskCopies list
 */
int Cstager_CastorFile_addDiskCopies(struct Cstager_CastorFile_t* instance, struct Cstager_DiskCopy_t* obj);

/**
 * Remove a struct Cstager_DiskCopy_t* object from diskCopies
 */
int Cstager_CastorFile_removeDiskCopies(struct Cstager_CastorFile_t* instance, struct Cstager_DiskCopy_t* obj);

/**
 * Get the list of struct Cstager_DiskCopy_t* objects held by diskCopies
 */
int Cstager_CastorFile_diskCopies(struct Cstager_CastorFile_t* instance, struct Cstager_DiskCopy_t*** var, int* len);

/**
 * Add a struct Cstager_TapeCopy_t* object to the tapeCopies list
 */
int Cstager_CastorFile_addTapeCopies(struct Cstager_CastorFile_t* instance, struct Cstager_TapeCopy_t* obj);

/**
 * Remove a struct Cstager_TapeCopy_t* object from tapeCopies
 */
int Cstager_CastorFile_removeTapeCopies(struct Cstager_CastorFile_t* instance, struct Cstager_TapeCopy_t* obj);

/**
 * Get the list of struct Cstager_TapeCopy_t* objects held by tapeCopies
 */
int Cstager_CastorFile_tapeCopies(struct Cstager_CastorFile_t* instance, struct Cstager_TapeCopy_t*** var, int* len);

#endif // CASTOR_STAGER_CASTORFILE_H
