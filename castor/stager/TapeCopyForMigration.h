/******************************************************************************
 *                      castor/stager/TapeCopyForMigration.h
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

#ifndef CASTOR_STAGER_TAPECOPYFORMIGRATION_H
#define CASTOR_STAGER_TAPECOPYFORMIGRATION_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_DiskCopy_t;
struct Cstager_TapeCopyForMigration_t;
struct Cstager_TapeCopy_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class TapeCopyForMigration
// This class is a wrapper around a Tape Copy that represents a TapCopy ready for
// Migration. It thus has information about the physical file to be copied.
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_TapeCopyForMigration_create(struct Cstager_TapeCopyForMigration_t** obj);

/**
 * Empty Destructor
 */
int Cstager_TapeCopyForMigration_delete(struct Cstager_TapeCopyForMigration_t* obj);

/**
 * Cast into TapeCopy
 */
struct Cstager_TapeCopy_t* Cstager_TapeCopyForMigration_getTapeCopy(struct Cstager_TapeCopyForMigration_t* obj);

/**
 * Dynamic cast from TapeCopy
 */
struct Cstager_TapeCopyForMigration_t* Cstager_TapeCopyForMigration_fromTapeCopy(struct Cstager_TapeCopy_t* obj);

/**
 * Get the value of diskServer
 * The disk server on which the file to be migrated resides
 */
int Cstager_TapeCopyForMigration_diskServer(struct Cstager_TapeCopyForMigration_t* instance, const char** var);

/**
 * Set the value of diskServer
 * The disk server on which the file to be migrated resides
 */
int Cstager_TapeCopyForMigration_setDiskServer(struct Cstager_TapeCopyForMigration_t* instance, const char* new_var);

/**
 * Get the value of mountPoint
 * The mountpoint of the filesystem where the file to be migrated resides
 */
int Cstager_TapeCopyForMigration_mountPoint(struct Cstager_TapeCopyForMigration_t* instance, const char** var);

/**
 * Set the value of mountPoint
 * The mountpoint of the filesystem where the file to be migrated resides
 */
int Cstager_TapeCopyForMigration_setMountPoint(struct Cstager_TapeCopyForMigration_t* instance, const char* new_var);

/**
 * Get the value of castorFileID
 * The castorFile ID of the file to migrate
 */
int Cstager_TapeCopyForMigration_castorFileID(struct Cstager_TapeCopyForMigration_t* instance, u_signed64* var);

/**
 * Set the value of castorFileID
 * The castorFile ID of the file to migrate
 */
int Cstager_TapeCopyForMigration_setCastorFileID(struct Cstager_TapeCopyForMigration_t* instance, u_signed64 new_var);

/**
 * Get the value of nsHost
 * The name server dealing with the file to migrate
 */
int Cstager_TapeCopyForMigration_nsHost(struct Cstager_TapeCopyForMigration_t* instance, const char** var);

/**
 * Set the value of nsHost
 * The name server dealing with the file to migrate
 */
int Cstager_TapeCopyForMigration_setNsHost(struct Cstager_TapeCopyForMigration_t* instance, const char* new_var);

/**
 * Get the value of fileSize
 * The size of the file to migrate
 */
int Cstager_TapeCopyForMigration_fileSize(struct Cstager_TapeCopyForMigration_t* instance, u_signed64* var);

/**
 * Set the value of fileSize
 * The size of the file to migrate
 */
int Cstager_TapeCopyForMigration_setFileSize(struct Cstager_TapeCopyForMigration_t* instance, u_signed64 new_var);

/**
 * Get the value of diskCopy
 */
int Cstager_TapeCopyForMigration_diskCopy(struct Cstager_TapeCopyForMigration_t* instance, struct Cstager_DiskCopy_t** var);

/**
 * Set the value of diskCopy
 */
int Cstager_TapeCopyForMigration_setDiskCopy(struct Cstager_TapeCopyForMigration_t* instance, struct Cstager_DiskCopy_t* new_var);

#endif // CASTOR_STAGER_TAPECOPYFORMIGRATION_H
