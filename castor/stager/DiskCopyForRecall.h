/******************************************************************************
 *                      castor/stager/DiskCopyForRecall.h
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

#ifndef CASTOR_STAGER_DISKCOPYFORRECALL_H
#define CASTOR_STAGER_DISKCOPYFORRECALL_H

// Include Files and Forward declarations for the C world
struct Cstager_DiskCopyForRecall_t;
struct Cstager_DiskCopy_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class DiskCopyForRecall
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_DiskCopyForRecall_create(struct Cstager_DiskCopyForRecall_t** obj);

/**
 * Empty Destructor
 */
int Cstager_DiskCopyForRecall_delete(struct Cstager_DiskCopyForRecall_t* obj);

/**
 * Cast into DiskCopy
 */
struct Cstager_DiskCopy_t* Cstager_DiskCopyForRecall_getDiskCopy(struct Cstager_DiskCopyForRecall_t* obj);

/**
 * Dynamic cast from DiskCopy
 */
struct Cstager_DiskCopyForRecall_t* Cstager_DiskCopyForRecall_fromDiskCopy(struct Cstager_DiskCopy_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_DiskCopyForRecall_print(struct Cstager_DiskCopyForRecall_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_DiskCopyForRecall_TYPE(int* ret);

/**
 * Gets the type of the object
 */
int Cstager_DiskCopyForRecall_type(struct Cstager_DiskCopyForRecall_t* instance,
                                   int* ret);

/**
 * Get the value of mountPoint
 * The mountpoint of the filesystem where the file to be recalled should be put
 */
int Cstager_DiskCopyForRecall_mountPoint(struct Cstager_DiskCopyForRecall_t* instance, const char** var);

/**
 * Set the value of mountPoint
 * The mountpoint of the filesystem where the file to be recalled should be put
 */
int Cstager_DiskCopyForRecall_setMountPoint(struct Cstager_DiskCopyForRecall_t* instance, const char* new_var);

/**
 * Get the value of diskServer
 * The disk server on which the file to be recalled should be put
 */
int Cstager_DiskCopyForRecall_diskServer(struct Cstager_DiskCopyForRecall_t* instance, const char** var);

/**
 * Set the value of diskServer
 * The disk server on which the file to be recalled should be put
 */
int Cstager_DiskCopyForRecall_setDiskServer(struct Cstager_DiskCopyForRecall_t* instance, const char* new_var);

#endif // CASTOR_STAGER_DISKCOPYFORRECALL_H
