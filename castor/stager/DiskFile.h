/******************************************************************************
 *                      castor/stager/DiskFile.h
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

#ifndef CASTOR_STAGER_DISKFILE_H
#define CASTOR_STAGER_DISKFILE_H

// Include Files and Forward declarations for the C world
struct Cstager_DiskFile_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class DiskFile
// A file residing in the disk pool
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_DiskFile_create(struct Cstager_DiskFile_t** obj);

/**
 * Empty Destructor
 */
int Cstager_DiskFile_delete(struct Cstager_DiskFile_t* obj);

#endif // CASTOR_STAGER_DISKFILE_H
