/******************************************************************************
 *                      castor/stager/StageOutRequest.h
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
 * @(#)$RCSfile: StageOutRequest.h,v $ $Revision: 1.6 $ $Release$ $Date: 2004/11/04 10:26:14 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_STAGEOUTREQUEST_H
#define CASTOR_STAGER_STAGEOUTREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_Request_t;
struct Cstager_StageOutRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageOutRequest
// A stageout request 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageOutRequest_create(struct Cstager_StageOutRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageOutRequest_delete(struct Cstager_StageOutRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageOutRequest_getRequest(struct Cstager_StageOutRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageOutRequest_t* Cstager_StageOutRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageOutRequest_print(struct Cstager_StageOutRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageOutRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageOutRequest_setId(struct Cstager_StageOutRequest_t* instance,
                                  u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageOutRequest_id(struct Cstager_StageOutRequest_t* instance,
                               u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageOutRequest_type(struct Cstager_StageOutRequest_t* instance,
                                 int* ret);

/**
 * Get the value of openmode
 * Specifies the permissions to use when creating the internal disk file. Modified
 * by the process's umask (mode & ~umask).
 */
int Cstager_StageOutRequest_openmode(struct Cstager_StageOutRequest_t* instance, int* var);

/**
 * Set the value of openmode
 * Specifies the permissions to use when creating the internal disk file. Modified
 * by the process's umask (mode & ~umask).
 */
int Cstager_StageOutRequest_setOpenmode(struct Cstager_StageOutRequest_t* instance, int new_var);

#endif // CASTOR_STAGER_STAGEOUTREQUEST_H
