/******************************************************************************
 *                      castor/stager/StageUpdateFileStatusRequest.h
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
 * @(#)$RCSfile: StageUpdateFileStatusRequest.h,v $ $Revision: 1.3 $ $Release$ $Date: 2004/11/09 13:04:55 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_STAGEUPDATEFILESTATUSREQUEST_H
#define CASTOR_STAGER_STAGEUPDATEFILESTATUSREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_FileRequest_t;
struct Cstager_StageUpdateFileStatusRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageUpdateFileStatusRequest
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageUpdateFileStatusRequest_create(struct Cstager_StageUpdateFileStatusRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageUpdateFileStatusRequest_delete(struct Cstager_StageUpdateFileStatusRequest_t* obj);

/**
 * Cast into FileRequest
 */
struct Cstager_FileRequest_t* Cstager_StageUpdateFileStatusRequest_getFileRequest(struct Cstager_StageUpdateFileStatusRequest_t* obj);

/**
 * Dynamic cast from FileRequest
 */
struct Cstager_StageUpdateFileStatusRequest_t* Cstager_StageUpdateFileStatusRequest_fromFileRequest(struct Cstager_FileRequest_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageUpdateFileStatusRequest_print(struct Cstager_StageUpdateFileStatusRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageUpdateFileStatusRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageUpdateFileStatusRequest_setId(struct Cstager_StageUpdateFileStatusRequest_t* instance,
                                               u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageUpdateFileStatusRequest_id(struct Cstager_StageUpdateFileStatusRequest_t* instance,
                                            u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageUpdateFileStatusRequest_type(struct Cstager_StageUpdateFileStatusRequest_t* instance,
                                              int* ret);

#endif // CASTOR_STAGER_STAGEUPDATEFILESTATUSREQUEST_H
