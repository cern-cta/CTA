/******************************************************************************
 *                      castor/stager/StageUpdcRequest.h
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
 * @(#)$RCSfile: StageUpdcRequest.h,v $ $Revision: 1.3 $ $Release$ $Date: 2004/10/05 13:37:34 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_STAGEUPDCREQUEST_H
#define CASTOR_STAGER_STAGEUPDCREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct Cstager_ReqIdRequest_t;
struct Cstager_StageUpdcRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageUpdcRequest
// A stageupdc request
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageUpdcRequest_create(struct Cstager_StageUpdcRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageUpdcRequest_delete(struct Cstager_StageUpdcRequest_t* obj);

/**
 * Cast into ReqIdRequest
 */
struct Cstager_ReqIdRequest_t* Cstager_StageUpdcRequest_getReqIdRequest(struct Cstager_StageUpdcRequest_t* obj);

/**
 * Dynamic cast from ReqIdRequest
 */
struct Cstager_StageUpdcRequest_t* Cstager_StageUpdcRequest_fromReqIdRequest(struct Cstager_ReqIdRequest_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageUpdcRequest_print(struct Cstager_StageUpdcRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageUpdcRequest_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageUpdcRequest_setId(struct Cstager_StageUpdcRequest_t* instance,
                                   u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageUpdcRequest_id(struct Cstager_StageUpdcRequest_t* instance,
                                u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageUpdcRequest_type(struct Cstager_StageUpdcRequest_t* instance,
                                  int* ret);

#endif // CASTOR_STAGER_STAGEUPDCREQUEST_H
