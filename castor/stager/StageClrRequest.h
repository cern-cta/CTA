/******************************************************************************
 *                      castor/stager/StageClrRequest.h
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
 * @(#)$RCSfile: StageClrRequest.h,v $ $Revision: 1.4 $ $Release$ $Date: 2004/10/07 14:34:03 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_STAGER_STAGECLRREQUEST_H
#define CASTOR_STAGER_STAGECLRREQUEST_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_int_t;
struct Cstager_Request_t;
struct Cstager_StageClrRequest_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class StageClrRequest
// Stage Clear Request
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_StageClrRequest_create(struct Cstager_StageClrRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_StageClrRequest_delete(struct Cstager_StageClrRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_StageClrRequest_getRequest(struct Cstager_StageClrRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_StageClrRequest_t* Cstager_StageClrRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_StageClrRequest_print(struct Cstager_StageClrRequest_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_StageClrRequest_TYPE(struct C_int_t* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_StageClrRequest_setId(struct Cstager_StageClrRequest_t* instance,
                                  u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_StageClrRequest_id(struct Cstager_StageClrRequest_t* instance,
                               u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_StageClrRequest_type(struct Cstager_StageClrRequest_t* instance,
                                 struct C_int_t* ret);

#endif // CASTOR_STAGER_STAGECLRREQUEST_H
