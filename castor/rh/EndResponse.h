/******************************************************************************
 *                      castor/rh/EndResponse.h
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

#ifndef CASTOR_RH_ENDRESPONSE_H
#define CASTOR_RH_ENDRESPONSE_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_int_t;
struct Crh_EndResponse_t;
struct Crh_Response_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class EndResponse
// This type of response is used to inform the client that the server has nothing
// more to send back.
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Crh_EndResponse_create(struct Crh_EndResponse_t** obj);

/**
 * Empty Destructor
 */
int Crh_EndResponse_delete(struct Crh_EndResponse_t* obj);

/**
 * Cast into Response
 */
struct Crh_Response_t* Crh_EndResponse_getResponse(struct Crh_EndResponse_t* obj);

/**
 * Dynamic cast from Response
 */
struct Crh_EndResponse_t* Crh_EndResponse_fromResponse(struct Crh_Response_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Crh_EndResponse_print(struct Crh_EndResponse_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Crh_EndResponse_TYPE(struct C_int_t* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Crh_EndResponse_setId(struct Crh_EndResponse_t* instance,
                          u_signed64 id);

/**
 * gets the id of the object
 */
int Crh_EndResponse_id(struct Crh_EndResponse_t* instance,
                       u_signed64* ret);

/**
 * Gets the type of the object
 */
int Crh_EndResponse_type(struct Crh_EndResponse_t* instance,
                         struct C_int_t* ret);

#endif // CASTOR_RH_ENDRESPONSE_H
