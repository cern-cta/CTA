/******************************************************************************
 *                      castor/stager/TapePool.h
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

#ifndef CASTOR_STAGER_TAPEPOOL_H
#define CASTOR_STAGER_TAPEPOOL_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct Cstager_TapePool_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class TapePool
// A Pool of tapes
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_TapePool_create(struct Cstager_TapePool_t** obj);

/**
 * Empty Destructor
 */
int Cstager_TapePool_delete(struct Cstager_TapePool_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_TapePool_getIObject(struct Cstager_TapePool_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_TapePool_t* Cstager_TapePool_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_TapePool_print(struct Cstager_TapePool_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_TapePool_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_TapePool_setId(struct Cstager_TapePool_t* instance,
                           u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_TapePool_id(struct Cstager_TapePool_t* instance,
                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_TapePool_type(struct Cstager_TapePool_t* instance,
                          int* ret);

/**
 * Get the value of name
 * Name of this pool
 */
int Cstager_TapePool_name(struct Cstager_TapePool_t* instance, const char** var);

/**
 * Set the value of name
 * Name of this pool
 */
int Cstager_TapePool_setName(struct Cstager_TapePool_t* instance, const char* new_var);

#endif // CASTOR_STAGER_TAPEPOOL_H
