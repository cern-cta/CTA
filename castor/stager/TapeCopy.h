/******************************************************************************
 *                      castor/stager/TapeCopy.h
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

#ifndef CASTOR_STAGER_TAPECOPY_H
#define CASTOR_STAGER_TAPECOPY_H

// Include Files and Forward declarations for the C world
#include "castor/stager/TapeCopyStatusCodes.h"
#include "osdep.h"
struct C_IObject_t;
struct Cstager_CastorFile_t;
struct Cstager_Segment_t;
struct Cstager_Stream_t;
struct Cstager_TapeCopy_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class TapeCopy
// One copy of a given file on a tape
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_TapeCopy_create(struct Cstager_TapeCopy_t** obj);

/**
 * Empty Destructor
 */
int Cstager_TapeCopy_delete(struct Cstager_TapeCopy_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_TapeCopy_getIObject(struct Cstager_TapeCopy_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_TapeCopy_t* Cstager_TapeCopy_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_TapeCopy_print(struct Cstager_TapeCopy_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_TapeCopy_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/

/**
 * Sets the id of the object
 */
int Cstager_TapeCopy_setId(struct Cstager_TapeCopy_t* instance,
                           u_signed64 id);

/**
 * gets the id of the object
 */
int Cstager_TapeCopy_id(struct Cstager_TapeCopy_t* instance,
                        u_signed64* ret);

/**
 * Gets the type of the object
 */
int Cstager_TapeCopy_type(struct Cstager_TapeCopy_t* instance,
                          int* ret);

/**
 * Get the value of copyNb
 * The copy number allows to identify the different copies on tape of a single file
 */
int Cstager_TapeCopy_copyNb(struct Cstager_TapeCopy_t* instance, unsigned int* var);

/**
 * Set the value of copyNb
 * The copy number allows to identify the different copies on tape of a single file
 */
int Cstager_TapeCopy_setCopyNb(struct Cstager_TapeCopy_t* instance, unsigned int new_var);

/**
 * Add a struct Cstager_Stream_t* object to the stream list
 */
int Cstager_TapeCopy_addStream(struct Cstager_TapeCopy_t* instance, struct Cstager_Stream_t* obj);

/**
 * Remove a struct Cstager_Stream_t* object from stream
 */
int Cstager_TapeCopy_removeStream(struct Cstager_TapeCopy_t* instance, struct Cstager_Stream_t* obj);

/**
 * Get the list of struct Cstager_Stream_t* objects held by stream
 */
int Cstager_TapeCopy_stream(struct Cstager_TapeCopy_t* instance, struct Cstager_Stream_t*** var, int* len);

/**
 * Add a struct Cstager_Segment_t* object to the segments list
 */
int Cstager_TapeCopy_addSegments(struct Cstager_TapeCopy_t* instance, struct Cstager_Segment_t* obj);

/**
 * Remove a struct Cstager_Segment_t* object from segments
 */
int Cstager_TapeCopy_removeSegments(struct Cstager_TapeCopy_t* instance, struct Cstager_Segment_t* obj);

/**
 * Get the list of struct Cstager_Segment_t* objects held by segments
 */
int Cstager_TapeCopy_segments(struct Cstager_TapeCopy_t* instance, struct Cstager_Segment_t*** var, int* len);

/**
 * Get the value of castorFile
 */
int Cstager_TapeCopy_castorFile(struct Cstager_TapeCopy_t* instance, struct Cstager_CastorFile_t** var);

/**
 * Set the value of castorFile
 */
int Cstager_TapeCopy_setCastorFile(struct Cstager_TapeCopy_t* instance, struct Cstager_CastorFile_t* new_var);

/**
 * Get the value of status
 */
int Cstager_TapeCopy_status(struct Cstager_TapeCopy_t* instance, enum Cstager_TapeCopyStatusCodes_t* var);

/**
 * Set the value of status
 */
int Cstager_TapeCopy_setStatus(struct Cstager_TapeCopy_t* instance, enum Cstager_TapeCopyStatusCodes_t new_var);

#endif // CASTOR_STAGER_TAPECOPY_H
