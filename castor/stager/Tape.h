/******************************************************************************
 *                      castor/stager/Tape.h
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

#ifndef CASTOR_STAGER_TAPE_H
#define CASTOR_STAGER_TAPE_H

// Include Files and Forward declarations for the C world
#include "castor/stager/TapeStatusCodes.h"
struct C_IObject_t;
struct Cstager_Segment_t;
struct Cstager_Tape_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class Tape
// Tape Information  The Tape Object contains all tape information required to be
// stored in the stager request catalog. The remaining tape parameters are taken
// from VMGR when the request is processed by the rtcpclientd daemon.
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_Tape_create(struct Cstager_Tape_t** obj);

/**
 * Empty Destructor
 */
int Cstager_Tape_delete(struct Cstager_Tape_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Cstager_Tape_getIObject(struct Cstager_Tape_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Cstager_Tape_t* Cstager_Tape_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_Tape_print(struct Cstager_Tape_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Cstager_Tape_TYPE(int* ret);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/
/**
 * Sets the id of the object
 */
int Cstager_Tape_setId(struct Cstager_Tape_t* instance,
                       unsigned long id);

/**
 * gets the id of the object
 */
int Cstager_Tape_id(struct Cstager_Tape_t* instance,
                    unsigned long* ret);

/**
 * Gets the type of the object
 */
int Cstager_Tape_type(struct Cstager_Tape_t* instance,
                      int* ret);

/**
 * Get the value of vid
 * Tape visual identifier
 */
int Cstager_Tape_vid(struct Cstager_Tape_t* instance, const char** var);

/**
 * Set the value of vid
 * Tape visual identifier
 */
int Cstager_Tape_setVid(struct Cstager_Tape_t* instance, const char* new_var);

/**
 * Get the value of side
 * Side (for future support of two sided media)
 */
int Cstager_Tape_side(struct Cstager_Tape_t* instance, int* var);

/**
 * Set the value of side
 * Side (for future support of two sided media)
 */
int Cstager_Tape_setSide(struct Cstager_Tape_t* instance, int new_var);

/**
 * Get the value of tpmode
 * Tape access mode (WRITE_DISABLE or WRITE_ENABLE)
 */
int Cstager_Tape_tpmode(struct Cstager_Tape_t* instance, int* var);

/**
 * Set the value of tpmode
 * Tape access mode (WRITE_DISABLE or WRITE_ENABLE)
 */
int Cstager_Tape_setTpmode(struct Cstager_Tape_t* instance, int new_var);

/**
 * Get the value of errMsgTxt
 */
int Cstager_Tape_errMsgTxt(struct Cstager_Tape_t* instance, const char** var);

/**
 * Set the value of errMsgTxt
 */
int Cstager_Tape_setErrMsgTxt(struct Cstager_Tape_t* instance, const char* new_var);

/**
 * Get the value of errorCode
 * RTCOPY serrno if status == SEGMENT_FAILED
 */
int Cstager_Tape_errorCode(struct Cstager_Tape_t* instance, int* var);

/**
 * Set the value of errorCode
 * RTCOPY serrno if status == SEGMENT_FAILED
 */
int Cstager_Tape_setErrorCode(struct Cstager_Tape_t* instance, int new_var);

/**
 * Get the value of severity
 */
int Cstager_Tape_severity(struct Cstager_Tape_t* instance, int* var);

/**
 * Set the value of severity
 */
int Cstager_Tape_setSeverity(struct Cstager_Tape_t* instance, int new_var);

/**
 * Get the value of vwAddress
 * Vid worker address for killing requests
 */
int Cstager_Tape_vwAddress(struct Cstager_Tape_t* instance, const char** var);

/**
 * Set the value of vwAddress
 * Vid worker address for killing requests
 */
int Cstager_Tape_setVwAddress(struct Cstager_Tape_t* instance, const char* new_var);

/**
 * Add a struct Cstager_Segment_t* object to the segments list
 */
int Cstager_Tape_addSegments(struct Cstager_Tape_t* instance, struct Cstager_Segment_t* obj);

/**
 * Remove a struct Cstager_Segment_t* object from segments
 */
int Cstager_Tape_removeSegments(struct Cstager_Tape_t* instance, struct Cstager_Segment_t* obj);

/**
 * Get the list of struct Cstager_Segment_t* objects held by segments
 */
int Cstager_Tape_segments(struct Cstager_Tape_t* instance, struct Cstager_Segment_t*** var, int* len);

/**
 * Get the value of status
 */
int Cstager_Tape_status(struct Cstager_Tape_t* instance, enum Cstager_TapeStatusCodes_t* var);

/**
 * Set the value of status
 */
int Cstager_Tape_setStatus(struct Cstager_Tape_t* instance, enum Cstager_TapeStatusCodes_t new_var);

#endif // CASTOR_STAGER_TAPE_H
