/******************************************************************************
 *                      castor/rh/ScheduleSubReqResponse.h
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
 * @(#)$RCSfile: ScheduleSubReqResponse.h,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/24 11:52:24 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_RH_SCHEDULESUBREQRESPONSE_H
#define CASTOR_RH_SCHEDULESUBREQRESPONSE_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IObject_t;
struct Crh_Response_t;
struct Crh_ScheduleSubReqResponse_t;
struct Cstager_DiskCopy_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class ScheduleSubReqResponse
// 
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Crh_ScheduleSubReqResponse_create(struct Crh_ScheduleSubReqResponse_t** obj);

/**
 * Empty Destructor
 */
int Crh_ScheduleSubReqResponse_delete(struct Crh_ScheduleSubReqResponse_t* obj);

/**
 * Cast into Response
 */
struct Crh_Response_t* Crh_ScheduleSubReqResponse_getResponse(struct Crh_ScheduleSubReqResponse_t* obj);

/**
 * Dynamic cast from Response
 */
struct Crh_ScheduleSubReqResponse_t* Crh_ScheduleSubReqResponse_fromResponse(struct Crh_Response_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Crh_ScheduleSubReqResponse_getIObject(struct Crh_ScheduleSubReqResponse_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Crh_ScheduleSubReqResponse_t* Crh_ScheduleSubReqResponse_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Crh_ScheduleSubReqResponse_print(struct Crh_ScheduleSubReqResponse_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Crh_ScheduleSubReqResponse_TYPE(int* ret);

/*********************************************/
/* Implementation of Response abstract class */
/*********************************************/

/**
 * Get the value of errorCode
 * The error code in case of error
 */
int Crh_ScheduleSubReqResponse_errorCode(struct Crh_ScheduleSubReqResponse_t* instance, unsigned int* var);

/**
 * Set the value of errorCode
 * The error code in case of error
 */
int Crh_ScheduleSubReqResponse_setErrorCode(struct Crh_ScheduleSubReqResponse_t* instance, unsigned int new_var);

/**
 * Get the value of errorMessage
 * The error message in case of error
 */
int Crh_ScheduleSubReqResponse_errorMessage(struct Crh_ScheduleSubReqResponse_t* instance, const char** var);

/**
 * Set the value of errorMessage
 * The error message in case of error
 */
int Crh_ScheduleSubReqResponse_setErrorMessage(struct Crh_ScheduleSubReqResponse_t* instance, const char* new_var);

/***************************************/
/* Implementation of IObject interface */
/***************************************/

/**
 * Sets the id of the object
 */
int Crh_ScheduleSubReqResponse_setId(struct Crh_ScheduleSubReqResponse_t* instance,
                                     u_signed64 id);

/**
 * gets the id of the object
 */
int Crh_ScheduleSubReqResponse_id(struct Crh_ScheduleSubReqResponse_t* instance,
                                  u_signed64* ret);

/**
 * Gets the type of the object
 */
int Crh_ScheduleSubReqResponse_type(struct Crh_ScheduleSubReqResponse_t* instance,
                                    int* ret);

/**
 * virtual method to clone any object
 */
int Crh_ScheduleSubReqResponse_clone(struct Crh_ScheduleSubReqResponse_t* instance,
                                     struct C_IObject_t* ret);

/**
 * Get the value of diskCopy
 */
int Crh_ScheduleSubReqResponse_diskCopy(struct Crh_ScheduleSubReqResponse_t* instance, struct Cstager_DiskCopy_t** var);

/**
 * Set the value of diskCopy
 */
int Crh_ScheduleSubReqResponse_setDiskCopy(struct Crh_ScheduleSubReqResponse_t* instance, struct Cstager_DiskCopy_t* new_var);

/**
 * Add a struct Cstager_DiskCopy_t* object to the sources list
 */
int Crh_ScheduleSubReqResponse_addSources(struct Crh_ScheduleSubReqResponse_t* instance, struct Cstager_DiskCopy_t* obj);

/**
 * Remove a struct Cstager_DiskCopy_t* object from sources
 */
int Crh_ScheduleSubReqResponse_removeSources(struct Crh_ScheduleSubReqResponse_t* instance, struct Cstager_DiskCopy_t* obj);

/**
 * Get the list of struct Cstager_DiskCopy_t* objects held by sources
 */
int Crh_ScheduleSubReqResponse_sources(struct Crh_ScheduleSubReqResponse_t* instance, struct Cstager_DiskCopy_t*** var, int* len);

#endif // CASTOR_RH_SCHEDULESUBREQRESPONSE_H
