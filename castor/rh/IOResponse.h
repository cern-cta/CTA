/******************************************************************************
 *                      castor/rh/IOResponse.h
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

#ifndef CASTOR_RH_IORESPONSE_H
#define CASTOR_RH_IORESPONSE_H

// Include Files and Forward declarations for the C world
struct Crh_FileResponse_t;
struct Crh_IOResponse_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class IOResponse
// Response dealing with an I/O. It gives everything to access the file considered.
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Crh_IOResponse_create(struct Crh_IOResponse_t** obj);

/**
 * Empty Destructor
 */
int Crh_IOResponse_delete(struct Crh_IOResponse_t* obj);

/**
 * Cast into FileResponse
 */
struct Crh_FileResponse_t* Crh_IOResponse_getFileResponse(struct Crh_IOResponse_t* obj);

/**
 * Dynamic cast from FileResponse
 */
struct Crh_IOResponse_t* Crh_IOResponse_fromFileResponse(struct Crh_FileResponse_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Crh_IOResponse_print(struct Crh_IOResponse_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Crh_IOResponse_TYPE(int* ret);

/**
 * Gets the type of the object
 */
int Crh_IOResponse_type(struct Crh_IOResponse_t* instance,
                        int* ret);

/**
 * Get the value of fileName
 * The file name
 */
int Crh_IOResponse_fileName(struct Crh_IOResponse_t* instance, const char** var);

/**
 * Set the value of fileName
 * The file name
 */
int Crh_IOResponse_setFileName(struct Crh_IOResponse_t* instance, const char* new_var);

/**
 * Get the value of server
 * The server where to find the file
 */
int Crh_IOResponse_server(struct Crh_IOResponse_t* instance, const char** var);

/**
 * Set the value of server
 * The server where to find the file
 */
int Crh_IOResponse_setServer(struct Crh_IOResponse_t* instance, const char* new_var);

/**
 * Get the value of port
 * The port where to find the file
 */
int Crh_IOResponse_port(struct Crh_IOResponse_t* instance, int* var);

/**
 * Set the value of port
 * The port where to find the file
 */
int Crh_IOResponse_setPort(struct Crh_IOResponse_t* instance, int new_var);

/**
 * Get the value of protocol
 * The protocol to use to retrieve the file
 */
int Crh_IOResponse_protocol(struct Crh_IOResponse_t* instance, const char** var);

/**
 * Set the value of protocol
 * The protocol to use to retrieve the file
 */
int Crh_IOResponse_setProtocol(struct Crh_IOResponse_t* instance, const char* new_var);

#endif // CASTOR_RH_IORESPONSE_H
