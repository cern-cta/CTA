/******************************************************************************
 *                      castor/IClient.h
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

#ifndef CASTOR_ICLIENT_H
#define CASTOR_ICLIENT_H

// Include Files and Forward declarations for the C world
struct C_IClient_t;
struct C_IObject_t;
struct Cstager_Request_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class IClient
// A generic abstract base class for castor clients
//------------------------------------------------------------------------------

/**
 * Empty Destructor
 */
int C_IClient_delete(struct C_IClient_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* C_IClient_getIObject(struct C_IClient_t* obj);

/**
 * Dynamic cast from IObject
 */
struct C_IClient_t* C_IClient_fromIObject(struct C_IObject_t* obj);

/**
 * Reconnects to the client in case the connection was closed or lost
 */
int C_IClient_reconnect(struct C_IClient_t* instance);

/**
 * Disconnects from the client.
 */
int C_IClient_disconnect(struct C_IClient_t* instance);

/**
 */
int C_IClient_sendRep(struct C_IClient_t* instance);

/**
 * Outputs this object in a human readable format
 */
int C_IClient_print(struct C_IClient_t* instance);

/**
 * Get the value of request
 */
int C_IClient_request(struct C_IClient_t* instance, struct Cstager_Request_t** var);

/**
 * Set the value of request
 */
int C_IClient_setRequest(struct C_IClient_t* instance, struct Cstager_Request_t* new_var);

#endif // CASTOR_ICLIENT_H
