/******************************************************************************
 *                      castor/rh/Client.h
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

#ifndef CASTOR_RH_CLIENT_H
#define CASTOR_RH_CLIENT_H

// Include Files and Forward declarations for the C world
#include "osdep.h"
struct C_IClient_t;
struct C_IObject_t;
struct Crh_Client_t;
struct Cstager_Request_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class Client
// A simple castor client defined by its machine name and the port it is listening
// to in this machine.
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Crh_Client_create(struct Crh_Client_t** obj);

/**
 * Empty Destructor
 */
int Crh_Client_delete(struct Crh_Client_t* obj);

/**
 * Cast into IClient
 */
struct C_IClient_t* Crh_Client_getIClient(struct Crh_Client_t* obj);

/**
 * Dynamic cast from IClient
 */
struct Crh_Client_t* Crh_Client_fromIClient(struct C_IClient_t* obj);

/**
 * Cast into IObject
 */
struct C_IObject_t* Crh_Client_getIObject(struct Crh_Client_t* obj);

/**
 * Dynamic cast from IObject
 */
struct Crh_Client_t* Crh_Client_fromIObject(struct C_IObject_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Crh_Client_print(struct Crh_Client_t* instance);

/**
 * Gets the type of this kind of objects
 */
int Crh_Client_TYPE(int* ret);

/********************************************/
/* Implementation of IClient abstract class */
/********************************************/

/**
 * Reconnects to the client in case the connection was closed or lost
 */
int Crh_Client_reconnect(struct Crh_Client_t* instance);

/**
 * Disconnects from the client.
 */
int Crh_Client_disconnect(struct Crh_Client_t* instance);

/**
 */
int Crh_Client_sendRep(struct Crh_Client_t* instance);

/**
 * Get the value of request
 */
int Crh_Client_request(struct Crh_Client_t* instance, struct Cstager_Request_t** var);

/**
 * Set the value of request
 */
int Crh_Client_setRequest(struct Crh_Client_t* instance, struct Cstager_Request_t* new_var);

/********************************************/
/* Implementation of IObject abstract class */
/********************************************/

/**
 * Sets the id of the object
 */
int Crh_Client_setId(struct Crh_Client_t* instance,
                     u_signed64 id);

/**
 * gets the id of the object
 */
int Crh_Client_id(struct Crh_Client_t* instance,
                  u_signed64* ret);

/**
 * Gets the type of the object
 */
int Crh_Client_type(struct Crh_Client_t* instance,
                    int* ret);

/**
 * Get the value of ipAddress
 * The address of the machine where the client is running
 */
int Crh_Client_ipAddress(struct Crh_Client_t* instance, unsigned long* var);

/**
 * Set the value of ipAddress
 * The address of the machine where the client is running
 */
int Crh_Client_setIpAddress(struct Crh_Client_t* instance, unsigned long new_var);

/**
 * Get the value of port
 */
int Crh_Client_port(struct Crh_Client_t* instance, unsigned short* var);

/**
 * Set the value of port
 */
int Crh_Client_setPort(struct Crh_Client_t* instance, unsigned short new_var);

#endif // CASTOR_RH_CLIENT_H
