/******************************************************************************
 *                      castor/stager/QryRequest.h
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

#ifndef CASTOR_STAGER_QRYREQUEST_H
#define CASTOR_STAGER_QRYREQUEST_H

// Include Files and Forward declarations for the C world
struct Cstager_QryRequest_t;
struct Cstager_Request_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class QryRequest
// An abstract ancester for all query requests
//------------------------------------------------------------------------------

/**
 * Empty Constructor
 */
int Cstager_QryRequest_create(struct Cstager_QryRequest_t** obj);

/**
 * Empty Destructor
 */
int Cstager_QryRequest_delete(struct Cstager_QryRequest_t* obj);

/**
 * Cast into Request
 */
struct Cstager_Request_t* Cstager_QryRequest_getRequest(struct Cstager_QryRequest_t* obj);

/**
 * Dynamic cast from Request
 */
struct Cstager_QryRequest_t* Cstager_QryRequest_fromRequest(struct Cstager_Request_t* obj);

/**
 * Outputs this object in a human readable format
 */
int Cstager_QryRequest_print(struct Cstager_QryRequest_t* instance);

#endif // CASTOR_STAGER_QRYREQUEST_H
