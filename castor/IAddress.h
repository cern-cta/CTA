/******************************************************************************
 *                      castor/IAddress.h
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

#ifndef CASTOR_IADDRESS_H
#define CASTOR_IADDRESS_H

// Include Files and Forward declarations for the C world
struct C_IAddress_t;

//------------------------------------------------------------------------------
// This defines a C interface to the following class
// class IAddress
//  Base class for all addresses. An address allows to find a foreign representation
// of an object. Fully empty except for an id telling the type of the address and
// the infrastructure for printing any address
//------------------------------------------------------------------------------

/**
 * Empty Destructor
 */
int C_IAddress_delete(struct C_IAddress_t* obj);

/**
 * gets the object type, that is the type of object whose representation is pointed
 * to by this address
 */
int C_IAddress_objType(struct C_IAddress_t* instance,
                       const unsigned int* ret);

/**
 * sets the object type, that is the type of object whose representation is pointed
 * to by this address.
 */
int C_IAddress_setObjType(struct C_IAddress_t* instance,
                          unsigned int type);

/**
 * gets the name of the conversion service able to deal with this address
 */
int C_IAddress_cnvSvcName(struct C_IAddress_t* instance,
                          const string* ret);

/**
 * gets the type of the conversion service able to deal with this address
 */
int C_IAddress_cnvSvcType(struct C_IAddress_t* instance,
                          const unsigned int* ret);

/**
 * prints the address into an output stream
 */
int C_IAddress_print(struct C_IAddress_t* instance,
                     ostream& s);

#endif // CASTOR_IADDRESS_H
