/******************************************************************************
 *                      Services.h
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
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_SERVICES_H
#define CASTOR_SERVICES_H 1

/**
 * Forward declaration of IObject for the C world
 */
struct C_IObject_t;

/**
 * Forward declaration of IAddress for the C world
 */
struct C_IAddress_t;

/**
 * Forward declaration of Services for the C world
 */
struct C_Services_t;

/**
 * Constructor
 */
int C_Services_create(struct C_Services_t** svcs);

/**
 * Destructor
 */
int C_Services_delete(struct C_Services_t* svcs);

/**
 * create foreign representation from a C++ Object
 * @param address where to store the representation of
 * the object
 * @param object the object to deal with
 * @param autocommit whether the changes to the database
 * should be commited or not.
 * @exception Exception throws an Exception in cas of error
 */
int C_Services_createRep(struct C_Services_t* svcs,
                         struct C_IAddress_t* address,
                         struct C_IObject_t* object,
                         char autocommit);

/**
 * Updates foreign representation from a C++ Object.
 * @param address where the representation of
 * the object is stored
 * @param object the object to deal with
 * @param autocommit whether the changes to the database
 * should be commited or not
 * @exception Exception throws an Exception in cas of error
 */
int C_Services_updateRep(struct C_Services_t* svcs,
                         struct C_IAddress_t* address,
                         struct C_IObject_t* object,
                         char autocommit);

/**
 * deletes foreign representation of a C++ Object
 * @param address where the representation of
 * the object is stored
 * @param object the object to deal with
 * @param autocommit whether the changes to the database
 * should be commited or not.
 * @exception Exception throws an Exception in case of error
 */
int C_Services_deleteRep(struct C_Services_t* svcs,
                         struct C_IAddress_t* address,
                         struct C_IObject_t* object,
                         char autocommit);

/**
 * create C++ object from foreign representation
 * @param address the place where to find the foreign
 * representation
 * @return the C++ object created from its reprensentation
 * or 0 if unsuccessful. Note that the caller is responsible
 * for the deallocation of the newly created object
 * @exception Exception throws an Exception in cas of error
 */
int C_Services_createObj(struct C_Services_t* svcs,
                         struct C_IAddress_t* address,
                         struct C_IObject_t** object);

#endif // CASTOR_SERVICES_H
