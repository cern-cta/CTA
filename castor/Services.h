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

/// Forward declarations for the C world
struct C_Services_t;
struct C_IObject_t;
struct C_IAddress_t;
struct C_IService_t;

/**
 * Constructor
 */
int C_Services_create(struct C_Services_t** svcs);

/**
 * Destructor
 */
int C_Services_delete(struct C_Services_t* svcs);

/**
 * gets a service by name.
 * @param svcs the services object to use
 * @param name the name of the service
 * @param id the type of service. If id not 0 and the
 * service does not exist, it will created with this type
 * @param svc the service
 * @return -1 in case of error, 0 if successful
 * A detailed error message can be retrieved by calling
 * C_Services_errorMsg
 */
int C_Services_service(struct C_Services_t* svcs,
                       char* name,
                       unsigned int id,
                       struct C_IService_t** svc);

/**
 * create foreign representation from a C++ Object
 * @param svcs the services object to use
 * @param address where to store the representation of
 * the object
 * @param object the object to deal with
 * @param autocommit whether the changes to the database
 * should be commited or not. Default is not.
 * @return -1 in case of error, 0 if successful
 * A detailed error message can be retrieved by calling
 * C_Services_errorMsg
 */
int C_Services_createRep(struct C_Services_t* svcs,
                         struct C_IAddress_t* address,
                         struct C_IObject_t* object,
                         char autocommit);

/**
 * Updates foreign representation from a C++ Object
 * @param svcs the services object to use
 * @param address where the representation of
 * the object is stored
 * @param object the object to deal with
 * @param autocommit whether the changes to the database
 * should be commited or not. Default is not.
 * @return -1 in case of error, 0 if successful
 * A detailed error message can be retrieved by calling
 * C_Services_errorMsg
 */
int C_Services_updateRep(struct C_Services_t* svcs,
                         struct C_IAddress_t* address,
                         struct C_IObject_t* object,
                         char autocommit);

/**
 * deletes foreign representation of a C++ Object
 * @param svcs the services object to use
 * @param address where the representation of
 * the object is stored
 * @param object the object to deal with
 * @param autocommit whether the changes to the database
 * should be commited or not. Default is not.
 * @return -1 in case of error, 0 if successful
 * A detailed error message can be retrieved by calling
 * C_Services_errorMsg
 */
int C_Services_deleteRep(struct C_Services_t* svcs,
                         struct C_IAddress_t* address,
                         struct C_IObject_t* object,
                         char autocommit);

/**
 * create C++ object from foreign representation
 * @param svcs the services object to use
 * @param address the place where to find the foreign
 * representation
 * @param object the C++ object created from its reprensentation
 * or 0 if unsuccessful. Note that the caller is responsible
 * for the deallocation of the newly created object
 * @return -1 in case of error, 0 if successful
 * A detailed error message can be retrieved by calling
 * C_Services_errorMsg
 */
int C_Services_createObj(struct C_Services_t* svcs,
                         struct C_IAddress_t* address,
                         struct C_IObject_t** object);

/**
 * Updates C++ object from its foreign representation.
 * @param svcs the services object to use
 * @param address where to find the object
 * @param object the object to deal with
 * @return -1 in case of error, 0 if successful
 * A detailed error message can be retrieved by calling
 * C_Services_errorMsg
 */
int C_Services_updateObj(struct C_Services_t* svcs,
                         struct C_IAddress_t* address,
                         struct C_IObject_t* object);

/**
 * Fill the foreign representation with some of the objects
 * refered by a given C++ object.
 * @param svcs the services object to use
 * @param address the place where to find the foreign representation
 * @param object the original C++ object
 * @param type the type of the refered objects to store
 * @param autocommit whether the changes to the database
 * should be commited or not. Default is not.
 * @return -1 in case of error, 0 if successful
 * A detailed error message can be retrieved by calling
 * C_Services_errorMsg
 */
int C_Services_fillRep(struct C_Services_t* svcs,
                       struct C_IAddress_t* address,
                       struct C_IObject_t* object,
                       unsigned int type,
                       char autocommit);
    
/**
 * Retrieve from the foreign representation some of the
 * objects refered by a given C++ object.
 * @param svcs the services object to use
 * @param address the place where to find the foreign representation
 * @param object the original object
 * @param type the type of the refered objects to retrieve
 * @param autocommit whether the changes to the database
 * should be commited or not
 * @return -1 in case of error, 0 if successful
 * A detailed error message can be retrieved by calling
 * C_Services_errorMsg
 */
int C_Services_fillObj(struct C_Services_t* svcs,
                       struct C_IAddress_t* address,
                       struct C_IObject_t* object,
                       unsigned int type,
                       char autocommit);

/**
 * Forces the commit of the last changes in a given DB
 * @param svcs the services object to use
 * @param address what to commit
 * @return -1 in case of error, 0 if successful
 * A detailed error message can be retrieved by calling
 * C_Services_errorMsg
 */
int C_Services_commit(struct C_Services_t* svcs,
                      struct C_IAddress_t* address);

/**
 * Forces the rollback of the last changes in a given DB
 * @param svcs the services object to use
 * @param address what to rollback
 * @return -1 in case of error, 0 if successful
 * A detailed error message can be retrieved by calling
 * C_Services_errorMsg
 */
int C_Services_rollback(struct C_Services_t* svcs,
                        struct C_IAddress_t* address);

/**
 * Returns the error message associated to the last error.
 * Note that the error message string should be deallocated
 * by the caller.
 * @return the error message
 */
const char* C_Services_errorMsg(struct C_Services_t* svcs);

#endif // CASTOR_SERVICES_H
