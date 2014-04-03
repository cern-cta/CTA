/******************************************************************************
 *                      IConverter.hpp
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
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#pragma once

// Include Files
#include <vector>
#include <map>

// Local Includes
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward Declarations
  class IAddress;
  class IObject;

  /**
   * abstract interface for all converters and conversion
   * services
   */
  class IConverter {

  public:

    /**
     * virtual destructor
     */
    virtual ~IConverter(){};

    /**
     * gets the object type, that is the type of
     * object this converter can convert
     */
    virtual unsigned int objType() const = 0;

    /**
     * gets the representation type, that is the type of
     * the representation this converter can deal with
     */
    virtual unsigned int repType() = 0;

    /**
     * create foreign representation from a C++ Object
     * @param address where to store the representation of
     * the object
     * @param object the object to deal with
     * @param endTransaction whether the changes to the database
     * should be commited or not
     * @param type if not OBJ_INVALID, the ids representing
     * the links to objects of this type will not set to 0
     * as is the default.
     * @exception Exception throws an Exception in case of error
     */
    virtual void createRep(IAddress* address,
                           IObject* object,
                           bool endTransaction,
                           unsigned int type)
      throw (castor::exception::Exception) = 0;
    
    /**
     * create foreign representations from a set of C++ Object
     * @param address where to store the representation of
     * the objects
     * @param objects the list of objects to deal with
     * @param endTransaction whether the changes to the database
     * should be commited or not
     * @param type if not OBJ_INVALID, the ids representing
     * the links to objects of this type will not set to 0
     * as is the default.
     * @exception Exception throws an Exception in case of error
     */
    virtual void bulkCreateRep(IAddress* address,
                               std::vector<IObject*> &objects,
			       bool endTransaction,
			       unsigned int type)
      throw (castor::exception::Exception) = 0;
    
    /**
     * Updates foreign representation from a C++ Object.
     * @param address where the representation of
     * the object is stored
     * @param object the object to deal with
     * @param endTransaction whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void updateRep(IAddress* address,
                           IObject* object,
                           bool endTransaction)
      throw (castor::exception::Exception) = 0;

    /**
     * deletes foreign representation of a C++ Object
     * @param address where the representation of
     * the object is stored
     * @param object the object to deal with
     * @param endTransaction whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void deleteRep(IAddress* address,
                           IObject* object,
                           bool endTransaction)
      throw (castor::exception::Exception) = 0;

    /**
     * create C++ object from foreign representation
     * @param address the place where to find the foreign
     * representation
     * @return the C++ object created from its representation
     * or 0 if unsuccessful. Note that the caller is responsible
     * for the deallocation of the newly created object
     * @exception Exception throws an Exception in case of error
     */
    virtual IObject* createObj(IAddress* address)
      throw (castor::exception::Exception) = 0;

    /**
     * create C++ objects from foreign representations
     * @param address the place where to find the foreign
     * representations
     * @return the C++ objects created from the representations
     * or empty vector if unsuccessful. Note that the caller is
     * responsible for the deallocation of the newly created objects
     * @exception Exception throws an Exception in case of error
     */
    virtual std::vector<IObject*> bulkCreateObj(IAddress* address)
      throw (castor::exception::Exception) = 0;

    /**
     * Updates C++ object from its foreign representation.
     * @param object the object to deal with
     * @exception Exception throws an Exception in case of error
     */
    virtual void updateObj(IObject* object)
      throw (castor::exception::Exception) = 0;


    /**
     * Fill the foreign representation with some of the objects
     * refered by a given C++ object.
     * @param address the place where to find the foreign representation
     * @param object the original C++ object
     * @param type the type of the refered objects to store
     * @param endTransaction whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void fillRep(castor::IAddress* address,
                         castor::IObject* object,
                         unsigned int type,
                         bool endTransaction = false)
      throw (castor::exception::Exception) = 0;
    
    /**
     * Retrieve from the foreign representation some of the
     * objects refered by a given C++ object.
     * @param address the place where to find the foreign representation
     * @param object the original object
     * @param type the type of the refered objects to retrieve
     * @param endTransaction whether the lock taken on the database
     * should be released or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void fillObj(castor::IAddress* address,
                         castor::IObject* object,
                         unsigned int type,
                         bool endTransaction = false)
      throw (castor::exception::Exception) = 0;

  };

} // end of namespace castor

