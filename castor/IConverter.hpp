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
 * @(#)$RCSfile: IConverter.hpp,v $ $Revision: 1.9 $ $Release$ $Date: 2004/10/11 13:43:48 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ICONVERTER_HPP
#define CASTOR_ICONVERTER_HPP 1

// Include Files
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
     * gets the object type, that is the type of
     * object this converter can convert
     */
    virtual const unsigned int objType() const = 0;

    /**
     * gets the representation type, that is the type of
     * the representation this converter can deal with
     */
    virtual const unsigned int repType() const = 0;

    /**
     * create foreign representation from a C++ Object
     * @param address where to store the representation of
     * the object
     * @param object the object to deal with
     * @param autocommit whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void createRep(IAddress* address,
                           IObject* object,
                           bool autocommit)
      throw (castor::exception::Exception) = 0;
    
    /**
     * Updates foreign representation from a C++ Object.
     * @param address where the representation of
     * the object is stored
     * @param object the object to deal with
     * @param autocommit whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in cas of error
     */
    virtual void updateRep(IAddress* address,
                           IObject* object,
                           bool autocommit)
      throw (castor::exception::Exception) = 0;

    /**
     * deletes foreign representation of a C++ Object
     * @param address where the representation of
     * the object is stored
     * @param object the object to deal with
     * @param autocommit whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void deleteRep(IAddress* address,
                           IObject* object,
                           bool autocommit)
      throw (castor::exception::Exception) = 0;

    /**
     * create C++ object from foreign representation
     * @param address the place where to find the foreign
     * representation
     * @return the C++ object created from its reprensentation
     * or 0 if unsuccessful. Note that the caller is responsible
     * for the deallocation of the newly created object
     * @exception Exception throws an Exception in cas of error
     */
    virtual IObject* createObj(IAddress* address)
      throw (castor::exception::Exception) = 0;

    /**
     * Updates C++ object from its foreign representation.
     * @param object the object to deal with
     * @exception Exception throws an Exception in case of error
     */
    virtual void updateObj(IObject* object)
      throw (castor::exception::Exception) = 0;

    /**
     * Removes a link from a parent to a child
     * @param parent the parent
     * @param child the child
     * @exception Exception throws an Exception in case of error
     */
    virtual void unlinkChild(const castor::IObject* parent,
                             const castor::IObject* child)
      throw (castor::exception::Exception) = 0;

  };

} // end of namespace castor

#endif // CASTOR_ICONVERTER_HPP
