/******************************************************************************
 *                      ICnvSvc.hpp
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
 * @(#)$RCSfile: ICnvSvc.hpp,v $ $Revision: 1.9 $ $Release$ $Date: 2004/10/12 14:44:49 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ICNVSVC_HPP
#define CASTOR_ICNVSVC_HPP 1

// Include Files
#include "IService.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward Declaration
  class IConverter;
  class IAddress;
  class IObject;

  /**
   * Abstract interface for services dealing with conversion
   */
  class ICnvSvc : public virtual IService {

  public:
    /** Add converter object to conversion service.
     *  @param pConverter Pointer to converter object
     *  @return boolean indicating success or failure.
     */
    virtual bool addConverter(IConverter* pConverter) = 0;

    /** Remove converter object from conversion service (if present).
     *  The converter is defined by the class type of the objects created.
     *  @param id ID of the converter
     *  @return boolean indicating success or failure.
     */
    virtual bool removeConverter(const unsigned int id) = 0;

    /** Retrieve converter from list
     *  @param id the id of the converter
     *  @return the converter corresponding to id or 0 if non was found
     */
    virtual IConverter* converter(const unsigned int id) = 0;

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
     * @exception Exception throws an Exception in case of error
     */
    virtual void updateRep(IAddress* address,
                           IObject* object,
                           bool autocommit)
      throw (castor::exception::Exception) = 0;

    /**
     * Deletes foreign representation of a C++ Object.
     * @param address where the representation of
     * the object is stored
     * @param object the object to deal with
     * @param autocommit whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void deleteRep(castor::IAddress* address,
                           castor::IObject* object,
                           bool autocommit = true)
      throw (castor::exception::Exception) = 0;

    /**
     * create C++ object from foreign representation
     * @param address the place where to find the foreign
     * representation
     * @return the C++ object created from its reprensentation
     * or 0 if unsuccessful. Note that the caller is responsible
     * for the deallocation of the newly created object
     * @exception Exception throws an Exception in case of error
     */
    virtual IObject* createObj (IAddress* address)
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
     * @param autocommit whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void fillRep(castor::IAddress* address,
                         castor::IObject* object,
                         unsigned int type,
                         bool autocommit)
      throw (castor::exception::Exception) = 0;
    
    /**
     * Retrieve from the foreign representation some of the
     * objects refered by a given C++ object.
     * @param address the place where to find the foreign representation
     * @param object the original object
     * @param type the type of the refered objects to retrieve
     * @exception Exception throws an Exception in case of error
     */
    virtual void fillObj(castor::IAddress* address,
                         castor::IObject* object,
                         unsigned int type)
      throw (castor::exception::Exception) = 0;

    /**
     * Forces the commit of the last changes.
     * @exception Exception throws an Exception in case of error
     */
    virtual void commit()
      throw (castor::exception::Exception) = 0;

    /**
     * Forces the rollback of the last changes
     * @exception Exception throws an Exception in case of error
     */
    virtual void rollback()
      throw (castor::exception::Exception) = 0;

    /**
     * Deletes foreign representation of a C++ Object without
     * needing the object. The cost of this method is higher
     * than the cost of deleteRep since it needs to call
     * createObj first
     * @param address where the representation of
     * the object is stored
     * @param autocommit whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in cas of error
     */
    virtual void deleteRepByAddress (IAddress* address,
                                     bool autocommit = true)
      throw (castor::exception::Exception) = 0;

  };

} // end of namespace castor

#endif // CASTOR_ICNVSVC_HPP
