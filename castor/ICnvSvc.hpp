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
 * @(#)$RCSfile: ICnvSvc.hpp,v $ $Revision: 1.7 $ $Release$ $Date: 2004/10/07 14:33:58 $ $Author: sponcec3 $
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
#include "ObjectSet.hpp"
#include "ObjectCatalog.hpp"

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
     * @param alreadyDone the set of objects which representation
     * was already created. This is needed to avoid looping in case
     * of circular dependencies
     * @param autocommit whether the changes to the database
     * should be commited or not
     * @param recursive if set to true, the objects refered
     * by object will be created/updated too and recursively
     * If it's set to false, the objects refered will not be touched
     * But an exception will be thrown if one is missing that is needed
     * @exception Exception throws an Exception in case of error
     */
    virtual void createRep(IAddress* address,
                           IObject* object,
                           ObjectSet& alreadyDone,
                           bool autocommit,
                           bool recursive)
      throw (castor::exception::Exception) = 0;

    /**
     * Updates foreign representation from a C++ Object.
     * @param address where the representation of
     * the object is stored
     * @param object the object to deal with
     * @param alreadyDone the set of objects which representation
     * was already updated. This is needed to avoid looping in case
     * of circular dependencies
     * @param autocommit whether the changes to the database
     * should be commited or not
     * @param recursive if set to true, the objects refered
     * by object will be updated to and recursively
     * @exception Exception throws an Exception in case of error
     */
    virtual void updateRep(IAddress* address,
                           IObject* object,
                           ObjectSet& alreadyDone,
                           bool autocommit,
                           bool recursive)
      throw (castor::exception::Exception) = 0;

    /**
     * Deletes foreign representation of a C++ Object.
     * @param address where the representation of
     * the object is stored
     * @param object the object to deal with
     * @param alreadyDone the set of objects which representation
     * was already deleted. This is needed to avoid looping in case
     * of circular dependencies
     * @param autocommit whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void deleteRep(castor::IAddress* address,
                           castor::IObject* object,
                           castor::ObjectSet& alreadyDone,
                           bool autocommit = true)
      throw (castor::exception::Exception) = 0;

    /**
     * create C++ object from foreign representation
     * @param address the place where to find the foreign
     * representation
     * @param newlyCreated a map of object that were created as part of the
     * last user call to createObj, indexed by id. If a reference to one if
     * these id is found, the existing associated object should be used.
     * This trick basically allows circular dependencies.
     * @return the C++ object created from its reprensentation
     * or 0 if unsuccessful. Note that the caller is responsible
     * for the deallocation of the newly created object
     * @param recursive if set to true, the objects refered
     * by the returned object will be created too and recursively.
     * In case the object was in the newlyCreated catalog, it will
     * not be touched and may thus contain references.
     * @exception Exception throws an Exception in case of error
     */
    virtual IObject* createObj (IAddress* address,
                                ObjectCatalog& newlyCreated,
                                bool recursive)
      throw (castor::exception::Exception) = 0;

    /**
     * Updates C++ object from its foreign representation.
     * @param object the object to deal with
     * @param alreadyDone the set of objects already updated.
     * This is needed to avoid looping in case of circular dependencies
     * @exception Exception throws an Exception in case of error
     */
    virtual void updateObj(IObject* object,
                           ObjectCatalog& alreadyDone)
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
