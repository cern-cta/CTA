/******************************************************************************
 *                      Services.hpp
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

#ifndef CASTOR_SERVICES_HPP
#define CASTOR_SERVICES_HPP 1

//Include Files
#include <vector>
#include <map>
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward declarations
  class IService;
  class IAddress;
  class ICnvSvc;
  class IObject;

  /**
   * A class holding the list of available services.
   */
  class Services {

  public:

    /** Default constructor */
    Services();

    /** Default destructor */
    virtual ~Services();

    /**
     * gets a service by name.
     * If id not 0 and the service does not exist, it
     * is created.
     */
    IService* service (const std::string name,
                       const unsigned int id = 0)
      throw(castor::exception::Exception);

    /**
     * gets a conversion service by name.
     * If id not 0 and the service does not exist, it
     * is created.
     */
    ICnvSvc* cnvService (const std::string name,
                         const unsigned int id = 0)
      throw(castor::exception::Exception);

    /**
     * create foreign representation from a C++ Object
     * @param address where to store the representation of
     * the object
     * @param object the object to deal with
     * @param endTransaction whether the changes to the database
     * should be commited or not. Default is yes.
     * @exception Exception throws an Exception in case of error
     */
    void createRep(IAddress* address,
                   IObject* object,
                   bool endTransaction = true)
      throw (castor::exception::Exception);

    /**
     * create foreign representations from a set of C++ Object
     * @param address where to store the representation of
     * the objects
     * @param objects the list of objects to deal with
     * @param endTransaction whether the changes to the database
     * should be commited or not. Default is yes.
     * @exception Exception throws an Exception in case of error
     */
    virtual void bulkCreateRep(IAddress* address,
                               std::vector<IObject*> &objects,
			       bool endTransaction = true)
      throw (castor::exception::Exception);
    
    /**
     * Updates foreign representation from a C++ Object.
     * @param address where the representation of
     * the object is stored
     * @param object the object to deal with
     * @param endTransaction whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    void updateRep(IAddress* address,
                   IObject* object,
                   bool endTransaction = true)
      throw (castor::exception::Exception);

    /**
     * deletes foreign representation of a C++ Object
     * @param address where the representation of
     * the object is stored
     * @param object the object to deal with
     * @param endTransaction whether the changes to the database
     * should be commited or not. Default is not.
     * @exception Exception throws an Exception in case of error
     */
    void deleteRep(IAddress* address,
                   IObject* object,
                   bool endTransaction = false)
      throw (castor::exception::Exception);

    /**
     * create C++ object from foreign representation
     * @param address the place where to find the foreign
     * representation
     * @return the C++ object created from its reprensentation
     * or 0 if unsuccessful. Note that the caller is responsible
     * for the deallocation of the newly created object
     * @exception Exception throws an Exception in case of error
     */
    IObject* createObj(IAddress* address)
      throw (castor::exception::Exception);

    /**
     * create C++ objects from foreign representations
     * @param address the place where to find the foreign
     * representations
     * @return the C++ objects created from the representations
     * or empty vector if unsuccessful. Note that the caller is
     * responsible for the deallocation of the newly created objects
     * @exception Exception throws an Exception in case of error
     */
    std::vector<IObject*> bulkCreateObj(IAddress* address)
      throw (castor::exception::Exception);

    /**
     * Updates C++ object from its foreign representation.
     * @param address where to find the object
     * @param object the object to deal with
     * @param alreadyDone the set of objects already updated.
     * This is needed to avoid looping in case of circular dependencies
     * @exception Exception throws an Exception in case of error
     */
    virtual void updateObj(castor::IAddress* address,
                           castor::IObject* object)
      throw (castor::exception::Exception);

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
                         bool endTransaction)
      throw (castor::exception::Exception);
    
    /**
     * Retrieve from the foreign representation some of the
     * objects refered by a given C++ object.
     * @param address the place where to find the foreign representation
     * @param object the original object
     * @param type the type of the refered objects to retrieve
     * @param endTransaction whether the locks taken in the database
     * should be released or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void fillObj(castor::IAddress* address,
                         castor::IObject* object,
                         unsigned int type,
                         bool endTransaction = false)
      throw (castor::exception::Exception);

    /**
     * Forces the commit of the last changes in a given DB
     * @param address what to commit
     * @exception Exception throws an Exception in case of error
     */
    virtual void commit(castor::IAddress* address)
      throw (castor::exception::Exception);

    /**
     * Forces the rollback of the last changes in a given DB
     * @param address what to rollback
     * @exception Exception throws an Exception in case of error
     */
    virtual void rollback(castor::IAddress* address)
      throw (castor::exception::Exception);

  private:
    /**
     * gets a conversion service able to deal with a given
     * address
     */
    castor::ICnvSvc* cnvSvcFromAddress(castor::IAddress* address)
      throw (castor::exception::Exception);

    /** the list of services, by name */
    std::map<const std::string, IService*> m_services;

    /** the list of dlopen handles to be closed at the end */
    std::vector<void*> m_dlopenHandles;

  };

} // end of namespace castor

#endif // CASTOR_SERVICES_HPP
