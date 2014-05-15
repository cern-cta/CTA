/******************************************************************************
 *                      BaseCnvSvc.hpp
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
#include <map>

// Local Includes
#include "ICnvSvc.hpp"
#include "Constants.hpp"
#include "BaseSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/SQLError.hpp"

namespace castor {

  // Forward Declarations
  class IConverter;
  class IAddress;
  class IObject;

  /**
   * Base class for conversion services.
   * It essentially handles a map of converters.
   */
  class BaseCnvSvc : public BaseSvc, public virtual ICnvSvc {

  public:

    /**
     * constructor
     */
    BaseCnvSvc(const std::string name);

    /**
     * Destructor
     */
    virtual ~BaseCnvSvc() throw();

    /**
     * resets the service
     */
    virtual void reset() throw();

    /**
     * Add an alias, that a rule that says that a given
     * type should be converted using converters of another
     * type
     * @param alias the alias type
     * @param real the actual type, whose converters should
     * be used for the alias type
     */
    virtual void addAlias(const unsigned int alias,
                            const unsigned int real);

    /**
     * Remove an alias
     * @param id Id of the alias type
     */
    virtual void removeAlias(const unsigned int id);

    /**
     * Retrieve converter from list
     * @param objType the type of object to be converted
     * @return the converter corresponding to teh object
     * type or 0 if non was found
     * @exception Exception throws an Exception if no converter
     * is found
     */
    virtual IConverter* converter(const unsigned int objType)
      ;

    /**
     * gets the representation type, that is the type of
     * the representation this converter can deal with
     */
    virtual unsigned int repType() const = 0;

    /**
     * Creates foreign representation from a C++ Object
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
                           unsigned int type = OBJ_INVALID)
      ;

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
      ;
    
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
      ;
    
    /**
     * Deletes foreign representation of a C++ Object.
     * @param address where the representation of
     * the object is stored
     * @param object the object to deal with
     * @param endTransaction whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void deleteRep(castor::IAddress* address,
                           castor::IObject* object,
                           bool endTransaction)
      ;
    
    /**
     * Creates C++ object from foreign representation
     * @param address the place where to find the foreign
     * representation
     * @return the C++ object created from its reprensentation
     * or 0 if unsuccessful. Note that the caller is responsible
     * for the deallocation of the newly created object
     * @exception Exception throws an Exception in case of error
     */
    virtual IObject* createObj (IAddress* address)
      ;

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
      ;

    /**
     * Updates C++ object from its foreign representation.
     * @param object the object to deal with
     * @exception Exception throws an Exception in case of error
     */
    virtual void updateObj(IObject* object)
      ;

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
      ;
    
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
      ;

    /**
     * Forces the commit of the last changes.
     * This is a default, empty implementation for conversion
     * services where commiting is not relevant. It should
     * be overriden when necessary
     * @exception Exception throws an Exception in case of error
     */
    virtual void commit()
      ;

    /**
     * Forces the rollback of the last changes
     * This is a default, empty implementation for conversion
     * services where rollbacking is not relevant. It should
     * be overriden when necessary
     * @exception Exception throws an Exception in case of error
     */
    virtual void rollback()
      ;

    /**
     * Creates a prepared statement wrapped with the
     * db-independent CDBC API
     * @param stmt the string statement to be prepared 
     */
    virtual castor::db::IDbStatement* createStatement(const std::string& stmt)
      ;
	  
    /**
     * Deletes foreign representation of a C++ Object without
     * needing the object. The cost of this method is higher
     * than the cost of deleteRep since it needs to call
     * createObj first
     * @param address where the representation of
     * the object is stored
     * @param endTransaction whether the changes to the database
     * should be commited or not
     * @exception Exception throws an Exception in case of error
     */
    virtual void deleteRepByAddress (IAddress* address,
                                     bool endTransaction = true)
      ;

  private:
    /**
     * The list of available converters, indexed by id
     */
    std::map<unsigned int, IConverter*> m_converters;

    /**
     * The list of aliases. That is the list of types
     * for which we should look for a converter of another
     * type.
     */
    std::map<unsigned int, unsigned int> m_aliases;

  };

} // end of namespace castor

