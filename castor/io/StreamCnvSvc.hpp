/******************************************************************************
 *                      StreamCnvSvc.hpp
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
 * @(#)$RCSfile: StreamCnvSvc.hpp,v $ $Revision: 1.5 $ $Release$ $Date: 2004/10/11 13:43:53 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef IO_STREAMCNVSVC_HPP
#define IO_STREAMCNVSVC_HPP 1

#include "castor/BaseCnvSvc.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward Declarations
  class IAddress;
  class IObject;

  namespace io {

    /**
     * Conversion service for stream marshalling/unmarshalling
     */
    class StreamCnvSvc : public BaseCnvSvc {

    public:

      /** default constructor */
      StreamCnvSvc(const std::string name);

      /** default destructor */
      ~StreamCnvSvc() throw();

      /** Get the service id */
      virtual inline const unsigned int id() const;

      /** Get the service id */
      static const unsigned int ID();

      /**
       * gets the representation type, that is the type of
       * the representation this conversion service can deal with
       */
      virtual const unsigned int repType() const;

      /**
       * gets the representation type, that is the type of
       * the representation this conversion service can deal with
       */
      static const unsigned int REPTYPE();

      /**
       * Creates foreign representation from a C++ Object
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
        throw (castor::exception::Exception);

      /**
       * create C++ object from foreign representation.
       * Reimplemented from BaseCnvSvc. This version is able to
       * make use of StreamAdresses and to deduce the object type
       * by unmarshaling it from the stream
       * @param address the place where to find the foreign
       * representation
       * @return the C++ object created from its reprensentation
       * or 0 if unsuccessful. Note that the caller is responsible
       * for the deallocation of the newly created object
       * @exception Exception throws an Exception in cas of error
       */
      IObject* createObj (IAddress* address)
        throw (castor::exception::Exception);

      /**
       * Marshals an object using a StreamAddress.
       * If the object is in alreadyDone, just marshal its id. Otherwise, call createRep
       * and recursively marshal the refered objects.
       * @param object the object to marshal
       * @param address the address where to marshal
       * @param alreadyDone the list of objects already marshalled
       * @exception Exception throws an Exception in case of error
       */
      virtual void marshalObject(castor::IObject* object,
                                 castor::io::StreamAddress* address,
                                 castor::ObjectSet& alreadyDone)
        throw (castor::exception::Exception);

      /**
       * Unmarshals an object from a StreamAddress.
       * @param address the address where to unmarshall the data
       * @param newlyCreated a set of already created objects
       * that is used in case of circular dependencies
       * @return a pointer to the new object. If their was some
       * memory allocation (creation of a new object), the caller
       * is responsible for its deallocation
       * @exception Exception throws an Exception in case of error
       */
      virtual castor::IObject* unmarshalObject(StreamAddress& address,
                                               castor::ObjectCatalog& newlyCreated)
        throw (castor::exception::Exception);

    };

  } // end of namespace db

} // end of namespace castor

#endif // IO_STREAMCNVSVC_HPP
