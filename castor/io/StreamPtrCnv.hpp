/******************************************************************************
 *                      StreamPtrCnv.hpp
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
 * @(#)$RCSfile: StreamPtrCnv.hpp,v $ $Revision: 1.11 $ $Release$ $Date: 2004/10/14 16:34:46 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef IO_STREAMPTRCNV_HPP
#define IO_STREAMPTRCNV_HPP 1

// Include Files
#include "castor/IObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/IAddress.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/io/StreamBaseCnv.hpp"

namespace castor {

  // Forward declarations
  class IObject;

  namespace io {

    /**
     * class StreamPtrCnv
     * A converter for marshalling/unmarshalling pointers to objects
     * into/from stl streams
     */
    class StreamPtrCnv : public StreamBaseCnv {

    public:

      /**
       * Constructor
       */
      StreamPtrCnv();

      /**
       * Destructor
       */
      virtual ~StreamPtrCnv() throw();

      /**
       * Gets the object type.
       * That is the type of object this converter can convert
       */
      static const unsigned int ObjType();

      /**
       * Gets the object type.
       * That is the type of object this converter can convert
       */
      virtual const unsigned int objType() const;

      /**
       * create foreign representation from a C++ Object.
       * @param address where to store the representation of
       * the object
       * @param object the object to deal with
       * @param autocommit whether the changes to the database
       * should be commited or not
       * @param type if not OBJ_INVALID, the ids representing
       * the links to objects of this type will not set to 0
       * as is the default.
       * @exception Exception throws an Exception in cas of error
       */
      virtual void createRep(castor::IAddress* address,
                             castor::IObject* object,
                             bool autocommit,
                             unsigned int type)
        throw (castor::exception::Exception);

      /**
       * Creates C++ object from foreign representation
       * @param address the place where to find the foreign
       * representation
       * @return the C++ object created from its reprensentation
       * or 0 if unsuccessful. Note that the caller is responsible
       * for the deallocation of the newly created object
       * @exception Exception throws an Exception in cas of error
       */
      virtual castor::IObject* createObj(castor::IAddress* address)
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
                                 StreamAddress* address,
                                 castor::ObjectSet& alreadyDone)
        throw (castor::exception::Exception);

      /**
       * Unmarshals an object from a StreamAddress.
       * @param stream the stream from which to unmarshal
       * @param newlyCreated a set of already created objects
       * that is used in case of circular dependencies
       * @return a pointer to the new object. If their was some
       * memory allocation (creation of a new object), the caller
       * is responsible for its deallocation
       * @exception Exception throws an Exception in case of error
       */
      virtual castor::IObject* unmarshalObject(castor::io::biniostream& stream,
                                               castor::ObjectCatalog& newlyCreated)
        throw (castor::exception::Exception);

    }; // end of class StreamFileCnv

  }; // end of namespace io

}; // end of namespace castor

#endif // IO_STREAMPTRCNV_HPP
