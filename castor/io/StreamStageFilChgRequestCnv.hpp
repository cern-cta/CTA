/******************************************************************************
 *                      castor/io/StreamStageFilChgRequestCnv.hpp
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
 * @(#)$RCSfile: StreamStageFilChgRequestCnv.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/10/07 14:34:01 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_IO_STAGEFILCHGREQUEST_HPP
#define CASTOR_IO_STAGEFILCHGREQUEST_HPP

// Include Files
#include "castor/IAddress.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/StreamBaseCnv.hpp"

namespace castor {

  // Forward declarations
  class IObject;

  namespace io {

    /**
     * class StreamStageFilChgRequestCnv
     * A converter for marshalling/unmarshalling StageFilChgRequest into/from stl
     * streams
     */
    class StreamStageFilChgRequestCnv : public StreamBaseCnv {

    public:

      /**
       * Constructor
       */
      StreamStageFilChgRequestCnv();

      /**
       * Destructor
       */
      virtual ~StreamStageFilChgRequestCnv() throw();

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
       * Creates foreign representation from a C++ Object.
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
       * @exception Exception throws an Exception in cas of error
       */
      virtual void createRep(castor::IAddress* address,
                             castor::IObject* object,
                             castor::ObjectSet& alreadyDone,
                             bool autocommit,
                             bool recursive)
        throw (castor::exception::Exception);

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
       * by object will be updated too and recursively
       * @exception Exception throws an Exception in cas of error
       */
      virtual void updateRep(castor::IAddress* address,
                             castor::IObject* object,
                             castor::ObjectSet& alreadyDone,
                             bool autocommit,
                             bool recursive)
        throw (castor::exception::Exception);

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
       * @exception Exception throws an Exception in cas of error
       */
      virtual void deleteRep(castor::IAddress* address,
                             castor::IObject* object,
                             castor::ObjectSet& alreadyDone,
                             bool autocommit)
        throw (castor::exception::Exception);

      /**
       * Creates C++ object from foreign representation
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
       * @exception Exception throws an Exception in cas of error
       */
      virtual castor::IObject* createObj(castor::IAddress* address,
                                         castor::ObjectCatalog& newlyCreated,
                                         bool recursive)
        throw (castor::exception::Exception);

      /**
       * Updates C++ object from its foreign representation.
       * @param obj the object to deal with
       * @param alreadyDone the set of objects already updated.
       * This is needed to avoid looping in case of circular dependencies
       * @exception Exception throws an Exception in cas of error
       */
      virtual void updateObj(castor::IObject* obj,
                             castor::ObjectCatalog& alreadyDone)
        throw (castor::exception::Exception);

    }; // end of class StreamStageFilChgRequestCnv

  }; // end of namespace io

}; // end of namespace castor

#endif // CASTOR_IO_STAGEFILCHGREQUEST_HPP
