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
 * @(#)$RCSfile: StreamCnvSvc.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2004/10/07 14:34:01 $ $Author: sponcec3 $
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
       * create C++ object from foreign representation.
       * Reimplemented from BaseCnvSvc. This version is able to
       * make use of StreamAdresses and to deduce the object type
       * by unmarshaling it from the stream
       * @param address the place where to find the foreign
       * representation
       * @param newlyCreated a map of object that were created as part of the
       * last user call to createObj, indexed by id. If a reference to one if
       * these id is found, the existing associated object should be used.
       * This trick basically allows circular dependencies.
       * @param recursive if set to true, the objects refered
       * by the returned object will be created too and recursively.
       * In case the object was in the newlyCreated catalog, it will
       * not be touched and may thus contain references.
       * @return the C++ object created from its reprensentation
       * or 0 if unsuccessful. Note that the caller is responsible
       * for the deallocation of the newly created object
       * @exception Exception throws an Exception in cas of error
       */
      IObject* createObj (IAddress* address,
                          ObjectCatalog& newlyCreated,
                          bool recursive)
        throw (castor::exception::Exception);
      
    };

  } // end of namespace db

} // end of namespace castor

#endif // IO_STREAMCNVSVC_HPP
