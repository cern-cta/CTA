/******************************************************************************
 *                      StreamBaseCnv.hpp
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
 * @(#)$RCSfile: StreamBaseCnv.hpp,v $ $Revision: 1.8 $ $Release$ $Date: 2004/11/01 09:13:02 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef IO_STREAMBASECNV_HPP 
#define IO_STREAMBASECNV_HPP 1

// Include files
#include "castor/io/biniostream.h"
#include "castor/io/IStreamConverter.hpp"
#include "castor/BaseObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/ObjectCatalog.hpp"

namespace castor {

  // Forward Declarations
  class IObject;

  namespace io {

    // Forward Declarations
    class StreamCnvSvc;
    class StreamAddress;

    /**
     * A base converter for marshal/unmarshaling objects
     * into/from streams
     */
    class StreamBaseCnv : public BaseObject, public IStreamConverter {
      
    public:
      
      /**
       * Constructor
       */
      StreamBaseCnv();

      /**
       * Destructor
       */
      virtual ~StreamBaseCnv() throw();

      /**
       * gets the representation type, that is the type of
       * the representation this converter can deal with
       */
      static const unsigned int RepType();

      /**
       * gets the representation type, that is the type of
       * the representation this converter can deal with
       */
      virtual const unsigned int repType() const;
      
      /**
       * Updates foreign representation from a C++ Object.
       * @param address where the representation of
       * the object is stored
       * @param object the object to deal with
       * @param autocommit whether the changes to the database
       * should be commited or not
       * @exception Exception throws an Exception in cas of error
       */
      virtual void updateRep(castor::IAddress* address,
                             castor::IObject* object,
                             bool autocommit)
        throw (castor::exception::Exception);

      /**
       * Deletes foreign representation of a C++ Object.
       * @param address where the representation of
       * the object is stored
       * @param object the object to deal with
       * @param autocommit whether the changes to the database
       * should be commited or not
       * @exception Exception throws an Exception in cas of error
       */
      virtual void deleteRep(castor::IAddress* address,
                             castor::IObject* object,
                             bool autocommit)
        throw (castor::exception::Exception);

      /**
       * Updates C++ object from its foreign representation.
       * @param obj the object to deal with
       * @exception Exception throws an Exception in cas of error
       */
      virtual void updateObj(castor::IObject* obj)
        throw (castor::exception::Exception);

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
        throw (castor::exception::Exception);
      
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
        throw (castor::exception::Exception);

    protected:
      
      /**
       * access to the stream conversion service for child classes
       */
      castor::io::StreamCnvSvc* cnvSvc() const;

    private:

      /***********/
      /* Members */
      /***********/

      /// The corresponding conversion service
      castor::io::StreamCnvSvc* m_cnvSvc;

    };

  } // end of namespace io

} // end of namespace castor

#endif // IO_STREAMBASECNV_HPP
