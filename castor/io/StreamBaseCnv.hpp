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
 * @(#)$RCSfile: StreamBaseCnv.hpp,v $ $Revision: 1.10 $ $Release$ $Date: 2006/05/02 10:13:02 $ $Author: itglp $
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
  class ICnvSvc;

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
      StreamBaseCnv(castor::ICnvSvc* cnvSvc);

      /**
       * Destructor
       */
      virtual ~StreamBaseCnv() throw();

      /**
       * Gets the representation type, that is the type of
       * the representation this converter can deal with
       */
      static const unsigned int RepType();

      /**
       * Gets the representation type, that is the type of
       * the representation this converter can deal with
       */
      virtual const unsigned int repType() const;
      
      /**
       * Updates foreign representation from a C++ Object.
       * This streaming implementation always throws an exception.
       */
      virtual void updateRep(castor::IAddress* address,
                             castor::IObject* object,
                             bool autocommit)
        throw (castor::exception::Exception);

      /**
       * Deletes foreign representation of a C++ Object.
       * This streaming implementation always throws an exception.
       */
      virtual void deleteRep(castor::IAddress* address,
                             castor::IObject* object,
                             bool autocommit)
        throw (castor::exception::Exception);

      /**
       * Updates C++ object from its foreign representation.
       * This streaming implementation always throws an exception.
       */
      virtual void updateObj(castor::IObject* obj)
        throw (castor::exception::Exception);

      /**
       * Fill the foreign representation with some of the objects
       * refered by a given C++ object.
       * This streaming implementation always throws an exception.
       */
      virtual void fillRep(castor::IAddress* address,
                           castor::IObject* object,
                           unsigned int type,
                           bool autocommit = false)
        throw (castor::exception::Exception);
      
      /**
       * Retrieve from the foreign representation some of the
       * objects refered by a given C++ object.
       * This streaming implementation always throws an exception.
       */
      virtual void fillObj(castor::IAddress* address,
                           castor::IObject* object,
                           unsigned int type,
                           bool autocommit = false)
        throw (castor::exception::Exception);

    protected:
      
      /**
       * Access to the stream conversion service for child classes
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
