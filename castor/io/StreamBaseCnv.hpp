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
 * @(#)$RCSfile: StreamBaseCnv.hpp,v $ $Revision: 1.5 $ $Release$ $Date: 2004/10/11 13:43:52 $ $Author: sponcec3 $
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
       * Removes a link from a parent to a child.
       * This implementation always raises an exception
       * since this method makes no sense for streaming.
       * @param parent the parent
       * @param child the child
       * @exception Exception Always thrown, see above.
       */
      virtual void unlinkChild(const castor::IObject* parent,
                               const castor::IObject* child)
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
