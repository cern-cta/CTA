/******************************************************************************
 *                      IStreamConverter.hpp
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
 * @(#)$RCSfile: IStreamConverter.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/11 16:48:14 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef IO_ISTREAMCONVERTER_HPP
#define IO_ISTREAMCONVERTER_HPP 1

// Include Files
#include "castor/IConverter.hpp"
#include "castor/io/biniostream.h"
#include "castor/exception/Exception.hpp"

namespace castor {

  /**
   * Forward declarations
   */
  class ObjectSet;
  class ObjectCatalog;

  namespace io {  
    /**
     * Forward declarations
     */
    class StreamAddress;
  }
  

  /**
   * abstract interface for all stream converters
   */
  class IStreamConverter : public IConverter {

  public:

    /**
     * Marshals an object using a StreamAddress.
     * If the object is in alreadyDone, just marshal its id.
     * Otherwise, call createRep and recursively marshal the
     * refered objects.
     * @param obj the object to marshal
     * @param address the address where to marshal
     * @param alreadyDone the list of objects already marshalled
     * @exception Exception throws an Exception in case of error"
     */
    virtual void marshalObject(castor::IObject* obj,
                               castor::io::StreamAddress* address,
                               castor::ObjectSet& alreadyDone)
      throw (castor::exception::Exception) = 0;

    /**
     * Unmarshals an object from a StreamAddress
     * @param stream the stream from which to unmarshal
     * @param newlyCreated a set of already created objects
     * that is used in case of circular dependencies
     * @return a pointer to the new object. If their was some
     * memory allocation (creation of a new object), the caller
     * is responsible for its deallocation
     * @exception Exception throws an Exception in case of error"
     */
    virtual castor::IObject* unmarshalObject(castor::io::biniostream& stream,
                                             castor::ObjectCatalog& newlyCreated)
      throw (castor::exception::Exception) = 0;

  };

} // end of namespace castor

#endif // IO_ISTREAMCONVERTER_HPP
