/******************************************************************************
 *                      StreamPtrCnv.cpp
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
 * @(#)$RCSfile: StreamPtrCnv.cpp,v $ $Revision: 1.13 $ $Release$ $Date: 2004/10/29 15:25:32 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "StreamPtrCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/IAddress.hpp"
#include "castor/IConverter.hpp"
#include "castor/IFactory.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/io/StreamAddress.hpp"

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamPtrCnv> s_factoryStreamPtrCnv;
const castor::IFactory<castor::IConverter>& StreamPtrCnvFactory = 
  s_factoryStreamPtrCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamPtrCnv::StreamPtrCnv() :
  StreamBaseCnv() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamPtrCnv::~StreamPtrCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamPtrCnv::ObjType() {
  return OBJ_Ptr;
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamPtrCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamPtrCnv::createRep(castor::IAddress* address,
                                         castor::IObject* object,
                                         bool autocommit,
                                         unsigned int type)
  throw (castor::exception::Exception) {
  // This is normally never called, so just raise an exception
  castor::exception::Internal ex;
  ex.getMessage() << "castor::io::StreamPtrCnv::createRep "
                  << "should never be called";
  throw ex;
}

//------------------------------------------------------------------------------
// createObj
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamPtrCnv::createObj
(castor::IAddress* address)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "castor::io::StreamPtrCnv::createObj "
                  << "should never be called";
  throw ex;
}

//------------------------------------------------------------------------------
// marshalObject
//------------------------------------------------------------------------------
void castor::io::StreamPtrCnv::marshalObject(castor::IObject* object,
                                             castor::io::StreamAddress* address,
                                             castor::ObjectSet& alreadyDone)
  throw (castor::exception::Exception) {
  if (0 != object) {
    // This is normally never called, so just raise an exception
    castor::exception::Internal ex;
    ex.getMessage() << "castor::io::StreamPtrCnv::marshalObject "
                    << "should only be called with null objects";
    throw ex;
  }
  // marshall null id
  address->stream() << castor::OBJ_Ptr << ((unsigned int)0);
}

//------------------------------------------------------------------------------
// unmarshalObject
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamPtrCnv::unmarshalObject(castor::io::biniostream& stream,
                                                           castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  castor::io::StreamAddress ad(stream, "StreamCnvSvc", SVC_STREAMCNV);
  // Just retrieve the object from the newlyCreated catalog
  // using the id stored in the stream
  unsigned int id;
  ad.stream() >> id;
  // Case of a null pointer
  if (id == 0) return 0;
  // Case of a regular pointer
  if (newlyCreated.find(id) != newlyCreated.end()) {
    return newlyCreated[id];
  }
  castor::exception::Internal ex;
  ex.getMessage() << "Deserialization error : wrong id found in stream : "
                  << id;
  throw ex;
}
