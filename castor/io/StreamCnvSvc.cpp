/******************************************************************************
 *                      StreamCnvSvc.cpp
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
 * @(#)$RCSfile: StreamCnvSvc.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2004/11/29 15:40:35 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/IObject.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/IStreamConverter.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/ObjectCatalog.hpp"

// Local Files
#include "StreamCnvSvc.hpp"
#include "StreamAddress.hpp"

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::io::StreamCnvSvc> s_factoryStreamCnvSvc;
const castor::IFactory<castor::IService>& StreamCnvSvcFactory = s_factoryStreamCnvSvc;

// -----------------------------------------------------------------------
// StreamCnvSvc
// -----------------------------------------------------------------------
castor::io::StreamCnvSvc::StreamCnvSvc(const std::string name) :
  BaseCnvSvc(name) {}

// -----------------------------------------------------------------------
// ~StreamCnvSvc
// -----------------------------------------------------------------------
castor::io::StreamCnvSvc::~StreamCnvSvc() throw() {}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::io::StreamCnvSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::io::StreamCnvSvc::ID() {
  return castor::SVC_STREAMCNV;
}

// -----------------------------------------------------------------------
// repType
// -----------------------------------------------------------------------
const unsigned int castor::io::StreamCnvSvc::repType() const {
  return REPTYPE();
}

// -----------------------------------------------------------------------
// REPTYPE
// -----------------------------------------------------------------------
const unsigned int castor::io::StreamCnvSvc::REPTYPE() {
  return castor::REP_STREAM;
}

// -----------------------------------------------------------------------
// createRep
// -----------------------------------------------------------------------
void castor::io::StreamCnvSvc::createRep(castor::IAddress* address,
                                         castor::IObject* object,
                                         bool autocommit,
                                         unsigned int type)
  throw (castor::exception::Exception) {
  castor::io::StreamAddress* ad =
    dynamic_cast <castor::io::StreamAddress*>(address);
  ObjectSet alreadyDone;
  marshalObject(object, ad, alreadyDone);
}

// -----------------------------------------------------------------------
// createObj
// -----------------------------------------------------------------------
castor::IObject* castor::io::StreamCnvSvc::createObj
(castor::IAddress* address)
  throw (castor::exception::Exception) {
  castor::io::StreamAddress* ad =
    dynamic_cast<castor::io::StreamAddress*>(address);
  ObjectCatalog newlyCreated;
  return unmarshalObject(*ad, newlyCreated);
}

//------------------------------------------------------------------------------
// marshalObject
//------------------------------------------------------------------------------
void castor::io::StreamCnvSvc::marshalObject(castor::IObject* object,
                                             castor::io::StreamAddress* address,
                                             castor::ObjectSet& alreadyDone)
  throw (castor::exception::Exception) {
  // Look for an adapted converter
  IConverter* conv;
  if (0 != object) {
    // The converter is always valid if no exception is thrown
    conv = converter(object->type());
  } else {
    // Marshalling a null pointer using PtrConverter
    conv = converter(castor::OBJ_Ptr);
  }
  // Get Stream dedicated converter
  IStreamConverter* sconv =
    dynamic_cast<IStreamConverter*>(conv);
  // convert
  sconv->marshalObject(object, address, alreadyDone);
}

//------------------------------------------------------------------------------
// unmarshalObject
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamCnvSvc::unmarshalObject(castor::io::StreamAddress& address,
                                                           castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  // If the address has no type, find it out
  if (OBJ_INVALID == address.objType()) {
    unsigned int objType;
    address.stream() >> objType;
    address.setObjType(objType);
  }
  // Look for an adapted converter
  // The converter is always valid if no exception is thrown
  castor::IConverter* conv = converter(address.objType());
  IStreamConverter* sconv =
    dynamic_cast<IStreamConverter*>(conv);
  return sconv->unmarshalObject(address.stream(), newlyCreated);
}
