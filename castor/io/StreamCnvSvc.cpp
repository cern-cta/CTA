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
 * @(#)$RCSfile: StreamCnvSvc.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2004/10/07 14:34:01 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/SvcFactory.hpp"

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
// createObj
// -----------------------------------------------------------------------
castor::IObject* castor::io::StreamCnvSvc::createObj
(castor::IAddress* address,
 ObjectCatalog& newlyCreated,
 bool recursive)
  throw (castor::exception::Exception) {
  // If the address has no type, find it out
  if (OBJ_INVALID == address->objType()) {
    StreamAddress* ad = dynamic_cast<StreamAddress*>(address);
    unsigned int objType;
    ad->stream() >> objType;
    ad->setObjType(objType);
  }
  // call method of parent object
  return this->BaseCnvSvc::createObj(address, newlyCreated, recursive);
}
