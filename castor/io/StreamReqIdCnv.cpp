/******************************************************************************
 *                      castor/io/StreamReqIdCnv.cpp
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
 * @(#)$RCSfile: StreamReqIdCnv.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/10/05 13:37:29 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "StreamReqIdCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/IConverter.hpp"
#include "castor/IFactory.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/StreamAddress.hpp"
#include "castor/io/StreamCnvSvc.hpp"
#include "castor/stager/ReqId.hpp"
#include "castor/stager/ReqIdRequest.hpp"
#include "osdep.h"
#include <string>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamReqIdCnv> s_factoryStreamReqIdCnv;
const castor::IFactory<castor::IConverter>& StreamReqIdCnvFactory = 
  s_factoryStreamReqIdCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamReqIdCnv::StreamReqIdCnv() :
  StreamBaseCnv() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamReqIdCnv::~StreamReqIdCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamReqIdCnv::ObjType() {
  return castor::stager::ReqId::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamReqIdCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamReqIdCnv::createRep(castor::IAddress* address,
                                           castor::IObject* object,
                                           castor::ObjectSet& alreadyDone,
                                           bool autocommit,
                                           bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::ReqId* obj = 
    dynamic_cast<castor::stager::ReqId*>(object);
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  ad->stream() << obj->type();
  ad->stream() << obj->value();
  ad->stream() << obj->id();
  // Mark object as done
  alreadyDone.insert(obj);
  marshalObject(obj->request(), ad, alreadyDone);
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::io::StreamReqIdCnv::updateRep(castor::IAddress* address,
                                           castor::IObject* object,
                                           castor::ObjectSet& alreadyDone,
                                           bool autocommit,
                                           bool recursive)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update representation in case of streaming."
                  << std::endl;
  throw ex;
}

//------------------------------------------------------------------------------
// deleteRep
//------------------------------------------------------------------------------
void castor::io::StreamReqIdCnv::deleteRep(castor::IAddress* address,
                                           castor::IObject* object,
                                           castor::ObjectSet& alreadyDone,
                                           bool autocommit)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot delete representation in case of streaming."
                  << std::endl;
  throw ex;
}

//------------------------------------------------------------------------------
// createObj
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamReqIdCnv::createObj(castor::IAddress* address,
                                                       castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  // create the new Object
  castor::stager::ReqId* object = new castor::stager::ReqId();
  // Now retrieve and set members
  std::string value;
  ad->stream() >> value;
  object->setValue(value);
  u_signed64 id;
  ad->stream() >> id;
  object->setId(id);
  newlyCreated.insert(object);
  IObject* objRequest = unmarshalObject(ad->stream(), newlyCreated);
  object->setRequest(dynamic_cast<castor::stager::ReqIdRequest*>(objRequest));
  return object;
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::io::StreamReqIdCnv::updateObj(castor::IObject* obj,
                                           castor::ObjectCatalog& alreadyDone)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update object in case of streaming."
                  << std::endl;
  throw ex;
}

