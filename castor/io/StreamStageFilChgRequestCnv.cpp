/******************************************************************************
 *                      castor/io/StreamStageFilChgRequestCnv.cpp
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
 * @(#)$RCSfile: StreamStageFilChgRequestCnv.cpp,v $ $Revision: 1.5 $ $Release$ $Date: 2004/10/11 14:13:50 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "StreamStageFilChgRequestCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/IClient.hpp"
#include "castor/IConverter.hpp"
#include "castor/IFactory.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/StreamAddress.hpp"
#include "castor/io/StreamCnvSvc.hpp"
#include "castor/stager/StageFilChgRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "osdep.h"
#include <string>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamStageFilChgRequestCnv> s_factoryStreamStageFilChgRequestCnv;
const castor::IFactory<castor::IConverter>& StreamStageFilChgRequestCnvFactory = 
  s_factoryStreamStageFilChgRequestCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamStageFilChgRequestCnv::StreamStageFilChgRequestCnv() :
  StreamBaseCnv() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamStageFilChgRequestCnv::~StreamStageFilChgRequestCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamStageFilChgRequestCnv::ObjType() {
  return castor::stager::StageFilChgRequest::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamStageFilChgRequestCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamStageFilChgRequestCnv::createRep(castor::IAddress* address,
                                                        castor::IObject* object,
                                                        bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::StageFilChgRequest* obj = 
    dynamic_cast<castor::stager::StageFilChgRequest*>(object);
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  ad->stream() << obj->type();
  ad->stream() << obj->flags();
  ad->stream() << obj->userName();
  ad->stream() << obj->euid();
  ad->stream() << obj->egid();
  ad->stream() << obj->mask();
  ad->stream() << obj->pid();
  ad->stream() << obj->machine();
  ad->stream() << obj->svcClassName();
  ad->stream() << obj->id();
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::io::StreamStageFilChgRequestCnv::updateRep(castor::IAddress* address,
                                                        castor::IObject* object,
                                                        bool autocommit)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update representation in case of streaming."
                  << std::endl;
  throw ex;
}

//------------------------------------------------------------------------------
// deleteRep
//------------------------------------------------------------------------------
void castor::io::StreamStageFilChgRequestCnv::deleteRep(castor::IAddress* address,
                                                        castor::IObject* object,
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
castor::IObject* castor::io::StreamStageFilChgRequestCnv::createObj(castor::IAddress* address)
  throw (castor::exception::Exception) {
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  // create the new Object
  castor::stager::StageFilChgRequest* object = new castor::stager::StageFilChgRequest();
  // Now retrieve and set members
  u_signed64 flags;
  ad->stream() >> flags;
  object->setFlags(flags);
  std::string userName;
  ad->stream() >> userName;
  object->setUserName(userName);
  unsigned long euid;
  ad->stream() >> euid;
  object->setEuid(euid);
  unsigned long egid;
  ad->stream() >> egid;
  object->setEgid(egid);
  unsigned long mask;
  ad->stream() >> mask;
  object->setMask(mask);
  unsigned long pid;
  ad->stream() >> pid;
  object->setPid(pid);
  std::string machine;
  ad->stream() >> machine;
  object->setMachine(machine);
  std::string svcClassName;
  ad->stream() >> svcClassName;
  object->setSvcClassName(svcClassName);
  u_signed64 id;
  ad->stream() >> id;
  object->setId(id);
  return object;
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::io::StreamStageFilChgRequestCnv::updateObj(castor::IObject* obj)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update object in case of streaming."
                  << std::endl;
  throw ex;
}

//------------------------------------------------------------------------------
// marshalObject
//------------------------------------------------------------------------------
void castor::io::StreamStageFilChgRequestCnv::marshalObject(castor::IObject* object,
                                                            castor::io::StreamAddress* address,
                                                            castor::ObjectSet& alreadyDone)
  throw (castor::exception::Exception) {
  castor::stager::StageFilChgRequest* obj = 
    dynamic_cast<castor::stager::StageFilChgRequest*>(object);
  if (0 == obj) {
    // Case of a null pointer
    address->stream() << castor::OBJ_Ptr << 0;
  } else if (alreadyDone.find(obj) == alreadyDone.end()) {
    // Case of a pointer to a non streamed object
    cnvSvc()->createRep(address, obj, true);
    // Mark object as done
    alreadyDone.insert(obj);
    address->stream() << obj->subRequests().size();
    for (std::vector<castor::stager::SubRequest*>::iterator it = obj->subRequests().begin();
         it != obj->subRequests().end();
         it++) {
      cnvSvc()->marshalObject(*it, address, alreadyDone);
    }
    cnvSvc()->marshalObject(obj->client(), address, alreadyDone);
  } else {
    // case of a pointer to an already streamed object
    address->stream() << castor::OBJ_Ptr << alreadyDone[obj];
  }
}

//------------------------------------------------------------------------------
// unmarshalObject
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamStageFilChgRequestCnv::unmarshalObject(castor::io::biniostream& stream,
                                                                          castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  castor::io::StreamAddress ad(stream, "StreamCnvSvc", SVC_STREAMCNV);
  castor::IObject* object = cnvSvc()->createObj(&ad);
  // Mark object as created
  newlyCreated.insert(object);
  // Fill object with associations
  castor::stager::StageFilChgRequest* obj = 
    dynamic_cast<castor::stager::StageFilChgRequest*>(object);
  unsigned int subRequestsNb;
  ad.stream() >> subRequestsNb;
  for (unsigned int i = 0; i < subRequestsNb; i++) {
    IObject* objSubRequests = cnvSvc()->unmarshalObject(ad, newlyCreated);
    obj->addSubRequests(dynamic_cast<castor::stager::SubRequest*>(objSubRequests));
  }
  IObject* objClient = cnvSvc()->unmarshalObject(ad, newlyCreated);
  obj->setClient(dynamic_cast<castor::IClient*>(objClient));
}

