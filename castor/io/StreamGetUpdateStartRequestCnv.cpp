/******************************************************************************
 *                      castor/io/StreamGetUpdateStartRequestCnv.cpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "StreamGetUpdateStartRequestCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/IClient.hpp"
#include "castor/ICnvFactory.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/StreamAddress.hpp"
#include "castor/io/StreamCnvSvc.hpp"
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/SvcClass.hpp"
#include "osdep.h"
#include <string>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamGetUpdateStartRequestCnv> s_factoryStreamGetUpdateStartRequestCnv;
const castor::ICnvFactory& StreamGetUpdateStartRequestCnvFactory = 
  s_factoryStreamGetUpdateStartRequestCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamGetUpdateStartRequestCnv::StreamGetUpdateStartRequestCnv(castor::ICnvSvc* cnvSvc) :
  StreamBaseCnv(cnvSvc) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamGetUpdateStartRequestCnv::~StreamGetUpdateStartRequestCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamGetUpdateStartRequestCnv::ObjType() {
  return castor::stager::GetUpdateStartRequest::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamGetUpdateStartRequestCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamGetUpdateStartRequestCnv::createRep(castor::IAddress* address,
                                                           castor::IObject* object,
                                                           bool autocommit,
                                                           unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::GetUpdateStartRequest* obj = 
    dynamic_cast<castor::stager::GetUpdateStartRequest*>(object);
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
  ad->stream() << obj->userTag();
  ad->stream() << obj->reqId();
  ad->stream() << obj->subreqId();
  ad->stream() << obj->diskServer();
  ad->stream() << obj->fileSystem();
  ad->stream() << obj->id();
}

//------------------------------------------------------------------------------
// createObj
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamGetUpdateStartRequestCnv::createObj(castor::IAddress* address)
  throw (castor::exception::Exception) {
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  // create the new Object
  castor::stager::GetUpdateStartRequest* object = new castor::stager::GetUpdateStartRequest();
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
  std::string userTag;
  ad->stream() >> userTag;
  object->setUserTag(userTag);
  std::string reqId;
  ad->stream() >> reqId;
  object->setReqId(reqId);
  u_signed64 subreqId;
  ad->stream() >> subreqId;
  object->setSubreqId(subreqId);
  std::string diskServer;
  ad->stream() >> diskServer;
  object->setDiskServer(diskServer);
  std::string fileSystem;
  ad->stream() >> fileSystem;
  object->setFileSystem(fileSystem);
  u_signed64 id;
  ad->stream() >> id;
  object->setId(id);
  return object;
}

//------------------------------------------------------------------------------
// marshalObject
//------------------------------------------------------------------------------
void castor::io::StreamGetUpdateStartRequestCnv::marshalObject(castor::IObject* object,
                                                               castor::io::StreamAddress* address,
                                                               castor::ObjectSet& alreadyDone)
  throw (castor::exception::Exception) {
  castor::stager::GetUpdateStartRequest* obj = 
    dynamic_cast<castor::stager::GetUpdateStartRequest*>(object);
  if (0 == obj) {
    // Case of a null pointer
    address->stream() << castor::OBJ_Ptr << ((unsigned int)0);
  } else if (alreadyDone.find(obj) == alreadyDone.end()) {
    // Case of a pointer to a non streamed object
    createRep(address, obj, true);
    // Mark object as done
    alreadyDone.insert(obj);
    cnvSvc()->marshalObject(obj->svcClass(), address, alreadyDone);
    cnvSvc()->marshalObject(obj->client(), address, alreadyDone);
  } else {
    // case of a pointer to an already streamed object
    address->stream() << castor::OBJ_Ptr << alreadyDone[obj];
  }
}

//------------------------------------------------------------------------------
// unmarshalObject
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamGetUpdateStartRequestCnv::unmarshalObject(castor::io::biniostream& stream,
                                                                             castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  castor::io::StreamAddress ad(stream, "StreamCnvSvc", SVC_STREAMCNV);
  castor::IObject* object = createObj(&ad);
  // Mark object as created
  newlyCreated.insert(object);
  // Fill object with associations
  castor::stager::GetUpdateStartRequest* obj = 
    dynamic_cast<castor::stager::GetUpdateStartRequest*>(object);
  ad.setObjType(castor::OBJ_INVALID);
  IObject* objSvcClass = cnvSvc()->unmarshalObject(ad, newlyCreated);
  obj->setSvcClass(dynamic_cast<castor::stager::SvcClass*>(objSvcClass));
  ad.setObjType(castor::OBJ_INVALID);
  IObject* objClient = cnvSvc()->unmarshalObject(ad, newlyCreated);
  obj->setClient(dynamic_cast<castor::IClient*>(objClient));
  return object;
}

