/******************************************************************************
 *                      castor/io/StreamStageInRequestCnv.cpp
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
 * @(#)$RCSfile: StreamStageInRequestCnv.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/01 14:26:17 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "StreamStageInRequestCnv.hpp"
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
#include "castor/stager/RequestStatusCodes.hpp"
#include "castor/stager/StageInRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "osdep.h"
#include <string>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamStageInRequestCnv> s_factoryStreamStageInRequestCnv;
const castor::IFactory<castor::IConverter>& StreamStageInRequestCnvFactory = 
  s_factoryStreamStageInRequestCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamStageInRequestCnv::StreamStageInRequestCnv() :
  StreamBaseCnv() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamStageInRequestCnv::~StreamStageInRequestCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamStageInRequestCnv::ObjType() {
  return castor::stager::StageInRequest::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamStageInRequestCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamStageInRequestCnv::createRep(castor::IAddress* address,
                                                    castor::IObject* object,
                                                    castor::ObjectSet& alreadyDone,
                                                    bool autocommit,
                                                    bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::StageInRequest* obj = 
    dynamic_cast<castor::stager::StageInRequest*>(object);
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
  ad->stream() << obj->projectName();
  ad->stream() << obj->openflags();
  ad->stream() << obj->id();
  ad->stream() << obj->status();
  // Mark object as done
  alreadyDone.insert(obj);
  ad->stream() << obj->subRequests().size();
  for (std::vector<castor::stager::SubRequest*>::iterator it = obj->subRequests().begin();
       it != obj->subRequests().end();
       it++) {
    marshalObject(*it, ad, alreadyDone);
  }
  marshalObject(obj->client(), ad, alreadyDone);
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::io::StreamStageInRequestCnv::updateRep(castor::IAddress* address,
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
void castor::io::StreamStageInRequestCnv::deleteRep(castor::IAddress* address,
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
castor::IObject* castor::io::StreamStageInRequestCnv::createObj(castor::IAddress* address,
                                                                castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  // create the new Object
  castor::stager::StageInRequest* object = new castor::stager::StageInRequest();
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
  std::string projectName;
  ad->stream() >> projectName;
  object->setProjectName(projectName);
  int openflags;
  ad->stream() >> openflags;
  object->setOpenflags(openflags);
  unsigned long id;
  ad->stream() >> id;
  object->setId(id);
  int status;
  ad->stream() >> status;
  object->setStatus((castor::stager::RequestStatusCodes)status);
  newlyCreated.insert(object);
  unsigned int subRequestsNb;
  ad->stream() >> subRequestsNb;
  for (unsigned int i = 0; i < subRequestsNb; i++) {
    IObject* obj = unmarshalObject(ad->stream(), newlyCreated);
    object->addSubRequests(dynamic_cast<castor::stager::SubRequest*>(obj));
  }
  IObject* objClient = unmarshalObject(ad->stream(), newlyCreated);
  object->setClient(dynamic_cast<castor::IClient*>(objClient));
  return object;
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::io::StreamStageInRequestCnv::updateObj(castor::IObject* obj,
                                                    castor::ObjectCatalog& alreadyDone)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update object in case of streaming."
                  << std::endl;
  throw ex;
}

