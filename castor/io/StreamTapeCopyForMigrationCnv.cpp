/******************************************************************************
 *                      castor/io/StreamTapeCopyForMigrationCnv.cpp
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
#include "StreamTapeCopyForMigrationCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/ICnvFactory.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/io/StreamAddress.hpp"
#include "castor/io/StreamCnvSvc.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/TapeCopyForMigration.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "osdep.h"
#include <string>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamTapeCopyForMigrationCnv> s_factoryStreamTapeCopyForMigrationCnv;
const castor::ICnvFactory& StreamTapeCopyForMigrationCnvFactory = 
  s_factoryStreamTapeCopyForMigrationCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamTapeCopyForMigrationCnv::StreamTapeCopyForMigrationCnv(castor::ICnvSvc* cnvSvc) :
  StreamBaseCnv(cnvSvc) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamTapeCopyForMigrationCnv::~StreamTapeCopyForMigrationCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamTapeCopyForMigrationCnv::ObjType() {
  return castor::stager::TapeCopyForMigration::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamTapeCopyForMigrationCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamTapeCopyForMigrationCnv::createRep(castor::IAddress* address,
                                                          castor::IObject* object,
                                                          bool autocommit,
                                                          unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::TapeCopyForMigration* obj = 
    dynamic_cast<castor::stager::TapeCopyForMigration*>(object);
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  ad->stream() << obj->type();
  ad->stream() << obj->copyNb();
  ad->stream() << obj->id();
  ad->stream() << obj->diskServer();
  ad->stream() << obj->mountPoint();
  ad->stream() << obj->status();
}

//------------------------------------------------------------------------------
// createObj
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamTapeCopyForMigrationCnv::createObj(castor::IAddress* address)
  throw (castor::exception::Exception) {
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  // create the new Object
  castor::stager::TapeCopyForMigration* object = new castor::stager::TapeCopyForMigration();
  // Now retrieve and set members
  unsigned int copyNb;
  ad->stream() >> copyNb;
  object->setCopyNb(copyNb);
  u_signed64 id;
  ad->stream() >> id;
  object->setId(id);
  std::string diskServer;
  ad->stream() >> diskServer;
  object->setDiskServer(diskServer);
  std::string mountPoint;
  ad->stream() >> mountPoint;
  object->setMountPoint(mountPoint);
  int status;
  ad->stream() >> status;
  object->setStatus((castor::stager::TapeCopyStatusCodes)status);
  return object;
}

//------------------------------------------------------------------------------
// marshalObject
//------------------------------------------------------------------------------
void castor::io::StreamTapeCopyForMigrationCnv::marshalObject(castor::IObject* object,
                                                              castor::io::StreamAddress* address,
                                                              castor::ObjectSet& alreadyDone)
  throw (castor::exception::Exception) {
  castor::stager::TapeCopyForMigration* obj = 
    dynamic_cast<castor::stager::TapeCopyForMigration*>(object);
  if (0 == obj) {
    // Case of a null pointer
    address->stream() << castor::OBJ_Ptr << ((unsigned int)0);
  } else if (alreadyDone.find(obj) == alreadyDone.end()) {
    // Case of a pointer to a non streamed object
    createRep(address, obj, true);
    // Mark object as done
    alreadyDone.insert(obj);
    address->stream() << obj->stream().size();
    for (std::vector<castor::stager::Stream*>::iterator it = obj->stream().begin();
         it != obj->stream().end();
         it++) {
      cnvSvc()->marshalObject(*it, address, alreadyDone);
    }
    address->stream() << obj->segments().size();
    for (std::vector<castor::stager::Segment*>::iterator it = obj->segments().begin();
         it != obj->segments().end();
         it++) {
      cnvSvc()->marshalObject(*it, address, alreadyDone);
    }
    cnvSvc()->marshalObject(obj->castorFile(), address, alreadyDone);
  } else {
    // case of a pointer to an already streamed object
    address->stream() << castor::OBJ_Ptr << alreadyDone[obj];
  }
}

//------------------------------------------------------------------------------
// unmarshalObject
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamTapeCopyForMigrationCnv::unmarshalObject(castor::io::biniostream& stream,
                                                                            castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  castor::io::StreamAddress ad(stream, "StreamCnvSvc", SVC_STREAMCNV);
  castor::IObject* object = createObj(&ad);
  // Mark object as created
  newlyCreated.insert(object);
  // Fill object with associations
  castor::stager::TapeCopyForMigration* obj = 
    dynamic_cast<castor::stager::TapeCopyForMigration*>(object);
  unsigned int streamNb;
  ad.stream() >> streamNb;
  for (unsigned int i = 0; i < streamNb; i++) {
    ad.setObjType(castor::OBJ_INVALID);
    IObject* objStream = cnvSvc()->unmarshalObject(ad, newlyCreated);
    obj->addStream(dynamic_cast<castor::stager::Stream*>(objStream));
  }
  unsigned int segmentsNb;
  ad.stream() >> segmentsNb;
  for (unsigned int i = 0; i < segmentsNb; i++) {
    ad.setObjType(castor::OBJ_INVALID);
    IObject* objSegments = cnvSvc()->unmarshalObject(ad, newlyCreated);
    obj->addSegments(dynamic_cast<castor::stager::Segment*>(objSegments));
  }
  ad.setObjType(castor::OBJ_INVALID);
  IObject* objCastorFile = cnvSvc()->unmarshalObject(ad, newlyCreated);
  obj->setCastorFile(dynamic_cast<castor::stager::CastorFile*>(objCastorFile));
  return object;
}

