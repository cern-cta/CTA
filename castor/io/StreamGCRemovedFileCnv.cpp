/******************************************************************************
 *                      castor/io/StreamGCRemovedFileCnv.cpp
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
 * @(#)$RCSfile: StreamGCRemovedFileCnv.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/02/09 17:05:39 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "StreamGCRemovedFileCnv.hpp"
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
#include "castor/stager/GCRemovedFile.hpp"
#include "osdep.h"

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamGCRemovedFileCnv> s_factoryStreamGCRemovedFileCnv;
const castor::ICnvFactory& StreamGCRemovedFileCnvFactory = 
  s_factoryStreamGCRemovedFileCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamGCRemovedFileCnv::StreamGCRemovedFileCnv(castor::ICnvSvc* cnvSvc) :
  StreamBaseCnv(cnvSvc) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamGCRemovedFileCnv::~StreamGCRemovedFileCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamGCRemovedFileCnv::ObjType() {
  return castor::stager::GCRemovedFile::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamGCRemovedFileCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamGCRemovedFileCnv::createRep(castor::IAddress* address,
                                                   castor::IObject* object,
                                                   bool autocommit,
                                                   unsigned int type)
  throw (castor::exception::Exception) {
  castor::stager::GCRemovedFile* obj = 
    dynamic_cast<castor::stager::GCRemovedFile*>(object);
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  ad->stream() << obj->type();
  ad->stream() << obj->diskCopyId();
  ad->stream() << obj->id();
}

//------------------------------------------------------------------------------
// createObj
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamGCRemovedFileCnv::createObj(castor::IAddress* address)
  throw (castor::exception::Exception) {
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  // create the new Object
  castor::stager::GCRemovedFile* object = new castor::stager::GCRemovedFile();
  // Now retrieve and set members
  u_signed64 diskCopyId;
  ad->stream() >> diskCopyId;
  object->setDiskCopyId(diskCopyId);
  u_signed64 id;
  ad->stream() >> id;
  object->setId(id);
  return object;
}

//------------------------------------------------------------------------------
// marshalObject
//------------------------------------------------------------------------------
void castor::io::StreamGCRemovedFileCnv::marshalObject(castor::IObject* object,
                                                       castor::io::StreamAddress* address,
                                                       castor::ObjectSet& alreadyDone)
  throw (castor::exception::Exception) {
  castor::stager::GCRemovedFile* obj = 
    dynamic_cast<castor::stager::GCRemovedFile*>(object);
  if (0 == obj) {
    // Case of a null pointer
    address->stream() << castor::OBJ_Ptr << ((unsigned int)0);
  } else if (alreadyDone.find(obj) == alreadyDone.end()) {
    // Case of a pointer to a non streamed object
    createRep(address, obj, true);
    // Mark object as done
    alreadyDone.insert(obj);
  } else {
    // case of a pointer to an already streamed object
    address->stream() << castor::OBJ_Ptr << alreadyDone[obj];
  }
}

//------------------------------------------------------------------------------
// unmarshalObject
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamGCRemovedFileCnv::unmarshalObject(castor::io::biniostream& stream,
                                                                     castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  castor::io::StreamAddress ad(stream, "StreamCnvSvc", SVC_STREAMCNV);
  castor::IObject* object = createObj(&ad);
  // Mark object as created
  newlyCreated.insert(object);
  return object;
}

