/******************************************************************************
 *                      castor/io/StreamFileSystemCnv.cpp
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
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "StreamFileSystemCnv.hpp"
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
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/FileSystem.hpp"
#include "osdep.h"
#include <string>
#include <vector>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamFileSystemCnv> s_factoryStreamFileSystemCnv;
const castor::IFactory<castor::IConverter>& StreamFileSystemCnvFactory = 
  s_factoryStreamFileSystemCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamFileSystemCnv::StreamFileSystemCnv() :
  StreamBaseCnv() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamFileSystemCnv::~StreamFileSystemCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamFileSystemCnv::ObjType() {
  return castor::stager::FileSystem::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamFileSystemCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamFileSystemCnv::createRep(castor::IAddress* address,
                                                castor::IObject* object,
                                                castor::ObjectSet& alreadyDone,
                                                bool autocommit,
                                                bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::FileSystem* obj = 
    dynamic_cast<castor::stager::FileSystem*>(object);
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  ad->stream() << obj->type();
  ad->stream() << obj->free();
  ad->stream() << obj->weight();
  ad->stream() << obj->fsDeviation();
  ad->stream() << obj->randomize();
  ad->stream() << obj->mountPoint();
  ad->stream() << obj->id();
  // Mark object as done
  alreadyDone.insert(obj);
  ad->stream() << obj->copies().size();
  for (std::vector<castor::stager::DiskCopy*>::iterator it = obj->copies().begin();
       it != obj->copies().end();
       it++) {
    marshalObject(*it, ad, alreadyDone);
  }
  marshalObject(obj->diskserver(), ad, alreadyDone);
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::io::StreamFileSystemCnv::updateRep(castor::IAddress* address,
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
void castor::io::StreamFileSystemCnv::deleteRep(castor::IAddress* address,
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
castor::IObject* castor::io::StreamFileSystemCnv::createObj(castor::IAddress* address,
                                                            castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  // create the new Object
  castor::stager::FileSystem* object = new castor::stager::FileSystem();
  // Now retrieve and set members
  u_signed64 free;
  ad->stream() >> free;
  object->setFree(free);
  float weight;
  ad->stream() >> weight;
  object->setWeight(weight);
  float fsDeviation;
  ad->stream() >> fsDeviation;
  object->setFsDeviation(fsDeviation);
  int randomize;
  ad->stream() >> randomize;
  object->setRandomize(randomize);
  std::string mountPoint;
  ad->stream() >> mountPoint;
  object->setMountPoint(mountPoint);
  u_signed64 id;
  ad->stream() >> id;
  object->setId(id);
  newlyCreated.insert(object);
  unsigned int copiesNb;
  ad->stream() >> copiesNb;
  for (unsigned int i = 0; i < copiesNb; i++) {
    IObject* obj = unmarshalObject(ad->stream(), newlyCreated);
    object->addCopies(dynamic_cast<castor::stager::DiskCopy*>(obj));
  }
  IObject* objDiskserver = unmarshalObject(ad->stream(), newlyCreated);
  object->setDiskserver(dynamic_cast<castor::stager::DiskServer*>(objDiskserver));
  return object;
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::io::StreamFileSystemCnv::updateObj(castor::IObject* obj,
                                                castor::ObjectCatalog& alreadyDone)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update object in case of streaming."
                  << std::endl;
  throw ex;
}

