/******************************************************************************
 *                      castor/io/StreamDiskCopyCnv.cpp
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
#include "StreamDiskCopyCnv.hpp"
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
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskCopyStatusCode.hpp"
#include "castor/stager/FileSystem.hpp"
#include "osdep.h"
#include <string>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamDiskCopyCnv> s_factoryStreamDiskCopyCnv;
const castor::IFactory<castor::IConverter>& StreamDiskCopyCnvFactory = 
  s_factoryStreamDiskCopyCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamDiskCopyCnv::StreamDiskCopyCnv() :
  StreamBaseCnv() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamDiskCopyCnv::~StreamDiskCopyCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamDiskCopyCnv::ObjType() {
  return castor::stager::DiskCopy::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamDiskCopyCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamDiskCopyCnv::createRep(castor::IAddress* address,
                                              castor::IObject* object,
                                              castor::ObjectSet& alreadyDone,
                                              bool autocommit,
                                              bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::DiskCopy* obj = 
    dynamic_cast<castor::stager::DiskCopy*>(object);
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  ad->stream() << obj->type();
  ad->stream() << obj->path();
  ad->stream() << obj->id();
  ad->stream() << obj->status();
  // Mark object as done
  alreadyDone.insert(obj);
  marshalObject(obj->fileSystem(), ad, alreadyDone);
  marshalObject(obj->castorFile(), ad, alreadyDone);
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::io::StreamDiskCopyCnv::updateRep(castor::IAddress* address,
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
void castor::io::StreamDiskCopyCnv::deleteRep(castor::IAddress* address,
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
castor::IObject* castor::io::StreamDiskCopyCnv::createObj(castor::IAddress* address,
                                                          castor::ObjectCatalog& newlyCreated,
                                                          bool recursive)
  throw (castor::exception::Exception) {
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  // create the new Object
  castor::stager::DiskCopy* object = new castor::stager::DiskCopy();
  // Now retrieve and set members
  std::string path;
  ad->stream() >> path;
  object->setPath(path);
  u_signed64 id;
  ad->stream() >> id;
  object->setId(id);
  int status;
  ad->stream() >> status;
  object->setStatus((castor::stager::DiskCopyStatusCode)status);
  newlyCreated.insert(object);
  IObject* objFileSystem = unmarshalObject(ad->stream(), newlyCreated);
  object->setFileSystem(dynamic_cast<castor::stager::FileSystem*>(objFileSystem));
  IObject* objCastorFile = unmarshalObject(ad->stream(), newlyCreated);
  object->setCastorFile(dynamic_cast<castor::stager::CastorFile*>(objCastorFile));
  return object;
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::io::StreamDiskCopyCnv::updateObj(castor::IObject* obj,
                                              castor::ObjectCatalog& alreadyDone)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update object in case of streaming."
                  << std::endl;
  throw ex;
}

