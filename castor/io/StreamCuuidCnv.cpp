/******************************************************************************
 *                      castor/io/StreamFileCnv.cpp
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
 * @(#)$RCSfile: StreamCuuidCnv.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2004/10/05 13:37:29 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "StreamCuuidCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/IAddress.hpp"
#include "castor/IConverter.hpp"
#include "castor/IFactory.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/io/StreamAddress.hpp"
#include "castor/io/StreamCnvSvc.hpp"
#include "castor/stager/Cuuid.hpp"
#include <string>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamCuuidCnv> s_factoryStreamCuuidCnv;
const castor::IFactory<castor::IConverter>& StreamCuuidCnvFactory =
  s_factoryStreamCuuidCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamCuuidCnv::StreamCuuidCnv() :
  StreamBaseCnv() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamCuuidCnv::~StreamCuuidCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamCuuidCnv::ObjType() {
  return castor::stager::Cuuid::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamCuuidCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamCuuidCnv::createRep(castor::IAddress* address,
                                           castor::IObject* object,
                                           castor::ObjectSet& alreadyDone,
                                           bool autocommit,
                                           bool /*recursive*/)
  throw (castor::exception::Exception) {
  castor::stager::Cuuid* obj =
    dynamic_cast<castor::stager::Cuuid*>(object);
  StreamAddress* ad =
    dynamic_cast<StreamAddress*>(address);
  Cuuid_t content = obj->content();
  ad->stream() << content.time_low;
  ad->stream() << content.time_mid;
  ad->stream() << content.time_hi_and_version;
  ad->stream() << content.clock_seq_hi_and_reserved;
  ad->stream() << content.clock_seq_low;
  ad->stream() << content.node[0] << content.node[1]
               << content.node[2] << content.node[3]
               << content.node[4] << content.node[5];
  ad->stream() << obj->id();
  // Mark object as done
  alreadyDone.insert(obj);
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::io::StreamCuuidCnv::updateRep(castor::IAddress* address,
                                           castor::IObject* object,
                                           castor::ObjectSet& alreadyDone,
                                           bool autocommit,
                                           bool recursive)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update representation in case of streaming.";
  throw ex;
}

//------------------------------------------------------------------------------
// deleteRep
//------------------------------------------------------------------------------
void castor::io::StreamCuuidCnv::deleteRep(castor::IAddress* address,
                                           castor::IObject* object,
                                           castor::ObjectSet& alreadyDone,
                                           bool autocommit)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot delete representation in case of streaming.";
  throw ex;
}

//------------------------------------------------------------------------------
// createObj
//------------------------------------------------------------------------------
castor::IObject* castor::io::StreamCuuidCnv::createObj(castor::IAddress* address,
                                                       castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  StreamAddress* ad =
    dynamic_cast<StreamAddress*>(address);
  // create the new Object
  castor::stager::Cuuid* object = new castor::stager::Cuuid();
  // Now retrieve and set members
  Cuuid_t content;
  ad->stream() >> content.time_low;
  ad->stream() >> content.time_mid;
  ad->stream() >> content.time_hi_and_version;
  ad->stream() >> content.clock_seq_hi_and_reserved;
  ad->stream() >> content.clock_seq_low;
  ad->stream() >> content.node[0] >> content.node[1]
               >> content.node[2] >> content.node[3]
               >> content.node[4] >> content.node[5];
  object->setContent(content);
  u_signed64 id;
  ad->stream() >> id;
  object->setId(id);
  newlyCreated.insert(object);
  return object;
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::io::StreamCuuidCnv::updateObj(castor::IObject* object,
                                           castor::ObjectCatalog& alreadyDone)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update object in case of streaming."
                  << std::endl;
  throw ex;
}
