/******************************************************************************
 *                      castor/io/StreamSegmentCnv.cpp
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
#include "StreamSegmentCnv.hpp"
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
#include "castor/stager/Segment.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "osdep.h"
#include <string>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::io::StreamSegmentCnv> s_factoryStreamSegmentCnv;
const castor::IFactory<castor::IConverter>& StreamSegmentCnvFactory = 
  s_factoryStreamSegmentCnv;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::io::StreamSegmentCnv::StreamSegmentCnv() :
  StreamBaseCnv() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::io::StreamSegmentCnv::~StreamSegmentCnv() throw() {
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamSegmentCnv::ObjType() {
  return castor::stager::Segment::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::io::StreamSegmentCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::io::StreamSegmentCnv::createRep(castor::IAddress* address,
                                             castor::IObject* object,
                                             castor::ObjectSet& alreadyDone,
                                             bool autocommit,
                                             bool recursive)
  throw (castor::exception::Exception) {
  castor::stager::Segment* obj = 
    dynamic_cast<castor::stager::Segment*>(object);
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  ad->stream() << obj->type();
  ad->stream() << obj->blockid()[0] << obj->blockid()[1] << obj->blockid()[2] << obj->blockid()[3];
  ad->stream() << obj->fseq();
  ad->stream() << obj->offset();
  ad->stream() << obj->bytes_in();
  ad->stream() << obj->bytes_out();
  ad->stream() << obj->host_bytes();
  ad->stream() << obj->segmCksumAlgorithm();
  ad->stream() << obj->segmCksum();
  ad->stream() << obj->errMsgTxt();
  ad->stream() << obj->errorCode();
  ad->stream() << obj->severity();
  ad->stream() << obj->id();
  ad->stream() << obj->status();
  // Mark object as done
  alreadyDone.insert(obj);
  marshalObject(obj->tape(), ad, alreadyDone);
  marshalObject(obj->copy(), ad, alreadyDone);
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::io::StreamSegmentCnv::updateRep(castor::IAddress* address,
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
void castor::io::StreamSegmentCnv::deleteRep(castor::IAddress* address,
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
castor::IObject* castor::io::StreamSegmentCnv::createObj(castor::IAddress* address,
                                                         castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  StreamAddress* ad = 
    dynamic_cast<StreamAddress*>(address);
  // create the new Object
  castor::stager::Segment* object = new castor::stager::Segment();
  // Now retrieve and set members
  unsigned char blockid[4];
  ad->stream() >> blockid[0] >> blockid[1] >> blockid[2] >> blockid[3];
  object->setBlockid((unsigned char*)blockid);
  int fseq;
  ad->stream() >> fseq;
  object->setFseq(fseq);
  u_signed64 offset;
  ad->stream() >> offset;
  object->setOffset(offset);
  u_signed64 bytes_in;
  ad->stream() >> bytes_in;
  object->setBytes_in(bytes_in);
  u_signed64 bytes_out;
  ad->stream() >> bytes_out;
  object->setBytes_out(bytes_out);
  u_signed64 host_bytes;
  ad->stream() >> host_bytes;
  object->setHost_bytes(host_bytes);
  std::string segmCksumAlgorithm;
  ad->stream() >> segmCksumAlgorithm;
  object->setSegmCksumAlgorithm(segmCksumAlgorithm);
  unsigned long segmCksum;
  ad->stream() >> segmCksum;
  object->setSegmCksum(segmCksum);
  std::string errMsgTxt;
  ad->stream() >> errMsgTxt;
  object->setErrMsgTxt(errMsgTxt);
  int errorCode;
  ad->stream() >> errorCode;
  object->setErrorCode(errorCode);
  int severity;
  ad->stream() >> severity;
  object->setSeverity(severity);
  u_signed64 id;
  ad->stream() >> id;
  object->setId(id);
  int status;
  ad->stream() >> status;
  object->setStatus((castor::stager::SegmentStatusCodes)status);
  newlyCreated.insert(object);
  IObject* objTape = unmarshalObject(ad->stream(), newlyCreated);
  object->setTape(dynamic_cast<castor::stager::Tape*>(objTape));
  IObject* objCopy = unmarshalObject(ad->stream(), newlyCreated);
  object->setCopy(dynamic_cast<castor::stager::TapeCopy*>(objCopy));
  return object;
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::io::StreamSegmentCnv::updateObj(castor::IObject* obj,
                                             castor::ObjectCatalog& alreadyDone)
  throw (castor::exception::Exception) {
  castor::exception::Internal ex;
  ex.getMessage() << "Cannot update object in case of streaming."
                  << std::endl;
  throw ex;
}

