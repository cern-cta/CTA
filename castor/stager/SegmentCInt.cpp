/******************************************************************************
 *                      castor/stager/SegmentCInt.cpp
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
#include "Cuuid.h"
#include "castor/IObject.hpp"
#include "castor/stager/Cuuid.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_Segment_create
  //----------------------------------------------------------------------------
  int Cstager_Segment_create(castor::stager::Segment** obj) {
    *obj = new castor::stager::Segment();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_Segment_delete
  //----------------------------------------------------------------------------
  int Cstager_Segment_delete(castor::stager::Segment* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_Segment_getIObject(castor::stager::Segment* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::Segment* Cstager_Segment_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::Segment*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_print
  //----------------------------------------------------------------------------
  int Cstager_Segment_print(castor::stager::Segment* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_TYPE
  //----------------------------------------------------------------------------
  int Cstager_Segment_TYPE(int* ret) {
    *ret = castor::stager::Segment::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setId
  //----------------------------------------------------------------------------
  int Cstager_Segment_setId(castor::stager::Segment* instance,
                            unsigned long id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_id
  //----------------------------------------------------------------------------
  int Cstager_Segment_id(castor::stager::Segment* instance,
                         unsigned long* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_type
  //----------------------------------------------------------------------------
  int Cstager_Segment_type(castor::stager::Segment* instance,
                           int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_blockid
  //----------------------------------------------------------------------------
  int Cstager_Segment_blockid(castor::stager::Segment* instance, const unsigned char** var) {
    *var = instance->blockid();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setBlockid
  //----------------------------------------------------------------------------
  int Cstager_Segment_setBlockid(castor::stager::Segment* instance, const unsigned char* new_var) {
    instance->setBlockid(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_fseq
  //----------------------------------------------------------------------------
  int Cstager_Segment_fseq(castor::stager::Segment* instance, int* var) {
    *var = instance->fseq();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setFseq
  //----------------------------------------------------------------------------
  int Cstager_Segment_setFseq(castor::stager::Segment* instance, int new_var) {
    instance->setFseq(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_offset
  //----------------------------------------------------------------------------
  int Cstager_Segment_offset(castor::stager::Segment* instance, u_signed64* var) {
    *var = instance->offset();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setOffset
  //----------------------------------------------------------------------------
  int Cstager_Segment_setOffset(castor::stager::Segment* instance, u_signed64 new_var) {
    instance->setOffset(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_bytes_in
  //----------------------------------------------------------------------------
  int Cstager_Segment_bytes_in(castor::stager::Segment* instance, u_signed64* var) {
    *var = instance->bytes_in();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setBytes_in
  //----------------------------------------------------------------------------
  int Cstager_Segment_setBytes_in(castor::stager::Segment* instance, u_signed64 new_var) {
    instance->setBytes_in(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_bytes_out
  //----------------------------------------------------------------------------
  int Cstager_Segment_bytes_out(castor::stager::Segment* instance, u_signed64* var) {
    *var = instance->bytes_out();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setBytes_out
  //----------------------------------------------------------------------------
  int Cstager_Segment_setBytes_out(castor::stager::Segment* instance, u_signed64 new_var) {
    instance->setBytes_out(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_host_bytes
  //----------------------------------------------------------------------------
  int Cstager_Segment_host_bytes(castor::stager::Segment* instance, u_signed64* var) {
    *var = instance->host_bytes();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setHost_bytes
  //----------------------------------------------------------------------------
  int Cstager_Segment_setHost_bytes(castor::stager::Segment* instance, u_signed64 new_var) {
    instance->setHost_bytes(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_segmCksumAlgorithm
  //----------------------------------------------------------------------------
  int Cstager_Segment_segmCksumAlgorithm(castor::stager::Segment* instance, const char** var) {
    *var = instance->segmCksumAlgorithm().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setSegmCksumAlgorithm
  //----------------------------------------------------------------------------
  int Cstager_Segment_setSegmCksumAlgorithm(castor::stager::Segment* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setSegmCksumAlgorithm(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_segmCksum
  //----------------------------------------------------------------------------
  int Cstager_Segment_segmCksum(castor::stager::Segment* instance, unsigned long* var) {
    *var = instance->segmCksum();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setSegmCksum
  //----------------------------------------------------------------------------
  int Cstager_Segment_setSegmCksum(castor::stager::Segment* instance, unsigned long new_var) {
    instance->setSegmCksum(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_errMsgTxt
  //----------------------------------------------------------------------------
  int Cstager_Segment_errMsgTxt(castor::stager::Segment* instance, const char** var) {
    *var = instance->errMsgTxt().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setErrMsgTxt
  //----------------------------------------------------------------------------
  int Cstager_Segment_setErrMsgTxt(castor::stager::Segment* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrMsgTxt(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_errorCode
  //----------------------------------------------------------------------------
  int Cstager_Segment_errorCode(castor::stager::Segment* instance, int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setErrorCode
  //----------------------------------------------------------------------------
  int Cstager_Segment_setErrorCode(castor::stager::Segment* instance, int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_severity
  //----------------------------------------------------------------------------
  int Cstager_Segment_severity(castor::stager::Segment* instance, int* var) {
    *var = instance->severity();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setSeverity
  //----------------------------------------------------------------------------
  int Cstager_Segment_setSeverity(castor::stager::Segment* instance, int new_var) {
    instance->setSeverity(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_tape
  //----------------------------------------------------------------------------
  int Cstager_Segment_tape(castor::stager::Segment* instance, castor::stager::Tape** var) {
    *var = instance->tape();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setTape
  //----------------------------------------------------------------------------
  int Cstager_Segment_setTape(castor::stager::Segment* instance, castor::stager::Tape* new_var) {
    instance->setTape(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_copy
  //----------------------------------------------------------------------------
  int Cstager_Segment_copy(castor::stager::Segment* instance, castor::stager::TapeCopy** var) {
    *var = instance->copy();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setCopy
  //----------------------------------------------------------------------------
  int Cstager_Segment_setCopy(castor::stager::Segment* instance, castor::stager::TapeCopy* new_var) {
    instance->setCopy(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_stgReqId
  //----------------------------------------------------------------------------
  int Cstager_Segment_stgReqId(castor::stager::Segment* instance, Cuuid_t* var) {
    if (0 != instance->stgReqId()) {
      *var = instance->stgReqId()->content();
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setStgReqId
  //----------------------------------------------------------------------------
  int Cstager_Segment_setStgReqId(castor::stager::Segment* instance, Cuuid_t new_var) {
    castor::stager::Cuuid* new_varcpp = new castor::stager::Cuuid();
    new_varcpp->setContent(new_var);
    instance->setStgReqId(new_varcpp);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_status
  //----------------------------------------------------------------------------
  int Cstager_Segment_status(castor::stager::Segment* instance, castor::stager::SegmentStatusCodes* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Segment_setStatus
  //----------------------------------------------------------------------------
  int Cstager_Segment_setStatus(castor::stager::Segment* instance, castor::stager::SegmentStatusCodes new_var) {
    instance->setStatus(new_var);
    return 0;
  }

} // End of extern "C"
