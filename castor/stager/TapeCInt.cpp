/******************************************************************************
 *                      castor/stager/TapeCInt.cpp
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
#include "castor/IObject.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapeStatusCodes.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_Tape_create
  //----------------------------------------------------------------------------
  int Cstager_Tape_create(castor::stager::Tape** obj) {
    *obj = new castor::stager::Tape();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_Tape_delete
  //----------------------------------------------------------------------------
  int Cstager_Tape_delete(castor::stager::Tape* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_Tape_getIObject(castor::stager::Tape* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::Tape* Cstager_Tape_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::Tape*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_print
  //----------------------------------------------------------------------------
  int Cstager_Tape_print(castor::stager::Tape* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_TYPE
  //----------------------------------------------------------------------------
  int Cstager_Tape_TYPE(int* ret) {
    *ret = castor::stager::Tape::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_setId
  //----------------------------------------------------------------------------
  int Cstager_Tape_setId(castor::stager::Tape* instance,
                         u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_id
  //----------------------------------------------------------------------------
  int Cstager_Tape_id(castor::stager::Tape* instance,
                      u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_type
  //----------------------------------------------------------------------------
  int Cstager_Tape_type(castor::stager::Tape* instance,
                        int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_clone
  //----------------------------------------------------------------------------
  int Cstager_Tape_clone(castor::stager::Tape* instance,
                         castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_vid
  //----------------------------------------------------------------------------
  int Cstager_Tape_vid(castor::stager::Tape* instance, const char** var) {
    *var = instance->vid().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_setVid
  //----------------------------------------------------------------------------
  int Cstager_Tape_setVid(castor::stager::Tape* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setVid(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_side
  //----------------------------------------------------------------------------
  int Cstager_Tape_side(castor::stager::Tape* instance, int* var) {
    *var = instance->side();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_setSide
  //----------------------------------------------------------------------------
  int Cstager_Tape_setSide(castor::stager::Tape* instance, int new_var) {
    instance->setSide(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_tpmode
  //----------------------------------------------------------------------------
  int Cstager_Tape_tpmode(castor::stager::Tape* instance, int* var) {
    *var = instance->tpmode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_setTpmode
  //----------------------------------------------------------------------------
  int Cstager_Tape_setTpmode(castor::stager::Tape* instance, int new_var) {
    instance->setTpmode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_errMsgTxt
  //----------------------------------------------------------------------------
  int Cstager_Tape_errMsgTxt(castor::stager::Tape* instance, const char** var) {
    *var = instance->errMsgTxt().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_setErrMsgTxt
  //----------------------------------------------------------------------------
  int Cstager_Tape_setErrMsgTxt(castor::stager::Tape* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setErrMsgTxt(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_errorCode
  //----------------------------------------------------------------------------
  int Cstager_Tape_errorCode(castor::stager::Tape* instance, int* var) {
    *var = instance->errorCode();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_setErrorCode
  //----------------------------------------------------------------------------
  int Cstager_Tape_setErrorCode(castor::stager::Tape* instance, int new_var) {
    instance->setErrorCode(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_severity
  //----------------------------------------------------------------------------
  int Cstager_Tape_severity(castor::stager::Tape* instance, int* var) {
    *var = instance->severity();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_setSeverity
  //----------------------------------------------------------------------------
  int Cstager_Tape_setSeverity(castor::stager::Tape* instance, int new_var) {
    instance->setSeverity(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_vwAddress
  //----------------------------------------------------------------------------
  int Cstager_Tape_vwAddress(castor::stager::Tape* instance, const char** var) {
    *var = instance->vwAddress().c_str();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_setVwAddress
  //----------------------------------------------------------------------------
  int Cstager_Tape_setVwAddress(castor::stager::Tape* instance, const char* new_var) {
    std::string snew_var(new_var, strlen(new_var));
    instance->setVwAddress(snew_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_stream
  //----------------------------------------------------------------------------
  int Cstager_Tape_stream(castor::stager::Tape* instance, castor::stager::Stream** var) {
    *var = instance->stream();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_setStream
  //----------------------------------------------------------------------------
  int Cstager_Tape_setStream(castor::stager::Tape* instance, castor::stager::Stream* new_var) {
    instance->setStream(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_addSegments
  //----------------------------------------------------------------------------
  int Cstager_Tape_addSegments(castor::stager::Tape* instance, castor::stager::Segment* obj) {
    instance->addSegments(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_removeSegments
  //----------------------------------------------------------------------------
  int Cstager_Tape_removeSegments(castor::stager::Tape* instance, castor::stager::Segment* obj) {
    instance->removeSegments(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_segments
  //----------------------------------------------------------------------------
  int Cstager_Tape_segments(castor::stager::Tape* instance, castor::stager::Segment*** var, int* len) {
    std::vector<castor::stager::Segment*>& result = instance->segments();
    *len = result.size();
    *var = (castor::stager::Segment**) malloc((*len) * sizeof(castor::stager::Segment*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_status
  //----------------------------------------------------------------------------
  int Cstager_Tape_status(castor::stager::Tape* instance, castor::stager::TapeStatusCodes* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Tape_setStatus
  //----------------------------------------------------------------------------
  int Cstager_Tape_setStatus(castor::stager::Tape* instance, castor::stager::TapeStatusCodes new_var) {
    instance->setStatus(new_var);
    return 0;
  }

} // End of extern "C"
