/******************************************************************************
 *                      castor/stager/StreamCInt.cpp
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
#include "castor/IObject.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/StreamStatusCodes.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_Stream_create
  //----------------------------------------------------------------------------
  int Cstager_Stream_create(castor::stager::Stream** obj) {
    *obj = new castor::stager::Stream();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_Stream_delete
  //----------------------------------------------------------------------------
  int Cstager_Stream_delete(castor::stager::Stream* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_Stream_getIObject(castor::stager::Stream* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::Stream* Cstager_Stream_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::Stream*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_print
  //----------------------------------------------------------------------------
  int Cstager_Stream_print(castor::stager::Stream* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_TYPE
  //----------------------------------------------------------------------------
  int Cstager_Stream_TYPE(int* ret) {
    *ret = castor::stager::Stream::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_setId
  //----------------------------------------------------------------------------
  int Cstager_Stream_setId(castor::stager::Stream* instance,
                           u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_id
  //----------------------------------------------------------------------------
  int Cstager_Stream_id(castor::stager::Stream* instance,
                        u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_type
  //----------------------------------------------------------------------------
  int Cstager_Stream_type(castor::stager::Stream* instance,
                          int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_initialSizeToTransfer
  //----------------------------------------------------------------------------
  int Cstager_Stream_initialSizeToTransfer(castor::stager::Stream* instance, u_signed64* var) {
    *var = instance->initialSizeToTransfer();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_setInitialSizeToTransfer
  //----------------------------------------------------------------------------
  int Cstager_Stream_setInitialSizeToTransfer(castor::stager::Stream* instance, u_signed64 new_var) {
    instance->setInitialSizeToTransfer(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_status
  //----------------------------------------------------------------------------
  int Cstager_Stream_status(castor::stager::Stream* instance, castor::stager::StreamStatusCodes* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_Stream_setStatus
  //----------------------------------------------------------------------------
  int Cstager_Stream_setStatus(castor::stager::Stream* instance, castor::stager::StreamStatusCodes new_var) {
    instance->setStatus(new_var);
    return 0;
  }

} // End of extern "C"
