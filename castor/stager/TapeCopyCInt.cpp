/******************************************************************************
 *                      castor/stager/TapeCopyCInt.cpp
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
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "osdep.h"
#include <vector>

extern "C" {

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_create
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_create(castor::stager::TapeCopy** obj) {
    *obj = new castor::stager::TapeCopy();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_delete
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_delete(castor::stager::TapeCopy* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Cstager_TapeCopy_getIObject(castor::stager::TapeCopy* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_fromIObject
  //----------------------------------------------------------------------------
  castor::stager::TapeCopy* Cstager_TapeCopy_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::stager::TapeCopy*>(obj);
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_print
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_print(castor::stager::TapeCopy* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_TYPE
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_TYPE(int* ret) {
    *ret = castor::stager::TapeCopy::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_type
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_type(castor::stager::TapeCopy* instance,
                            int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_clone
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_clone(castor::stager::TapeCopy* instance,
                             castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_copyNb
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_copyNb(castor::stager::TapeCopy* instance, unsigned int* var) {
    *var = instance->copyNb();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_setCopyNb
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_setCopyNb(castor::stager::TapeCopy* instance, unsigned int new_var) {
    instance->setCopyNb(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_id
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_id(castor::stager::TapeCopy* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_setId
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_setId(castor::stager::TapeCopy* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_addStream
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_addStream(castor::stager::TapeCopy* instance, castor::stager::Stream* obj) {
    instance->addStream(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_removeStream
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_removeStream(castor::stager::TapeCopy* instance, castor::stager::Stream* obj) {
    instance->removeStream(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_stream
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_stream(castor::stager::TapeCopy* instance, castor::stager::Stream*** var, int* len) {
    std::vector<castor::stager::Stream*>& result = instance->stream();
    *len = result.size();
    *var = (castor::stager::Stream**) malloc((*len) * sizeof(castor::stager::Stream*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_addSegments
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_addSegments(castor::stager::TapeCopy* instance, castor::stager::Segment* obj) {
    instance->addSegments(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_removeSegments
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_removeSegments(castor::stager::TapeCopy* instance, castor::stager::Segment* obj) {
    instance->removeSegments(obj);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_segments
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_segments(castor::stager::TapeCopy* instance, castor::stager::Segment*** var, int* len) {
    std::vector<castor::stager::Segment*>& result = instance->segments();
    *len = result.size();
    *var = (castor::stager::Segment**) malloc((*len) * sizeof(castor::stager::Segment*));
    for (int i = 0; i < *len; i++) {
      (*var)[i] = result[i];
    }
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_castorFile
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_castorFile(castor::stager::TapeCopy* instance, castor::stager::CastorFile** var) {
    *var = instance->castorFile();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_setCastorFile
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_setCastorFile(castor::stager::TapeCopy* instance, castor::stager::CastorFile* new_var) {
    instance->setCastorFile(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_status
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_status(castor::stager::TapeCopy* instance, castor::stager::TapeCopyStatusCodes* var) {
    *var = instance->status();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cstager_TapeCopy_setStatus
  //----------------------------------------------------------------------------
  int Cstager_TapeCopy_setStatus(castor::stager::TapeCopy* instance, castor::stager::TapeCopyStatusCodes new_var) {
    instance->setStatus(new_var);
    return 0;
  }

} // End of extern "C"
