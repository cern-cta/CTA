/******************************************************************************
 *                      castor/IObjectCInt.cpp
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
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // C_IObject_delete
  //----------------------------------------------------------------------------
  int C_IObject_delete(castor::IObject* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IObject_setId
  //----------------------------------------------------------------------------
  int C_IObject_setId(castor::IObject* instance,
                      u_signed64 id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IObject_id
  //----------------------------------------------------------------------------
  int C_IObject_id(castor::IObject* instance,
                   u_signed64* ret) {
    *ret = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IObject_type
  //----------------------------------------------------------------------------
  int C_IObject_type(castor::IObject* instance,
                     int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IObject_clone
  //----------------------------------------------------------------------------
  int C_IObject_clone(castor::IObject* instance,
                      castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IObject_print
  //----------------------------------------------------------------------------
  int C_IObject_print(castor::IObject* instance) {
    instance->print();
    return 0;
  }

} // End of extern "C"
