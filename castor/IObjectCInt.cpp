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
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/IObject.hpp"
#include "castor/IPersistent.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // C_IObject_delete
  //----------------------------------------------------------------------------
  int C_IObject_delete(castor::IObject* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IObject_getIPersistent
  //----------------------------------------------------------------------------
  castor::IPersistent* C_IObject_getIPersistent(castor::IObject* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // C_IObject_fromIPersistent
  //----------------------------------------------------------------------------
  castor::IObject* C_IObject_fromIPersistent(castor::IPersistent* obj) {
    return dynamic_cast<castor::IObject*>(obj);
  }

  //----------------------------------------------------------------------------
  // C_IObject_setId
  //----------------------------------------------------------------------------
  int C_IObject_setId(castor::IObject* instance,
                      unsigned long id) {
    instance->setId(id);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IObject_id
  //----------------------------------------------------------------------------
  int C_IObject_id(castor::IObject* instance,
                   unsigned long* ret) {
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
  // C_IObject_print
  //----------------------------------------------------------------------------
  int C_IObject_print(castor::IObject* instance) {
    instance->print();
    return 0;
  }

} // End of extern "C"
