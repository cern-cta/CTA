/******************************************************************************
 *                      castor/IAddressCInt.cpp
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
#include "castor/IAddress.hpp"
#include <iostream>
#include <string>

extern "C" {

  //----------------------------------------------------------------------------
  // C_IAddress_delete
  //----------------------------------------------------------------------------
  int C_IAddress_delete(castor::IAddress* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IAddress_objType
  //----------------------------------------------------------------------------
  int C_IAddress_objType(castor::IAddress* instance,
                         unsigned int* ret) {
    *ret = instance->objType();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IAddress_setObjType
  //----------------------------------------------------------------------------
  int C_IAddress_setObjType(castor::IAddress* instance,
                            unsigned int type) {
    instance->setObjType(type);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IAddress_cnvSvcName
  //----------------------------------------------------------------------------
  int C_IAddress_cnvSvcName(castor::IAddress* instance,
                            std::string* ret) {
    *ret = instance->cnvSvcName();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IAddress_cnvSvcType
  //----------------------------------------------------------------------------
  int C_IAddress_cnvSvcType(castor::IAddress* instance,
                            unsigned int* ret) {
    *ret = instance->cnvSvcType();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IAddress_print
  //----------------------------------------------------------------------------
  int C_IAddress_print(castor::IAddress* instance,
                       std::ostream& s) {
    instance->print(s);
    return 0;
  }

} // End of extern "C"
