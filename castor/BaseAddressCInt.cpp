/******************************************************************************
 *                      BaseAddressCInt.cpp
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
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/BaseAddress.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // C_BaseAddress_create
  //----------------------------------------------------------------------------
  int C_BaseAddress_create(const char* cnvSvcName,
                           const unsigned int cnvSvcType,
                           castor::BaseAddress** addr) {
    *addr = new castor::BaseAddress(cnvSvcName, cnvSvcType);
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_delete
  //----------------------------------------------------------------------------
  int C_BaseAddress_delete(castor::BaseAddress* addr) {
    delete addr;
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_getIAddress
  //----------------------------------------------------------------------------
  castor::IAddress* C_BaseAddress_getIAddress(castor::BaseAddress* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // C_BaseAddress_fromIAddress
  //----------------------------------------------------------------------------
  castor::BaseAddress* C_BaseAddress_fromIAddress(castor::IAddress* obj) {
    return dynamic_cast<castor::BaseAddress*>(obj);
  }

} // End of extern "C"
