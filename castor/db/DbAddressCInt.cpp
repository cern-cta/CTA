/******************************************************************************
 *                      DbAddressCInt.cpp
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
 * @(#)$RCSfile: DbAddressCInt.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/10/05 13:37:28 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/db/DbAddress.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // Cdb_DbAddress_create
  //----------------------------------------------------------------------------
  int Cdb_DbAddress_create(const u_signed64 id,
			   const char* cnvSvcName,
			   const unsigned int cnvSvcType,
			   castor::db::DbAddress** addr) {
    *addr = new castor::db::DbAddress(id, cnvSvcName, cnvSvcType);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cdb_DbAddress_delete
  //----------------------------------------------------------------------------
  int Cdb_DbAddress_delete(castor::db::DbAddress* addr) {
    delete addr;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Cdb_DbAddress_getId
  //----------------------------------------------------------------------------
  u_signed64 Cdb_DbAddress_id(castor::db::DbAddress* addr) {
    return addr->id();
  }

  //----------------------------------------------------------------------------
  // Cdb_DbAddress_setId
  //----------------------------------------------------------------------------
  void Cdb_DbAddress_setId(castor::db::DbAddress* addr, u_signed64 id) {
    addr->setId(id);
  }

  //----------------------------------------------------------------------------
  // Cdb_DbAddress_getIAddress
  //----------------------------------------------------------------------------
  castor::BaseAddress* Cdb_DbAddress_getBaseAddress(castor::db::DbAddress* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Cdb_DbAddress_fromIAddress
  //----------------------------------------------------------------------------
  castor::db::DbAddress* Cdb_DbAddress_fromBaseAddress(castor::BaseAddress* obj) {
    return dynamic_cast<castor::db::DbAddress*>(obj);
  }

} // End of extern "C"
