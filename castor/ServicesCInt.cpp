/******************************************************************************
 *                      ServicesCInt.cpp
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
 * @(#)$RCSfile: ServicesCInt.cpp,v $ $Revision: 1.1.1.1 $ $Release$ $Date: 2004/05/12 12:13:34 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Services.hpp"

extern "C" {

  //------------------------------------------------------------------------------
  // C_Services_create
  //------------------------------------------------------------------------------
  int C_Services_create(castor::Services** svcs) {
    *svcs = new castor::Services();
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_delete
  //------------------------------------------------------------------------------
  int C_Services_delete(castor::Services* svcs) {
    delete svcs;
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_createRep
  //------------------------------------------------------------------------------
  int C_Services_createRep(castor::Services* svcs,
                           castor::IAddress* address,
                           castor::IObject* object,
                           char autocommit = 1) {
    svcs->createRep(address, object, autocommit);
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_updateRep
  //------------------------------------------------------------------------------
  int C_Services_updateRep(castor::Services* svcs,
                           castor::IAddress* address,
                           castor::IObject* object,
                           char autocommit = 1) {
    svcs->updateRep(address, object, autocommit);
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_deleteRep
  //------------------------------------------------------------------------------
  int C_Services_deleteRep(castor::Services* svcs,
                           castor::IAddress* address,
                           castor::IObject* object,
                           char autocommit = 0) {
    svcs->deleteRep(address, object, autocommit);
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_createObj
  //------------------------------------------------------------------------------
  int C_Services_createObj(castor::Services* svcs,
                           castor::IAddress* address,
                           castor::IObject** object) {
    *object = svcs->createObj(address);
    return 0;
  }

} // End of extern "C"
