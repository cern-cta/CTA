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
 * @(#)$RCSfile: ServicesCInt.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/05/13 17:35:26 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Services.hpp"
#include <iostream>
#include <errno.h>

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
    try {
      svcs->createRep(address, object, autocommit);
    } catch (castor::Exception e) {
      errno = EINVAL;
      svcs->setLastErrorMsg(e.getMessage().str());
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_updateRep
  //------------------------------------------------------------------------------
  int C_Services_updateRep(castor::Services* svcs,
                           castor::IAddress* address,
                           castor::IObject* object,
                           char autocommit = 1) {
    try {
      svcs->updateRep(address, object, autocommit);
    } catch (castor::Exception e) {
      errno = EINVAL;      
      svcs->setLastErrorMsg(e.getMessage().str());
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_deleteRep
  //------------------------------------------------------------------------------
  int C_Services_deleteRep(castor::Services* svcs,
                           castor::IAddress* address,
                           castor::IObject* object,
                           char autocommit = 0) {
    try {
      svcs->deleteRep(address, object, autocommit);
    } catch (castor::Exception e) {
      errno = EINVAL;      
      svcs->setLastErrorMsg(e.getMessage().str());
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_createObj
  //------------------------------------------------------------------------------
  int C_Services_createObj(castor::Services* svcs,
                           castor::IAddress* address,
                           castor::IObject** object) {
    try {
      *object = svcs->createObj(address);
    } catch (castor::Exception e) {
      errno = EINVAL;
      svcs->setLastErrorMsg(e.getMessage().str());
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_errorMsg
  //------------------------------------------------------------------------------
  const char* C_Services_errorMsg(castor::Services* svcs) {
    return svcs->lastErrorMsg().c_str();
  }
  
} // End of extern "C"
