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
 * @(#)$RCSfile: ServicesCInt.cpp,v $ $Revision: 1.19 $ $Release$ $Date: 2004/10/12 14:44:49 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// C++ Include Files
#include <string>
#include <iostream>
#include <errno.h>
#include <serrno.h>
#include "castor/IService.hpp"
#include "castor/BaseObject.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/ServicesCInt.hpp"

extern "C" {

  //------------------------------------------------------------------------------
  // C_Services_create
  //------------------------------------------------------------------------------
  int C_Services_create(C_Services_t** svcs) {
    try {
      *svcs = new C_Services_t();
      (*svcs)->svcs = castor::BaseObject::services();
      (*svcs)->errorMsg = "";
      return 0;
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      return -1;
    }
  }

  //------------------------------------------------------------------------------
  // C_Services_delete
  //------------------------------------------------------------------------------
  int C_Services_delete(C_Services_t* svcs) {
    delete svcs;
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_service
  //------------------------------------------------------------------------------
  int C_Services_service(C_Services_t* svcs,
                         char* name,
                         unsigned int id,
                         castor::IService** svc) {
    if (0 == svcs->svcs) {
      errno = EINVAL;
      svcs->errorMsg = "Empty context";
      return -1;
    }
    *svc = svcs->svcs->service(name, id);
    if (0 == *svc) {
      serrno = SEINTERNAL;
      svcs->errorMsg = "Unable to locate/create service";
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_createRep
  //------------------------------------------------------------------------------
  int C_Services_createRep(C_Services_t* svcs,
                           castor::IAddress* address,
                           castor::IObject* object,
                           char autocommit = 0) {
    if (0 == svcs->svcs) {
      errno = EINVAL;
      svcs->errorMsg = "Empty context";
      return -1;
    }
    try {
      svcs->svcs->createRep(address, object, autocommit);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      svcs->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_updateRep
  //------------------------------------------------------------------------------
  int C_Services_updateRep(C_Services_t* svcs,
                           castor::IAddress* address,
                           castor::IObject* object,
                           char autocommit = 0) {
    if (0 == svcs->svcs) {
      errno = EINVAL;
      svcs->errorMsg = "Empty context";
      return -1;
    }
    try {
      svcs->svcs->updateRep(address, object, autocommit);
    } catch (castor::exception::Exception e) {
      serrno = e.code();      
      svcs->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_deleteRep
  //------------------------------------------------------------------------------
  int C_Services_deleteRep(C_Services_t* svcs,
                           castor::IAddress* address,
                           castor::IObject* object,
                           char autocommit = 0) {
    if (0 == svcs->svcs) {
      errno = EINVAL;
      svcs->errorMsg = "Empty context";
      return -1;
    }
    try {
      svcs->svcs->deleteRep(address, object, autocommit);
    } catch (castor::exception::Exception e) {
      serrno = e.code();      
      svcs->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_createObj
  //------------------------------------------------------------------------------
  int C_Services_createObj(C_Services_t* svcs,
                           castor::IAddress* address,
                           castor::IObject** object) {
    if (0 == svcs->svcs) {
      errno = EINVAL;
      svcs->errorMsg = "Empty context";
      return -1;
    }
    try {
      *object = svcs->svcs->createObj(address);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      svcs->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_updateObj
  //------------------------------------------------------------------------------
  int C_Services_updateObj(C_Services_t* svcs,
                           castor::IAddress* address,
                           castor::IObject* object) {
    if (0 == svcs->svcs) {
      errno = EINVAL;
      svcs->errorMsg = "Empty context";
      return -1;
    }
    try {
      svcs->svcs->updateObj(address, object);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      svcs->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_fillRep
  //------------------------------------------------------------------------------
  int C_Services_fillRep(C_Services_t* svcs,
                         castor::IAddress* address,
                         castor::IObject* object,
                         unsigned int type,
                         char autocommit = 0) {
    if (0 == svcs->svcs) {
      errno = EINVAL;
      svcs->errorMsg = "Empty context";
      return -1;
    }
    try {
      svcs->svcs->fillRep(address, object, type, autocommit);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      svcs->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_fillObj
  //------------------------------------------------------------------------------
  int C_Services_fillObj(C_Services_t* svcs,
                         castor::IAddress* address,
                         castor::IObject* object,
                         unsigned int type) {
    if (0 == svcs->svcs) {
      errno = EINVAL;
      svcs->errorMsg = "Empty context";
      return -1;
    }
    try {
      svcs->svcs->fillObj(address, object, type);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      svcs->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_commit
  //------------------------------------------------------------------------------
  int C_Services_commit(C_Services_t* svcs,
                        castor::IAddress* address) {
    if (0 == svcs->svcs) {
      errno = EINVAL;
      svcs->errorMsg = "Empty context";
      return -1;
    }
    try {
      svcs->svcs->commit(address);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      svcs->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }

  //------------------------------------------------------------------------------
  // C_Services_rollback
  //------------------------------------------------------------------------------
  int C_Services_rollback(C_Services_t* svcs,
                          castor::IAddress* address) {
    if (0 == svcs->svcs) {
      errno = EINVAL;
      svcs->errorMsg = "Empty context";
      return -1;
    }
    try {
      svcs->svcs->rollback(address);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      svcs->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;    
  }

  //------------------------------------------------------------------------------
  // C_Services_errorMsg
  //------------------------------------------------------------------------------
  const char* C_Services_errorMsg(C_Services_t* svcs) {
    return svcs->errorMsg.c_str();
  }
  
} // End of extern "C"
