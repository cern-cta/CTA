/******************************************************************************
 *                      IRequestHandlerCInt.cpp
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
 * @(#)$RCSfile: IRequestHandlerCInt.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/06/28 13:41:25 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "errno.h"
#include "castor/IService.hpp"
#include "castor/IRequestHandler.hpp"
#include "castor/IRequestHandlerCInt.hpp"

extern "C" {
  
  //----------------------------------------------------------------------------
  // C_IRequestHandler_fromIService
  //----------------------------------------------------------------------------
  struct C_IRequestHandler_t*
  C_IRequestHandler_fromIService(castor::IService* svc) {
    struct C_IRequestHandler_t *result = new C_IRequestHandler_t();
    result->rh = dynamic_cast<castor::IRequestHandler*>(svc);
    result->errorMsg = "";
    return result;
  }
  
  //----------------------------------------------------------------------------
  // C_IRequestHandler_delete
  //----------------------------------------------------------------------------
  int C_IRequestHandler_delete(struct C_IRequestHandler_t* rhsvc) {
    try {
      if (0 != rhsvc->rh) delete rhsvc->rh;
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      rhsvc->errorMsg = e.getMessage().str();
      return -1;
    }
    free(rhsvc);
    return 0;
  }
  
  //----------------------------------------------------------------------------
  // C_IRequestHandler_nextRequestAddress
  //----------------------------------------------------------------------------  
  int C_IRequestHandler_nextRequestAddress(struct C_IRequestHandler_t* rhsvc,
                                           castor::IAddress** address) {
    if (0 == rhsvc->rh) {
      errno = EINVAL;
      rhsvc->errorMsg = "Empty context";
      return -1;
    }
    try {
      *address = rhsvc->rh->nextRequestAddress();
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      rhsvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }
  
  //----------------------------------------------------------------------------
  // C_IRequestHandler_nbRequestsToProcess
  //----------------------------------------------------------------------------
  int C_IRequestHandler_nbRequestsToProcess(struct C_IRequestHandler_t* rhsvc,
                                            unsigned int *nbReq) {
    if (0 == rhsvc->rh) {
      errno = EINVAL;
      rhsvc->errorMsg = "Empty context";
      return -1;
    }
    try {
      *nbReq = rhsvc->rh->nbRequestsToProcess();
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      rhsvc->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0; 
  }
  
  //----------------------------------------------------------------------------
  // C_IRequestHandler_errorMsg
  //----------------------------------------------------------------------------
  const char* C_IRequestHandler_errorMsg(struct C_IRequestHandler_t* rhsvc) {
    return rhsvc->errorMsg.c_str();
  }

} // End of extern "C"
