/******************************************************************************
 *                      RequestReplierCInt.cpp
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
 * @(#)$RCSfile: RequestReplierCInt.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/16 15:45:34 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/replier/RequestReplier.hpp"
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include <errno.h>
#include <string>

extern "C" {
  
  // C include files
  #include "castor/replier/RequestReplierCInt.hpp"

  //------------------------------------------------------------------------------
  // checkRequestReplier
  //------------------------------------------------------------------------------
  bool checkRequestReplier(Creplier_RequestReplier_t* rr) {
    if (0 == rr || 0 == rr->rr) {
      errno = EINVAL;
      return false;
    }
    return true;
  }

  //---------------------------------------------------------------------------
  // Creplier_RequestReplier_getInstance
  //---------------------------------------------------------------------------
  Creplier_RequestReplier_t *Creplier_RequestReplier_getInstance() {
    Creplier_RequestReplier_t* rr = new Creplier_RequestReplier_t();
    rr->errorMsg = "";
    rr->rr = castor::replier::RequestReplier::getInstance();
    return rr;
  }
  
  //---------------------------------------------------------------------------
  // Creplier_RequestReplier_replyToClient
  //---------------------------------------------------------------------------
  int Creplier_RequestReplier_replyToClient(Creplier_RequestReplier_t *rr,
                                            castor::IClient *client,
                                            castor::IObject *response,
                                            int isLastResponse) {
    if (!checkRequestReplier(rr)) return -1;
    try {
      rr->rr->replyToClient(client, response, isLastResponse != 0);
    } catch (castor::exception::Exception e) {
      serrno = e.code();
      rr->errorMsg = e.getMessage().str();
      return -1;
    }
    return 0;
  }
  
  //---------------------------------------------------------------------------
  // Creplier_RequestReplier_release
  //---------------------------------------------------------------------------
  void Creplier_RequestReplier_release(Creplier_RequestReplier_t *rr) {
    if (0 != rr) {
      delete rr;
    }
  }

  //---------------------------------------------------------------------------
  // Creplier_RequestReplier_errorMsg
  //---------------------------------------------------------------------------
  const char* Creplier_RequestReplier_errorMsg(Creplier_RequestReplier_t* rr) {
    return rr->errorMsg.c_str();
  }
  
}
