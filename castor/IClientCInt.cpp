/******************************************************************************
 *                      castor/IClientCInt.cpp
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
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/stager/Request.hpp"

extern "C" {

  //----------------------------------------------------------------------------
  // C_IClient_delete
  //----------------------------------------------------------------------------
  int C_IClient_delete(castor::IClient* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IClient_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* C_IClient_getIObject(castor::IClient* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // C_IClient_fromIObject
  //----------------------------------------------------------------------------
  castor::IClient* C_IClient_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::IClient*>(obj);
  }

  //----------------------------------------------------------------------------
  // C_IClient_reconnect
  //----------------------------------------------------------------------------
  int C_IClient_reconnect(castor::IClient* instance) {
    instance->reconnect();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IClient_disconnect
  //----------------------------------------------------------------------------
  int C_IClient_disconnect(castor::IClient* instance) {
    instance->disconnect();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IClient_sendRep
  //----------------------------------------------------------------------------
  int C_IClient_sendRep(castor::IClient* instance) {
    instance->sendRep();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IClient_print
  //----------------------------------------------------------------------------
  int C_IClient_print(castor::IClient* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IClient_request
  //----------------------------------------------------------------------------
  int C_IClient_request(castor::IClient* instance, castor::stager::Request** var) {
    *var = instance->request();
    return 0;
  }

  //----------------------------------------------------------------------------
  // C_IClient_setRequest
  //----------------------------------------------------------------------------
  int C_IClient_setRequest(castor::IClient* instance, castor::stager::Request* new_var) {
    instance->setRequest(new_var);
    return 0;
  }

} // End of extern "C"
