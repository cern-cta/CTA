/******************************************************************************
 *                      castor/rh/ClientCInt.cpp
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
#include "castor/rh/Client.hpp"
#include "castor/stager/Request.hpp"
#include "osdep.h"

extern "C" {

  //----------------------------------------------------------------------------
  // Crh_Client_create
  //----------------------------------------------------------------------------
  int Crh_Client_create(castor::rh::Client** obj) {
    *obj = new castor::rh::Client();
    return 0;
  }
  //----------------------------------------------------------------------------
  // Crh_Client_delete
  //----------------------------------------------------------------------------
  int Crh_Client_delete(castor::rh::Client* obj) {
    delete obj;
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_getIClient
  //----------------------------------------------------------------------------
  castor::IClient* Crh_Client_getIClient(castor::rh::Client* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_fromIClient
  //----------------------------------------------------------------------------
  castor::rh::Client* Crh_Client_fromIClient(castor::IClient* obj) {
    return dynamic_cast<castor::rh::Client*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_Client_getIObject
  //----------------------------------------------------------------------------
  castor::IObject* Crh_Client_getIObject(castor::rh::Client* obj) {
    return obj;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_fromIObject
  //----------------------------------------------------------------------------
  castor::rh::Client* Crh_Client_fromIObject(castor::IObject* obj) {
    return dynamic_cast<castor::rh::Client*>(obj);
  }

  //----------------------------------------------------------------------------
  // Crh_Client_print
  //----------------------------------------------------------------------------
  int Crh_Client_print(castor::rh::Client* instance) {
    instance->print();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_TYPE
  //----------------------------------------------------------------------------
  int Crh_Client_TYPE(int* ret) {
    *ret = castor::rh::Client::TYPE();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_request
  //----------------------------------------------------------------------------
  int Crh_Client_request(castor::rh::Client* instance, castor::stager::Request** var) {
    *var = instance->request();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_setRequest
  //----------------------------------------------------------------------------
  int Crh_Client_setRequest(castor::rh::Client* instance, castor::stager::Request* new_var) {
    instance->setRequest(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_type
  //----------------------------------------------------------------------------
  int Crh_Client_type(castor::rh::Client* instance,
                      int* ret) {
    *ret = instance->type();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_clone
  //----------------------------------------------------------------------------
  int Crh_Client_clone(castor::rh::Client* instance,
                       castor::IObject** ret) {
    *ret = instance->clone();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_ipAddress
  //----------------------------------------------------------------------------
  int Crh_Client_ipAddress(castor::rh::Client* instance, unsigned long* var) {
    *var = instance->ipAddress();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_setIpAddress
  //----------------------------------------------------------------------------
  int Crh_Client_setIpAddress(castor::rh::Client* instance, unsigned long new_var) {
    instance->setIpAddress(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_port
  //----------------------------------------------------------------------------
  int Crh_Client_port(castor::rh::Client* instance, unsigned short* var) {
    *var = instance->port();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_setPort
  //----------------------------------------------------------------------------
  int Crh_Client_setPort(castor::rh::Client* instance, unsigned short new_var) {
    instance->setPort(new_var);
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_id
  //----------------------------------------------------------------------------
  int Crh_Client_id(castor::rh::Client* instance, u_signed64* var) {
    *var = instance->id();
    return 0;
  }

  //----------------------------------------------------------------------------
  // Crh_Client_setId
  //----------------------------------------------------------------------------
  int Crh_Client_setId(castor::rh::Client* instance, u_signed64 new_var) {
    instance->setId(new_var);
    return 0;
  }

} // End of extern "C"
