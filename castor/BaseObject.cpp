/******************************************************************************
 *                      BaseObject.cpp
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
 * @(#)$RCSfile: BaseObject.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2004/07/08 08:26:33 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/MsgSvc.hpp"
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "Cglobals.h"
#include <Cthread_api.h>

// -----------------------------------------------------------------------
// static values initialization
// -----------------------------------------------------------------------
std::string castor::BaseObject::s_msgSvcName("");

// -----------------------------------------------------------------------
// constructor
// -----------------------------------------------------------------------
castor::BaseObject::BaseObject() throw() {}

// -----------------------------------------------------------------------
// destructor
// -----------------------------------------------------------------------
castor::BaseObject::~BaseObject() throw() {}

// -----------------------------------------------------------------------
// msgSvc
// -----------------------------------------------------------------------
castor::MsgSvc* castor::BaseObject::msgSvc(std::string name)
  throw (castor::exception::Exception) {
  IService* svc = svcs()->service(name, castor::MsgSvc::ID());
  if (0 == svc) {
    castor::exception::Internal e;
    e.getMessage() << "Unable to retrieve MsgSvc";
    throw(e);
  }
  castor::MsgSvc* msgSvc = dynamic_cast<castor::MsgSvc*> (svc);
  if (0 == msgSvc) {
    svc->release();
    castor::exception::Internal e;
    e.getMessage() << "Got something weird when retrieving MsgSvc";
    throw(e);
  }
  return msgSvc;
}

//------------------------------------------------------------------------------
// svcs
//------------------------------------------------------------------------------
castor::Services* castor::BaseObject::svcs()
  throw (castor::exception::Exception) {
  return services();
}

//------------------------------------------------------------------------------
// services
//------------------------------------------------------------------------------
castor::Services* castor::BaseObject::services()
  throw (castor::exception::Exception) {
  void **tls;
  getTLS((void **)&tls);
  if (0 == *tls) {
    *tls = (void *)(new castor::Services());
  }
  return (castor::Services*) *tls;
};

//------------------------------------------------------------------------------
// getTLS
//------------------------------------------------------------------------------
void castor::BaseObject::getTLS(void **thip)
 throw (castor::exception::Exception) {
  static int Cns_api_key = -1;
  int rc = Cglobals_get (&Cns_api_key, (void **) thip, sizeof(void *));
  if (*thip == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() << "Could not get thread local storage";
    throw ex;
  }
  if (rc == 1) {
    *(void **)(*thip) = 0;
  }
}

//------------------------------------------------------------------------------
// initlog
//------------------------------------------------------------------------------
void castor::BaseObject::initLog(std::string name) throw() {
  Cthread_mutex_lock(&s_msgSvcName);
  s_msgSvcName = name;
  Cthread_mutex_unlock(&s_msgSvcName);
}

//------------------------------------------------------------------------------
// clog
//------------------------------------------------------------------------------
castor::logstream& castor::BaseObject::clog()
 throw(castor::exception::Exception) {
  return msgSvc(s_msgSvcName)->stream();
}
