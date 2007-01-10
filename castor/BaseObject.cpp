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
 * @(#)$RCSfile: BaseObject.cpp,v $ $Revision: 1.14 $ $Release$ $Date: 2007/01/10 16:02:51 $ $Author: sponcec3 $
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
#include <Cmutex.h>

// -----------------------------------------------------------------------
// static values initialization
// -----------------------------------------------------------------------
std::string castor::BaseObject::s_msgSvcName("");
unsigned long castor::BaseObject::s_msgSvcId(0);

// -----------------------------------------------------------------------
// constructor
// -----------------------------------------------------------------------
castor::BaseObject::BaseObject() throw() {}

// -----------------------------------------------------------------------
// destructor
// -----------------------------------------------------------------------
castor::BaseObject::~BaseObject() throw() {
  // clean the TLS for Services
  void **tls;
  static int C_BaseObject_TLSkey = -1;
  getTLS(&C_BaseObject_TLSkey, (void **)&tls);
  if (0 != *tls) {
    *tls = 0;
  }
}

// -----------------------------------------------------------------------
// msgSvc
// -----------------------------------------------------------------------
castor::MsgSvc* castor::BaseObject::msgSvc(std::string name,
                                           const unsigned long id)
  throw (castor::exception::Exception) {
  IService* svc = services()->service(name, id);
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
  static int C_BaseObject_TLSkey = -1;
  getTLS(&C_BaseObject_TLSkey, (void **)&tls);
  if (0 == *tls) {
    *tls = (void *)(new castor::Services());
  }
  return (castor::Services*) *tls;
}

//------------------------------------------------------------------------------
// getTLS
//------------------------------------------------------------------------------
void castor::BaseObject::getTLS(int *key, void **thip)
  throw (castor::exception::Exception) {
  int rc = Cglobals_get (key, (void **) thip, sizeof(void *));
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
void castor::BaseObject::initLog(std::string name,
                                 const unsigned long id)
  throw() {
  Cmutex_lock(&s_msgSvcId, -1);
  // Nothing to do if already called
  if (0 == s_msgSvcId) {
    s_msgSvcName = name;
    s_msgSvcId = id;
  }
  Cmutex_unlock(&s_msgSvcId);
}

//------------------------------------------------------------------------------
// clog
//------------------------------------------------------------------------------
castor::logstream& castor::BaseObject::clog()
 throw(castor::exception::Exception) {
  if (0 == s_msgSvcId) {
    castor::exception::Internal e;
    e.getMessage() << "clog() was called before initialization of the log facility.\n"
                   << "Please call initLog first.";
    throw e;
  }
  return msgSvc(s_msgSvcName, s_msgSvcId)->stream();
}
