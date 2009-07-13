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
 * @(#)$RCSfile: BaseObject.cpp,v $ $Revision: 1.16 $ $Release$ $Date: 2009/07/13 06:22:05 $ $Author: waldron $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "Cglobals.h"
#include <Cmutex.h>

//------------------------------------------------------------------------------
// static values initialization
//------------------------------------------------------------------------------
castor::Services* castor::BaseObject::s_sharedServices(0);

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::BaseObject::BaseObject() throw() {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::BaseObject::~BaseObject() throw() {
  // clean the TLS for Services
  void **tls;
  static int C_BaseObject_TLSkey = -1;
  getTLS(&C_BaseObject_TLSkey, (void **)&tls);
  if (0 != *tls) {
    *tls = 0;
  }
}

//------------------------------------------------------------------------------
// sharedServices
//------------------------------------------------------------------------------
castor::Services* castor::BaseObject::sharedServices()
  throw (castor::exception::Exception) {
  // since this is the shared version, the instantiation of the singleton
  // has to be thread-safe.
  if (0 == s_sharedServices) {
    Cmutex_lock(&s_sharedServices, -1);
    if (0 == s_sharedServices) {
      s_sharedServices = new castor::Services();
    }
    Cmutex_unlock(&s_sharedServices);
  }
  return s_sharedServices;
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
