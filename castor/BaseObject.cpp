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
#include <Cmutex.h>

//------------------------------------------------------------------------------
// static values initialization
//------------------------------------------------------------------------------
castor::Services* castor::BaseObject::s_sharedServices(0);
pthread_key_t castor::BaseObject::s_servicesKey(0);
pthread_once_t castor::BaseObject::s_servicesOnce(PTHREAD_ONCE_INIT);

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::BaseObject::BaseObject() throw() {}

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
// makeServicesKey
//------------------------------------------------------------------------------
void castor::BaseObject::makeServicesKey()
  throw (castor::exception::Exception)  {
  int rc = pthread_key_create(&s_servicesKey, NULL);
  if (rc != 0) {
    castor::exception::Exception e(rc);
    e.getMessage() << "Error caught in call to pthread_key_create";
    throw e;
  }
}

//------------------------------------------------------------------------------
// services
//------------------------------------------------------------------------------
castor::Services* castor::BaseObject::services()
  throw (castor::exception::Exception) {
  // make a new key if we are in the first call to this
  int rc = pthread_once(&castor::BaseObject::s_servicesOnce,
			castor::BaseObject::makeServicesKey);
  if (rc != 0) {
    castor::exception::Exception e(rc);
    e.getMessage() << "Error caught in call to pthread_once";
    throw e;
  }
  // retrieve the thread specific data
  void **tls = 0;
  getTLS(castor::BaseObject::s_servicesKey, tls);
  if (0 == *tls) {
    // and create content if it was just allocated
    *tls = (void *)(new castor::Services());
  }
  return (castor::Services*)(*tls);
}

//------------------------------------------------------------------------------
// getTLS
//------------------------------------------------------------------------------
void castor::BaseObject::getTLS(pthread_key_t &key, void **&tls)
  throw(castor::exception::Exception) {
  // retrieve the thread specific data
  tls = (void**)pthread_getspecific(key);
  if (NULL == tls) {
    // the thread specific data does not exist yet, let's create it
    tls = (void**)calloc(1,sizeof(void*));
    int rc = pthread_setspecific(key, (void*)tls);
    if (rc != 0) {
      castor::exception::Exception e(rc);
      e.getMessage() << "Error caught in call to pthread_setspecific";
      throw e;
    }
    *tls = 0;
  }
}
