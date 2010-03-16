/******************************************************************************
 *                castor/stager/daemon/NsOverride.cpp
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
 * @(#)$RCSfile: NsOverride.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2009/05/20 09:07:45 $ $Author: itglp $
 *
 * Singleton class for the NameServer override feature
 *
 * @author castor dev team
 *****************************************************************************/

#include <getconfent.h>
#include <Cmutex.h>
#include "castor/BaseObject.hpp"
#include "castor/Services.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/daemon/NsOverride.hpp"

castor::stager::daemon::NsOverride* castor::stager::daemon::NsOverride::s_instance(0);

//------------------------------------------------------------------------------
// getInstance
//------------------------------------------------------------------------------
castor::stager::daemon::NsOverride* castor::stager::daemon::NsOverride::getInstance()
  throw () {
  // make the instantiation of the singleton thread-safe,
  // even though this class is supposed to be instantiated before spawning threads
  if (0 == s_instance) {
    Cmutex_lock(&s_instance, -1);
    if (0 == s_instance) {
      s_instance = new castor::stager::daemon::NsOverride();
    }
    Cmutex_unlock(&s_instance);
  }
  return s_instance;
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::daemon::NsOverride::NsOverride() throw () {
  char* cnsHost = getconfent("CNS", "HOST", 0);
  if (cnsHost == 0 || *cnsHost == 0) {
    // no override in place
    return;
  }
  m_cnsHost = cnsHost;
  try {
    // get the stager service. Note that we use one of the thread specific
    // instances of Services, but there's no risk of db connection mixing
    // as the values are cached for future use from all threads
    castor::Services* svcs = castor::BaseObject::services();
    castor::IService* svc = svcs->service("DbStagerSvc", castor::SVC_DBSTAGERSVC);
    castor::stager::IStagerSvc *stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);
    // and now get the target NS host from the CastorConfig table;
    // ignore any failure, we assume no override is configured
    m_targetCnsHost = stgSvc->getConfigOption("stager", "nsHost", m_cnsHost);
  }
  catch (castor::exception::Exception ignored) {}
}
