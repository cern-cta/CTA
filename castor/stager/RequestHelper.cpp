/******************************************************************************
 *                      castor/stager/RequestHelper.cpp
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
 * @(#)$RCSfile: RequestHelper.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2005/01/20 13:53:03 $ $Author: bcouturi $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/stager/RequestHelper.hpp"
#include <iostream>
#include <string>

const char *castor::stager::SVCCLASS_ENV = "STAGE_SVCCLASS";
const char *castor::stager::SVCCLASS_ENV_ALT = "SVCCLASS";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::RequestHelper::RequestHelper(castor::stager::Request::Request* req) throw() :
  m_request(req) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::RequestHelper::~RequestHelper() throw() {};

//------------------------------------------------------------------------------
// setOptions
//------------------------------------------------------------------------------
void castor::stager::RequestHelper::setOptions(struct stage_options* opts) {
  // Setting the service class
  if (0 != opts && opts->service_class != 0) {
    m_request->setSvcClassName(std::string(opts->service_class));
  } else {
    char *svc = 0;
    svc = (char *)getenv(castor::stager::SVCCLASS_ENV);
    if (0 == svc) {
      svc = (char *)getenv(castor::stager::SVCCLASS_ENV_ALT);
    }
    if (0 != svc) {
      m_request->setSvcClassName(std::string(svc));
    }
  }
}


