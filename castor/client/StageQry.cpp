/******************************************************************************
 *                      StageQry.cpp
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
 * @(#)$RCSfile: StageQry.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2004/10/01 14:26:12 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <iostream>
#include <vector>
#include "castor/ObjectSet.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/StageQryRequest.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/client/StageQryResponseHandler.hpp"
#include "stage_constants.h"

// Local Files
#include "StageQry.hpp"

//------------------------------------------------------------------------------
// buildRequest
//------------------------------------------------------------------------------
castor::stager::Request* castor::client::StageQry::buildRequest()
  throw (castor::exception::Exception) {
  // First reject some flags parsed by BaseCmdLineClient
  std::vector<std::string> rejected;
  rejected.push_back("K");
  rejected.push_back("A");
  rejected.push_back("silent");
  rejected.push_back("nowait");
  rejected.push_back("rdonly");
  rejectFlags(rejected, "stageqry");
  // uses some other flags
  u_signed64 flags = 0;
  std::string poolName = getPoolName();
  if (m_inputFlags.find("a") != m_inputFlags.end()) {
    flags |= STAGE_ALL;
  }
  if (m_inputFlags.find("f") != m_inputFlags.end()) {
    flags |= STAGE_FILENAME;
  }
  if (m_inputFlags.find("noretry") != m_inputFlags.end()) {
    flags |= STAGE_NORETRY;
  }
  // Build request
  castor::stager::StageQryRequest* req =
    new castor::stager::StageQryRequest();
  req->setFlags(flags);
  int n = 0;
  for (std::vector<std::string>::const_iterator it = m_inputArguments.begin();
       it != m_inputArguments.end();
       it++) {
    castor::stager::SubRequest* s = new castor::stager::SubRequest();
    s->setFileName(*it);
    s->setPoolName(poolName);
    s->setXsize(0);
    n++;
    req->addSubRequests(s);
    s->setRequest(req);
  }
  return req;
}

//------------------------------------------------------------------------------
// responseHandler
//------------------------------------------------------------------------------
castor::client::IResponseHandler*
castor::client::StageQry::responseHandler() throw() {
  return new StageQryResponseHandler();
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::client::StageQry::usage(std::string error) throw() {
  std::cout << error << std::endl;
  std::cout << "usage : stageqry [-af] [-h rh_host] [-p poolname] [--noretry] hsmfile..."
            << std::endl;
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char** argv) {
  castor::client::StageQry req;
  req.run(argc, argv);
  return 0;
}
