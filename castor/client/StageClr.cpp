/******************************************************************************
 *                      StageClr.cpp
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
 * @(#)$RCSfile: StageClr.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/10/01 14:26:12 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <iostream>
#include <vector>
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/StageClrRequest.hpp"
#include "castor/exception/Exception.hpp"
#include "stage_constants.h"

// Local Files
#include "StageClr.hpp"

//------------------------------------------------------------------------------
// buildRequest
//------------------------------------------------------------------------------
castor::stager::Request* castor::client::StageClr::buildRequest()
  throw (castor::exception::Exception) {
  // First reject some flags parsed by BaseCmdLineClient
  std::vector<std::string> rejected;
  rejected.push_back("K");
  rejected.push_back("A");
  rejected.push_back("a");
  rejected.push_back("s");
  rejected.push_back("silent");
  rejected.push_back("nowait");
  rejected.push_back("noretry");
  rejected.push_back("rdonly");
  rejectFlags(rejected, "stageqry");
  // Build request
  std::string poolName = getPoolName();
  castor::stager::StageClrRequest* req =
    new castor::stager::StageClrRequest();
  req->setFlags(0);
  int n = 0;
  for (std::vector<std::string>::const_iterator it = m_inputArguments.begin();
       it != m_inputArguments.end();
       it++) {
    castor::stager::SubRequest* s = new castor::stager::SubRequest();
    s->setFileName(*it);
    s->setPoolName(poolName);
    n++;
    req->addSubRequests(s);
    s->setRequest(req);
  }
  return req;
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::client::StageClr::usage(std::string error) throw() {
  std::cout << error << std::endl;
  std::cout << "usage : stageclr [-h rh_host] [ -p pool ] hsmfile..."
            << std::endl;
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char** argv) {
  castor::client::StageClr req;
  req.run(argc, argv);
  return 0;
}
