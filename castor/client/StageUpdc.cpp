/******************************************************************************
 *                      StageUpdc.cpp
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
 * @(#)$RCSfile: StageUpdc.cpp,v $ $Revision: 1.7 $ $Release$ $Date: 2004/09/01 13:45:28 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <iostream>
#include <vector>
#include "castor/rh/File.hpp"
#include "castor/rh/StageUpdcRequest.hpp"
#include "castor/rh/ReqId.hpp"
#include "castor/exception/Exception.hpp"
#include "stage_constants.h"

// Local Files
#include "StageUpdc.hpp"

//------------------------------------------------------------------------------
// buildRequest
//------------------------------------------------------------------------------
castor::rh::Request* castor::client::StageUpdc::buildRequest()
  throw (castor::exception::Exception) {
  // First reject some flags parsed by BaseCmdLineClient
  std::vector<std::string> rejected;
  rejected.push_back("p");
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
  castor::rh::StageUpdcRequest* req =
    new castor::rh::StageUpdcRequest();
  req->setFlags(0);
  int n = 0;
  for (std::vector<std::string>::const_iterator it = m_inputArguments.begin();
       it != m_inputArguments.end();
       it++) {
    castor::rh::ReqId* r = new castor::rh::ReqId();
    r->setValue(*it);
    n++;
    req->addReqids(r);
    r->setRequest(req);
  }
  return req;
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::client::StageUpdc::usage(std::string error) throw() {
  std::cout << error << std::endl;
  std::cout << "usage : stageupdc [-h rh_host] hsmfile..." << std::endl;
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char** argv) {
  castor::client::StageUpdc req;
  req.run(argc, argv);
  return 0;
}
