/******************************************************************************
 *                      StageOut.cpp
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
 * @(#)$RCSfile: StageOut.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2004/10/01 14:26:12 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <iostream>
#include <vector>
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/StageOutRequest.hpp"
#include "castor/exception/Exception.hpp"
#include "stage_constants.h"

// Local Files
#include "StageOut.hpp"

//------------------------------------------------------------------------------
// buildRequest
//------------------------------------------------------------------------------
castor::stager::Request* castor::client::StageOut::buildRequest()
  throw (castor::exception::Exception) {
  // First reject some flags parsed by BaseCmdLineClient
  std::vector<std::string> rejected;
  rejected.push_back("A");
  rejected.push_back("a");
  rejected.push_back("silent");
  rejected.push_back("nowait");
  rejected.push_back("rdonly");
  rejectFlags(rejected, "stageout");
  // uses some other flags
  u_signed64 flags = 0;
  std::string poolName = getPoolName();
  if (m_inputFlags.find("K") != m_inputFlags.end()) {
    flags |= STAGE_KEEP;
  }
  // Size
  std::vector<u_signed64> sizes = getSizes();
  // NoRetry
  if (m_inputFlags.find("noretry") != m_inputFlags.end()) {
    flags |= STAGE_NORETRY;
  }
  // Build request
  castor::stager::StageOutRequest* req =
    new castor::stager::StageOutRequest();
  req->setFlags(flags);
  req->setOpenmode(0);  // XXX To be fixed. Ask Jean-Damien
  int n = 0;
  for (std::vector<std::string>::const_iterator it = m_inputArguments.begin();
       it != m_inputArguments.end();
       it++) {
    castor::stager::SubRequest* s = new castor::stager::SubRequest();
    s->setFileName(*it);
    s->setPoolName(poolName);
    if (n < sizes.size()) {
      s->setXsize(sizes[n]);
    } else {
      s->setXsize(0);
    }
    n++;
    req->addSubRequests(s);
    s->setRequest(req);
  }
  return req;
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::client::StageOut::usage(std::string error) throw() {
  std::cout << error << std::endl;
  std::cout << "usage : stageout [-h rh_host] [-K] [-p pool] [-s size] "
            << "[--noretry] [--nowait] [--silent] [--rdonly]"
            << " hsmfile..." << std::endl;
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(int argc, char** argv) {
  castor::client::StageOut req;
  req.run(argc, argv);
  return 0;
}
