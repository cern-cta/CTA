/******************************************************************************
 *                      StageIn.cpp
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
 * @(#)$RCSfile: StageIn.cpp,v $ $Revision: 1.12 $ $Release$ $Date: 2004/10/01 14:26:12 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <iostream>
#include <vector>
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/StageInRequest.hpp"
#include "castor/exception/Exception.hpp"
#include "stage_constants.h"

// Local Files
#include "StageIn.hpp"

//------------------------------------------------------------------------------
// buildRequest
//------------------------------------------------------------------------------
castor::stager::Request* castor::client::StageIn::buildRequest()
  throw (castor::exception::Exception) {
  // First reject some flags parsed by BaseCmdLineClient
  std::vector<std::string> rejected;
  rejected.push_back("K");
  rejected.push_back("a");
  rejectFlags(rejected, "stagein");
  // uses some other flags
  u_signed64 flags = 0;
  int openflags = 0;
  std::string poolName = getPoolName();
  // Allocation policy
  if (m_inputFlags.find("A") != m_inputFlags.end()) {
    if (m_inputFlags["A"] == "deferred") {
      flags |= STAGE_DEFERRED;
    } else if (m_inputFlags["A"] != "immediate") {
      castor::exception::Exception e(ETPRM);
      e.getMessage()
        << "Invalid argument for -A option.\n"
        << "Should be either deferred or immediate."
        << std::endl;
      throw e;
    }
  }
  // Size
  std::vector<u_signed64> sizes = getSizes();
  // Silent
  if (m_inputFlags.find("silent") != m_inputFlags.end()) {
    flags |= STAGE_SILENT;
  }
  // NoWait
  if (m_inputFlags.find("nowait") != m_inputFlags.end()) {
    flags |= STAGE_NOWAIT;
  }
  // NoRetry
  if (m_inputFlags.find("noretry") != m_inputFlags.end()) {
    flags |= STAGE_NORETRY;
  }
  // RdOnly
  if (m_inputFlags.find("rdonly") != m_inputFlags.end()) {
    openflags |= STAGE_RDONLY;
  }
  // Build request
  castor::stager::StageInRequest* req =
    new castor::stager::StageInRequest();
  req->setFlags(flags);
  req->setOpenflags(openflags);
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
void castor::client::StageIn::usage(std::string error) throw() {
  std::cout << error << std::endl;
  std::cout << "usage : stagein [-A alloc_mode] [-h rh_host] [-p pool] [-s size] "
            << "[--noretry] [--nowait] [--silent] [--rdonly]"
            << " hsmfile..." << std::endl;
}
