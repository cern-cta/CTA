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
 * @(#)$RCSfile: StageQry.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/07/29 12:17:05 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <iostream>
#include <vector>
#include "castor/ObjectSet.hpp"
#include "castor/rh/File.hpp"
#include "castor/rh/StageQryRequest.hpp"
#include "castor/exception/Exception.hpp"
#include "stage_constants.h"

// Local Files
#include "StageQry.hpp"

//------------------------------------------------------------------------------
// buildRequest
//------------------------------------------------------------------------------
castor::rh::Request* castor::client::StageQry::buildRequest()
  throw (castor::exception::Exception) {
  u_signed64 flags = 0;
  setRhHost();
  setRhPort();
  std::string poolName = getPoolName();
  // -K option (parsed by BaseClient)
  if (m_inputFlags.find("K") != m_inputFlags.end()) {
    castor::exception::Exception e(ETPRM);
    e.getMessage()
      << "-K option is not valid in the stageqry command."
      << std::endl;
    throw e;
  }
  // Allocation policy
  if (m_inputFlags.find("A") != m_inputFlags.end()) {
    castor::exception::Exception e(ETPRM);
    e.getMessage()
      << "-A option is not valid in the stageqry command."
      << std::endl;
    throw e;    
  }
  // Size
  if (m_inputFlags.find("s") != m_inputFlags.end()) {
    castor::exception::Exception e(ETPRM);
    e.getMessage()
      << "-A option is not valid in the stageqry command."
      << std::endl;
    throw e;    
  }
  // Silent
  if (m_inputFlags.find("silent") != m_inputFlags.end()) {
    castor::exception::Exception e(ETPRM);
    e.getMessage()
      << "--silent option is not valid in the stageqry command."
      << std::endl;
    throw e;    
  }
  // NoWait
  if (m_inputFlags.find("nowait") != m_inputFlags.end()) {
    castor::exception::Exception e(ETPRM);
    e.getMessage()
      << "--nowait option is not valid in the stageqry command."
      << std::endl;
    throw e;    
  }
  // NoRetry
  if (m_inputFlags.find("noretry") != m_inputFlags.end()) {
    flags |= STAGE_NORETRY;
  }
  // RdOnly
  if (m_inputFlags.find("rdonly") != m_inputFlags.end()) {
    castor::exception::Exception e(ETPRM);
    e.getMessage()
      << "--rdonly option is not valid in the stageqry command."
      << std::endl;
    throw e;    
  }
  // Build request
  castor::rh::StageQryRequest* req =
    new castor::rh::StageQryRequest();
  req->setFlags(flags);
  int n = 0;
  for (std::vector<std::string>::const_iterator it = m_inputArguments.begin();
       it != m_inputArguments.end();
       it++) {
    castor::rh::File* f = new castor::rh::File();
    f->setName(*it);
    f->setPoolname(poolName);
    f->setXsize(0);
    n++;
    req->addFiles(f);
    f->setRequest(req);
  }
  return req;
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::client::StageQry::usage(std::string error) throw() {
  std::cout << error << std::endl;
  std::cout << "usage : stageqry [-h rh_host] [-p poolname] [--noretry] hsmfile..."
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
