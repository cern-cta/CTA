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
 * @(#)$RCSfile: StageQry.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/07/13 13:36:29 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <iostream>
#include <cstdlib>
#include <vector>
#include "castor/ObjectSet.hpp"
#include "castor/rh/File.hpp"
#include "castor/rh/StageQryRequest.hpp"
#include "castor/exception/Exception.hpp"
#include "stage_constants.h"
#include "common.h"
#include "stage_api.h"

// Local Files
#include "StageQry.hpp"

//------------------------------------------------------------------------------
// buildRequest
//------------------------------------------------------------------------------
castor::rh::Request* castor::client::StageQry::buildRequest()
  throw (castor::exception::Exception) {
  u_signed64 flags = 0;
  {
    // RH server host. Can be passed in the -h option
    // or given through the RH_HOST environment variable
    // or given in the castor.conf file as a RH/HOST entry
    char* host;
    if (m_inputFlags.find("h") != m_inputFlags.end()) {
      m_rhHost = m_inputFlags["h"];
    } else if ((host = getenv ("RH_HOST")) != 0 ||
               (host = getconfent("RH","HOST",0)) != 0) {
      m_rhHost = host;
    } else {
      castor::exception::Exception e(ETPRM);
      e.getMessage()
        << "Unable to deduce the name of the RH server.\n"
        << "No -h option was given, RH_HOST is not set and "
        << "your castor.conf file does not contain a RH/HOST entry."
        << std::endl;
      throw e;
    }
  }
  {
    char* port;
    // RH server port. Can be given through the environment
    // variable RH_PORT or in the castor.conf file as a
    // RH/PORT entry
    if ((port = getenv ("RH_PORT")) != 0 ||
        (port = getconfent("RH","PORT",0)) != 0) {
      int iport;
      char* dp;
      if (stage_strtoi(&iport, port, &dp, 0) != 0) {
        castor::exception::Exception e(errno);
        e.getMessage() << "Bad port value." << std::endl;
        throw e;
      }
      if (iport < 0) {
        castor::exception::Exception e(errno);
        e.getMessage()
          << "Invalid port value : " << iport
          << ". Must be > 0." << std::endl;
        throw e;        
      }
      m_rhPort = iport;
    } else {
      castor::exception::Exception e(ETPRM);
      e.getMessage()
        << "Unable to deduce the RH server port.\n"
        << "RH_PORT is not set and your castor.conf file "
        << "does not contain a RH/PORT entry."
        << std::endl;
      throw e;
    }
  }
  std::string poolName;
  {
    char* pool;
    // Pool name. Can be given by the -p option
    // or through the environment variable STAGE_POOL
    if (m_inputFlags.find("p") != m_inputFlags.end()) {
      poolName = m_inputFlags["p"];
    } else if ((pool = getenv ("STAGE_POOL")) != 0) {
      poolName = pool;
    } else {
      castor::exception::Exception e(ETPRM);
      e.getMessage()
        << "Unable to deduce the name of the stage pool.\n"
        << "No -p option was given and STAGE_POOL is not set."
        << std::endl;
      throw e;
    }
  }
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
