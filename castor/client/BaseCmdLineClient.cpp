/******************************************************************************
 *                      BaseCmdLineClient.cpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * Base class for command line clients. It provides parsing
 * of the input arguments and print out of the result plus
 * some canvas for the implementation of actual clients
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "util.h"
#include "castor/Constants.hpp"
#include "castor/stager/Request.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/client/CmdLineResponseHandler.hpp"
extern "C" {
#include <Cgetopt.h>
}
#include "u64subr.h"

// Local includes
#include "BaseCmdLineClient.hpp"

// XXX To be removed. Extern should not be used but the
// XXX include common.h leads to other problems we don't
// XXX want to fix now
EXTERN_C char DLL_DECL *getconfent _PROTO((char *, char *, int));

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::client::BaseCmdLineClient::BaseCmdLineClient() throw() :
  BaseClient() {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::client::BaseCmdLineClient::~BaseCmdLineClient() throw() {}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::client::BaseCmdLineClient::run(int argc, char** argv)
  throw() {
  initLog("ClientLog", SVC_STDMSG);
  try {
    // parses the command line
    if (!parseInput(argc, argv)) {
      return;
    }
    // builds a request
    castor::stager::Request* req;
    try {
      req = buildRequest();
    } catch (castor::exception::Exception e) {
      usage(e.getMessage().str());
      return;
    }
    // build a ResponseHandler
    IResponseHandler* rh = responseHandler();
    // send it
    sendRequest(req, rh);
    // free memory
    delete req;
    delete rh;
  } catch (castor::exception::Exception ex) {
    std::cout << ex.getMessage().str() << std::endl;
  }
}

//------------------------------------------------------------------------------
// parseInput
//------------------------------------------------------------------------------
bool castor::client::BaseCmdLineClient::parseInput(int argc, char** argv)
  throw (castor::exception::Exception) {
  int nowait_flag = 0;  /** --nowait option flag */
  int noretry_flag = 0; /** --noretry option flag */
  int rdonly_flag = 0;  /** --rdonly option flag */
  int silent_flag = 0;  /** --silent option flag */
  /**
   * List of options supported
   */
  struct Coptions longopts[] = {
    {"allocation_mode", REQUIRED_ARGUMENT,  NULL,        'A'},
    {"host",            REQUIRED_ARGUMENT,  NULL,        'h'},
    {"keep",            NO_ARGUMENT,        NULL,        'K'},
    {"poolname",        REQUIRED_ARGUMENT,  NULL,        'p'},
    {"size",            REQUIRED_ARGUMENT,  NULL,        's'},
    {"silent",          NO_ARGUMENT,        &silent_flag,  1},
    {"nowait",          NO_ARGUMENT,        &nowait_flag,  1},
    {"noretry",         NO_ARGUMENT,        &noretry_flag, 1},
    {"rdonly",          NO_ARGUMENT,        &rdonly_flag,  1},
    {NULL,              0,                  NULL,          0}
  };

  Coptind = 1;    /* Required */
  Copterr = 1;    /* Some stderr output if you want */
  Coptreset = 1;  /* In case we are parsing several times the same argv */
  int ch;

	while ((ch = Cgetopt_long (argc, argv, "aA:fh:Kp:s:", longopts, NULL)) != -1) {
    switch (ch) {
		case 'A':
		case 'h':
		case 's':
		case 'p':
			m_inputFlags[std::string(1, ch)] = Coptarg;
      break;
    case 'K':
    case 'a':
    case 'f':
			m_inputFlags[std::string(1, ch)] = "";
      break;
    case 0:
			// Long option without short option correspondance
      // they will be handled at the end
      break;
    default:
      std::string s("Unknown option : -");
      s += ch;
      usage(s);
      // Unsuccessful completion
      return false;
    }
  }
  argc -= Coptind;
  argv += Coptind;
  // Handling of long opts with no corresponding short option
  if (nowait_flag)  m_inputFlags["nowait"] = "";
  if (noretry_flag) m_inputFlags["noretry"] = "";
  if (rdonly_flag)  m_inputFlags["rdonly"] = "";
  if (silent_flag)  m_inputFlags["silent"] = "";
  // Dealing with arguments
  for (int i = 0; i < argc; i++) {
    m_inputArguments.push_back(argv[i]);
  }
  // Successful completion
  return true;
}

//------------------------------------------------------------------------------
// responseHandler
//------------------------------------------------------------------------------
castor::client::IResponseHandler*
castor::client::BaseCmdLineClient::responseHandler() throw() {
  return new CmdLineResponseHandler();
}

//------------------------------------------------------------------------------
// setRhHost
//------------------------------------------------------------------------------
void castor::client::BaseCmdLineClient::setRhHost()
  throw (castor::exception::Exception) {
  // RH server host. Can be passed in the -h option
  // or given through the RH_HOST environment variable
  // or given in the castor.conf file as a RH/HOST entry
  char* host;
  if (m_inputFlags.find("h") != m_inputFlags.end()) {
    m_rhHost = m_inputFlags["h"];
  } else {
    this->BaseClient::setRhHost();
  }
}

//------------------------------------------------------------------------------
// getPoolName
//------------------------------------------------------------------------------
std::string castor::client::BaseCmdLineClient::getPoolName()
  throw (castor::exception::Exception) {
  char* pool;
  // Pool name. Can be given by the -p option
  // or through the environment variable STAGE_POOL
  if (m_inputFlags.find("p") != m_inputFlags.end()) {
    return m_inputFlags["p"];
  } else if ((pool = getenv ("STAGE_POOL")) != 0) {
    return pool;
  } else {
    clog() << "No pool name, will be set by server."
           << std::endl;
    return "";
  }
}

//------------------------------------------------------------------------------
// getSizes
//------------------------------------------------------------------------------
std::vector<u_signed64> castor::client::BaseCmdLineClient::getSizes()
  throw (castor::exception::Exception) {
  std::vector<u_signed64> sizes;
  if (m_inputFlags.find("s") != m_inputFlags.end()) {
    std::string ssize = m_inputFlags["s"];
    char* size = (char*) malloc(ssize.length()+1);
    strncpy(size, ssize.c_str(), ssize.length());
    size[ssize.length()] = 0;
    char* p = strtok (size, ":");
    while (p != 0) {
      u_signed64 size;
      if (check_for_strutou64(p) < 0 ||
          (size = strutou64(p)) <= 0) {
        castor::exception::Exception e(ETPRM);
        e.getMessage()
          << "Invalid syntax in -s option."
          << std::endl;
        throw e;
      }
      sizes.push_back(size);
      p = strtok(0, ":");
    }
    free(size);
  }
  return sizes;
}

//------------------------------------------------------------------------------
// rejectFlags
//------------------------------------------------------------------------------
void castor::client::BaseCmdLineClient::rejectFlags(std::vector<std::string> &flags,
                                                    std::string cmdName)
  throw (castor::exception::Exception) {
  for (std::vector<std::string>::const_iterator it =
         flags.begin();
       it != flags.end();
       it++) {
    if (m_inputFlags.find(*it) != m_inputFlags.end()) {
      castor::exception::Exception e(ETPRM);
      e.getMessage() << "-";
      if (it->size() > 1) e.getMessage() << "-";
      e.getMessage()
        << *it << " option is not valid in the "
        << cmdName << " command.";
      throw e;    
    }
  }
}
