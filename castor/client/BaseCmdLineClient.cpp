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
#include "castor/Constants.hpp"
#include "castor/rh/Request.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/client/CmdLineResponseHandler.hpp"
extern "C" {
#include <Cgetopt.h>
}

// Local includes
#include "BaseCmdLineClient.hpp"

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
    castor::rh::Request* req;
    try {
      req = buildRequest();
    } catch (castor::exception::Exception e) {
      usage(e.getMessage().str());
      return;
    }
    // build a ResponseHandler
    IResponseHandler* rh = new CmdLineResponseHandler();
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

	while ((ch = Cgetopt_long (argc, argv, "A:h:Kp:s:", longopts, NULL)) != -1) {
    switch (ch) {
		case 'A':
		case 'h':
		case 's':
		case 'p':
			m_inputFlags[std::string(1, ch)] = Coptarg;
      break;
    case 'K':
			m_inputFlags["K"] = "";
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
