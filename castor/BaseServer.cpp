/******************************************************************************
 *                      BaseServer.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: BaseServer.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2004/11/05 17:47:19 $ $Author: sponcec3 $
 *
 *
 *
 * @author Benjamin Couturier
 *****************************************************************************/

// Include Files
#include <signal.h>
#include "castor/BaseServer.hpp"
#include "castor/MsgSvc.hpp"
#include "castor/Services.hpp"
#include "Cgetopt.h"
#include "Cinit.h"
#include "Cpool_api.h"
#include "castor/logstream.h"
#include <sstream>
#include <iomanip>
#include <Cthread_api.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::BaseServer::BaseServer(const std::string serverName,
                               const int nbThreads):
  m_foreground(false), m_singleThreaded(false), m_threadPoolId(-1),
  m_threadNumber(nbThreads), m_serverName(serverName) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::BaseServer::~BaseServer() throw() {
  // First release the MsgSvc of our BaseObject
  if (0 != m_msgSvc) {
    m_msgSvc->release();
    m_msgSvc = 0;
  }
  // hack to release thread specific allocated memory
  castor::Services* svcs = services();
  if (0 != svcs) {
    delete svcs;
  }
}

//------------------------------------------------------------------------------
// start
//------------------------------------------------------------------------------
int castor::BaseServer::start() {
  int rc;
  if (!m_foreground) {
    if ((rc = Cinitdaemon ((char *)m_serverName.c_str(), 0)) < 0) {
      return -1;
    }
  }
  return serverMain();
}

//------------------------------------------------------------------------------
// serverMain
//------------------------------------------------------------------------------
int  castor::BaseServer::serverMain () {
  // create threads if in multithreaded mode
  int nbThreads, actualNbThreads;
  nbThreads = m_threadNumber;
  if (!m_singleThreaded) {
    m_threadPoolId = Cpool_create(nbThreads, &actualNbThreads);
    if (m_threadPoolId < 0) {
      clog() << FATAL << "Thread pool creation error : "
             << m_threadPoolId
             << std::endl;
      return -1;
    } else {
      clog() << DEBUG << "Thread pool created : "
             << m_threadPoolId << ", "
             << actualNbThreads << std::endl;
      m_threadNumber = actualNbThreads;
    }
  }
  // Ignore SIGPIPE AND SIGXFSZ
#if ! defined(_WIN32)
	signal (SIGPIPE,SIG_IGN);
	signal (SIGXFSZ,SIG_IGN);
#endif

  return main();
}

//------------------------------------------------------------------------------
// threadAssign
//------------------------------------------------------------------------------
int castor::BaseServer::threadAssign(void *param) {

  // Initializing the arguments to pass to the static request processor
  struct processRequestArgs *args = new processRequestArgs();
  args->handler = this;
  args->param = param;

  if (!m_singleThreaded) {
    int assign_rc = Cpool_assign(m_threadPoolId,
                                 &castor::staticProcessRequest,
                                 args,
                                 -1);
    if (assign_rc < 0) {
      clog() << "Error while assigning request to pool" << std::endl;
      return -1;
    }
  } else {
    castor::staticProcessRequest(args);
  }
  return 0;
}


//------------------------------------------------------------------------------
// staticProcessRequest
//------------------------------------------------------------------------------
void *castor::staticProcessRequest(void *param) {
  castor::BaseServer *server = 0;
  struct processRequestArgs *args;

  if (param == NULL) {
    return 0;
  }

  // Recovering the processRequestArgs
  args = (struct processRequestArgs *)param;

  if (args->handler == NULL) {
    delete args;
    return 0;
  }

  server = dynamic_cast<castor::BaseServer *>(args->handler);
  if (server == 0) {
    delete args;
    return 0;
  }

  // Executing the method
  void *res = server->processRequest(args->param);
  delete args;
  return res;
}

//------------------------------------------------------------------------------
// setForeground
//------------------------------------------------------------------------------
void castor::BaseServer::setForeground(bool value) {
  m_foreground = value;
}

//------------------------------------------------------------------------------
// setSingleThreaded
//------------------------------------------------------------------------------
void castor::BaseServer::setSingleThreaded(bool value) {
  m_singleThreaded = value;
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::BaseServer::parseCommandLine(int argc, char *argv[]) {

  static struct Coptions longopts[] =
    {
      {"foreground",         NO_ARGUMENT,        NULL,      'f'},
      {"st",         NO_ARGUMENT,        NULL,      's'},
      {NULL,                 0,                  NULL,        0}
    };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long (argc, argv, "fs", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 's':
      m_singleThreaded = true;
      break;
    }
  }
}
