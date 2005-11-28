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
 * @(#)$RCSfile: BaseServer.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/11/28 09:42:51 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include <signal.h>
#include "castor/server/BaseServer.hpp"
#include "castor/MsgSvc.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "Cgetopt.h"
#include "Cinit.h"
#include "Cuuid.h"
#include "Cpool_api.h"
#include "castor/logstream.h"
#include <sstream>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::BaseServer::BaseServer(const std::string serverName) :
  m_foreground(false), m_serverName(serverName) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::BaseServer::~BaseServer() throw()
{
  // tries to stop and free all thread pools
  std::map<const char, castor::server::BaseThreadPool*>::iterator tp;
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    delete tp->second;
  }

  // hack to release thread specific allocated memory
  castor::Services* svcs = services();
  if (0 != svcs) {
    delete svcs;
  }
}

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::BaseServer::init() throw (castor::exception::Exception)
{
  int rc;
  if (!m_foreground) {
    if ((rc = Cinitdaemon ((char *)m_serverName.c_str(), 0)) < 0) {
      castor::exception::Internal ex;
      ex.getMessage() << "Background daemon initialization failed with result "
                      << rc << std::endl;
      throw ex;
    }
  }

// Ignore SIGPIPE AND SIGXFSZ
#if !defined(_WIN32)
	signal (SIGPIPE,SIG_IGN);
	signal (SIGXFSZ,SIG_IGN);
#endif
}

//------------------------------------------------------------------------------
// start
//------------------------------------------------------------------------------
void castor::server::BaseServer::start() throw (castor::exception::Exception)
{
  init();

  std::map<const char, castor::server::BaseThreadPool*>::iterator tp;
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    tp->second->init();
    // in case of exception, don't go further and propagate it
    }

  // if we got here, we're ready to start all the pools
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    tp->second->run();  // this supposes that run() detaches and returns immediately!
    // XXX To be understood how to distinguish threadpools
    // XXX waiting for instance on a socket.accept()...
    // XXX For the time being, only the "last" pool can do this.
  }
}

//-----------------------------------------------------------------------------
// addThreadPool
//-----------------------------------------------------------------------------
void castor::server::BaseServer::addThreadPool(castor::server::BaseThreadPool* pool) throw()
{
  if(pool != 0) {
    const char id = pool->getPoolId();
    const BaseThreadPool* p = m_threadPools[id];
    if(p != 0) delete p;
    m_threadPools[id] = pool;
  }
}

//-----------------------------------------------------------------------------
// getThreadPool
//-----------------------------------------------------------------------------
const castor::server::BaseThreadPool*
 castor::server::BaseServer::getThreadPool(const char id) throw()
{
  return m_threadPools[id];
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::server::BaseServer::parseCommandLine(int argc, char *argv[])
{
  static struct Coptions longopts[] =
    {
      {"foreground",        NO_ARGUMENT,        NULL,      'f'},
      {"st",                NO_ARGUMENT,        NULL,      's'},
      {"nbThreads",         REQUIRED_ARGUMENT,  NULL,      'n'},
      {NULL,                0,                  NULL,       0 }
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
      //m_threadPools[...]->setSingleThreaded();
      break;
    case 'n':
      //m_threadPools[...]->setNbThreads(atoi(Coptarg));
      break;
    case 'h':
      help(argv[0]);
      break;
    }
  }
}

//------------------------------------------------------------------------------
// help
//------------------------------------------------------------------------------
void castor::server::BaseServer::help(std::string programName)
{
  std::cout << "Usage: " << programName << " [options]\n"
	  "\n"
	  "where options can be:\n"
	  "\n"
	  "\t--foreground   or -f                \tForeground\n"
	  "\t--help         or -h                \tThis help\n"
	  "\t--nthreads     or -n {integer >= 0} \tNumber of service threads\n"
	  "\n"
	  "Comments to: Castor.Support@cern.ch\n";
}

