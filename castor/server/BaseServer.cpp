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
 * @(#)$RCSfile: BaseServer.cpp,v $ $Revision: 1.5 $ $Release$ $Date: 2005/12/08 14:04:33 $ $Author: itglp $
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
#include "castor/Constants.hpp"
#include "castor/exception/Internal.hpp"
#include "Cgetopt.h"
#include "Cinit.h"
#include "Cuuid.h"
#include "Cpool_api.h"
#include "castor/logstream.h"
#include <iostream>
#include <sstream>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::BaseServer::BaseServer(const std::string serverName) :
  m_foreground(false), m_serverName(serverName)
{
  m_cmdLineParams << "fh";
}

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
  // init daemon if to be run in background 
  if (!m_foreground) {
    int rc;
    if ((rc = Cinitdaemon((char *)m_serverName.c_str(), 0)) < 0) {
      castor::exception::Internal ex;
      ex.getMessage() << "Background daemon initialization failed with result "
                      << rc << std::endl;
      throw ex;
    }
  }

  // Ignore SIGPIPE AND SIGXFSZ
  // to avoid crashing when a file is too big or
  // when the connection is lost with a client
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
  std::cout << "Starting " << m_serverName << std::endl;

  std::map<const char, castor::server::BaseThreadPool*>::iterator tp;
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    tp->second->init();
    // in case of exception, don't go further and propagate it
  }

  // if we got here, we're ready to start all the pools and detach corresponding threads
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    tp->second->run();  // here run returns immediately
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
    m_cmdLineParams << id << ":";
  }
}

//-----------------------------------------------------------------------------
// getThreadPool
//-----------------------------------------------------------------------------
castor::server::BaseThreadPool* castor::server::BaseServer::getThreadPool(const char id) throw()
{
  return m_threadPools[id];
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::server::BaseServer::parseCommandLine(int argc, char *argv[])
{
  const char* cmdParams = m_cmdLineParams.str().c_str();
  Coptions_t* longopts = new Coptions_t[m_threadPools.size()+3];
  char tparam[] = "Xthreads";
  
  longopts[0].name = "foreground";
  longopts[0].has_arg = NO_ARGUMENT;
  longopts[0].flag = NULL;
  longopts[0].val = 'f';
  longopts[1].name = "help";
  longopts[1].has_arg = NO_ARGUMENT;
  longopts[1].flag = NULL;
  longopts[1].val = 'h';
  
  std::map<const char, castor::server::BaseThreadPool*>::iterator tp;
  int i = 2;
  for(tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++, i++) {
    tparam[0] = tp->first;
    longopts[i].name = strdup(tparam);
    longopts[i].has_arg = REQUIRED_ARGUMENT;
    longopts[i].flag = NULL;
    longopts[i].val = tp->first;
    };
  longopts[i].name = 0;

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long(argc, argv, (char*)cmdParams, longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'h':
      help(argv[0]);
      break;
    default:
      BaseThreadPool* p = m_threadPools[c];
      if(p != 0) {
        p->setNbThreads(atoi(Coptarg));
      }
      else {
        help(argv[0]);
        exit(0);
      }
      break;
    }
  }
  delete longopts;
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
	  "\n"
	  "Comments to: Castor.Support@cern.ch\n";
}

