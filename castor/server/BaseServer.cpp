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
 * @(#)$RCSfile: BaseServer.cpp,v $ $Revision: 1.40 $ $Release$ $Date: 2009/08/18 09:42:54 $ $Author: waldron $
 *
 * A base multithreaded server for simple listening servers
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/server/BaseServer.hpp"
#include "castor/server/ThreadNotification.hpp"
#include "castor/io/UDPSocket.hpp"
#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/System.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/dlf/Message.hpp"

#include "Cgetopt.h"
#include "Cinit.h"
#include "Cuuid.h"
#include "Cpool_api.h"
#include "marshall.h"
#include <iostream>
#include <sstream>
#include <signal.h>
#include <pwd.h>
#include <sys/time.h>
#include <unistd.h>
#include <netdb.h>                      /* For struct servent */
#include <net.h>
#include <stdio.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::server::BaseServer::BaseServer(const std::string serverName) :
  m_foreground(false),
  m_runAsStagerSuperuser(false),
  m_serverName(serverName)
{
  m_cmdLineParams.clear();
  m_cmdLineParams << "fc:h";
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::server::BaseServer::~BaseServer() throw()
{
  // tries to free all thread pools
  std::map<const char, castor::server::BaseThreadPool*>::iterator tp;
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    delete tp->second;
  }
}

//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::server::BaseServer::init() throw (castor::exception::Exception)
{
  // init daemon if to be run in background
  if (!m_foreground) {

    // we could set our working directory to '/' here with a call to chdir(2).
    // For the time being we don't and leave it to the initd script to change
    // to a suitable directory for us!
    int pid = fork();
    if (pid < 0) {
      castor::exception::Internal ex;
      ex.getMessage() << "Background daemon initialization failed with result "
                      << pid << std::endl;
      throw ex;
    }
    else if (pid > 0) {
      // the parent exits normally
      exit(EXIT_SUCCESS);
    }

    // run the program in a new session
    setsid();

    // redirect the standard file descriptors to /dev/null
    if ((freopen("/dev/null", "r", stdin)  == NULL) ||
        (freopen("/dev/null", "w", stdout) == NULL) ||
        (freopen("/dev/null", "w", stderr) == NULL)) {
      castor::exception::Internal ex;
      ex.getMessage() << "Failed to redirect standard file descriptors to "
                      << "/dev/null" << std::endl;
      throw ex;
    }
  }

  // change identity to Castor superuser if requested
  if (m_runAsStagerSuperuser) {
    castor::System::switchToCastorSuperuser();
  }

  // Ignore SIGPIPE (connection lost with client)
  // and SIGXFSZ (a file is too big)
  signal(SIGPIPE, SIG_IGN);
  signal(SIGXFSZ, SIG_IGN);
}

//-----------------------------------------------------------------------------
// dlfInit
//-----------------------------------------------------------------------------
void castor::server::BaseServer::dlfInit(castor::dlf::Message messages[])
  throw (castor::exception::Exception)
{
  castor::dlf::dlf_init((char*)m_serverName.c_str(), messages);
  // Add framework specific messages
  castor::dlf::Message frameworkMessages[] =
    {{  1, "Error while reading datagrams" },
     {  2, "Error while accepting connections" },
     {  3, "Thread pool started" },
     {  4, "Exception caught in the user thread" },
     {  5, "Thread run error" },
     {  6, "NotifierThread exception" },
     {  8, "Exception caught while initializing the child process" },
     {  9, "Error while processing an object from the pipe" },
     { 10, "Uncaught exception in a thread from pool" },
     { 11, "Uncaught GENERAL exception in a thread from pool" },
     { 12, "Caught signal - GRACEFUL STOP" },
     { 14, "Caught signal - CHILD STOPPED" },
     { 15, "Signal caught but not handled - IMMEDIATE STOP" },
     { 16, "Exception during wait for signal loop" },
     { 18, "No idle thread in pool to process request" },
     { 19, "Error while dispatching to a thread" },
     { 20, "Spawning a new thread in pool" },
     { 21, "Terminating a thread in pool" },
     { -1, "" }};
  castor::dlf::dlf_addMessages(DLF_BASE_FRAMEWORK, frameworkMessages);
}

//-----------------------------------------------------------------------------
// start
//-----------------------------------------------------------------------------
void castor::server::BaseServer::start() throw (castor::exception::Exception)
{
  if (m_foreground) {
    std::cout << "Starting " << m_serverName << std::endl;
  }

  std::map<const char, castor::server::BaseThreadPool*>::const_iterator tp;
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    tp->second->init();
    // in case of exception, don't go further and propagate it
  }

  // daemonization
  init();

  // if we got here, we're ready to start all the pools and detach corresponding
  // threads
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    tp->second->run();  // here run returns immediately
  }
}

//-----------------------------------------------------------------------------
// addThreadPool
//-----------------------------------------------------------------------------
void castor::server::BaseServer::addThreadPool
(castor::server::BaseThreadPool* pool) throw()
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
castor::server::BaseThreadPool* castor::server::BaseServer::getThreadPool
(const char id) throw()
{
  return m_threadPools[id];
}

//-----------------------------------------------------------------------------
// parseCommandLine
//-----------------------------------------------------------------------------
void castor::server::BaseServer::parseCommandLine(int argc, char *argv[])
{
  Coptions_t* longopts = new Coptions_t[m_threadPools.size() + 4];
  char tparam[] = "Xthreads";

  longopts[0].name = "foreground";
  longopts[0].has_arg = NO_ARGUMENT;
  longopts[0].flag = NULL;
  longopts[0].val = 'f';
  longopts[1].name = "config";
  longopts[1].has_arg = REQUIRED_ARGUMENT;
  longopts[1].flag = NULL;
  longopts[1].val = 'c';
  longopts[2].name = "help";
  longopts[2].has_arg = NO_ARGUMENT;
  longopts[2].flag = NULL;
  longopts[2].val = 'h';


  std::map<const char, castor::server::BaseThreadPool*>::const_iterator tp;
  unsigned i = 3;
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
  Coptreset = 1;

  char c;
  while ((c = Cgetopt_long(argc, argv, (char*)m_cmdLineParams.str().c_str(), longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'c':
      setenv("PATH_CONFIG", Coptarg, 1);
      std::cout << "Using configuration file " << Coptarg << std::endl;
      break;
    case 'h':
      help(argv[0]);
      exit(0);
      break;
    default:
      tp = m_threadPools.find(c);
      if(tp != m_threadPools.end()) {
        tp->second->setNbThreads(atoi(Coptarg));
      }
      break;
    }
  }
  // free memory
  for(unsigned j = 3; j < i;j++) {
    free((char*)longopts[j].name);
  };
  delete[] longopts;
}

//-----------------------------------------------------------------------------
// help
//-----------------------------------------------------------------------------
void castor::server::BaseServer::help(std::string programName)
{
  std::cout << "Usage: " << programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--foreground            or -f         \tRemain in the Foreground\n"
    "\t--config <config-file>  or -c         \tConfiguration file\n"
    "\t--help                  or -h         \tPrint this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch\n";
}

//-----------------------------------------------------------------------------
// sendNotification
//-----------------------------------------------------------------------------
void castor::server::BaseServer::sendNotification
(std::string host, int port, char tpName, int nbThreads)
  throw()
{
  try {
    // create notification message
    castor::server::ThreadNotification notif;
    notif.setTpName(tpName);
    notif.setNbThreads(nbThreads);

    // create UDP socket and send packet
    castor::io::UDPSocket sock(port, host);
    sock.sendObject(notif);
    sock.close();
  }
  catch (castor::exception::Exception ignored) {
    // this is a best effort service, ignore any failure
  }
}

