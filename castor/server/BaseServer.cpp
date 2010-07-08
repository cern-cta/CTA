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
  m_cmdLineParams << "fc:mh";
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
     { 22, "Task processed" },
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
// resetAllMetrics
//-----------------------------------------------------------------------------
void castor::server::BaseServer::resetAllMetrics() throw()
{
  std::map<const char, castor::server::BaseThreadPool*>::const_iterator tp;
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    try {
      tp->second->resetMetrics();
    } catch (castor::exception::Exception ignore) {}
  }
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
