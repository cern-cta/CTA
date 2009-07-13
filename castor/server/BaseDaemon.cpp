/******************************************************************************
 *                      BaseDaemon.cpp
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
 * @(#)BaseDaemon.cpp,v 1.8 $Release$ 2006/08/14 19:10:38 itglp
 *
 * A base multithreaded daemon supporting signal handling
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include <signal.h>
#include <sys/wait.h>
#include "castor/server/BaseDaemon.hpp"
#include "castor/server/UDPListenerThreadPool.hpp"
#include "castor/exception/Internal.hpp"
#include "Cgetopt.h"
#include "Cinit.h"
#include "Cuuid.h"
#include "Cthread_api.h"
#include <sstream>
#include <iostream>
#include <iomanip>
#include <vector>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::BaseDaemon::BaseDaemon(const std::string serverName) :
  castor::server::BaseServer(serverName) {}


//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::init() throw (castor::exception::Exception)
{
  // Server initialization (in foreground or not)
  castor::server::BaseServer::init();

  // Initialize CASTOR Thread interface
  Cthread_init();

  // Initialize mutex variable in case of a signal. Timeout = 10 seconds
  m_signalMutex = new Mutex(0);

  // Initialize errno, serrno
  errno = serrno = 0;

  // Mask all signals so that user threads are not unpredictably
  // interrupted by them
  sigemptyset(&m_signalSet);
  if(m_foreground) {
    // in foreground we catch Ctrl-C as well
    sigaddset(&m_signalSet, SIGINT);
  }
  sigaddset(&m_signalSet, SIGTERM);
  sigaddset(&m_signalSet, SIGHUP);
  sigaddset(&m_signalSet, SIGABRT);
  sigaddset(&m_signalSet, SIGCHLD);
  sigaddset(&m_signalSet, SIGPIPE);

  int c;
  if ((c = pthread_sigmask(SIG_BLOCK, &m_signalSet, NULL)) != 0) {
    errno = c;
    castor::exception::Internal ex;
    ex.getMessage() << "Failed pthread_sigmask" << std::endl;
    throw ex;
  }

  // Start the thread handling all the signals
  Cthread_create_detached((void *(*)(void *))&_signalHandler, this);

  // Create the DLF related threads. This is done here because it is after
  // daemonization and signal handler creation.
  dlf_create_threads(0);
}


//------------------------------------------------------------------------------
// start
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::start() throw (castor::exception::Exception)
{
  castor::server::BaseServer::start();

  /* Wait forever on a caught signal */
  handleSignals();
}


//------------------------------------------------------------------------------
// addNotifierThreadPool
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::addNotifierThreadPool(int port)
{
  if(m_threadPools['_'] != 0) delete m_threadPools['_'];   // sanity check

  // This is a pool for internal use, we don't use addThreadPool
  // so to not change the command line parsing behavior
  m_threadPools['_'] =
    new castor::server::UDPListenerThreadPool("_NotifierThread",
                                              castor::server::NotifierThread::getInstance(this), port);

  // we run the notifier in the same thread as the listening one
  m_threadPools['_']->setNbThreads(0);
}


//------------------------------------------------------------------------------
// handleSignals
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::handleSignals()
{
  while(true) {
    try {
      m_signalMutex->lock();

      // poll the signalMutex
      while (!m_signalMutex->getValue()) {
        // Note: Without COND_TIMEOUT > 0 this will never work - because the
        // condition variable is changed in a signal handler - we cannot use
        // condition signal in this signal handler
        m_signalMutex->wait();
      }

      int sigValue = m_signalMutex->getValue();

      switch (sigValue) {
      case STOP_GRACEFULLY:
        {
          // "Caught signal - GRACEFUL STOP"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Type", "SIGTERM"),
             castor::dlf::Param("Message", "Shutting down the service")};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                                  DLF_BASE_FRAMEWORK + 12, 2, params);
          // Wait on all threads/processes to terminate
          waitAllThreads();
          // "Caught signal - GRACEFUL STOP"
          castor::dlf::Param params2[] =
            {castor::dlf::Param("Type", "SIGTERM"),
             castor::dlf::Param("Message", "Shut down successfully completed")};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                                  DLF_BASE_FRAMEWORK + 12, 2, params2);
          dlf_shutdown(10);
          exit(EXIT_SUCCESS);
          break;
        }
      case CHILD_STOPPED:
        {
          // Reap dead processes to prevent defunct processes
          pid_t pid = 0;
          int status;
          while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
            if (WIFEXITED(status)) {
              if (WEXITSTATUS(status) == 0) {
                // "Caught signal - CHILD STOPPED"
                castor::dlf::Param params[] =
                  {castor::dlf::Param("Signal", "SIGCHLD"),
                   castor::dlf::Param("PID", pid),
                   castor::dlf::Param("Message", "Terminated normally")};
                castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                        DLF_BASE_FRAMEWORK + 14, 3, params);
              } else {
                // "Caught signal - CHILD STOPPED"
                castor::dlf::Param params[] =
                  {castor::dlf::Param("Signal", "SIGCHLD"),
                   castor::dlf::Param("PID", pid),
                   castor::dlf::Param("Message", "Terminated unexpectedly"),
                   castor::dlf::Param("ExitCode", WTERMSIG(status))};
                castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                                        DLF_BASE_FRAMEWORK + 14, 4, params);
              }
            } else if (WIFSIGNALED(status)) {
              // "Caught signal - CHILD STOPPED"
              castor::dlf::Param params[] =
                {castor::dlf::Param("Signal", "SIGCHLD"),
                 castor::dlf::Param("PID", pid),
                 castor::dlf::Param("Message", "Exited with signal"),
                 castor::dlf::Param("ExitCode", WTERMSIG(status))};
              castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                                      DLF_BASE_FRAMEWORK + 14, 4, params);
            }
          }
          break;
        }

      default:
        {
          // "Signal caught but not handled - IMMEDIATE STOP"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Signal", (-1 * sigValue))};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                                  DLF_BASE_FRAMEWORK + 15, 1, params);
          dlf_shutdown(1);
          exit(EXIT_FAILURE);
        }
      }

      // release the mutex
      m_signalMutex->setValueNoMutex(0);
      m_signalMutex->release();
    }
    catch (castor::exception::Exception e) {
      try {
        m_signalMutex->release();
      } catch (castor::exception::Exception ignored) {}
      // "Exception during wait for signal loop"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Error", sstrerror(e.code())),
         castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                              DLF_BASE_FRAMEWORK + 16, 2, params);
      // wait a bit and try again
      sleep(1);
    }
  }
}


//------------------------------------------------------------------------------
// waitAllThreads
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::waitAllThreads() throw()
{
  std::map<const char, castor::server::BaseThreadPool*>::iterator tp;
  std::vector<castor::server::BaseThreadPool*> busyTPools;
  // Shutdown and destroy all pools, but keep the busy ones
  for(tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    if(!tp->second->shutdown(false)) {
      busyTPools.push_back(tp->second);
    }
    else
      delete tp->second;
  }

  // Reap child processes
  pid_t pid;
  while( (pid = waitpid(-1, NULL, WNOHANG)) > 0) {}

  // Now loop waiting on the remaining busy ones;
  // this is a best effort attempt, which may get interrupted
  // by the OS aborting the process.
  while(busyTPools.size() > 0) {
    usleep(100000);     // wait before trying again
    for(unsigned i = 0; i < busyTPools.size(); ) {
      if(busyTPools[i]->shutdown(false)) {
        // it's idle now, let's remove it
        delete busyTPools[i];
        busyTPools.erase(busyTPools.begin() + i);
      }
      else
        i++;
    }
  }
}


//------------------------------------------------------------------------------
// _signalHandler
//------------------------------------------------------------------------------
void* castor::server::BaseDaemon::_signalHandler(void* arg)
{
  castor::server::BaseDaemon* daemon = (castor::server::BaseDaemon*)arg;

  // keep looping waiting signals
  int sig_number;
  while (true) {
    if (sigwait(&daemon->m_signalSet, &sig_number) == 0) {

      // Note: from now on this is unsafe but here we go, we cannot use
      // mutex/condition/printing etc... e.g. things unsafe in a signal handler
      // so from now on this function is calling nothing external

      // Because of the way in which signals are handled in the signalHandler
      // method its more than possible that multiple signals overwrite each other.
      // So for example, a killall -15 on a multi threaded, multi process daemon
      // will trap a SIGTERM followed by multiple SIGCHLD's from the dying
      // children. The SIGCHLD's will overwrite the SIGTERM and it will never be
      // processed. So, we spin lock on the signal value in the mutex until its
      // non zero, indicating that it has been processed.
      while (daemon->m_signalMutex->getValue()) {
        usleep(500);
      }

      switch (sig_number) {
      case SIGABRT:
      case SIGTERM:
      case SIGINT:
      case SIGHUP:
        daemon->m_signalMutex->setValueNoMutex(castor::server::STOP_GRACEFULLY);
        break;

      case SIGCHLD:
        daemon->m_signalMutex->setValueNoMutex(castor::server::CHILD_STOPPED);
        break;

      case SIGPIPE:
        // ignore
        break;

      default:
        // all other signals
        daemon->m_signalMutex->setValueNoMutex(-1*sig_number);
        break;
      }
      // signal the main thread to process the signal we got
      daemon->m_signalMutex->signal();
    }
  }
  return 0;
}
