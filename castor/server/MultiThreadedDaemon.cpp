/*******************************************************************************
 *                      castor/server/MultiThreadedDaemon.hpp
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
 * @author castor dev team
 ******************************************************************************/

#include "castor/server/metrics/MetricsCollector.hpp"
#include "castor/server/MultiThreadedDaemon.hpp"
#include "castor/server/UDPListenerThreadPool.hpp"
#include "h/Cgetopt.h"
#include "h/Cthread_api.h"

#include <signal.h>
#include <sys/wait.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::MultiThreadedDaemon::MultiThreadedDaemon(std::ostream &stdOut,
  std::ostream &stdErr, log::Logger &log) throw():
  Daemon(stdOut, stdErr, log), m_signalMutex(NULL) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::MultiThreadedDaemon::~MultiThreadedDaemon() throw() {
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::server::MultiThreadedDaemon::parseCommandLine(int argc,
  char *argv[])  {
  bool foreground = false; // Should the daemon run in the foreground?
  Coptions_t* longopts = new Coptions_t[m_threadPools.size() + 5];
  char tparam[] = "Xthreads";

  longopts[0].name = "foreground";
  longopts[0].has_arg = NO_ARGUMENT;
  longopts[0].flag = NULL;
  longopts[0].val = 'f';
  longopts[1].name = "config";
  longopts[1].has_arg = REQUIRED_ARGUMENT;
  longopts[1].flag = NULL;
  longopts[1].val = 'c';
  longopts[2].name = "metrics";
  longopts[2].has_arg = NO_ARGUMENT;
  longopts[2].flag = NULL;
  longopts[2].val = 'm';
  longopts[3].name = "help";
  longopts[3].has_arg = NO_ARGUMENT;
  longopts[3].flag = NULL;
  longopts[3].val = 'h';

  std::map<const char, castor::server::BaseThreadPool*>::const_iterator tp;
  unsigned i = 4;
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
  while ((c = Cgetopt_long(argc, argv, (char*)m_cmdLineParams.str().c_str(),
    longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      foreground = true;
      break;
    case 'c':
      setenv("PATH_CONFIG", Coptarg, 1);
      m_stdOut << "Using configuration file " << Coptarg << std::endl;
      break;
    case 'h':
      help(argv[0]);
      exit(0);
      break;
    case 'm':
      // initialize the metrics collector thread
      metrics::MetricsCollector::getInstance(this);
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
  for (unsigned j = 4; j < i; j++) {
    free((char*)longopts[j].name);
  };
  delete[] longopts;

  setCommandLineHasBeenParsed(foreground);
}

//------------------------------------------------------------------------------
// help
//------------------------------------------------------------------------------
void castor::server::MultiThreadedDaemon::help(const std::string &programName)
  throw() {
  m_stdOut << "Usage: " << programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--foreground            or -f         \tRemain in the Foreground\n"
    "\t--config <config-file>  or -c         \tConfiguration file\n"
    "\t--metrics               or -m         \tEnable metrics collection\n"
    "\t--help                  or -h         \tPrint this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch\n";
}

//------------------------------------------------------------------------------
// addThreadPool
//------------------------------------------------------------------------------
void castor::server::MultiThreadedDaemon::addThreadPool(
  BaseThreadPool *const pool) throw() {
  if(pool != 0) {
    const char id = pool->getPoolId();
    const BaseThreadPool* p = m_threadPools[id];
    if(p != 0) delete p;
    m_threadPools[id] = pool;
    m_cmdLineParams << id << ":";
  }
}

//------------------------------------------------------------------------------
// start
//------------------------------------------------------------------------------
void castor::server::MultiThreadedDaemon::start(
  const bool runAsStagerSuperuser) {
  if (getForeground()) {
    m_stdOut << "Starting " << getServerName() << std::endl;
  }

  std::map<const char, castor::server::BaseThreadPool*>::const_iterator tp;
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    tp->second->init();
    // in case of exception, don't go further and propagate it
  }

  // Daemonize if not configiured to run in the foreground
  daemonizeIfNotRunInForeground(runAsStagerSuperuser);

  // Initialize CASTOR Thread interface
  Cthread_init();

  setupMultiThreadedSignalHandling();

  // if we got here, we're ready to start all the pools and detach corresponding
  // threads
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    tp->second->run();  // here run returns immediately
  }

  /* Wait forever on a caught signal */
  handleSignals();
}

//------------------------------------------------------------------------------
// setUpMultiThreadedSignalHandling
//------------------------------------------------------------------------------
void castor::server::MultiThreadedDaemon::setupMultiThreadedSignalHandling()
   {
  // Initialize mutex variable in case of a signal. Timeout = 10 seconds
  try {
    m_signalMutex = new Mutex(0);
  } catch(castor::exception::Exception &ie) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create m_signalMutex: " <<
      ie.getMessage().str();
    throw ex;
  }

  // Mask all signals so that user threads are not unpredictably
  // interrupted by them
  sigemptyset(&m_signalSet);

  // In order to facilitate debugging with gdb, only mask Ctrl-C whilst
  // running in the background
  //
  // The gdb debugger has its own SIGINT handler to pause the process
  // anywhere
  if(!getForeground()) {
    sigaddset(&m_signalSet, SIGINT);
  }
  sigaddset(&m_signalSet, SIGTERM);
  sigaddset(&m_signalSet, SIGHUP);
  sigaddset(&m_signalSet, SIGABRT);
  sigaddset(&m_signalSet, SIGCHLD);
  sigaddset(&m_signalSet, SIGPIPE);

  const int sigmask_rc = pthread_sigmask(SIG_BLOCK, &m_signalSet, NULL);
  if (0 != sigmask_rc) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed pthread_sigmask" << std::endl;
    throw ex;
  }

  // Start the thread handling all the signals
  Cthread_create_detached((void *(*)(void *))&s_signalHandler, this);
}

//------------------------------------------------------------------------------
// handleSignals
//------------------------------------------------------------------------------
void castor::server::MultiThreadedDaemon::handleSignals() {
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
             castor::dlf::Param("Action", "Shutting down the service")};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                                  DLF_BASE_FRAMEWORK + 12, 2, params);
          // Wait on all threads/processes to terminate
          waitAllThreads();
          // "Caught signal - GRACEFUL STOP"
          castor::dlf::Param params2[] =
            {castor::dlf::Param("Type", "SIGTERM"),
             castor::dlf::Param("Action", "Shut down successfully completed")};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                                  DLF_BASE_FRAMEWORK + 12, 2, params2);
          dlf_shutdown();
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
                   castor::dlf::Param("Action", "Terminated normally")};
                castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                        DLF_BASE_FRAMEWORK + 14, 3, params);
              } else {
                // "Caught signal - CHILD STOPPED"
                castor::dlf::Param params[] =
                  {castor::dlf::Param("Signal", "SIGCHLD"),
                   castor::dlf::Param("PID", pid),
                   castor::dlf::Param("Action", "Terminated unexpectedly"),
                   castor::dlf::Param("ExitCode", WTERMSIG(status))};
                castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                                        DLF_BASE_FRAMEWORK + 14, 4, params);
              }
            } else if (WIFSIGNALED(status)) {
              // "Caught signal - CHILD STOPPED"
              castor::dlf::Param params[] =
                {castor::dlf::Param("Signal", "SIGCHLD"),
                 castor::dlf::Param("PID", pid),
                 castor::dlf::Param("Action", "Exited with signal"),
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
          dlf_shutdown();
          exit(EXIT_FAILURE);
        }
      }

      // release the mutex
      m_signalMutex->setValueNoMutex(0);
      m_signalMutex->release();
    }
    catch (castor::exception::Exception& e) {
      try {
        m_signalMutex->release();
      } catch (castor::exception::Exception& ignored) {}
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
void castor::server::MultiThreadedDaemon::waitAllThreads() throw() {
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
// resetAllMetrics
//------------------------------------------------------------------------------
void castor::server::MultiThreadedDaemon::resetAllMetrics() throw() {
  std::map<const char, castor::server::BaseThreadPool*>::const_iterator tp;
  for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {
    try {
      tp->second->resetMetrics();
    } catch (castor::exception::Exception& ignore) {}
  }
}

//------------------------------------------------------------------------------
// getThreadPool
//------------------------------------------------------------------------------
castor::server::BaseThreadPool *
  castor::server::MultiThreadedDaemon::getThreadPool(const char nameIn)
   {
  std::map<const char, BaseThreadPool*>::const_iterator tpIt =
    m_threadPools.find(nameIn);
  if(tpIt == m_threadPools.end()) {
    castor::exception::Exception notFound(ENOENT);
    notFound.getMessage() << "No thread pool found with initial " << nameIn;
    throw notFound;
  }
  return tpIt->second;
}

//------------------------------------------------------------------------------
// addNotifierThreadPool
//------------------------------------------------------------------------------
void castor::server::MultiThreadedDaemon::addNotifierThreadPool(const int port)
{
  if(m_threadPools['_'] != 0) delete m_threadPools['_'];   // sanity check

  // This is a pool for internal use, we don't use addThreadPool
  // so to not change the command line parsing behavior
  m_threadPools['_'] = new castor::server::UDPListenerThreadPool(
    "_NotifierThread",
    castor::server::NotifierThread::getInstance(this),
    port);

  // we run the notifier in the same thread as the listening one
  m_threadPools['_']->setNbThreads(0);
}

//------------------------------------------------------------------------------
// shutdownGracefully
//------------------------------------------------------------------------------
void castor::server::MultiThreadedDaemon::shutdownGracefully() throw() {
  m_signalMutex->setValueNoMutex(STOP_GRACEFULLY);
}

//------------------------------------------------------------------------------
// s_signalHandler
//------------------------------------------------------------------------------
void* castor::server::MultiThreadedDaemon::s_signalHandler(void* arg)
{
  castor::server::MultiThreadedDaemon* daemon =
    (castor::server::MultiThreadedDaemon*)arg;

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
        daemon->shutdownGracefully();
        break;

      case SIGCHLD:
        daemon->m_signalMutex->setValueNoMutex(CHILD_STOPPED);
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
