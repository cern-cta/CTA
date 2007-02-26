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
#include "castor/server/BaseDaemon.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/MsgSvc.hpp"
#include "Cgetopt.h"
#include "Cinit.h"
#include "Cuuid.h"
#include "Cthread_api.h"
#include "castor/logstream.h"
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
void castor::server::BaseDaemon::init() throw(castor::exception::Exception)
{
  // Server initialization (in foreground or not)
  castor::server::BaseServer::init();

  /* Initialize CASTOR Thread interface */
  Cthread_init();

  /* Initialize mutex variable in case of a signal */
  m_signalMutex = new Mutex(0);

  /* Initialize errno, serrno */
  errno = serrno = 0;

  sigemptyset(&m_signalSet);
  sigaddset(&m_signalSet, SIGINT);
  sigaddset(&m_signalSet, SIGTERM);
  sigaddset(&m_signalSet, SIGHUP);
  sigaddset(&m_signalSet, SIGABRT);

  int c;
  if ((c = pthread_sigmask(SIG_BLOCK,&m_signalSet,NULL)) != 0) {
    errno = c;
    castor::exception::Internal ex;
    ex.getMessage() << "Failed pthread_sigmask" << std::endl;
    throw ex;
  }

  /* Start the thread handling all the signals */
  Cthread_create_detached(castor::server::_signalThread_run, this);
}

//------------------------------------------------------------------------------
// start
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::start() throw(castor::exception::Exception)
{
  castor::server::BaseServer::start();
  
  /* Wait forever on a caught signal */
  signalHandler();
}


//------------------------------------------------------------------------------
// signalHandler
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::signalHandler()
{
  try {
    m_signalMutex->lock();

    while (!m_signalMutex->getValue()) {
      // Note: Without COND_TIMEOUT > 0 this will never work - because the condition variable
      // is changed in a signal handler - we cannot use condition signal in this signal handler
      m_signalMutex->wait();
    }

    m_signalMutex->release();
    int stagerSignaled = m_signalMutex->getValue();
	
    switch (stagerSignaled) {
      case RESTART_GRACEFULLY:
        clog() << SYSTEM << "GRACEFUL RESTART [SIGHUP]" << std::endl;
        /* wait on all threads to terminate */
        waitAllThreads();
        /* and restart them all */
        start();   // XXX this is a blind attempt to restart, as it was implemented in stager.c:1065
        break;
      
      case STOP_GRACEFULLY:
        clog() << SYSTEM << "GRACEFUL STOP [SIGTERM]" << std::endl;
        /* wait on all threads to terminate */
        waitAllThreads();
        dlf_shutdown(10);
        exit(EXIT_SUCCESS);
        break;
      
      case STOP_NOW:
        clog() << ERROR << "IMMEDIATE STOP [SIGINT]" << std::endl;
        /* just stop as fast as possible */
        dlf_shutdown(1);
        exit(EXIT_SUCCESS);
        break;
      
      default:
        /* Impossible !? */
        clog() << ERROR << "Caught a not handled signal - IMMEDIATE STOP" << std::endl;
        dlf_shutdown(1);
        exit(EXIT_FAILURE);
    }

  }
  catch (castor::exception::Exception e) {
    clog() << ERROR << "Exception during wait for signal loop: " << e.getMessage().str() << std::endl;
    dlf_shutdown(1);
    exit(EXIT_FAILURE);
  }
}



//------------------------------------------------------------------------------
// waitAllThreads
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::waitAllThreads() throw()
{
  std::vector<castor::server::SignalThreadPool*> idleTPools;

  /* old C implementation in stager.c:1169 */
  while(true) {
    idleTPools.clear();

    try {
      std::map<const char, castor::server::BaseThreadPool*>::iterator tp;
      for (tp = m_threadPools.begin(); tp != m_threadPools.end(); tp++) {

        if(typeid(tp->second) == typeid(castor::server::SignalThreadPool)) {
          // only SignalThreadPools have to be checked
          if(((castor::server::SignalThreadPool*)tp->second)->getActiveThreads() > 0) {
            tp->second->getThread()->stop();    // this is advisory, but SelectProcessThread implements it
            castor::exception::Internal ex;
            throw ex;
          }
          else  // fine, it's idle
            idleTPools.push_back((castor::server::SignalThreadPool*)tp->second);
        }
      }
      break;   // if all thread pools have 0 active threads, we're done
    }
    catch (castor::exception::Exception e) {
      // a thread lock could not be acquired or a thread pool still has some threads running
      // hence release previously acquired locks and try again
      for(unsigned int i = 0; i < idleTPools.size(); i++) {
        idleTPools[i]->getMutex()->release();
      }
      
      sleep(1);         // wait before trying again
    }
  }
}


//------------------------------------------------------------------------------
// _signalThread_run
//------------------------------------------------------------------------------
void* castor::server::_signalThread_run(void* arg)
{
  int sig_number = SIGHUP;
  castor::server::BaseDaemon* daemon = (castor::server::BaseDaemon*)arg;

  // keep looping until any caught signal except restart
  while (sig_number == SIGHUP) {
    if (sigwait(&daemon->m_signalSet, &sig_number) == 0) {
      /* Note: from now on this is unsafe but here we go, we cannot use mutex/condition/printing etc... */
      /* e.g. things unsafe in a signal handler */
      /* so from now on this function is calling nothing external */
      
      switch (sig_number) {
        case SIGHUP:
          /* Administrator want us to restart */
          // this wakes up the main thread (see line 104)
          daemon->m_signalMutex->setValueNoMutex(castor::server::RESTART_GRACEFULLY);
          break;
        
        case SIGABRT:
        case SIGTERM:
          /* Administrator want us to stop gracefully */
          daemon->m_signalMutex->setValueNoMutex(castor::server::STOP_GRACEFULLY);
          break;
        
        case SIGINT:
          /* Administrator want us to stop now */
          daemon->m_signalMutex->setValueNoMutex(castor::server::STOP_NOW);
          break;
        
        default:
          daemon->clog() << SYSTEM << "Caught signal: " << sig_number << std::endl;
          break;
      }
	  }
  }
  return 0;
}
