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
 * @(#)$RCSfile: BaseDaemon.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2006/05/02 10:10:33 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include <signal.h>
#include "castor/server/BaseDaemon.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/MsgSvc.hpp"
#include "castor/Services.hpp"
#include "Cgetopt.h"
#include "Cinit.h"
#include "Cuuid.h"
#include "Cthread_api.h"
#include "castor/logstream.h"
#include <sstream>
#include <iostream>
#include <iomanip>


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
  //sigaddset(&m_signalSet, SIGHUP);
  //sigaddset(&m_signalSet, SIGABRT);

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

  /* Wait forever on a catched signal */
  try {
    m_signalMutex->lock();

    while (!m_signalMutex->getValue()) {
      // Note: Without SINGLE_SERVICE_COND_TIMEOUT > 0 this will never work - because the condition variable
      // is changed in a signal handler - we cannot use condition signal in this signal handler
      m_signalMutex->wait();
    }

    m_signalMutex->release();
  }
  catch (castor::exception::Exception e) {
    clog() << ERROR << "Exception during wait for signal loop: " << e.getMessage().str() << std::endl;
  }
}



//------------------------------------------------------------------------------
// waitAllThreads
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::waitAllThreads() throw()
{
  /*
  while (true) {
    int nbthread;

    if (Cthread_mutex_timedlock_ext(singlePoolNidCthreadStructure,SINGLE_SERVICE_MUTEX_TIMEOUT) != 0) {
      sleep(1);
      continue;
    }

    nbthread = singlePoolNid;
    Cthread_mutex_unlock_ext(singlePoolNidCthreadStructure);

    if (nbthread <= 0) {
      break;  // finished
    }
  }
  */
}


//------------------------------------------------------------------------------
// signalThread_run
//------------------------------------------------------------------------------
void* castor::server::_signalThread_run(void* arg)
{
  int sig_number;
  castor::server::BaseDaemon* daemon = (castor::server::BaseDaemon*)arg;

  while (true) {
    if (sigwait(&daemon->m_signalSet, &sig_number) == 0) {
      /* Note: from now on this is unsafe but here we go, we cannot use mutex/condition/printing etc... */
      /* e.g. things unsafe in a signal handler */
      /* so from now on this function is calling nothing external */
      daemon->m_signalMutex->setValueNoMutex(1);
      exit(0);  // EXIT_SUCCESS
    }
  }
  return 0;
}

