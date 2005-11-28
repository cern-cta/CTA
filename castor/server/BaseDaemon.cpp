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
 * @(#)$RCSfile: BaseDaemon.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/11/28 09:42:51 $ $Author: itglp $
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
  BaseServer(serverName) {}


//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::init() throw(castor::exception::Exception)
{
  // Server initialization (in foreground or not)
  BaseServer::init();

  /* Initialize CASTOR Thread interface */
  Cthread_init();

  /* Initialize mutex variable in case of a signal */
  m_signaled = false;

  /* Initialize errno, serrno */
  errno = serrno = 0;

  sigemptyset(&m_signalSet);
  sigaddset(&m_signalSet,SIGINT);
  sigaddset(&m_signalSet,SIGTERM);
  //sigaddset(&m_signalSet,SIGHUP);
  //sigaddset(&m_signalSet,SIGABRT);

  int c;
  if ((c = pthread_sigmask(SIG_BLOCK,&m_signalSet,NULL)) != 0) {
    errno = c;
    castor::exception::Internal ex;
    ex.getMessage() << "Failed pthread_sigmask" << std::endl;
    throw ex;
  }

  // SINGLE_CREATE_LOCK(singleSignaled,singleSignalCthreadStructure);
  // -> create a Mutex for this var.


  /* Start the thread handling all the signals */
  /* ========================================= */
  // XXX to be done -> the entrypoint is at the end of this file
}

//------------------------------------------------------------------------------
// start
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::start() throw(castor::exception::Exception)
{
  BaseServer::start();

  /* Wait on a catched signal */
  /* ======================== */

  /*  XXX this is not yet supported XXX

  if (Cthread_mutex_timedlock_ext(singleSignalCthreadStructure,SINGLE_SERVICE_MUTEX_TIMEOUT) != 0) {
    castor::exception::Internal ex;
    throw ex;
  }

  while (singleSignaled < 0) {
    // Note: Without SINGLE_SERVICE_COND_TIMEOUT > 0 this will never work - because the condition variable
    // is changed in a signal handler - we cannot use condition signal in this signal handler
    Cthread_cond_timedwait_ext(singleSignalCthreadStructure,SINGLE_SERVICE_COND_TIMEOUT);
  }
  if (Cthread_mutex_unlock_ext(singleSignalCthreadStructure) != 0) {
    rc = EXIT_FAILURE;
    goto _singleMainReturn;
  }
  */
}


//------------------------------------------------------------------------------
// waitAllThreads
//------------------------------------------------------------------------------
void castor::server::BaseDaemon::waitAllThreads() throw()
{
  // XXX when the handling of signals is properly done
  // we need to do the following for all SignalThreadPools we own:
  /*
  while (1) {
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



/*
static void* _signalThread_run(void* arg)
{
  int sig_number;
  sigset_t signalSet;
  signalSet = arg->handler->getSignalSet();

  while (1) {
    if (sigwait(&signalSet, &sig_number) == 0) {

      /* Note: from now on this is unsafe but here we go, we cannot use mutex/condition/printing etc... */
      /* e.g. things unsafe in a signal handler */
      /* So from now on this function is calling nothing external *

      //singleSignaled = 1;
      exit(0);  // EXIT_SUCCESS
    }
  }
  return(NULL);
}
*/
