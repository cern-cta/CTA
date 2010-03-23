/******************************************************************************
 *                      ForkedProcessPool.cpp
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
 * @(#)$RCSfile: ForkedProcessPool.cpp,v $ $Revision: 1.18 $ $Release$ $Date: 2009/08/18 09:42:54 $ $Author: waldron $
 *
 * A pool of forked processes
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// System headers
#include "osdep.h"
#include <errno.h>                      /* For EINVAL etc... */
#include "serrno.h"                     /* CASTOR error numbers */
#include <sys/types.h>
#include <iostream>
#include <string>
#include <sys/select.h>
#include <sys/time.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>

// Include Files
#include "castor/server/ForkedProcessPool.hpp"
#include "castor/exception/Internal.hpp"

// We use a global variable here to indicate whether the child should stop or
// countinue waiting for requests from the dispatcher.
static bool _childStop = false;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::ForkedProcessPool::ForkedProcessPool
(const std::string poolName, castor::server::IThread* thread)
  throw(castor::exception::Exception) :
  BaseThreadPool(poolName, thread),
  m_writeHighFd(0) {
  FD_ZERO(&m_writePipes);
  m_stopped = true;   // dispatch is closed until real start
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::ForkedProcessPool::~ForkedProcessPool() throw()
{
  for(unsigned i = 0; i < m_nbThreads; i++) {
    if(m_childPipe[i] != NULL) {
      delete m_childPipe[i];
    }
  }
  delete[] m_childPid;
}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
bool castor::server::ForkedProcessPool::shutdown(bool) throw()
{
  m_stopped = true;
  // for the time being, we just kill the children without notifying them
  // that we have to shutdown. We need to kill otherwise they stay waiting
  // to read on the pipe being closed!
  for(unsigned i = 0; i < m_nbThreads; i++) {
    if (m_childPid[i] > 0) {
      kill(m_childPid[i], SIGTERM);
    }
    // We should really delete the pipe objects here but because of a potential
    // race condition between the dispatch phase and the shutdown we don't
    //delete m_childPipe[i];
    m_childPipe[i]->closeWrite();
  }
  return true;
}

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::init()
  throw (castor::exception::Exception)
{
  castor::server::BaseThreadPool::init();

  // don't do anything if nbThreads = 0
  if(m_nbThreads == 0) {
    return;
  }

  // create pool of forked processes
  // we do it here so it is done before daemonization (BaseServer::init())
  m_childPid = new int[m_nbThreads];
  for (unsigned i = 0; i < m_nbThreads; i++) {
    // create a pipe for the child
    castor::io::PipeSocket* ps = new castor::io::PipeSocket();
    m_childPid[i] = 0;
    // fork worker process
    int pid = fork();

    if(pid < 0) {
      castor::exception::Internal ex;
      ex.getMessage() << "Failed to fork in pool " << m_poolName;
      throw ex;
    }
    if(pid == 0) {
      // close all child file descriptors which are not used
      // for inter-process communication
      freopen("/dev/null", "r", stdin);
      freopen("/dev/null", "w", stdout);
      freopen("/dev/null", "w", stderr);

      ps->closeWrite();
      for (int fd = ps->getFdWrite(); fd < getdtablesize(); fd++) {
        if ((fd != ps->getFdRead()) && (fd != ps->getFdWrite())) {
          close(fd);
        }
      }
      childRun(ps);      // this is a blocking call
      exit(EXIT_SUCCESS);
    }
    else {
      // parent: save pipe and pid
      m_childPid[i] = pid;
      m_childPipe.push_back(ps);
      ps->closeRead();
      int fd = ps->getFdWrite();
      FD_SET(fd, &m_writePipes);
      if (fd > m_writeHighFd) {
        m_writeHighFd = fd;
      }
    }
  }
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::run()
  throw (castor::exception::Exception)
{
  // we already forked all processes, so not much to do here
  // open dispatch
  m_stopped = false;

  // "Thread pool started"
  castor::dlf::Param params[] =
    {castor::dlf::Param("ThreadPool", m_poolName),
     castor::dlf::Param("Type", "ForkedProcessPool"),
     castor::dlf::Param("NbChildren", m_nbThreads)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                          DLF_BASE_FRAMEWORK + 3, 3, params);
}


//------------------------------------------------------------------------------
// dispatch
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::dispatch(castor::IObject& obj)
  throw (castor::exception::Exception)
{
  if (m_stopped) {
    return;
  }
  // look for an idle process this is blocking in case all children are busy
  fd_set writepipes = m_writePipes;
  timeval timeout;
  int rc = 0;
  do {
    writepipes = m_writePipes;
    timeout.tv_sec  = 1;
    timeout.tv_usec = 0;
    rc = select(m_writeHighFd + 1, NULL, &writepipes, NULL, &timeout);
    if ((rc == 0) && (m_stopped)) {
      castor::exception::Internal e;
      e.getMessage() << "ForkedProcessPool: shutdown detected";
      throw e;
    }
  }
  while (rc == 0 || (rc == -1 && errno == EINTR));
  if (rc < 0) {
    serrno = (errno == 0 ? SEINTERNAL : errno);
    castor::exception::Exception e(serrno);
    e.getMessage() << "ForkedProcessPool: failed on select()";
    throw e;
  }

  // get an idle child
  for (unsigned child = 0; child < m_nbThreads; child++) {
    if (FD_ISSET(m_childPipe[child]->getFdWrite(), &writepipes)) {
      // dispatch the object to the child (this can throw exceptions)
      m_childPipe[child]->sendObject(obj);
      break;
    }
  }
}

//------------------------------------------------------------------------------
// handleSignals
//------------------------------------------------------------------------------
extern "C" {
  void handleSignals(int) {
    _childStop = true;
  }
}

//------------------------------------------------------------------------------
// childRun
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::childRun(castor::io::PipeSocket* ps)
{
  // ignore SIGTERM while recreating dlf thread
  signal(SIGTERM, SIG_IGN);
  _childStop = false;

  // setup a signal handler 'C sytle'
  signal(SIGTERM, handleSignals);

  // initialize process
  try {
    m_thread->init();
  }
  catch(castor::exception::Exception any) {
    // "Exception caught while initializing the child process"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", sstrerror(any.code())),
       castor::dlf::Param("Message", any.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                            DLF_BASE_FRAMEWORK + 8, 2, params);
    _childStop = true;
  }
  // loop forever waiting for something to do
  while(!_childStop) {
    try {
      castor::IObject* obj = ps->readObject();
      m_thread->run(obj);
      delete obj;
    }
    catch(castor::exception::Exception any) {
      int priority = DLF_LVL_ERROR;
      if (any.getMessage().str().find("closed by remote end", 0)) {
        priority = DLF_LVL_DEBUG;
      }
      // "Error while processing an object from the pipe"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Error", sstrerror(any.code())),
         castor::dlf::Param("Message", any.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, priority,
                              DLF_BASE_FRAMEWORK + 9, 2, params);

      // we shutdown here as communication failures across a pipe are rare! If
      // we got this far its most likely that the parent has disappeared e.g.
      // via a kill -9 therefore missing the call to the destructor asking the
      // children to stop.
      try {
        m_thread->stop();
      } catch (castor::exception::Exception ignore) {}
      dlf_shutdown();
      exit(EXIT_FAILURE);
    }
  }

  try {
    m_thread->stop();
  } catch (castor::exception::Exception any) {
    // "Thread run error"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error", sstrerror(any.code())),
       castor::dlf::Param("Message", any.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                            DLF_BASE_FRAMEWORK + 5, 2, params);
  }
  dlf_shutdown();
  exit(EXIT_SUCCESS);
}

