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
 * @(#)$RCSfile: ForkedProcessPool.cpp,v $ $Revision: 1.14 $ $Release$ $Date: 2008/04/02 17:32:03 $ $Author: itglp $
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
(const std::string poolName, castor::server::IThread* thread) :
  BaseThreadPool(poolName, thread), 
  m_writeHighFd(0),
  m_stopped(true) {
  FD_ZERO(&m_writePipes);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::ForkedProcessPool::~ForkedProcessPool() throw()
{
  for(int i = 0; i < m_nbThreads; i++) {
    if(m_childPipe[i] != NULL) {
      delete m_childPipe[i];
    }
  }
  delete[] m_childPid;
}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
bool castor::server::ForkedProcessPool::shutdown() throw()
{
  m_stopped = true;
  // for the time being, we just kill the children without notifying them
  // that we have to shutdown. We need to kill otherwise they stay waiting
  // to read on the pipe being closed!
  for(int i = 0; i < m_nbThreads; i++) {
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
  // don't do anything if nbThreads = 0
  if(m_nbThreads == 0) {
    return;
  }
  
  // create pool of forked processes
  // we do it here so it is done before daemonization (BaseServer::init())
  m_childPid = new int[m_nbThreads];
  for (int i = 0; i < m_nbThreads; i++) {
    // create a pipe for the child
    castor::io::PipeSocket* ps = new castor::io::PipeSocket();
    m_childPid[i] = 0;
    // fork worker process
    dlf_prepare();
    int pid = fork();
    
    if(pid < 0) { 
      dlf_parent();
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
      dlf_parent();
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

  clog() << DEBUG << "Thread pool " << m_poolName << " started with "
         << m_nbThreads << " processes" << std::endl;
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

  // get the idle child
  for (int child = 0; child < m_nbThreads; child++) {
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
  void handleSignals(int signal) {
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
  dlf_child();
  dlf_create_threads(1);
  _childStop = false;

  // setup a signal handler 'C sytle'
  signal(SIGTERM, handleSignals);

  // initialize process
  try {
    m_thread->init();
  }
  catch(castor::exception::Exception any) {
    clog() << ERROR << "Exception caught while initializing the child process: "
           << any.getMessage().str() << std::endl;
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
      if (any.getMessage().str().find("closed by remote end", 0)) {
        clog() << DEBUG;
      } else {
        clog() << ERROR;
      }
      clog() << "Error while processing an object from the pipe: "
             << any.getMessage().str() << std::endl;

      // we shutdown here as communication failures across a pipe are rare! If
      // we got this far its most likely that the parent has disappeared e.g.
      // via a kill -9 therefore missing the call to the destructor asking the
      // children to stop.
      dlf_shutdown(1);
      exit(EXIT_FAILURE);
    }
  }

  dlf_shutdown(10);
  exit(EXIT_SUCCESS);
}
