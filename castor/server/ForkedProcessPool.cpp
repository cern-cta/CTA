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
 * @(#)$RCSfile: ForkedProcessPool.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2007/07/25 15:36:11 $ $Author: itglp $
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
#include <signal.h>
#include <unistd.h>

// Include Files
#include "castor/server/ForkedProcessPool.hpp"
#include "castor/exception/Internal.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::ForkedProcessPool::ForkedProcessPool(const std::string poolName,
                                 castor::server::IThread* thread) :
  BaseThreadPool(poolName, thread), m_highFd(0), m_firstRun(true), m_stopped(true)
{
  FD_ZERO(&m_pipes);
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
      kill(m_childPid[i], SIGKILL);
    }
    delete m_childPipe[i];
    m_childPipe[i] = NULL;
  }
  return true;
}


//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::init() throw (castor::exception::Exception)
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
      ps->closeWrite();
      for (int fd = 0; fd < getdtablesize(); fd++) {
        if ((fd != ps->getFdRead()) && (fd != ps->getFdWrite())) {
          close(fd);
        }
      }
      dlf_child();
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
      FD_SET(fd, &m_pipes);
      if (fd > m_highFd) {
      	m_highFd = fd;
      } 
    }
  }
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::run() throw (castor::exception::Exception)
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
  
  // XXX we have observed that without this waiting time at the beginning
  // the first objects written into the pipes get screwed. We should understand
  // why... in the meantime we hack this way
  if (m_firstRun) {
    sleep(1);
    m_firstRun = false;
  }
  
  // look for an idle process
  // this is blocking in case all children are busy
  fd_set querypipes = m_pipes;
  int rc = 0;
  do {
    rc = select(m_highFd + 1, NULL, &querypipes, NULL, NULL);
  }
  while(rc == -1 && errno == EINTR); 
  if (rc < 0) {
    serrno = (errno == 0 ? SEINTERNAL : errno);
    castor::exception::Exception e(serrno);
    e.getMessage() << "ForkedProcessPool: failed on select()";
  }

  // get the idle child
  for(int child = 0; child < m_nbThreads; child++) {
    if(FD_ISSET(m_childPipe[child]->getFdWrite(), &querypipes)) {
      // dispatch the object to the child (this can throw exceptions)
      m_childPipe[child]->sendObject(obj);
      break;
    }
  }
}


//------------------------------------------------------------------------------
// childRun
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::childRun(castor::io::PipeSocket* ps)
{
  // initialize process
  try {
    m_thread->init();
  }
  catch(castor::exception::Exception any) {
    clog() << ERROR << "Exception caught while initializing the child process: "
           << any.getMessage().str() << std::endl;
  }
  // loop forever waiting for something to do
  while(true) {    // XXX todo: implement a way to stop the child
    try {
      castor::IObject* obj = ps->readObject();
      m_thread->run(obj);
      delete obj;
    }
    catch(castor::exception::Exception any) {
      clog() << ERROR << "Error while processing an object from the pipe: "
             << any.getMessage().str() << std::endl;
      sleep(1);
    }
  }
}
