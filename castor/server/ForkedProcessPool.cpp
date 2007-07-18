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
 * @(#)$RCSfile: ForkedProcessPool.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2007/07/18 10:01:57 $ $Author: waldron $
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
  BaseThreadPool(poolName, thread)
{
  FD_ZERO(&pipes);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::ForkedProcessPool::~ForkedProcessPool() throw()
{
  for(int i = 0; i < m_nbThreads; i++) {
    childPipe[i]->close();
    if (childPid[i] > 0) {
      kill(childPid[i], SIGKILL);
    }
    delete childPipe[i];
  }
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::run()
{
  // don't do anything if nbThreads = 0
  if(m_nbThreads == 0) {
    return;
  }
  
  // create pool of forked processes
  childPid = new int[m_nbThreads];
  for (int i = 0; i < m_nbThreads; i++) {
    // create a pipe for the child
    castor::io::PipeSocket* ps = new castor::io::PipeSocket();
    childPid[i] = 0;
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
      // close all child file descriptors which are not used for inter-process
      // communication
      for (int j = 0; j < getdtablesize(); j++) {
	if ((j != ps->openRead()) && (j != ps->openWrite())) {
	  close(j);
	}
      }
      // child
      dlf_child();
      childRun(ps);      // this is a blocking call
      exit(EXIT_SUCCESS);
    }
    else {
      // parent: save pipe
      dlf_parent();
      int fd = ps->openWrite();
      FD_SET(fd, &pipes);
      childPipe.push_back(ps);
      childPid[i] = pid;
    }
  }
  clog() << DEBUG << "Thread pool " << m_poolName << " started with "
         << m_nbThreads << " processes" << std::endl;
}


//------------------------------------------------------------------------------
// dispatch
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::dispatch(castor::IObject& obj)
  throw (castor::exception::Exception)
{
  // look for an idle process
  // this is blocking in case all children are busy
  fd_set querypipes = pipes;
  int rc = 0;
  do {
    rc = select(m_nbThreads, NULL, &querypipes, NULL, NULL);
  }
  while(rc == -1 && errno == EINTR); 
  if (rc < 0) {
    serrno = (errno == 0 ? SEINTERNAL : errno);
    castor::exception::Exception e(serrno);
    e.getMessage() << "ForkedProcessPool: failed on select()";
  }
  
  // get the idle child
  for(int child = 0; child < m_nbThreads; child++) {
    if(FD_ISSET(child, &querypipes)) {
      // dispatch the object to the child (this can throw exceptions)
      childPipe[child]->sendObject(obj);
      break;
    }
  }
}


//------------------------------------------------------------------------------
// childRun
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::childRun(castor::io::PipeSocket* ps)
{
  // open pipe for reading
  ps->openRead();
  
  // initialize process
  m_thread->init();
  
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
    }
  }
}
