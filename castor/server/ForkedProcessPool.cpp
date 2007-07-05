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
 * @(#)$RCSfile: ForkedProcessPool.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/07/05 18:10:29 $ $Author: itglp $
 *
 * A pool of forked processes
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include "castor/server/ForkedProcessPool.hpp"
#include "castor/exception/Internal.hpp"

extern "C" {
  char* getconfent (const char *, const char *, int);
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::ForkedProcessPool::ForkedProcessPool(const std::string poolName,
                                 castor::server::IThread* thread) :
  BaseThreadPool(poolName, thread)
  {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::ForkedProcessPool::~ForkedProcessPool() throw()
{
  for(int i = 0; i < m_nbThreads; i++) {
    delete childPipe[i];
  }
}


//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::init()
  throw (castor::exception::Exception)
{
  // nothing to do here ...
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
  for (int i = 0; i < m_nbThreads; i++) {
    int pipeFromChild = 0; // XXX
    int pipeToChild = 0; // XXX
    int pid = fork();
    if(pid < 0) { 
      castor::exception::Internal ex;
      ex.getMessage() << "Failed to fork pool " << m_poolName;
      throw ex;
    }
    if(pid == 0) {     
      // child
      dup2(pipeToChild, 0);    // stdin
      dup2(pipeFromChild, 1);  // stdout
      childRun();      // this is a blocking call
    }
    else {
      // parent
      childPipe.push_back(new castor::io::PipeSocket(pipeFromChild, pipeToChild));
      // do we need to keep child's pid?
    }
  }
  clog() << DEBUG << "Thread pool " << m_poolName << " started with "
         << m_nbThreads << " processes" << std::endl;
}


//------------------------------------------------------------------------------
// dispatch
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::dispatch(castor::IObject& obj)
{
  try {
    // look for an idle process
    // this is blocking in case all children are busy
    int child = 0;
    while(child == 0) {
      //select(m_nbThreads, ...);
      //child = ...
    }
    
    // dispatch the object to the child
    childPipe[child]->sendObject(obj);
  }
  catch(castor::exception::Exception any) {
    clog() << ERROR << "Error while dispatching object to child: "
           << any.getMessage().str() << std::endl;
  }
}


//------------------------------------------------------------------------------
// childRun
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::childRun()
{
  // create a socket from stdin,stdout
  castor::io::PipeSocket* pipe = new castor::io::PipeSocket(0, 1);
  
  // loop forever waiting for something to do
  for(;;) {
    try {
      castor::IObject* obj = pipe->readObject();
      m_thread->run(obj);
      delete obj;
    }
    catch(castor::exception::Exception any) {
      clog() << ERROR << "Error while processing an object from the pipe: "
             << any.getMessage().str() << std::endl;
    }
  }
}
