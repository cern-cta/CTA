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
 * @(#)$RCSfile: ForkedProcessPool.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/07/03 09:54:24 $ $Author: itglp $
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
  BaseThreadPool(poolName, thread),
  {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::ForkedProcessPool::~ForkedProcessPool() throw()
{
  delete m_poolMutex;
}


//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::ForkedProcessPool::init()
  throw (castor::exception::Exception)
{
  // Initialize shared variables (former singleService structure)
  m_nbActiveThreads = 0;  /* Number of threads currently running the service */
  m_nbTotalThreads = 0;   /* Total number of threads for this service */
  m_notified = 0;         /* By default no signal yet has been received */
  m_notTheFirstTime = false;

  // Create a mutex (could throw exception)
  m_poolMutex = new Mutex(-1, m_timeout);
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
  int n = 0;
  struct threadArgs *args = new threadArgs();
  args->handler = this;
  args->param = this;

  // create pool of forked processes
  for (int i = 0; i < m_nbThreads; i++) {
    ...
  }
  if (n <= 0) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to create pool " << m_poolName;
    throw ex;
  }
  else {
    m_nbThreads = n;
    clog() << DEBUG << "Thread pool " << m_poolName << " started with "
           << m_nbThreads << " processes" << std::endl;
  }
}

