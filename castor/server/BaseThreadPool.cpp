/******************************************************************************
 *                      BaseThreadPool.cpp
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
 * @(#)$RCSfile: BaseThreadPool.cpp,v $ $Revision: 1.21 $ $Release$ $Date: 2009/07/13 06:22:07 $ $Author: waldron $
 *
 * Abstract CASTOR thread pool
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include Files
#include <signal.h>
#include <sstream>
#include <iomanip>
#include <errno.h>

#include "Cinit.h"
#include "Cuuid.h"
#include "serrno.h"
#include "castor/server/BaseThreadPool.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::BaseThreadPool::BaseThreadPool(const std::string poolName,
                                               castor::server::IThread* thread,
                                               unsigned int nbThreads)
throw (castor::exception::Exception) :
  BaseObject(),
  m_nbThreads(nbThreads),
  m_poolName(poolName),
  m_thread(thread),
  m_stopped(false) {
  if (m_thread == 0) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "A valid thread must be specified";
    throw e;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::server::BaseThreadPool::~BaseThreadPool() throw()
{
  shutdown();
  if(m_thread != 0) {
    delete m_thread;
    m_thread = 0;
  }
}

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::BaseThreadPool::init() throw (castor::exception::Exception)
{
  castor::exception::Internal notImpl;
  notImpl.getMessage() <<
    "BaseThreadPool is (pseudo)abstract, you must use its derived classes";
  throw notImpl;
}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
bool castor::server::BaseThreadPool::shutdown(bool wait) throw()
{
  // Children of this class implement a proper shutdown mechanism,
  // here we just return.
  return true;
}

//------------------------------------------------------------------------------
// setNbThreads
//------------------------------------------------------------------------------
void castor::server::BaseThreadPool::setNbThreads(unsigned int value)
{
  m_nbThreads = value;
}
