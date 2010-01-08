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
#include "castor/metrics/MetricsCollector.hpp"
#include "castor/metrics/InternalCounter.hpp"

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
  m_stopped(false),
  m_nbActiveThreads(0),
  m_idleTime(0),
  m_activeTime(0),
  m_runsCount(0)
{
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
  delete m_thread;
}

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::server::BaseThreadPool::init() throw (castor::exception::Exception)
{
  // Enable internal monitoring if the metrics collector has already
  // been instantiated by the user application
  castor::metrics::MetricsCollector* mc = castor::metrics::MetricsCollector::getInstance();
  if(mc) {
    mc->getHistogram("AvgTaskTime")->addCounter(
      new castor::metrics::InternalCounter(*this, "ms",
        &castor::server::BaseThreadPool::getAvgTaskTime));
    mc->getHistogram("ActivityFactor")->addCounter(
      new castor::metrics::InternalCounter(*this, "%",
        &castor::server::BaseThreadPool::getActivityFactor));
    mc->getHistogram("LoadFactor")->addCounter(
      new castor::metrics::InternalCounter(*this, "%",
        &castor::server::BaseThreadPool::getLoadFactor));
  }
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::BaseThreadPool::run() throw (castor::exception::Exception)
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

//------------------------------------------------------------------------------
// resetMetrics
//------------------------------------------------------------------------------
void castor::server::BaseThreadPool::resetMetrics()
{
  /*
  Reset internal counters. Note we're not taking a lock on the mutex to not
  slow down the pool's mainstream activity: the consequence is only a less
  accurate accounting if a thread is modifying those values at the same time.
  Anyway, the metrics collector thread won't risk to run into a div-by-zero
  exception as it calls resetMetrics() *after* having collected all metrics.
  */
  m_activeTime = 0;
  m_idleTime = 0;
  m_runsCount = 0;
}

