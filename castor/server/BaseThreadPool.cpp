/******************************************************************************
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
 *
 * Abstract CASTOR thread pool
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/metrics/MetricsCollector.hpp"
#include "castor/server/metrics/InternalCounter.hpp"
#include "castor/Services.hpp"
#include "h/Cinit.h"
#include "h/Cuuid.h"
#include "h/serrno.h"

#include <signal.h>
#include <sstream>
#include <iomanip>
#include <errno.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::server::BaseThreadPool::BaseThreadPool(const std::string poolName,
                                               castor::server::IThread* thread,
                                               unsigned int nbThreads)
 :
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
void castor::server::BaseThreadPool::init() 
{
  // Enable internal monitoring if the metrics collector has already
  // been instantiated by the user application
  metrics::MetricsCollector* mc = metrics::MetricsCollector::getInstance();
  if(mc) {
    mc->getHistogram("AvgTaskTime")->addCounter(
      new metrics::InternalCounter(*this, "ms",
        &castor::server::BaseThreadPool::getAvgTaskTime));
    mc->getHistogram("ActivityFactor")->addCounter(
      new metrics::InternalCounter(*this, "%",
        &castor::server::BaseThreadPool::getActivityFactor));
    mc->getHistogram("LoadFactor")->addCounter(
      new metrics::InternalCounter(*this, "%",
        &castor::server::BaseThreadPool::getLoadFactor));
  }
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::server::BaseThreadPool::run() 
{
  castor::exception::Exception notImpl;
  notImpl.getMessage() <<
    "BaseThreadPool is (pseudo)abstract, you must use its derived classes";
  throw notImpl;
}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
bool castor::server::BaseThreadPool::shutdown(bool) throw()
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

