/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/server/BaseThreadPool.hpp"
#include "castor/server/metrics/Counter.hpp"

#include <string>

namespace castor  {
namespace server  {
namespace metrics {

/**
 * A dedicated Counter for monitoring internal metrics of a thread pool
 */
class InternalCounter : public Counter {

public:
  
  /**
   * Default constructor
   * @param tp the ThreadPool to be monitored
   * @param unit the SI unit of this metric
   * @param metricGetter a method from the ThreadPool, which
   * shall return a u_signed64 value as the metric value
   */
  InternalCounter(castor::server::BaseThreadPool& tp,
    std::string unit,
    castor::server::BaseThreadPool::MetricGetter metricGetter);
  
  /// Default destructor
  virtual ~InternalCounter() {};
  
  /**
   * Fake implementation for this internal counter
   */
   virtual int match(castor::IObject*) {
     return 0;
   }
  
  /**
   * Recomputes mobile averages.
   * First retrieves the value using the getter method, then
   * calls the inherited method.
   * @param si the sampling interval
   */
  virtual void updateRates(int si);

protected:
  
  /// The thread pool from which this counter gets the metric
  castor::server::BaseThreadPool& m_threadPool;
  
  /// The method to be used to read the metric, defined as:
  /// typedef u_signed64 (BaseThreadPool::*MetricGetter)();
  castor::server::BaseThreadPool::MetricGetter m_metricGetter;

}; // class InternalCounter

} // namespace metrics
} // namespace server
} // namespace castor

