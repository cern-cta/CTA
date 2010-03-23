/******************************************************************************
 *                      InternalCounter.hpp
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
 * @(#)$RCSfile: Counter.hpp,v $ $Revision: 1.6 $ $Release$ $Date: 2008/06/03 15:54:42 $ $Author: waldron $
 *
 *
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef CASTOR_METRICS_INTERNALCOUNTER_HPP
#define CASTOR_METRICS_INTERNALCOUNTER_HPP 1

// Include Files
#include <string>
#include "castor/server/BaseThreadPool.hpp"
#include "castor/metrics/Counter.hpp"

namespace castor {
  
  namespace metrics {

    /**
     * A dedicated Counter for monitoring internal metrics of a thread pool
     */
    class InternalCounter : public castor::metrics::Counter {
  
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
    
    };

  } // end of namespace metrics
  
} // end of namespace castor

#endif // CASTOR_METRICS_INTERNALCOUNTER_HPP
