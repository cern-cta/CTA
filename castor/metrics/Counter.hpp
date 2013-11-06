/******************************************************************************
 *                      Counter.hpp
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
 * @author castor-dev team
 *****************************************************************************/

#ifndef CASTOR_METRICS_COUNTER_HPP
#define CASTOR_METRICS_COUNTER_HPP 1

// Include Files
#include <string>
#include "castor/IObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/Mutex.hpp"

namespace castor {
  
  namespace metrics {

    class Counter;
    
    /**
     * Pointer to a generic factory method creating counters for new values.
     * @see Histogram::notifyNewValue().
     */
    typedef Counter* (*CounterInstantiator)(castor::IObject* obj);

    /**
     * A class storing a thread-safe counter, with the ability to compute
     * mobile averages and report data in XML format.
     */
    class Counter {
  
    public:
      
      /**
       * Default constructor
       * @param name Name of this counter
       * @param unit The SI unit for this counter
       * @param slWinConfCat/Value configuration parameters in castor.conf
       * to define a configurable sliding window average, not enabled by default
       */
      Counter(std::string name, std::string unit = "count",
        std::string slWinConfCat = "", std::string slWinConfName = "") :
        m_name(name), m_unit(unit), m_value(0), m_lastValue(0), m_mutex(0),
        m_slWinConfCategory(slWinConfCat), m_slWinConfName(slWinConfName),
        m_avg1m(0), m_avg10m(0), m_avg1h(0), m_avgForSlWin(0) {}
      
      /// Default destructor
      virtual ~Counter() {};
      
      /**
       * Abstract method which shall return whether for
       * the given object this counter needs to be incremented
       * @param obj the object being handled
       * @return the count by which this counter has to be
       * incremented, 0 for no match
       */
      virtual int match(castor::IObject* obj) = 0;
  
      /** 
       * Increments this counter. Internally uses a mutex.
       * @param value the amount by which the counter will be incremented
       * @throw exception on mutex errors
       */
      void inc(int value = 1) throw (castor::exception::Exception);
  
      /**
       * Recomputes mobile averages.
       * This method is called by the updater thread.
       * @param si the sampling interval
       */
      virtual void updateRates(int si);
  
      /// Get this counter's name
      std::string getName() {
        return m_name;
      }
      
      /// Get current counter value
      u_signed64 getValue() {
        return m_value;
      }
  
      /// Get current mobile average value
      virtual double getAvg(int avgType);
      
      /// Returns an XML representation of this counter
      virtual std::string printXml();
      
    protected:
      
      /// Name of this counter
      std::string m_name;
      
      /// SI unit for this counter
      std::string m_unit;
      
      /// Current counter value
      u_signed64 m_value;
       
      /// Counter value at last updateRates call
      u_signed64 m_lastValue;
      
      /// Mutex to protect the counter
      castor::server::Mutex m_mutex;
      
      /// Configuration parameters for the sliding window average
      std::string m_slWinConfCategory, m_slWinConfName;
      
      /// Mobile averages
      double m_avg1m, m_avg10m, m_avg1h, m_avgForSlWin;
    };

  } // end of namespace metrics
  
} // end of namespace castor

#endif // CASTOR_METRICS_COUNTER_HPP
