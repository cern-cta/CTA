/******************************************************************************
 *                      Histogram.hpp
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

#ifndef CASTOR_METRICS_HISTOGRAM_HPP
#define CASTOR_METRICS_HISTOGRAM_HPP 1

// Include Files
#include <string>
#include <map>
#include "castor/IObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/Mutex.hpp"
#include "castor/metrics/Counter.hpp"

namespace castor {
  
  namespace metrics {

    typedef std::map<std::string, Counter*>::iterator CountersIter; 

    /**
     * A class representing a collection of counters, which break down
     * a given "dimension" or metric into different values.
     */
    class Histogram {
      
    public:
      /**
       * Default constructor
       * @param name Name of this histogram
       * @param instantiator A function pointer to add counters
       * to this histogram when no match was found. The type is defined as:
       * typedef Counter* (*CounterInstantiator)(castor::IObject* obj);
       */
      Histogram(std::string name, CounterInstantiator instantiator);
      
      /// Default destructor
      virtual ~Histogram();
      
      /**
       * This method is called whenever no counters
       * matched the passed value for this histogram.
       * The default implementation checks that there's indeed no
       * matching counter in a thread-safe way and eventually
       * adds a new counter for this value by calling
       * the internal instantiator.
       * @param obj The value that should be counted 
       * @throw exception in case of mutex errors
       */
      virtual void notifyNewValue(castor::IObject* obj)
        throw (castor::exception::Exception);
      
      /**
       * Returns an XML representation of this histogram
       * @param counterName The counter to be included, '*' for all
       * @throw castor::exception::Exception(ENOENT) when counter not found
       */      
      std::string printXml(std::string counterName)
        throw (castor::exception::Exception);
      
      /// Gets this histogram's name
      std::string getName() {
        return m_name;
      }
      
      /// Inline method to access the counters through an iterator
      CountersIter cBegin() {
        return m_counters.begin();
      }
      
      /// Inline method to access the counters through an iterator
      CountersIter cEnd() {
        return m_counters.end();
      }

      /// Returns a counter given its name
      castor::metrics::Counter* getCounter(std::string name) {
        if(m_counters.find(name) != m_counters.end()) {
          return m_counters[name];
        }
        return 0;
      }
      
      /**
       * Add a new counter to this histogram. This method
       * is not protected by the mutex and it is deprecated
       * to use it from any user application code: users should
       * rely on the notifyNewValue method.
       * @param c The Counter to be added to this histogram
       */
      void addCounter(castor::metrics::Counter* c) {
        if(c) {
          m_counters[c->getName()] = c;
        }
      }        
  
    protected:

      /// Name of this histogram
      std::string m_name;
      
      /// Hash map of all counters for this histogram, indexed by name
      std::map<std::string, Counter*> m_counters;
      
      /// Instantiator method to add new counters on no match events
      CounterInstantiator m_instantiator;
      
      /// Mutex to protect the addition of new counters     
      castor::server::Mutex m_mutex;
    };

  } // end of namespace metrics
    
} // end of namespace castor

#endif // CASTOR_METRICS_HISTOGRAM_HPP
