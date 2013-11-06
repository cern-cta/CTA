/******************************************************************************
 *                      MetricsCollector.hpp
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

#ifndef CASTOR_METRICS_METRICSCOLLECTOR_HPP
#define CASTOR_METRICS_METRICSCOLLECTOR_HPP 1

// Include Files
#include <string>
#include <map>
#include "castor/IObject.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/BaseDaemon.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/metrics/Histogram.hpp"

namespace castor {
  
  namespace metrics {

    typedef std::map<std::string, Histogram*>::iterator HistogramsIter; 
    
    /**
     * A singleton class holding all collected metrics for all "dimensions"
     * or histograms defined by the user application.
     */
    class MetricsCollector : public castor::server::SignalThreadPool {
  
    public:    
      /// This class is a singleton
      static MetricsCollector* getInstance(castor::server::BaseDaemon* daemon = 0);
  
      /// Default destructor
      virtual ~MetricsCollector() throw ();
    
      /// Method to be called by application's threads
      /// to count the object represented by obj (e.g. a Request)
      void updateHistograms(castor::IObject* obj)
        throw (castor::exception::Exception);
  
      /// Add a new histogram to the system
      void addHistogram(castor::metrics::Histogram* h) {
         m_histograms[h->getName()] = h;
      }
      
      /**
       * Prints an XML representation of the requested histogram/counter
       * @param histName, countName the name of the histogram/counter
       * @throw castor::exception::Exception(ENOENT) when data not found
       */     
      std::string printXml(std::string histName, std::string counterName)
        throw (castor::exception::Exception);
        
      /**
       * Dumps the current metrics' values to a proc-like XML-formatted file.
       * The file location is specified in castor.conf.
       */
      void dumpToFile();
      
      /**
       * Calls resetAllMetrics on the daemon.
       * @see BaseServer.resetAllMetrics
       */
      void resetAllMetrics() throw() {
        m_daemon.resetAllMetrics();
      }

      /// Inline method to access the histograms through an iterator
      HistogramsIter histBegin() {
        return m_histograms.begin();
      }
      
      /// Inline method to access the histograms through an iterator
      HistogramsIter histEnd() {
        return m_histograms.end();
      }
      
      /// Returns an histogram given its name
      castor::metrics::Histogram* getHistogram(std::string name) {
        if(m_histograms.find(name) != m_histograms.end()) {
          return m_histograms[name];
        }
        return 0;
      }

    private:
      /// This singleton's instance
      static MetricsCollector* s_instance;
      
      /// Default constructor
      MetricsCollector(castor::server::BaseDaemon& daemon);
      
      /// Hash map of all histograms, indexed by name
      std::map<std::string, Histogram*> m_histograms;
      
      /// Reference to the daemon for logging purposes and for resetAllMetrics
      castor::server::BaseDaemon& m_daemon;
      
      /// Dump file location
      std::string m_dumpFileLocation;
      
      /// Startup time of the daemon
      time_t m_startupTime;
      
      /// Role of this daemon, for multi-role daemons (e.g. RH)
      std::string m_role;
    };

  } // end of namespace metrics
    
} // end of namespace castor

#endif // CASTOR_METRICS_METRICSCOLLECTOR_HPP
