/******************************************************************************
 *                      MetricsCollector.cpp
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
 * @(#)BaseClient.cpp,v 1.37 $Release$ 2006/02/16 15:56:58 sponcec3
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

// Include files
#include <typeinfo>
#include <fstream>
#include <time.h>
#include "h/getconfent.h"

// Local includes
#include "castor/metrics/MetricsCollector.hpp"
#include "castor/metrics/UpdateThread.hpp"
#include "castor/metrics/InternalCounter.hpp"

// Initialization of the singleton
castor::metrics::MetricsCollector* castor::metrics::MetricsCollector::s_instance(0);

//------------------------------------------------------------------------------
// getInstance
//------------------------------------------------------------------------------
castor::metrics::MetricsCollector* castor::metrics::MetricsCollector::getInstance(castor::server::BaseDaemon* daemon)
{
  if(s_instance == 0 && daemon) {
    // No need to protect this with mutexes as the instantiation
    // is always performed at startup before creating all threads
    s_instance = new MetricsCollector(*daemon);
    daemon->addThreadPool(s_instance);
  }
  return s_instance;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::metrics::MetricsCollector::MetricsCollector(castor::server::BaseDaemon& daemon) :
  castor::server::SignalThreadPool("metrics", 
    new UpdateThread()),
  m_daemon(daemon)
{
  m_nbThreads = 1;
  std::stringstream ss;
  char* buf = getconfent("Metrics", "FileLocation", 0);
  if(buf == 0) {
    ss << "/var/spool/castor/";
  }
  else {
    ss << buf << "/";
    delete buf;
  }
  ss << m_daemon.getServerName();
  buf = getenv("CASTOR_ROLE");
  if(buf != 0) {
    ss << "." << buf;
    delete buf;
  }
  ss << ".procinfo";
  m_dumpFileLocation = ss.str().c_str();
  m_startupTime = time(NULL);
  
  // add empty histograms for internal metrics. Note they don't have a counter instantiator,
  // as the thread pools register theirselves to the histograms at init time
  addHistogram(new Histogram("AvgTaskTime", 0));
  addHistogram(new Histogram("AvgQueuingTime", 0));
  addHistogram(new Histogram("ActivityFactor", 0));
  addHistogram(new Histogram("LoadFactor", 0));
  addHistogram(new Histogram("BacklogFactor", 0));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::metrics::MetricsCollector::~MetricsCollector() throw()
{
  for(HistogramsIter h = histBegin(); h != histEnd(); h++)
    delete h->second;
}

//------------------------------------------------------------------------------
// updateHistograms
//------------------------------------------------------------------------------
void castor::metrics::MetricsCollector::updateHistograms(castor::IObject* obj)
  throw (castor::exception::Exception)
{
  for(HistogramsIter h = histBegin(); h != histEnd(); h++) {
    CountersIter c;
    for(c = h->second->cBegin(); c != h->second->cEnd(); c++) {
      // skip internal counters
      if(typeid(c->second) == typeid(castor::metrics::InternalCounter)) {
        break;
      }
      if(int i = c->second->match(obj)) {
        c->second->inc(i);
        // only one match is counted
        break;
      }
    }
    
    // if no match, notify the histogram of this new value. This eventually
    // triggers the creation of a new counter, which is protected by
    // a mutex at the histogram level. 
    if(c == h->second->cEnd()) {
      h->second->notifyNewValue(obj);
    }
  }
}

//------------------------------------------------------------------------------
// addHistogram
//------------------------------------------------------------------------------
void castor::metrics::MetricsCollector::addHistogram(castor::metrics::Histogram* h)
{
  m_histograms[h->getName()] = h;
}

//------------------------------------------------------------------------------
// printXml
//------------------------------------------------------------------------------
std::string castor::metrics::MetricsCollector::printXml(std::string histName, std::string counterName)
  throw (castor::exception::Exception)
{
  std::ostringstream ss;
  time_t t = time(NULL);
  char currTime[20];

  // get timing information
  time_t upTime = t - m_startupTime;
  strftime(currTime, 20, "%Y-%m-%dT%H:%M:%S", localtime(&t));
  int s, m, h, d;
  s = upTime % 60;
  upTime /= 60;
  m = upTime % 60;
  upTime /= 60;
  h = upTime % 24;
  upTime /= 24;
  d = upTime % 30;
  upTime /= 30;
  
  // print some header info
  ss << "<metrics>\n"
     << "<daemon name='" << m_daemon.getServerName() << "' PID='" << getpid() << "'/>\n"
     << "<time upTime='";
  if(upTime > 0) { ss << upTime << "m "; }
  if(d > 0)      { ss << d << "d "; }
  ss << (h < 10 ? "0" : "") << h << (m < 10 ? ":0" : ":") << m
     << (s < 10 ? ":0" : ":") << s
     << "' currTime='" << currTime
     << "' timestamp='" << t << "'/>\n";
     
  // print data for the required histograms/counters
  if(histName == "*") {
    for(HistogramsIter h = histBegin(); h != histEnd(); h++) {
      ss << h->second->printXml("*");
    }
  }
  else {
    if(m_histograms.find(histName) != m_histograms.end()) {
     ss << m_histograms[histName]->printXml(counterName);
    } else {
      castor::exception::Exception e(ENOENT);
      e.getMessage() << "The requested histogram was not found";
      throw e;
    }
  }
  ss << "</metrics>\n";
  
  return ss.str();
}

//------------------------------------------------------------------------------
// dumpToFile
//------------------------------------------------------------------------------
void castor::metrics::MetricsCollector::dumpToFile()
{
  std::ofstream f(m_dumpFileLocation.c_str());
  if(f.is_open()) {
    f << printXml("*", "*") << std::endl; 
    f.close();
  }
}
