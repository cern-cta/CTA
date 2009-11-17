/******************************************************************************
 *                      Histogram.cpp
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

// Include Files
#include <errno.h>

// Local includes
#include "Histogram.hpp"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::metrics::Histogram::Histogram(std::string name, CounterInstantiator instantiator) :
  m_name(name), m_instantiator(instantiator), m_mutex(0)
{}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::metrics::Histogram::~Histogram()
{
  for(CountersIter c = cBegin(); c != cEnd(); c++) {
    delete c->second;
  }
}

//------------------------------------------------------------------------------
// addCounter
//------------------------------------------------------------------------------
void castor::metrics::Histogram::addCounter(castor::metrics::Counter* c)
  throw (castor::exception::Exception)
{
  // Take a lock so that the same counter can't be added twice
  try {
    m_mutex.lock();
    if(m_counters.find(c->getName()) != cEnd()) {
      // another counter already exists with this name, merge the two;
      // note that this may happen due to a race condition between multiple
      // threads all trying to add the same counter
      m_counters[c->getName()]->inc(c->getValue());
      // delete the argument as otherwise we leak memory; warning:
      // due to this the caller can't safely use the argument after this call!
      delete c;
    }
    else
      m_counters[c->getName()] = c;
    m_mutex.release();
  }
  catch (castor::exception::Exception& e) {
    // An exception here is most likely a problem with mutexes. Try to release
    // our mutex before rethrowing
    try {
      m_mutex.release();
    } catch (castor::exception::Exception ignore) {}
    throw e;
  }
}

//------------------------------------------------------------------------------
// notifyNewValue
//------------------------------------------------------------------------------
void castor::metrics::Histogram::notifyNewValue(castor::IObject* obj)
  throw (castor::exception::Exception)
{
  if(m_instantiator != 0) {
    addCounter((*m_instantiator)(obj));
  }
}

//------------------------------------------------------------------------------
// printXml
//------------------------------------------------------------------------------
std::string castor::metrics::Histogram::printXml(std::string counterName)
  throw (castor::exception::Exception)
{
  std::ostringstream ss;
  ss << "<histogram name='" << m_name << "'>\n";
  if(counterName == "*") {
    for(CountersIter c = cBegin(); c != cEnd(); c++) {
      ss << "  " << c->second->printXml();
    }
  }
  else if(m_counters.find(counterName) != m_counters.end()) {
    ss << "  " << m_counters[counterName]->printXml();
  }
  else {
    castor::exception::Exception e(ENOENT);
    e.getMessage() << "The requested counter was not found";
    throw e;
  }
  ss << "</histogram>\n";
  return ss.str();
}
