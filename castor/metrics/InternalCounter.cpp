/******************************************************************************
 *                      InternalCounter.cpp
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
 * @author castor-dev team
 *****************************************************************************/

// Include Files
#include <errno.h>
#include "castor/metrics/InternalCounter.hpp"


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::metrics::InternalCounter::InternalCounter(
  castor::server::BaseThreadPool& tp,
  std::string unit,
  castor::server::BaseThreadPool::MetricGetter metricGetter) :
  Counter(tp.getName(), unit), m_threadPool(tp), m_metricGetter(metricGetter)
{}

//------------------------------------------------------------------------------
// updateRates
//------------------------------------------------------------------------------
void castor::metrics::InternalCounter::updateRates(int si)
{
  // use the pointer-to-member operator to call the given getter method
  // from the given thread pool. This is the metric value used by this counter
  if(m_metricGetter == 0) {
    return;
  }
  // the si/60 correction factor makes sure we compute a direct average,
  // not a rate (cf. below).
  m_value = (m_threadPool.*m_metricGetter)() * si/60; 
  
  // reset last value: internal metrics are not ever growing and we
  // compute direct averages, not averages on deltas (cf. Counter).
  m_lastValue = 0;
  
  // update rates as per inherited logic
  castor::metrics::Counter::updateRates(si);
  
  // restore the correct value for later reporting
  m_value = m_value * 60/si;
}
