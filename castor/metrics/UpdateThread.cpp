/******************************************************************************
 *                      UpdateThread.cpp
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
 * A thread used by the MetricsCollector container/thread pool 
 * to update all collected metric values.
 *
 * @author castor-dev team
 *****************************************************************************/

// Local includes
#include "h/getconfent.h"
#include "castor/metrics/MetricsCollector.hpp"
#include "castor/metrics/UpdateThread.hpp"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::metrics::UpdateThread::UpdateThread() : m_t0(0)
{
  m_sampling = DEFAULT_SAMPLING_INTERVAL;
  char* sampling = getconfent("Metrics", "SamplingInterval", 0);
  if(sampling) {
    m_sampling = atoi(sampling);
    if(m_sampling == 0) {
      m_sampling = DEFAULT_SAMPLING_INTERVAL;
    }
  }
  if(m_sampling < 10) {
    m_sampling = 10;   // this is the minimum allowed
  }
}

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void castor::metrics::UpdateThread::init()
{
  // make sure the first run dumps the data
  m_t0 = time(0) - m_sampling - 1;
}

//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::metrics::UpdateThread::run(void* param)
{
  u_signed64 t = time(0);
  if((t - m_t0) / m_sampling == 0) {
    return;
  }
  m_t0 = t;
  
  // Collect user and internal metrics
  MetricsCollector* mc = (MetricsCollector*)param;
  for(HistogramsIter h = mc->histBegin(); h != mc->histEnd(); h++) {
    for(CountersIter c = h->second->cBegin(); c != h->second->cEnd(); c++) {
      c->second->updateRates(m_sampling);
    }
  }
  
  // Dump current values to a proc-like file
  mc->dumpToFile();
  
  // Reset all internal metrics' values
  mc->resetAllMetrics();
}
