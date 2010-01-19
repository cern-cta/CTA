/******************************************************************************
 *                      Counter.cpp
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
#include <stdlib.h>
#include <iomanip>
#include <errno.h>
#include <math.h>
#include "h/getconfent.h"

// Local includes
#include "Counter.hpp"

//------------------------------------------------------------------------------
// inc
//------------------------------------------------------------------------------
void castor::metrics::Counter::inc(int value) throw (castor::exception::Exception)
{
  m_mutex.lock();
  m_value += value;
  m_mutex.release();
}

//------------------------------------------------------------------------------
// updateRates
//------------------------------------------------------------------------------
void castor::metrics::Counter::updateRates(int si)
{
  // force calculation in floating point to handle negative numbers
  double v = m_value;
  
  // exponential moving averages, see e.g. http://en.wikipedia.org/wiki/Moving_average
  // the 60/si factor is to make the averages dimensionally equivalent to [Hz] but per minute 
  m_avg1m = 60.0/si * (exp(-si/60.0)*m_avg1m + (1-exp(-si/60.0))*(v - m_lastValue));
  m_avg10m = 60.0/si * (exp(-si/600.0)*m_avg10m + (1-exp(-si/600.0))*(v - m_lastValue));
  m_avg1h = 60.0/si * (exp(-si/3600.0)*m_avg1h + (1-exp(-si/3600.0))*(v - m_lastValue));
  /*
  // adopting another standard formula:
  m_avg1m = m_avg1m*(60 - si)/60 + (v - m_lastValue)*si/60;
  m_avg10m = m_avg10m*(600 - si)/600 + (v - m_lastValue)*si/600;
  m_avg1h = m_avg1h*(3600 - si)/3600 + (v - m_lastValue)*si/3600;
  */
  if(m_slWinConfName.length()) {
    // configurable sliding window average
    char* slWin = getconfent(m_slWinConfCategory.c_str(),
                             m_slWinConfName.c_str(), 0);
    if(slWin) {
      m_avgForSlWin = 60.0/si * (exp(-si*1.0/atol(slWin))*m_avgForSlWin + 
        (1-exp(-si*1.0/atol(slWin)))*(v - m_lastValue));
    }
  }
  
  // store current value for next calculation, so that averages
  // are dimensionally equivalent to rates
  m_lastValue = m_value;
}

//------------------------------------------------------------------------------
// getAvg
//------------------------------------------------------------------------------
double castor::metrics::Counter::getAvg(int avgType)
{
  switch(avgType) {
    case 0: return m_avg1m;
    case 1: return m_avg10m;
    case 2: return m_avg1h;
    case 3: return m_avgForSlWin;
    default: return 0;
  }
}

//------------------------------------------------------------------------------
// printXml
//------------------------------------------------------------------------------
std::string castor::metrics::Counter::printXml()
{
  std::ostringstream ss;
  ss << std::setprecision(3)
     << "<counter name='" << m_name
     << "' value='" << m_value
     << "' unit='" << m_unit
     << "' avg1m='" << (m_avg1m < 0.001 ? 0 : m_avg1m)
     << "' avg10m='" << (m_avg10m < 0.001 ? 0 : m_avg10m)
     << "' avg1h='" << (m_avg1h < 0.001 ? 0 : m_avg1h);
  if(m_avgForSlWin > 0) {
    ss << "' slWin='" << m_avgForSlWin;
  }
  ss << "'/>\n";
  return ss.str();
}
