/******************************************************************************
 *                      castor/tape/utils/Timer.cpp
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
 * This little class allows to easily time some piece of code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/tape/utils/Timer.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::utils::Timer::Timer() {
  reset();
}

//------------------------------------------------------------------------------
// usecs
//------------------------------------------------------------------------------
signed64 castor::tape::utils::Timer::usecs() {
  timeval now;
  gettimeofday(&now, 0);
  return ((now.tv_sec * 1000000) + now.tv_usec) - ((m_reference.tv_sec * 1000000) + m_reference.tv_usec);
}

//------------------------------------------------------------------------------
// secs
//------------------------------------------------------------------------------
double castor::tape::utils::Timer::secs() {
  return usecs() * 0.000001;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::tape::utils::Timer::reset() {
  gettimeofday(&m_reference, 0);
}

