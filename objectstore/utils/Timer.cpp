/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/******************************************************************************
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

#include "Timer.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::utils::Timer::Timer() {
  reset();
}

//------------------------------------------------------------------------------
// usecs
//------------------------------------------------------------------------------
int64_t cta::utils::Timer::usecs(reset_t reset) {
  timeval now;
  gettimeofday(&now, 0);
  int64_t ret = ((now.tv_sec * 1000000) + now.tv_usec) - 
                 ((m_reference.tv_sec * 1000000) + m_reference.tv_usec);
  if (reset == resetCounter) {
    m_reference = now;
  }
  return ret;
}

//------------------------------------------------------------------------------
// secs
//------------------------------------------------------------------------------
double cta::utils::Timer::secs(reset_t reset) {
  return usecs(reset) * 0.000001;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void cta::utils::Timer::reset() {
  gettimeofday(&m_reference, 0);
}

