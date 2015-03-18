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

#pragma once

#include "osdep.h"

#include <sys/time.h>

namespace castor {
namespace utils {

/**
 * A small timing class.
 * It basically remembers a reference time (by default the time of
 * its construction) and gives the elapsed time since then.
 * The reset method allows to reset the reference time to the current time
 */
class Timer {

public:
        
  enum reset_t {
    keepRunning,
    resetCounter
  };

  /**
   * Constructor.
   */
  Timer();

  /**
   * Destructor.
   */
  virtual ~Timer() {}

  /**
   * Gives elapsed time in microseconds with respect to the reference time
   * optionally resets the counter.
   */
  signed64 usecs(reset_t reset = keepRunning);

  /**
   * Gives elapsed time in seconds (with microsecond precision)
   * with respect to the reference time. Optionally resets the counter.
   */
  double secs(reset_t reset = keepRunning);

  /**
   * Resets the Timer reference's time to the current time.
   */
  void reset();

private:

  /**
   * Reference time for this timeri
   */
  timeval m_reference;

}; // class Timer

} // namespace utils
} // namespace castor

