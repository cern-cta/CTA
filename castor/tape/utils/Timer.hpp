/******************************************************************************
 *                      castor/tape/utils/Timer.hpp
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

#ifndef _CASTOR_TAPE_UTILS_TIMER_HPP_
#define _CASTOR_TAPE_UTILS_TIMER_HPP_

#include <sys/time.h>
#include "osdep.h"

namespace castor {
  namespace tape {
    namespace utils {

      /* A small timing class.
       * It basically remembers a reference time (by default the time of
       * its construction) and gives the elapsed time since then.
       * The reset method allows to reset the reference time to the current time
       */
      class Timer {

      public:

        /* constructor */
        Timer();

        /* destructor */
        virtual ~Timer() {}

        /* usecs
         * Gives elapsed time in microseconds with respect to the reference time
         */
        signed64 usecs();

        /* secs
         * Gives elapsed time in seconds (with microsecond precision)
         * with respect to the reference time
         */
        double secs();

        /* resets the Timer reference's time to the current time */
        void reset();

      private:

        /* reference time for this timer*/
        timeval m_reference;

      };
    } // namespace utils
  } // namespace tape
} // namespace castor

#endif /* _CASTOR_TAPE_UTILS_TIMER_HPP_ */
