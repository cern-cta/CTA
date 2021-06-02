/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

// Include Files
#include <time.h>
#include <ostream>

namespace cta {

  namespace log {

    /**
     * A simple object around a time stamp
     */
    class TimeStamp {

    public:

      /**
       * Constructor
       */
      TimeStamp(time_t time) : m_time(time) {};

      /**
       * Accessor
       */
      int time() const { return m_time; }

    private:

      /// the IP address, as an int
      int m_time;

    };

  } // end of namespace log

} // end of namespace cta


/**
 * non-member operator to stream an IpAdress
 */
std::ostream& operator<<(std::ostream& out, const cta::log::TimeStamp& ts);
