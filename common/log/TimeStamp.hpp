/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
