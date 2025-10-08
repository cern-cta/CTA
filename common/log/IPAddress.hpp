/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

// Include Files
#include <ostream>

namespace cta {

  namespace log {

    /**
     * A simple object around an IP address
     */
    class IPAddress {

    public:

      /**
       * Constructor
       */
      IPAddress(int ip) : m_ip(ip) {};

      /**
       * Accessor
       */
      int ip() const { return m_ip; }

    private:

      /// the IP address, as an int
      int m_ip;

    };

  } // end of namespace log

} // end of namespace cta

/**
 * non-member operator to stream an IpAdress
 */
std::ostream& operator<<(std::ostream& out, const cta::log::IPAddress& ip);
