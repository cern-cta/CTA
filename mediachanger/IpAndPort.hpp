/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <stdint.h>
#include <stdlib.h>

namespace cta::mediachanger {

/**
 * The IP number and the port number of a TCP/IP address.
 */
struct IpAndPort {
  /**
   * Constructor.
   *
   * @param i   The IP number of the TCP/IP address.
   * @param p The port number of the TCP/IP address.
   */
  IpAndPort(const unsigned long i, const unsigned short p) noexcept;

  /**
   * The IP number of the TCP/IP address.
   */
  unsigned long ip;

  /**
   * The port number of the TCP/IP address.
   */
  unsigned short port;

}; // struct IpAndPort

} // namespace cta::mediachanger
