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

#include <stdint.h>
#include <stdlib.h>

namespace cta {
namespace mediachanger {

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
  	
} // namespace mediachanger
} // namespace cta
