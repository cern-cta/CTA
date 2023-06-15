/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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

};  // struct IpAndPort

}  // namespace mediachanger
}  // namespace cta
