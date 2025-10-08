/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/IpAndPort.hpp"

namespace cta::mediachanger {

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
IpAndPort::IpAndPort(const unsigned long i, const unsigned short p) noexcept:
  ip(i),
  port(p) {
  // Do nothing
}

} // namespace cta::mediachanger
