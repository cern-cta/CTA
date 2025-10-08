/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/NoPortInRange.hpp"


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
cta::exception::NoPortInRange::NoPortInRange(
  const unsigned short lowPort,
  const unsigned short highPort) :
  cta::exception::Exception(),
  m_lowPort(lowPort),
  m_highPort(highPort)  {

  // Do nothing
}


//------------------------------------------------------------------------------
// getLowPort()
//------------------------------------------------------------------------------
unsigned short cta::exception::NoPortInRange::getLowPort() {
  return m_lowPort;
}


//------------------------------------------------------------------------------
// getHighPort()
//------------------------------------------------------------------------------
unsigned short cta::exception::NoPortInRange::getHighPort() {
  return m_highPort;
}
