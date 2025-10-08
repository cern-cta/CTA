/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/AcceptConnectionInterrupted.hpp"


// -----------------------------------------------------------------------------
// constructor
// -----------------------------------------------------------------------------
cta::exception::AcceptConnectionInterrupted::AcceptConnectionInterrupted(
  const time_t remainingTime) :
  cta::exception::Exception(),
  m_remainingTime(remainingTime) {

  // Do nothing
}

// -----------------------------------------------------------------------------
// remainingTime()
// -----------------------------------------------------------------------------
time_t cta::exception::AcceptConnectionInterrupted::remainingTime() const
  {
  return m_remainingTime;
}
