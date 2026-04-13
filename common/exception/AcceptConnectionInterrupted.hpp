/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

#include <sys/types.h>

namespace cta::exception {

/**
 * cta::mediachanger::acceptConnection() was interrupted
 */
class AcceptConnectionInterrupted : public cta::exception::Exception {
public:
  /**
   * Constructor
   *
   * @param remainingTime The number of remaining seconds when cta::mediachanger::acceptConnection() was interrupted
   */
  explicit AcceptConnectionInterrupted(time_t remainingTime)
      : cta::exception::Exception(),
        m_remainingTime(remainingTime) {}

  /**
   * @return the number of remaining seconds when cta::mediachanger::acceptConnection() was interrupted
   */
  time_t remainingTime() const { return m_remainingTime; }

private:
  /**
   * The number of remaining seconds when cta::mediachanger::acceptConnection() was interrupted
   */
  const time_t m_remainingTime;

};  // class AcceptConnectionInterrupted

}  // namespace cta::exception
