/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Exception.hpp"

#include <sys/types.h>

namespace cta::exception {

/**
 * castor::io::acceptConnection() was interrupted
 */
class AcceptConnectionInterrupted : public cta::exception::Exception {

public:

  /**
   * Constructor
   *
   * @param remainingTime The number of remaining seconds when castor::io::acceptConnection() was interrupted
   */
  explicit AcceptConnectionInterrupted(time_t remainingTime);

  /**
   * @return the number of remaining seconds when castor::io::acceptConnection() was interrupted
   */
  time_t remainingTime() const;

private:

  /**
   * The number of remaining seconds when castor::io::acceptConnection() was interrupted
   */
  const time_t m_remainingTime;

}; // class AcceptConnectionInterrupted

} // namespace cta::exception

