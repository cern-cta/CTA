/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

#include <string>

namespace cta::exception {

/**
 * No port in range exception.
 */
class NoPortInRange : public cta::exception::Exception {
public:
  /**
   * Constructor
   *
   * @param lowPort  The inclusive low port of the port number range.
   * @param highPort The inclusive high port of the port number range.
   */
  NoPortInRange(const unsigned short lowPort, const unsigned short highPort);

  /**
   * Empty Destructor, explicitely non-throwing (needed for std::exception
   * inheritance)
   */
  ~NoPortInRange() final = default;

  /**
   * Returns the inclusive low port of the port number range.
   */
  unsigned short getLowPort();

  /**
   * Returns the inclusive high port of the port number range.
   */
  unsigned short getHighPort();

private:
  /**
   * The inclusive low port of the port number range.
   */
  const unsigned short m_lowPort;

  /**
   * The inclusive high port of the port number range.
   */
  const unsigned short m_highPort;

};  // class NoPortInRange

}  // namespace cta::exception
