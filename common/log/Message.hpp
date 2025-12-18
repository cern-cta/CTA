/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

// Include Files
#include <string>

namespace cta {

namespace log {

/**
     * Container for a CASTOR log message
     */
struct Message {
  /// Message number
  int number;
  /// Message text
  std::string text;
};

}  // end of namespace log

}  // end of namespace cta
