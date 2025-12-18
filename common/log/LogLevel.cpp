/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "common/log/Constants.hpp"

namespace cta::log {

//------------------------------------------------------------------------------
// toLogLevel
//------------------------------------------------------------------------------
int toLogLevel(std::string_view s) {
  if (s == "EMERG") {
    return EMERG;
  }
  if (s == "ALERT") {
    return ALERT;
  }
  if (s == "CRIT") {
    return CRIT;
  }
  if (s == "ERR") {
    return ERR;
  }
  if (s == "WARNING") {
    return WARNING;
  }
  if (s == "NOTICE") {
    return NOTICE;
  }
  if (s == "INFO") {
    return INFO;
  }
  if (s == "DEBUG") {
    return DEBUG;
  }

  // It is a convention of CTA to use syslog level of LOG_NOTICE to label
  // user errors.
  if (s == "USERERR") {
    return NOTICE;
  }

  throw exception::Exception(std::string(s) + " is not a valid log level");
}

}  // namespace cta::log
