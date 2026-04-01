/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/log/Logger.hpp"

#include <list>
#include <stdint.h>
#include <type_traits>

namespace cta::catalogue {

/**
 * Retries calling the specified callable if it throws a LostDatabaseConnection
 * exception.
 *
 * @tparam T The type of the callable.
 * @param log Object representing the API to the CTA logging system.
 * @param callable The callable.
 * @param maxTriesToConnect The maximum number of times the callable should be called in the event of a
 * LostDatbaseConnection exception.
 * @return The result of calling the callable.
 */
template<typename T>
typename std::result_of<T()>::type
retryOnLostConnection(log::Logger& log, const T& callable, const uint32_t maxTriesToConnect) {
  for (uint32_t tryNb = 1; tryNb <= maxTriesToConnect; tryNb++) {
    try {
      return callable();
    } catch (exception::LostDatabaseConnection& le) {
      // Log lost connection
      std::vector<log::Param> params = {
        {"maxTriesToConnect", maxTriesToConnect    },
        {"tryNb",             tryNb                },
        {"msg",               le.getMessage().str()}
      };
      if (tryNb == maxTriesToConnect) {
        log(cta::log::ERR, "Unable to connect to database after multiple retries", params);
        le.getMessage() << " Unable to connect to database after " << maxTriesToConnect << " retries.";
        throw;
      } else {
        log(cta::log::WARNING, "Lost database connection", params);
      }
    }
  }
  throw exception::Exception("Failed to retry database connection");
}

}  // namespace cta::catalogue
