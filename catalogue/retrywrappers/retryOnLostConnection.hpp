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

#include <list>
#include <stdint.h>
#include <type_traits>

#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/log/Logger.hpp"

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
typename std::result_of<T()>::type retryOnLostConnection(log::Logger &log, const T &callable,
  const uint32_t maxTriesToConnect) {
  try {
    for (uint32_t tryNb = 1; tryNb <= maxTriesToConnect; tryNb++) {
      try {
        return callable();
      } catch (exception::LostDatabaseConnection &le) {
        // Log lost connection
        std::list<log::Param> params = {
          {"maxTriesToConnect", maxTriesToConnect},
          {"tryNb", tryNb},
          {"msg", le.getMessage().str()}
        };
        int logLevel = (tryNb == maxTriesToConnect) ? cta::log::CRIT : cta::log::WARNING;
        log(logLevel, "Lost database connection", params);
      }
    }

    exception::Exception ex;
    ex.getMessage() << "Lost the database connection after trying " << maxTriesToConnect << " times";
    throw ex;
  } catch(...) {
    throw;
  }
}

} // namespace cta::catalogue
