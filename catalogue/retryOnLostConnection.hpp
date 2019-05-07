/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
//#include "common/checksum/ChecksumTypeMismatch.hpp"
//#include "common/checksum/ChecksumValueMismatch.hpp"
#include "common/log/Logger.hpp"

#include <list>
#include <stdint.h>
#include <type_traits>

namespace cta {
namespace catalogue {

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
        log(cta::log::WARNING, "Lost database connection", params);
      }
    }

    exception::Exception ex;
    ex.getMessage() << "Lost the database connection after trying " << maxTriesToConnect << " times";
    throw ex;
  } catch(...) {
    throw;
  }
}

} // namespace catalogue
} // namespace cta
