/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "ZmqContextSingleton.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

#include <zmq.h>

namespace cta {
namespace mediachanger {

//------------------------------------------------------------------------------
// s_mutex
//------------------------------------------------------------------------------
std::mutex ZmqContextSingleton::s_mutex;

//------------------------------------------------------------------------------
// s_instance
//------------------------------------------------------------------------------
void *ZmqContextSingleton::s_instance = nullptr;

//------------------------------------------------------------------------------
// instance
//------------------------------------------------------------------------------
void *ZmqContextSingleton::instance() {
  try {
    std::lock_guard<std::mutex> lock(s_mutex);

    if(nullptr == s_instance) {
      const int sizeOfIOThreadPoolForZMQ = 1;
      s_instance = zmq_init(sizeOfIOThreadPoolForZMQ);
      const int savedErrno = errno;

      if(nullptr == s_instance) {
        const std::string message = utils::errnoToString(savedErrno);
        exception::Exception ex;
        ex.getMessage() << "Call to zmq_init(sizeOfIOThreadPoolForZMQ=" << sizeOfIOThreadPoolForZMQ << ") failed: " << 
          message;
        throw ex;
      }
    }
    return s_instance;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

} // namespace mediachanger
} // namespace cta
