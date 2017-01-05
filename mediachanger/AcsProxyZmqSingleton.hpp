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

#include "mediachanger/Constants.hpp"
#include "mediachanger/AcsProxyZmq.hpp"

#include <memory>
#include <mutex>

namespace cta {
namespace mediachanger {

/**
 * A singleton class.
 */
class AcsProxyZmqSingleton: public AcsProxyZmq {
public:

  /**
   * Delete the default constructor.
   */
  AcsProxyZmqSingleton() = delete;

  /**
   * Delete the copy constructor.
   */
  AcsProxyZmqSingleton(const AcsProxyZmqSingleton&) = delete;

  /**
   * Delete the move constructor.
   */
  AcsProxyZmqSingleton(AcsProxyZmqSingleton&&) = delete;

  /**
   * Delete the copy assignment oprator.
   */
  AcsProxyZmqSingleton &operator=(const AcsProxyZmqSingleton&) = delete;

  /**
   * Delete the move assignment oprator.
   */
  AcsProxyZmqSingleton &operator=(AcsProxyZmqSingleton&&) = delete;

  /**
   * Returns the singleton instance.
   *
   * @param zmqContext The ZMQ context to be used to construct the instance if
   * the instance does not already exist.
   * @return the singleton instance.
   */
  static AcsProxyZmqSingleton &instance(void *const zmqContext);

private:

  /**
   * Provate constructor to only be used by the instance() method.
   *
   * @param zmqContext The ZMQ context.
   * @param serverPort The TCP/IP port on which the CASTOR ACS daemon is
   * listening for ZMQ messages.
   */
  AcsProxyZmqSingleton(void *const zmqContext, const unsigned short serverPort = ACS_PORT) throw();

  /**
   * Mutex used to implement a critical region around the implementation of the
   * instance() method.
   */
  static std::mutex s_mutex;

  /**
   * The single instance.
   */
  static std::unique_ptr<AcsProxyZmqSingleton> s_instance;

}; // class AcsProxyZmqSingleton

} // namespace mediachanger
} // namespace cta
