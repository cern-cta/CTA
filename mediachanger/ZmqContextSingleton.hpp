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

#pragma once

#include "common/threading/Mutex.hpp"

#include <memory>

namespace cta {
namespace mediachanger {

/**
 * A singleton version of a ZMQ context.
 *
 * Please note that this class INTENTIONALLY does NOT terminate the ZMQ context
 * because ZMQ sends an abort on exit when cleaned up this way under some
 * circumstances, so we purposely do not clean up the context (zmq_term) and
 * leave a resource leak, which in our use case is a one-off situation per
 * process.
 */
class ZmqContextSingleton {
public:

  /**
   * Returns the single instance of the ZMQ context.
   */
  static void *instance();

  /**
   * It should not be possible to make instances of this class.
   */
  ZmqContextSingleton() = delete;

  /**
   * It should not be possible to copy instances of this class.
   */
  ZmqContextSingleton(const ZmqContextSingleton &) = delete;

  /**
   * It should not be possible to assign instances of this class.
   */
  void operator=(const ZmqContextSingleton &) = delete;

private:

  /**
   * Mutex used to implement a critical region around the implementation of the
   * instance() method.
   */
  static threading::Mutex s_mutex;

  /**
   * The single instance of a ZMQ context.  A value of NULL means the ZMQ
   * context has not yet been created.
   */
  static void *s_instance;

}; // class ZmqContextSingleton

} // namespace mediachanger
} // namespace cta
