/******************************************************************************
 *                castor/tape/aggregator/SynchronizedCounter.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_SYNCHRONIZEDCOUNTER_HPP
#define CASTOR_TAPE_AGGREGATOR_SYNCHRONIZEDCOUNTER_HPP 1

#include "castor/exception/Exception.hpp"

#include <pthread.h>
#include <stdint.h>


namespace castor     {
namespace tape       {
namespace aggregator {

/**
 * Thread-safe counter.
 */
class SynchronizedCounter {

private:

  /**
   * The current value of the counter.
   */
  int32_t m_count;

  /**
   * Mutex used to make the counter thread-safe.
   */
  pthread_mutex_t m_mutex;

public:

  /**
   * Constructor
   *
   * @param count The initial value of the counter.
   */
  SynchronizedCounter(const int32_t count) throw(castor::exception::Exception);

  /**
   * Resets the counter to the specified value.
   *
   * @param count The new value of the counter.
   */
  void reset(const int32_t count) throw(castor::exception::Exception);

  /**
   * Increments the counter by 1 and returns the new value.
   */
  int32_t next() throw(castor::exception::Exception);

  /**
   * Increments the counter by increment and returns the new value.
   *
   * @param increment The amount by which the counter should be incremented.
   */
  int32_t next(const int32_t increment) throw(castor::exception::Exception);
};

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_SYNCHRONIZEDCOUNTER_HPP
