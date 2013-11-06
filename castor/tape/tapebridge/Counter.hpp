/******************************************************************************
 *                castor/tape/tapebridge/Counter.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_COUNTER_HPP
#define CASTOR_TAPE_TAPEBRIDGE_COUNTER_HPP 1


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * A templated counter class.
 */
template<typename IntegerType> class Counter {

private:

  /**
   * The current value of the counter.
   */
  IntegerType m_count;


public:

  /**
   * Constructor
   *
   * @param count The initial value of the counter.
   */
  Counter(const IntegerType count) throw() {
    reset(count);
  }

  /**
   * Resets the counter to the specified value.
   *
   * @param count The new value of the counter.
   */
  void reset(const IntegerType count) throw() {
    m_count = count;
  }

  /**
   * Increments the counter by 1 and returns the new value.
   */
  IntegerType next() throw() {
    return next(1);
  }

  /**
   * Increments the counter by increment and returns the new value.
   *
   * @param increment The amount by which the counter should be incremented.
   */
  IntegerType next(const IntegerType increment) throw() {
    m_count += increment;

    return(m_count);
  }
};

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_COUNTER_HPP
