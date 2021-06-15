/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/exception/Exception.hpp"

#include <sys/types.h>

namespace cta    {
namespace exception {

/**
 * castor::io::acceptConnection() was interrupted.
 */
class AcceptConnectionInterrupted : public cta::exception::Exception {
      
public:
      
  /**
   * Constructor.
   *
   * @param remainingTime The number of remaining seconds when
   * castor::io::acceptConnection() was interrupted.
   */
  AcceptConnectionInterrupted(const time_t remainingTime);

  /**
   * Returns the number of remaining seconds when
   * castor::io::acceptConnection() was interrupted.
   */
  time_t remainingTime() const;

private:

  /**
   * The number of remaining seconds when
   * castor::io::acceptConnection() was interrupted.
   */
  const time_t m_remainingTime;

}; // class AcceptConnectionInterrupted
      
} // end of namespace exception
} // end of namespace cta

