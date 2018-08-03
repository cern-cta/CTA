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

#include "common/exception/Exception.hpp"

#include <string>


namespace cta    {
namespace exception {

/**
 * No port in range exception.
 */
class NoPortInRange : public cta::exception::Exception {
  
public:
  
  /**
   * Constructor
   *
   * @param lowPort  The inclusive low port of the port number range.
   * @param highPort The inclusive high port of the port number range.
   */
  NoPortInRange(const unsigned short lowPort, const unsigned short highPort)
   ;
  
  /**
   * Empty Destructor, explicitely non-throwing (needed for std::exception
   * inheritance)
   */
  virtual ~NoPortInRange() {}
  
  /**
   * Returns the inclusive low port of the port number range.
   */
  unsigned short getLowPort();

  /**
   * Returns the inclusive high port of the port number range.
   */
  unsigned short getHighPort();


private:

  /**
   * The inclusive low port of the port number range.
   */
  const unsigned short m_lowPort;

  /**
   * The inclusive high port of the port number range.
   */
  const unsigned short m_highPort;

}; // class NoPortInRange

} // namespace exception
} // namespace cta

