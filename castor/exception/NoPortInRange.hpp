/******************************************************************************
 *                      castor/exception/NoPortInRange.hpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_EXCEPTION_NO_PORT_IN_RANGE_HPP 
#define CASTOR_EXCEPTION_NO_PORT_IN_RANGE_HPP 1

#include "castor/exception/Exception.hpp"

#include <string>


namespace castor    {
namespace exception {

/**
 * No port in range exception.
 */
class NoPortInRange : public castor::exception::Exception {
  
public:
  
  /**
   * Constructor
   *
   * @param lowPort  The inclusive low port of the port number range.
   * @param highPort The inclusive high port of the port number range.
   */
  NoPortInRange(const unsigned short lowPort, const unsigned short highPort)
    throw();

  /**
   * Returns the inclusive low port of the port number range.
   */
  unsigned short getLowPort() throw();

  /**
   * Returns the inclusive high port of the port number range.
   */
  unsigned short getHighPort() throw();


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
} // namespace castor

#endif // CASTOR_EXCEPTION_NO_PORT_IN_RANGE_HPP
