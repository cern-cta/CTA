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

#include "common/exception/Exception.hpp"

#include <string>


namespace cta::exception {

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
  virtual ~NoPortInRange() = default;
  
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

} // namespace cta::exception

