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

namespace cta {

class System {
public:
  CTA_GENERATE_EXCEPTION_CLASS(OutOfMemory);

  /**
     * Gets the host name. Handles all errors that may occur with 
     * the gethostname() API.  
     * @exception in case of an error 
     */
  static std::string getHostName();

  /**
     * Converts a string into a port number, checking
     * that the value is in range [0-65535]
     * @param str the string giving the port number
     * @return the port as an int
     * @exception in case of invalid value
     */
  static int porttoi(char* str);

  /**
     * Sets the user and group of the current process to the specified values.
     *
     * @param userName The name of the user.
     * @param groupName The name of the group.
     */
  static void setUserAndGroup(const std::string& userName, const std::string& groupName);
};

}  // end of namespace cta
