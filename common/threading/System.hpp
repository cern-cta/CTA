/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
    static std::string getHostName() ;

    /**
     * Converts a string into a port number, checking
     * that the value is in range [0-65535]
     * @param str the string giving the port number
     * @return the port as an int
     * @exception in case of invalid value
     */
    static int porttoi(char* str) ;

    /**
     * Sets the user and group of the current process to the specified values.
     *
     * @param userName The name of the user.
     * @param groupName The name of the group.
     */
    static void setUserAndGroup(const std::string &userName, const std::string &groupName);

  };

} // end of namespace cta

