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
     * Sets the user and group of the current process to the specified values.
     *
     * @param userName The name of the user.
     * @param groupName The name of the group.
     */
  static void setUserAndGroup(const std::string& userName, const std::string& groupName);
};

}  // end of namespace cta
