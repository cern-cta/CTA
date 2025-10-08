/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/CreationLog.hpp"

#include <string>
#include <time.h>

namespace cta {

struct LogicalLibrary  {
  /**
   * Constructor
   */
  LogicalLibrary() = default;

  /**
   * Destructor
   */
  ~LogicalLibrary() = default;

  /**
   * Constructor
   *
   * @param name The name of the logical library
   * @param creationLog The who, where, when and why of this modification
   */
  LogicalLibrary(const std::string& name, const CreationLog& creationLog) :
    name(name), creationLog(creationLog) { }

  /**
   * The name of the logical library.
   */
  std::string name;

  /**
   * The state of the library online/offline
   */
  bool online;

  /**
   * The record of the entry's creation
   */
  CreationLog creationLog;
};

} // namespace cta
