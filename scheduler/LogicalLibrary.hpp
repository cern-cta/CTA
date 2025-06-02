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

#include "common/CreationLog.hpp"

#include <string>
#include <time.h>

namespace cta {

struct LogicalLibrary {
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
  LogicalLibrary(const std::string& name, const CreationLog& creationLog) : name(name), creationLog(creationLog) {}

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

}  // namespace cta
