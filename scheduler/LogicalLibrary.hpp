/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "common/CreationLog.hpp"

#include <string>
#include <time.h>

namespace cta {

/**
 * Class representing a logical library.
 */
struct LogicalLibrary  {

  /**
   * Constructor.
   */
  LogicalLibrary();

  /**
   * Destructor.
   */
  ~LogicalLibrary() throw();

  /**
   * Constructor.
   *
   * @param name The name of the logical library.
   * @param creationLog The who, where, when an why of this modification.
   * time is used.
   */
  LogicalLibrary(
    const std::string &name,
    const CreationLog &creationLog);

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

}; // class LogicalLibrary

} // namespace cta
