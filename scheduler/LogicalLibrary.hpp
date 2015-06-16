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

#include "scheduler/CreationLog.hpp"

#include <string>
#include <time.h>

namespace cta {

/**
 * Class representing a logical library.
 */
class LogicalLibrary  {
public:

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
   * Returns the name of the logical library.
   *
   * @return The name of the logical library.
   */
  const std::string &getName() const throw();
    
  /**
   * Get the creation log
   * @return Reference to the creation log
   */
  const CreationLog & getCreationLog() const throw();
private:

  /**
   * The name of the logical library.
   */
  std::string m_name;
  
  /**
   * The record of the entry's creation
   */
  CreationLog m_creationLog;
}; // class LogicalLibrary

} // namespace cta
