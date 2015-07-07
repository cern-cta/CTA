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

#include <stdint.h>
#include <string>

namespace cta {
  
class CreationLog;

/**
 * Class representing an archive storage-class.
 */
struct StorageClass {

  /**
   * Constructor.
   */
  StorageClass();

  /**
   * Destructor.
   */
  ~StorageClass() throw();

  /**
   * Constructor.
   *
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param creationLog The who, where, when an why of this modification.
   * time is used.
   */
  StorageClass(
    const std::string &name,
    const uint16_t nbCopies,
    const CreationLog &creationLog);

  /**
   * Returns true if the specified right-hand side is greater than this object.
   *
   * @param rhs The object on the right-hand side of the < operator.
   * @return True if the specified right-hand side is greater than this object.
   */
  bool operator<(const StorageClass &rhs) const;

  /**
   * The name of the storage class.
   */
  std::string name;

  /**
   * The number of copies a file associated with this storage
   * class should have on tape.
   */
  uint16_t nbCopies;
  
  /**
   * The record of the entry's creation
   */
  CreationLog creationLog;

}; // class StorageClass

} // namespace cta
