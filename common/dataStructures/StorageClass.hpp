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

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/EntryLog.hpp"
#include "VirtualOrganization.hpp"

namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct specifies the number of copies that a file tagged with it should 
 * have. it may also indicate the VO owning the file and what kind of data the 
 * file contains 
 */
struct StorageClass {

  /**
   * Constructor that sets all integer member-variables to zero and all string
   * member-variables to the empty string.
   */
  StorageClass();

  /**
   * Returns true if the specified StorageClass object is considered equal to
   * this one.
   *
   * Please note that two StorageClass objects are considered equal if and only
   * if their name member-variables match, i.e.
   *
   *    name==rhs.name;
   *
   * All other member-variables are intentionally ignored by this equality
   * operator.
   *
   * @param rhs The other StorageClass object on the right-hand side of the
   * equality operator.
   * @return True if the specified StorageClass object is considered equal to
   * this one.
   */
  bool operator==(const StorageClass &rhs) const;

  /**
   * Returns the logical negation of operator==().
   *
   * @param rhs The other StorageClass object on the right-hand side of the
   * inequality operator.
   * @return The logical negation of operator==().
   */
  bool operator!=(const StorageClass &rhs) const;

  /**
   * The name of the storage class.
   *
   * Please note that the name of a storage class is only gauranteed to be
   * unique within its disk instance.
   */
  std::string name;

  /**
   * The number of copies on tape.
   */
  uint64_t nbCopies;
  
  /**
   * The virtual organization to which this storage class belongs
   */
  VirtualOrganization vo;

  /**
   * The creation log.
   */
  EntryLog creationLog;

  /**
   * Th alst modification log.
   */
  EntryLog lastModificationLog;

  /**
   * The comment.
   */
  std::string comment;

}; // struct StorageClass

std::ostream &operator<<(std::ostream &os, const StorageClass &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
