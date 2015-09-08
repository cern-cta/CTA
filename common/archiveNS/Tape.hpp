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

#include "common/UserIdentity.hpp"
#include "common/CreationLog.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class representing a tape.
 */
struct Tape {

  /**
   * Constructor.
   */
  Tape();

  /**
   * Destructor.
   */
  ~Tape() throw();
  struct Status;
  /**
   * Constructor.
   *
   * @param vid The volume identifier of the tape.
   * @param nbFiles The total number of files stored on the tape.
   * @param logicalLibraryName The name of the logical library to which the tape
   * belongs.
   * @param tapePoolName The name of the tape pool to which the tape belongs.
   * @param capacityInBytes The capacity of the tape.
   * @param dataOnTapeInBytes The amount of data currently stored on the tape in
   * bytes.
   * @param creationLog The who, where, when an why of this modification.
   */
  Tape(
    const std::string &vid,
    const uint64_t nbFiles,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const uint64_t dataOnTapeInBytes,
    const CreationLog &creationLog,
    const Status & status);

  /**
   * Less than operator.
   *
   * @param rhs The right-hand side of the operator.
   */
  bool operator<(const Tape &rhs) const throw();

  /**
   * The volume identifier of the tape.
   */
  std::string vid;

  /**
   * The total number of files stored on the tape.
   */
  uint64_t nbFiles;

  /**
   * The name of the logical library to which the tape belongs.
   */
  std::string logicalLibraryName;

  /**
   * The name of the tape pool to which the tape belongs.
   */
  std::string tapePoolName;

  /**
   * The capacity of the tape.
   */
  uint64_t capacityInBytes;

  /**
   * The amount of data on the tape.
   */
  uint64_t dataOnTapeInBytes;

  /**
   * The record of the entry's creation
   */
  CreationLog creationLog;
  
  /**
   * Type holding the tape's status
   */
  struct Status {
    bool busy;
    bool archived;
    bool disabled;
    bool readonly;
    bool full;
    bool availableToWrite();
  };
  
  /**
   * The tape's status
   */
  Status status;
  
  

}; // class Tape

} // namespace cta
