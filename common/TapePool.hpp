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

#include "common/CreationLog.hpp"
#include "common/MountControl.hpp"

#include <string>

namespace cta {

/**
 * A tape pool.
 */
struct TapePool {

  /**
   * Constructor.
   */
  TapePool();

  /**
   * Destructor.
   */
  ~TapePool() throw();

  /**
   * Constructor.
   *
   * @param name The name of the tape pool.
   * @param nbPartialTapes The maximum number of tapes that can be partially
   * full at any moment in time.
   * @param creationLog The who, where, when an why of this modification.
   * time is used.
   */
  TapePool(
    const std::string &name,
    const uint32_t nbPartialTapes,
    const MountCriteriaByDirection & mountCriteriaByDirection,
    const CreationLog &creationLog);

  /**
   * Less than operator.
   *
   * @param rhs The right-hand side of the operator.
   */
  bool operator<(const TapePool &rhs) const throw();

  /**
   * The name of the tape pool.
   */
  std::string name;

  /**
   * The maximum number of tapes that can be partially full at any moment in
   * time.
   */
  uint32_t nbPartialTapes;
  
  /**
   * The collection of criteria for deciding (or not) to mount a tape.  
   */
  MountCriteriaByDirection mountCriteriaByDirection;
  
  /**
   * The record of the entry's creation
   */
  CreationLog creationLog;

}; // class TapePool

} // namespace cta
