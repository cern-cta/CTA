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

namespace cta {

/**
 * Class representing a tape pool.
 */
class TapePool {
public:

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
    const CreationLog &creationLog);

  /**
   * Less than operator.
   *
   * @param rhs The right-hand side of the operator.
   */
  bool operator<(const TapePool &rhs) const throw();

  /**
   * Returns the name of the tape pool.
   *
   * @return The name of the tape pool.
   */
  const std::string &getName() const throw();

  /**
   * Returns the maximum number of tapes that can be partially full at any
   * moment in time.
   *
   * @return The maximum number of tapes that can be partially full at any
   * moment in time.
   */
  uint32_t getNbPartialTapes() const throw();
  
  /**
   * Get the creation log
   * @return Reference to the creation log
   */
  const CreationLog & getCreationLog() const throw();

private:

  /**
   * The name of the tape pool.
   */
  std::string m_name;

  /**
   * The maximum number of tapes that can be partially full at any moment in
   * time.
   */
  uint32_t m_nbPartialTapes;
  
  /**
   * The record of the entry's creation
   */
  CreationLog m_creationLog;
}; // class TapePool

} // namespace cta
