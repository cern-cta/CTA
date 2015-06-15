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

#include "scheduler/UserIdentity.hpp"
#include "scheduler/CreationLog.hpp"

#include <stdint.h>
#include <string>
#include <time.h>

namespace cta {

class CreationLog;

/**
 * An archival route.
 */
class ArchivalRoute {
public:

  /**
   * Constructor.
   */
  ArchivalRoute();

  /**
   * Destructor.
   */
  ~ArchivalRoute() throw();

  /**
   * Constructor.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.  Copy numbers start from 1.
   * @param tapePoolName The name of the destination tape pool.
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment Comment describing this configuration item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  ArchivalRoute(
    const std::string &storageClassName,
    const uint16_t copyNb,
    const std::string &tapePoolName,
    const CreationLog &creationLog);

  /**
   * Returns the name of the storage class that identifies the source disk
   * files.
   *
   * @return The name of the storage class that identifies the source disk
   * files.
   */
  const std::string &getStorageClassName() const throw();

  /**
   * Returns the tape copy number.
   *
   * @return The tape copy number.
   */
  uint16_t getCopyNb() const throw();

  /**
   * Returns the name of the destination tape pool.
   *
   * @return The name of the destination tape pool.
   */
  const std::string &getTapePoolName() const throw();
  
  /**
   * Get the creation log
   * @return Reference to the creation log
   */
  const CreationLog & getCreationLog() const throw();

private:

  /**
   * The name of the storage class that identifies the source disk files.
   */
  std::string m_storageClassName;

  /**
   * The tape copy number.
   */
  uint32_t m_copyNb;

  /**
   * The name of the destination tape pool.
   */
  std::string m_tapePoolName;

  /**
   * The record of the entry's creation
   */
  CreationLog m_creationLog;
}; // class ArchivalRoute

} // namespace cta
