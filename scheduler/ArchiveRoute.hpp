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
#include "scheduler/CreationLog.hpp"

#include <stdint.h>
#include <string>
#include <time.h>

namespace cta {

/**
 * An archive route.
 */
struct ArchiveRoute {

  /**
   * Constructor.
   */
  ArchiveRoute();

  /**
   * Destructor.
   */
  ~ArchiveRoute() throw();

  /**
   * Constructor.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.  Copy numbers start from 1.
   * @param tapePoolName The name of the destination tape pool.
   * @param creationLog The who, where, when an why of this modification.
   * time is used.
   */
  ArchiveRoute(
    const std::string &storageClassName,
    const uint16_t copyNb,
    const std::string &tapePoolName,
    const CreationLog &creationLog);

  /**
   * The name of the storage class that identifies the source disk files.
   */
  std::string storageClassName;

  /**
   * The tape copy number.
   */
  uint32_t copyNb;

  /**
   * The name of the destination tape pool.
   */
  std::string tapePoolName;

  /**
   * The record of the entry's creation
   */
  CreationLog creationLog;

}; // class ArchiveRoute

} // namespace cta
