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

#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/TapeFile.hpp"

#include <list>

namespace cta {
namespace common {
namespace dataStructures {

/**
 * The queueing criteria returned by the catalogue for a file retrieve request.
 */
struct RetrieveFileQueueCriteria {

  /**
   * Constructor.
   */
  RetrieveFileQueueCriteria();

  /**
   * Constructor.
   *
   * @param tapeFiles The location(s) of the file on tape.
   * @param mountPolicy The mount policy.
   */
  RetrieveFileQueueCriteria(
    const std::list<TapeFile> &tapeFiles,
    const MountPolicy &mountPolicy);

  /**
   * The location(s) of the file on tape.
   */
  std::list<TapeFile> tapeFiles;

  /**
   * The mount policy.
   */
  MountPolicy mountPolicy;

}; // struct RetrieveFileQueueCriteria

} // namespace dataStructures
} // namespace common
} // namespace cta
