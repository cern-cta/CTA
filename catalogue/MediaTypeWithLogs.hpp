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

#include "catalogue/MediaType.hpp"
#include "common/dataStructures/EntryLog.hpp"

#include <cstdint>
#include <string>

namespace cta {
namespace catalogue {

/**
 * Structure describing a tape media type together with cfeation and last
 * modification logs.
 */
struct MediaTypeWithLogs: public MediaType {

  /**
   * The creation log.
   */
  common::dataStructures::EntryLog creationLog;

  /**
   * The last modification log.
   */
  common::dataStructures::EntryLog lastModificationLog;

}; // struct MediaType

} // namespace catalogue
} // namespace cta
