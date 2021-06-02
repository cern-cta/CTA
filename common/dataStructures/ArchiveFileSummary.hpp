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

#include <list>
#include <map>
#include <stdint.h>
#include <string>


namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct is used to hold stats of a list of files (when listing them) 
 */
struct ArchiveFileSummary {

  ArchiveFileSummary();

  bool operator==(const ArchiveFileSummary &rhs) const;

  bool operator!=(const ArchiveFileSummary &rhs) const;

  uint64_t totalBytes;
  uint64_t totalFiles;

}; // struct ArchiveFileSummary

std::ostream &operator<<(std::ostream &os, const ArchiveFileSummary &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
