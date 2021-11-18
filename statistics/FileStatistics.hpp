/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include <stdint.h>

namespace cta {
namespace statistics {

struct FileStatistics {
  FileStatistics();
  FileStatistics(const FileStatistics &other);
  uint64_t nbMasterFiles = 0;
  uint64_t masterDataInBytes = 0;
  uint64_t nbCopyNb1 = 0;
  uint64_t copyNb1InBytes = 0;
  uint64_t nbCopyNbGt1 = 0;
  uint64_t copyNbGt1InBytes = 0;

  FileStatistics &operator=(const FileStatistics &other);

  FileStatistics operator +=(const FileStatistics &other);
};

}  // namespace statistics
}  // namespace cta
