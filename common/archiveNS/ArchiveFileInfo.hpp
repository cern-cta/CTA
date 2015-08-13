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

#include <string>
#include "common/checksum/Checksum.hpp"

namespace cta {
/**
 * A class containing the location and properties of an archive file. 
 */
class ArchiveFileInfo {
public:
  ArchiveFileInfo(const std::string & lastKnownPath, uint64_t fileId, 
    uint64_t size, const Checksum & checksum, const time_t lastModificationTime);
  const std::string lastKnownPath; /**< The location of the file at NS lookup time */
  const uint64_t fileId; /**< The file ID (to be used in the tape copy header, among other */
  const uint64_t size; /**< The file's size */
  const Checksum checksum;
  const time_t lastModificationTime;
};
}