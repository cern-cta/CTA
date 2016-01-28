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

#include "ArchiveFile.hpp"

namespace cta { namespace common { namespace archiveNS {
  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveFile::ArchiveFile():
  fileId(0),
  size(0),
  checksum(0),
  lastModificationTime(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveFile::ArchiveFile(const std::string & path, const std::string & nsHostName, uint64_t fileId, 
  uint64_t size, const uint32_t checksum, const time_t lastModificationTime):
  path(path),
  nsHostName(nsHostName),          
  fileId(fileId),
  size(size),
  checksum(checksum),
  lastModificationTime(lastModificationTime) {}
}}} //namespace cta::common::archiveNS
