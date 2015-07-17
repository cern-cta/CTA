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

#include "common/archiveNS/TapeCopyLocationAndStatus.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapeCopyLocationAndStatus::TapeCopyLocationAndStatus() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapeCopyLocationAndStatus::TapeCopyLocationAndStatus(
  const std::string &vid,
  const uint64_t fSeq,
  const uint64_t blockId,
  const uint64_t size,
  const uint64_t fileId,
  const Checksum &checksum):
  vid(vid),
  fSeq(fSeq),
    blockId(blockId),
    size(size),
    fileId(fileId),
    checksum(checksum) {}
