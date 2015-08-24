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

#include "common/archiveNS/TapeFileLocation.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapeFileLocation::TapeFileLocation():
  fSeq(0),
  blockId(0),
  vid(""),
  copyNumber(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapeFileLocation::TapeFileLocation(const uint64_t fSeq, const uint64_t blockId, const std::string &vid, const uint8_t copyNumber):
  fSeq(fSeq),
  blockId(blockId),
  vid(vid),
  copyNumber(copyNumber) {
}
