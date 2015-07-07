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

#include "scheduler/Tape.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Tape::Tape():
    capacityInBytes(0),
    dataOnTapeInBytes(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::Tape::~Tape() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Tape::Tape(
    const std::string &vid,
    const uint64_t nbFiles,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const uint64_t dataOnTapeInBytes,
    const CreationLog & creationLog):
    vid(vid),
    nbFiles(nbFiles),
    logicalLibraryName(logicalLibraryName),
    tapePoolName(tapePoolName),
    capacityInBytes(capacityInBytes),
    dataOnTapeInBytes(dataOnTapeInBytes),
    creationLog(creationLog){
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool cta::Tape::operator<(const Tape &rhs) const throw() {
  return vid < rhs.vid;
}
