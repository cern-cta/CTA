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

#include "middletier/common/Tape.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Tape::Tape():
    m_capacityInBytes(0),
    m_dataOnTapeInBytes(0) {
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
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const uint64_t dataOnTapeInBytes,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime):
    ConfigurationItem(creator, comment, creationTime),
    m_vid(vid),
    m_logicalLibraryName(logicalLibraryName),
    m_tapePoolName(tapePoolName),
    m_capacityInBytes(capacityInBytes),
    m_dataOnTapeInBytes(dataOnTapeInBytes) {
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool cta::Tape::operator<(const Tape &rhs) const throw() {
  return m_vid < rhs.m_vid;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
const std::string &cta::Tape::getVid() const throw() {
  return m_vid;
}

//------------------------------------------------------------------------------
// getLogicalLibraryName
//------------------------------------------------------------------------------
const std::string &cta::Tape::getLogicalLibraryName() const throw() {
  return m_logicalLibraryName;
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
const std::string &cta::Tape::getTapePoolName() const throw() {
  return m_tapePoolName;
}

//------------------------------------------------------------------------------
// getCapacityInBytes
//------------------------------------------------------------------------------
uint64_t cta::Tape::getCapacityInBytes() const throw() {
  return m_capacityInBytes;
}

//------------------------------------------------------------------------------
// getDataOnTapeInBytes
//------------------------------------------------------------------------------
uint64_t cta::Tape::getDataOnTapeInBytes() const throw() {
  return m_dataOnTapeInBytes;
}
