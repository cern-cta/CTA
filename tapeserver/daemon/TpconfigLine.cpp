/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "tapeserver/daemon/TpconfigLine.hpp"
#include "common/exception/Exception.hpp"
#include "mediachanger/LibrarySlotParser.hpp"

namespace cta {
namespace tape {
namespace daemon {

//------------------------------------------------------------------------------
// Constructor.
//------------------------------------------------------------------------------
cta::tape::daemon::TpconfigLine::TpconfigLine(const std::string& unitName,
                                              const std::string& logicalLibrary,
                                              const std::string& devFilename,
                                              const std::string& librarySlot) :
unitName(unitName),
logicalLibrary(logicalLibrary),
devFilename(devFilename),
rawLibrarySlot(librarySlot),
m_librarySlot(mediachanger::LibrarySlotParser::parse(rawLibrarySlot)) {
  if (unitName.size() > maxNameLen) {
    throw cta::exception::Exception("In TpconfigLine::TpconfigLine: unitName too long");
  }
  if (logicalLibrary.size() > maxNameLen) {
    throw cta::exception::Exception("In TpconfigLine::TpconfigLine: logicalLibrary too long");
  }
  if (devFilename.size() > maxNameLen) {
    throw cta::exception::Exception("In TpconfigLine::TpconfigLine: devFilename too long");
  }
  if (librarySlot.size() > maxNameLen) {
    throw cta::exception::Exception("In TpconfigLine::TpconfigLine: librarySlot too long");
  }
}

//------------------------------------------------------------------------------
// Trivial constructor.
//------------------------------------------------------------------------------
TpconfigLine::TpconfigLine() {}

//------------------------------------------------------------------------------
// Copy constructor.
//------------------------------------------------------------------------------
TpconfigLine::TpconfigLine(const TpconfigLine& o) :
TpconfigLine(o.unitName, o.logicalLibrary, o.devFilename, o.rawLibrarySlot) {}

//------------------------------------------------------------------------------
// TpconfigLine::librarySlot
//------------------------------------------------------------------------------
const cta::mediachanger::LibrarySlot& TpconfigLine::librarySlot() const {
  return *m_librarySlot;
}

//------------------------------------------------------------------------------
// TpconfigLine::operator=
//------------------------------------------------------------------------------
TpconfigLine& TpconfigLine::operator=(const TpconfigLine& o) {
  unitName = o.unitName;
  logicalLibrary = o.logicalLibrary;
  devFilename = o.devFilename;
  rawLibrarySlot = o.rawLibrarySlot;
  m_librarySlot.reset(mediachanger::LibrarySlotParser::parse(rawLibrarySlot));
  return *this;
}

}  // namespace daemon
}  // namespace tape
}  // namespace cta
