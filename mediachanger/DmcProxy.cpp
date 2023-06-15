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

#include "mediachanger/DummyLibrarySlot.hpp"
#include "mediachanger/DmcProxy.hpp"

namespace cta {
namespace mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DmcProxy::DmcProxy(log::Logger& log) : m_log(log) {}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void DmcProxy::mountTapeReadOnly(const std::string& vid, const LibrarySlot& librarySlot) {
  std::list<log::Param> params = {log::Param("tapeVid", vid), log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Dummy mount for read-only access", params);
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void DmcProxy::mountTapeReadWrite(const std::string& vid, const LibrarySlot& librarySlot) {
  std::list<log::Param> params = {log::Param("tapeVid", vid), log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Dummy mount for read/write access", params);
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void DmcProxy::dismountTape(const std::string& vid, const LibrarySlot& librarySlot) {
  std::list<log::Param> params = {log::Param("tapeVid", vid), log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Dummy dismount", params);
}

}  // namespace mediachanger
}  // namespace cta
