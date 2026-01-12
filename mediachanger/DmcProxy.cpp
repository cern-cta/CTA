/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/DmcProxy.hpp"

#include "mediachanger/DummyLibrarySlot.hpp"

namespace cta::mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DmcProxy::DmcProxy(log::Logger& log) : m_log(log) {}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void DmcProxy::mountTapeReadOnly(const std::string& vid, const LibrarySlot& librarySlot) {
  std::vector<log::Param> params = {log::Param("tapeVid", vid), log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Dummy mount for read-only access", params);
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void DmcProxy::mountTapeReadWrite(const std::string& vid, const LibrarySlot& librarySlot) {
  std::vector<log::Param> params = {log::Param("tapeVid", vid), log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Dummy mount for read/write access", params);
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void DmcProxy::dismountTape(const std::string& vid, const LibrarySlot& librarySlot) {
  std::vector<log::Param> params = {log::Param("tapeVid", vid), log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Dummy dismount", params);
}

}  // namespace cta::mediachanger
