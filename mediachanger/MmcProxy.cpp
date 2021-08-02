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

#include "mediachanger/ManualLibrarySlot.hpp"
#include "mediachanger/MmcProxy.hpp"

namespace cta {
namespace mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MmcProxy::MmcProxy(log::Logger &log):
  m_log(log) {
}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void MmcProxy::mountTapeReadOnly( const std::string &vid, const LibrarySlot &librarySlot) {
  std::list<log::Param> params = {
    log::Param("tapeVid", vid),
    log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Tape should be manual mounted for read-only access", params);
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void MmcProxy::mountTapeReadWrite(const std::string &vid, const LibrarySlot &librarySlot) {
  std::list<log::Param> params = {
    log::Param("tapeVid", vid),
    log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Tape should be manual mounted for read/write access", params);
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void MmcProxy::dismountTape( const std::string &vid, const LibrarySlot &librarySlot) {
  std::list<log::Param> params = {
    log::Param("tapeVid", vid),
    log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Tape should be manual dismounted", params);
}

} // namespace medichanger
} // namespace cta
