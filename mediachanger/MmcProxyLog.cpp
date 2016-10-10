/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "mediachanger/MmcProxyLog.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::MmcProxyLog::MmcProxyLog(log::Logger &log) throw():
  m_log(log) {
}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void cta::mediachanger::MmcProxyLog::mountTapeReadOnly(
  const std::string &vid, const ManualLibrarySlot &librarySlot) {
  std::list<log::Param> params = {
    log::Param("TPVID", vid),
    log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Tape should be manual mounted for read-only access",
    params);
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void cta::mediachanger::MmcProxyLog::mountTapeReadWrite(
  const std::string &vid, const ManualLibrarySlot &librarySlot) {
  std::list<log::Param> params = {
    log::Param("TPVID", vid),
    log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Tape should be manual mounted for read/write access",
    params);
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void cta::mediachanger::MmcProxyLog::dismountTape(
  const std::string &vid, const ManualLibrarySlot &librarySlot) {
  std::list<log::Param> params = {
    log::Param("TPVID", vid),
    log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Tape should be manual dismounted", params);
}

//------------------------------------------------------------------------------
// forceDismountTape
//------------------------------------------------------------------------------
void cta::mediachanger::MmcProxyLog::forceDismountTape(
  const std::string &vid, const ManualLibrarySlot &librarySlot) {
  std::list<log::Param> params = {
    log::Param("TPVID", vid),
    log::Param("librarySlot", librarySlot.str())};
  m_log(log::WARNING, "Tape should be manual dismounted", params);
}
