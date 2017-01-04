/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "mediachanger/ManualLibrarySlot.hpp"
#include "mediachanger/MmcProxyNotSupported.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void cta::mediachanger::MmcProxyNotSupported::mountTapeReadOnly(const std::string &vid, const LibrarySlot &librarySlot) {
  cta::exception::Exception ex;
  ex.getMessage() << "Manual mounting is not supported";
  throw ex;
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void cta::mediachanger::MmcProxyNotSupported::mountTapeReadWrite(const std::string &vid, const LibrarySlot &librarySlot) {
  cta::exception::Exception ex;
  ex.getMessage() << "Manual mounting is not supported";
  throw ex;
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void cta::mediachanger::MmcProxyNotSupported::dismountTape(const std::string &vid, const LibrarySlot &librarySlot) {
  cta::exception::Exception ex;
  ex.getMessage() << "Manual dismounting is not supported";
  throw ex;
}

//------------------------------------------------------------------------------
// forceDismountTape
//------------------------------------------------------------------------------
void cta::mediachanger::MmcProxyNotSupported::forceDismountTape(const std::string &vid, const LibrarySlot &librarySlot) {
  cta::exception::Exception ex;
  ex.getMessage() << "Manual dismounting is not supported";
  throw ex;
} 
