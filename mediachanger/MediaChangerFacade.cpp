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

#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"
#include "mediachanger/AcsProxyZmq.hpp"
#include "mediachanger/Constants.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "mediachanger/MmcProxyLog.hpp"
#include "mediachanger/RmcProxyTcpIp.hpp"

namespace cta {
namespace mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MediaChangerFacade::MediaChangerFacade(log::Logger &log, void *const zmqContext) throw():
  m_log(log),
  m_zmqContext(zmqContext) {
}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadOnly(const std::string &vid, const LibrarySlot &slot) {
  try {
    return createMediaChangerProxy(slot.getLibraryType())->mountTapeReadOnly(vid, slot);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for read-only access: vid=" << vid << " slot=" << slot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadWrite(const std::string &vid, const LibrarySlot &slot) {
  try {
    return createMediaChangerProxy(slot.getLibraryType())->mountTapeReadWrite(vid, slot);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for read/write access: vid=" << vid << " slot=" << slot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void MediaChangerFacade::dismountTape(const std::string &vid, const LibrarySlot &slot) {
  try {
    return createMediaChangerProxy(slot.getLibraryType())->dismountTape(vid, slot);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to dismount tape: vid=" << vid << " slot=" << slot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// forceDismountTape
//------------------------------------------------------------------------------
void MediaChangerFacade::forceDismountTape(const std::string &vid, const LibrarySlot &slot) {
  try {
    return createMediaChangerProxy(slot.getLibraryType())->forceDismountTape(vid, slot);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to force dismount tape: vid=" << vid << " slot=" << slot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createMediaChangerProxy
//------------------------------------------------------------------------------
std::unique_ptr<MediaChangerProxy> MediaChangerFacade::createMediaChangerProxy(const TapeLibraryType libraryType) {
  try {
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS:
      return make_unique<AcsProxyZmq>(m_zmqContext);
    case TAPE_LIBRARY_TYPE_MANUAL:
      return make_unique<MmcProxyLog>(m_log);
    case TAPE_LIBRARY_TYPE_SCSI:
      return make_unique<RmcProxyTcpIp>();
    default:
      // Should never get here
      throw exception::Exception("Library slot has an unexpected library type");
    }
  } catch(cta::exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace mediachanger
} // namespace cta
