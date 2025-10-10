/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "mediachanger/MediaChangerFacade.hpp"

namespace cta::mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MediaChangerFacade::MediaChangerFacade(const RmcProxy& rmcProxy, log::Logger &log):
  m_rmcProxy(rmcProxy),
  m_dmcProxy(log) {
}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadOnly(const std::string &vid, const LibrarySlot &slot) {
  try {
    return getProxy(slot.getLibraryType()).mountTapeReadOnly(vid, slot);
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
    return getProxy(slot.getLibraryType()).mountTapeReadWrite(vid, slot);
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
    return getProxy(slot.getLibraryType()).dismountTape(vid, slot);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to dismount tape: vid=" << vid << " slot=" << slot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getProxy
//------------------------------------------------------------------------------
MediaChangerProxy &MediaChangerFacade::getProxy(const TapeLibraryType libraryType) {
  try {
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_DUMMY:
      return m_dmcProxy;
    case TAPE_LIBRARY_TYPE_SCSI:
      return m_rmcProxy;
    default:
      // Should never get here
      throw exception::Exception("Library slot has an unexpected library type");
    }
  } catch(cta::exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace cta::mediachanger
