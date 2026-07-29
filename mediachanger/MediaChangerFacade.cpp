/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/MediaChangerFacade.hpp"

#include "common/exception/Exception.hpp"

namespace cta::mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MediaChangerFacade::MediaChangerFacade(const RmcProxy& rmcProxy, log::Logger& log)
    : m_rmcProxy(rmcProxy),
      m_dmcProxy(log) {}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadOnly(const std::string& vid, const LibrarySlot& slot) {
  try {
    return getProxy(slot).mountTapeReadOnly(vid, slot);
  } catch (cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for read-only access: vid=" << vid << " slot=" << slot.str() << ": "
                    << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadWrite(const std::string& vid, const LibrarySlot& slot) {
  try {
    return getProxy(slot).mountTapeReadWrite(vid, slot);
  } catch (cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for read/write access: vid=" << vid << " slot=" << slot.str() << ": "
                    << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void MediaChangerFacade::dismountTape(const std::string& vid, const LibrarySlot& slot) {
  try {
    return getProxy(slot).dismountTape(vid, slot);
  } catch (cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to dismount tape: vid=" << vid << " slot=" << slot.str() << ": "
                    << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getProxy
//------------------------------------------------------------------------------
MediaChangerProxy& MediaChangerFacade::getProxy(const LibrarySlot& slot) {
  if (slot.isDummy()) {
    return m_dmcProxy;
  }
  return m_rmcProxy;
}

}  // namespace cta::mediachanger
