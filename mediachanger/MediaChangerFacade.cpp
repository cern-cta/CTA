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

#include "common/exception/Exception.hpp"
#include "mediachanger/MediaChangerFacade.hpp"

namespace cta {
namespace mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MediaChangerFacade::MediaChangerFacade(const RmcProxy& rmcProxy, log::Logger& log) :
m_rmcProxy(rmcProxy),
m_dmcProxy(log) {}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadOnly(const std::string& vid, const LibrarySlot& slot) {
  try {
    return getProxy(slot.getLibraryType()).mountTapeReadOnly(vid, slot);
  }
  catch (cta::exception::Exception& ne) {
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
    return getProxy(slot.getLibraryType()).mountTapeReadWrite(vid, slot);
  }
  catch (cta::exception::Exception& ne) {
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
    return getProxy(slot.getLibraryType()).dismountTape(vid, slot);
  }
  catch (cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to dismount tape: vid=" << vid << " slot=" << slot.str() << ": "
                    << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getProxy
//------------------------------------------------------------------------------
MediaChangerProxy& MediaChangerFacade::getProxy(const TapeLibraryType libraryType) {
  try {
    switch (libraryType) {
      case TAPE_LIBRARY_TYPE_DUMMY:
        return m_dmcProxy;
      case TAPE_LIBRARY_TYPE_SCSI:
        return m_rmcProxy;
      default:
        // Should never get here
        throw exception::Exception("Library slot has an unexpected library type");
    }
  }
  catch (cta::exception::Exception& ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

}  // namespace mediachanger
}  // namespace cta
