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

#include "mediachanger/MediaChangerFacade.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MediaChangerFacade::MediaChangerFacade(
  AcsProxy &acs,
  MmcProxy &mmc,
  RmcProxy &rmc) throw():
  m_acs(acs),
  m_mmc(mmc),
  m_rmc(rmc) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MediaChangerFacade::MediaChangerFacade(
  AcsProxy &acs,
  RmcProxy &rmc) throw():
  m_acs(acs),
  m_mmc(m_mmcNotSupported),
  m_rmc(rmc) {
}

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadOnly(
  const std::string &vid, const LibrarySlot &slot) {
  try {
    const TapeLibraryType libraryType = slot.getLibraryType();

    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS:
      return m_acs.mountTapeReadOnly(vid,
        dynamic_cast<const AcsLibrarySlot&>(slot));
    case TAPE_LIBRARY_TYPE_MANUAL:
      return m_mmc.mountTapeReadOnly(vid,
        dynamic_cast<const ManualLibrarySlot&>(slot));
    case TAPE_LIBRARY_TYPE_SCSI:
      // SCSI media-changers to not support read-only mounts
      return m_rmc.mountTapeReadWrite(vid,
         dynamic_cast<const ScsiLibrarySlot&>(slot));
    default:
      {
        // Should never get here
        cta::exception::Exception ex;
        ex.getMessage() << "Library slot has an unexpected library type";
        throw ex;
      }
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for read-only access"
      ": vid=" << vid << " slot=" << slot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void MediaChangerFacade::mountTapeReadWrite(
  const std::string &vid, const LibrarySlot &slot) {
  try {
    const TapeLibraryType libraryType = slot.getLibraryType();

    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS: 
      return m_acs.mountTapeReadWrite(vid,
        dynamic_cast<const AcsLibrarySlot&>(slot));
    case TAPE_LIBRARY_TYPE_MANUAL: 
      return m_mmc.mountTapeReadWrite(vid,
        dynamic_cast<const ManualLibrarySlot&>(slot));
    case TAPE_LIBRARY_TYPE_SCSI:
      return m_rmc.mountTapeReadWrite(vid,
        dynamic_cast<const ScsiLibrarySlot&>(slot));
    default:
      {
        // Should never get here
        cta::exception::Exception ex;
        ex.getMessage() << "Library slot has an unexpected library type";
        throw ex;
      }
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to mount tape for read/write access"
      ": vid=" << vid << " slot=" << slot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void MediaChangerFacade::dismountTape(
  const std::string &vid, const LibrarySlot &slot) {
  try {
    const TapeLibraryType libraryType = slot.getLibraryType();
  
    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS:
      return m_acs.dismountTape(vid,
        dynamic_cast<const AcsLibrarySlot&>(slot));
    case TAPE_LIBRARY_TYPE_MANUAL:
      return m_mmc.dismountTape(vid,
        dynamic_cast<const ManualLibrarySlot&>(slot));
    case TAPE_LIBRARY_TYPE_SCSI:
      return m_rmc.dismountTape(vid,
        dynamic_cast<const ScsiLibrarySlot&>(slot));
    default:
      {
        // Should never get here
        cta::exception::Exception ex;
        ex.getMessage() << "Library slot has an unexpected library type";
        throw ex;
      }
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to dismount tape"
      ": vid=" << vid << " slot=" << slot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// forceDismountTape
//------------------------------------------------------------------------------
void MediaChangerFacade::forceDismountTape(
  const std::string &vid, const LibrarySlot &slot) {
  try {
    const TapeLibraryType libraryType = slot.getLibraryType();

    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS:
      return m_acs.forceDismountTape(vid,
        dynamic_cast<const AcsLibrarySlot&>(slot));
    case TAPE_LIBRARY_TYPE_MANUAL:
      return m_mmc.forceDismountTape(vid,
        dynamic_cast<const ManualLibrarySlot&>(slot));
    case TAPE_LIBRARY_TYPE_SCSI:
      return m_rmc.forceDismountTape(vid,
        dynamic_cast<const ScsiLibrarySlot&>(slot));
    default:
      {
        // Should never get here
        cta::exception::Exception ex;
        ex.getMessage() << "Library slot has an unexpected library type";
        throw ex;
      }
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to force dismount tape"
      ": vid=" << vid << " slot=" << slot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

} // namespace mediachanger
} // namespace cta
