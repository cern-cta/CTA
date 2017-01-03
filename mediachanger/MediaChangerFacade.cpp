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

#include "mediachanger/AcsProxyZmq.hpp"
#include "mediachanger/Constants.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "mediachanger/MmcProxyLog.hpp"
#include "mediachanger/RmcProxyTcpIp.hpp"
#include "mediachanger/ZmqContextSingleton.hpp"
#include "common/exception/Exception.hpp"

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
    const TapeLibraryType libraryType = slot.getLibraryType();

    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS:
      {
        AcsProxyZmq acs(m_zmqContext);
        return acs.mountTapeReadOnly(vid, dynamic_cast<const AcsLibrarySlot&>(slot));
      }
    case TAPE_LIBRARY_TYPE_MANUAL:
      {
        MmcProxyLog mmc(m_log);
        return mmc.mountTapeReadOnly(vid, dynamic_cast<const ManualLibrarySlot&>(slot));
      }
    case TAPE_LIBRARY_TYPE_SCSI:
      {
        RmcProxyTcpIp rmc;

        // SCSI media-changers to not support read-only mounts
        return rmc.mountTapeReadWrite(vid, dynamic_cast<const ScsiLibrarySlot&>(slot));
      }
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
void MediaChangerFacade::mountTapeReadWrite(const std::string &vid, const LibrarySlot &slot) {
  try {
    const TapeLibraryType libraryType = slot.getLibraryType();

    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS: 
      {
        AcsProxyZmq acs(m_zmqContext);
        return acs.mountTapeReadWrite(vid, dynamic_cast<const AcsLibrarySlot&>(slot));
      }
    case TAPE_LIBRARY_TYPE_MANUAL: 
      {
        MmcProxyLog mmc(m_log);
        return mmc.mountTapeReadWrite(vid, dynamic_cast<const ManualLibrarySlot&>(slot));
      }
    case TAPE_LIBRARY_TYPE_SCSI:
      {
        RmcProxyTcpIp rmc;
        return rmc.mountTapeReadWrite(vid, dynamic_cast<const ScsiLibrarySlot&>(slot));
      }
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
void MediaChangerFacade::dismountTape(const std::string &vid, const LibrarySlot &slot) {
  try {
    const TapeLibraryType libraryType = slot.getLibraryType();
  
    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS:
      {
        AcsProxyZmq acs(m_zmqContext);
        return acs.dismountTape(vid, dynamic_cast<const AcsLibrarySlot&>(slot));
      }
    case TAPE_LIBRARY_TYPE_MANUAL:
      {
        MmcProxyLog mmc(m_log);
        return mmc.dismountTape(vid, dynamic_cast<const ManualLibrarySlot&>(slot));
      }
    case TAPE_LIBRARY_TYPE_SCSI:
      {
        RmcProxyTcpIp rmc;
        return rmc.dismountTape(vid, dynamic_cast<const ScsiLibrarySlot&>(slot));
      }
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
void MediaChangerFacade::forceDismountTape(const std::string &vid, const LibrarySlot &slot) {
  try {
    const TapeLibraryType libraryType = slot.getLibraryType();

    // Dispatch the appropriate helper method depending on library slot type
    switch(libraryType) {
    case TAPE_LIBRARY_TYPE_ACS:
      {
        AcsProxyZmq acs(m_zmqContext);
        return acs.forceDismountTape(vid, dynamic_cast<const AcsLibrarySlot&>(slot));
      }
    case TAPE_LIBRARY_TYPE_MANUAL:
      {
        MmcProxyLog mmc(m_log);
        return mmc.forceDismountTape(vid, dynamic_cast<const ManualLibrarySlot&>(slot));
      }
    case TAPE_LIBRARY_TYPE_SCSI:
      {
        RmcProxyTcpIp rmc;
        return rmc.forceDismountTape(vid, dynamic_cast<const ScsiLibrarySlot&>(slot));
      }
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
