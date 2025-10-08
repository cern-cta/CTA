/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <memory>
#include <string>

#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/CtaReadSession.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"

namespace castor::tape::tapeFile {

CtaReadSession::CtaReadSession(tapeserver::drive::DriveInterface &drive,
  const tapeserver::daemon::VolumeInfo &volInfo, const bool useLbp)
  : ReadSession(drive, volInfo, useLbp) {
  m_drive.rewind();
  m_drive.disableLogicalBlockProtection();
  {
    VOL1 vol1;
    m_drive.readExactBlock(reinterpret_cast<void *>(&vol1), sizeof(vol1),
      "[ReadSession::ReadSession()] - Reading VOL1");
    switch (vol1.getLBPMethod()) {
      case SCSI::logicBlockProtectionMethod::CRC32C:
        m_detectedLbp = true;
        if (m_useLbp) {
          m_drive.enableCRC32CLogicalBlockProtectionReadOnly();
        } else {
          m_drive.disableLogicalBlockProtection();
        }
        break;
      case SCSI::logicBlockProtectionMethod::ReedSolomon:
        throw cta::exception::Exception("In ReadSession::ReadSession(): "
            "ReedSolomon LBP method not supported");
      case SCSI::logicBlockProtectionMethod::DoNotUse:
        m_drive.disableLogicalBlockProtection();
        m_detectedLbp = false;
        break;
      default:
        throw cta::exception::Exception("In ReadSession::ReadSession(): unknown LBP method");
    }
  }

  // from this point the right LBP mode should be set or not set
  m_drive.rewind();
  {
    VOL1 vol1;
    m_drive.readExactBlock(reinterpret_cast<void *>(&vol1), sizeof(vol1),
      "[ReadSession::ReadSession()] - Reading VOL1");
    try {
      vol1.verify();
    } catch (std::exception &e) {
      throw TapeFormatError(e.what());
    }
    HeaderChecker::checkVOL1(vol1, volInfo.vid);
    // after which we are at the end of VOL1 header
    // (i.e. beginning of HDR1 of the first file) on success, or at BOT in case of exception
  }
}

} // namespace castor::tape::tapeFile
