/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <string>

#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/ReadSession.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"

namespace castor {
namespace tape {
namespace tapeFile {

ReadSession::ReadSession(tapeserver::drive::DriveInterface &drive,
  tapeserver::daemon::VolumeInfo volInfo, const bool useLbp)
  : m_drive(drive), m_vid(volInfo.vid), m_useLbp(useLbp), m_corrupted(false),
    m_locked(false), m_fseq(1), m_currentFilePart(PartOfFile::Header), m_volInfo(volInfo),
    m_detectedLbp(false) {
  if (!m_vid.compare("")) {
    throw cta::exception::InvalidArgument();
  }

  if (m_drive.isTapeBlank()) {
    cta::exception::Exception ex;
    ex.getMessage() << "[ReadSession::ReadSession()] - Tape is blank, cannot proceed with constructing the ReadSession";
    throw ex;
  }

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

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
