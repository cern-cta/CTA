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

#include <memory>
#include <string>

#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/OsmFileStructure.hpp"
#include "castor/tape/tapeserver/file/OsmReadSession.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
//#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "common/exception/Errnum.hpp"

namespace castor::tape::tapeFile {

OsmReadSession::OsmReadSession(tapeserver::drive::DriveInterface &drive,
  const tapeserver::daemon::VolumeInfo &volInfo, const bool useLbp)
  : ReadSession(drive, volInfo, useLbp) {
  m_drive.rewind();
  m_drive.disableLogicalBlockProtection();

  size_t uiRecSize = osm::LIMITS::MAXMRECSIZE;

  uint8_t uiLLBPMethod = SCSI::logicBlockProtectionMethod::DoNotUse;
  osm::LABEL osmLabel;

//  m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel()),
//    osm::LIMITS::MAXMRECSIZE,
//    "[OsmReadSession::OsmReadSession] - Reading OSM label - part 1");

// SCSI::logicBlockProtectionMethod::CRC32CLength

  try {
    m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel()),
      //osm::LIMITS::MAXMRECSIZE,
      uiRecSize,
      "[OsmReadSession::OsmReadSession] - Reading OSM label - part 1.1");
  } catch (cta::exception::Errnum &en) {
    if (en.errorNumber() == ENOMEM) {
      // try with CRC32C
      m_drive.rewind();
      // this is wrong as enabling will cause seting drive to block size 32776 !!! 
//[st3] Failed to read 32772 byte block with 32768 byte transfer.
//[st3] Failed to read 32772 byte block with 32768 byte transfer.
//[st3] Failed to read 32776 byte block with 32772 byte transfer.
// ??? so it is neccesary to read a bloc manualy by readBlock(...);
      //m_drive.enableCRC32CLogicalBlockProtectionReadOnly();
      uiRecSize = osm::LIMITS::MAXMRECSIZE + SCSI::logicBlockProtectionMethod::CRC32CLength;
      m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel()),
        //osm::LIMITS::MAXMRECSIZE,
        uiRecSize,
        "[OsmReadSession::OsmReadSession] - Reading OSM label - part 1.2"); 
    } else {
      throw;
    }
  }

//    uiBytesRead = m_session.m_drive.readBlock(pucTmpData, size);
//    // Special case - checking whether the data format contains CRC32 
//    if (cta::verifyCrc32cForMemoryBlockWithCrc32c(
//          SCSI::logicBlockProtectionMethod::CRC32CSeed, uiBytesRead, static_cast<const uint8_t*>(pucTmpData))) {
//      m_bDataWithCRC32 = true;
//      uiBytesRead -= SCSI::logicBlockProtectionMethod::CRC32CLength;
//    }

  m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + osm::LIMITS::MAXMRECSIZE),
    //osm::LIMITS::MAXMRECSIZE,
    uiRecSize,
    "[OsmReadSession::OsmReadSession] - Reading OSM label - part 2");

  try {
    osmLabel.decode();
    uiLLBPMethod = osmLabel.getLBPMethod();
  } catch (const std::exception& osmExc) {
    throw TapeFormatError(osmExc.what());
  }

  switch (uiLLBPMethod) {
    case SCSI::logicBlockProtectionMethod::CRC32C:
      m_detectedLbp = true;
      if (m_useLbp) {
        m_drive.enableCRC32CLogicalBlockProtectionReadOnly();
      } else {
        m_drive.disableLogicalBlockProtection();
      }
      break;
    case SCSI::logicBlockProtectionMethod::ReedSolomon:
      throw cta::exception::Exception("In OsmReadSession::OsmReadSession(): "
                                      "ReedSolomon LBP method not supported");
    case SCSI::logicBlockProtectionMethod::DoNotUse:
      m_drive.disableLogicalBlockProtection();
      m_detectedLbp = false;
      break;
    default:
      throw cta::exception::Exception("In OsmReadSession::OsmReadSession(): unknown LBP method");
  }
  // from this point the right LBP mode should be set or not set
  m_drive.rewind();
  {
    m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel()),
      //osm::LIMITS::MAXMRECSIZE,
      uiRecSize + 4,
      "[OsmReadSession::OsmReadSession] - Reading OSM label - part 1");
    m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + osm::LIMITS::MAXMRECSIZE),
      //osm::LIMITS::MAXMRECSIZE,
      uiRecSize + 4,
      "[OsmReadSession::OsmReadSession] - Reading OSM label - part 2");
    try {
      osmLabel.decode();
    } catch (const std::exception& e) {
      throw TapeFormatError(e.what());
    }
    HeaderChecker::checkOSM(osmLabel, volInfo.vid);
  }
  
//  HeaderChecker::checkOSM(osmLabel, volInfo.vid);
 
}

} // namespace castor::tape::tapeFile
