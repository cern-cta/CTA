/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/file/OsmReadSession.hpp"

#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/OsmFileStructure.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "common/exception/Errnum.hpp"

#include <memory>
#include <string>

namespace castor::tape::tapeFile {

OsmReadSession::OsmReadSession(tapeserver::drive::DriveInterface& drive,
                               const tapeserver::daemon::VolumeInfo& volInfo,
                               const bool useLbp)
    : ReadSession(drive, volInfo, useLbp) {
  m_drive.rewind();
  m_drive.disableLogicalBlockProtection();

  const size_t RECSIZE_WITH_CRC32C = osm::LIMITS::MAXMRECSIZE + SCSI::logicBlockProtectionMethod::CRC32CLength;
  size_t uiRecSize = 0;
  uint8_t uiLLBPMethod = SCSI::logicBlockProtectionMethod::DoNotUse;
  osm::LABEL osmLabel;
  /*
   * Some mutated OSM labels may have extra CRC32C bytes e.g.:
   * 00 3c 00 38 00 43 00 39 93 3c 5d 26 c7 4b 67 48 
   * -----------------------|-----------|
   *   DATA BLOCK              CRC32C
   * -----------------------|-----------|-----------|
   *   DATA BLOCK              CRC32C       CRC32C
   */
  uiRecSize = m_drive.readBlock(osmLabel.rawLabel(), RECSIZE_WITH_CRC32C);

  if (uiRecSize < RECSIZE_WITH_CRC32C && uiRecSize < osm::LIMITS::MAXMRECSIZE) {
    std::ostringstream ex_str;
    ex_str << "[OsmReadSession::OsmReadSession] - Reading OSM label - part 1 - invalid block size: " << uiRecSize;
    throw TapeFormatError(ex_str.str());
  }

  m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + osm::LIMITS::MAXMRECSIZE),
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
                           uiRecSize,
                           "[OsmReadSession::OsmReadSession] - Reading OSM label - part 1");
    m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + osm::LIMITS::MAXMRECSIZE),
                           uiRecSize,
                           "[OsmReadSession::OsmReadSession] - Reading OSM label - part 2");
    try {
      osmLabel.decode();
    } catch (const std::exception& e) {
      throw TapeFormatError(e.what());
    }
    HeaderChecker::checkOSM(osmLabel, volInfo.vid);
  }
}

}  // namespace castor::tape::tapeFile
