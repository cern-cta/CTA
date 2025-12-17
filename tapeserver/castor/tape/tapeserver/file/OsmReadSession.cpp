/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/file/OsmReadSession.hpp"

#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/OsmFileStructure.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"

#include <memory>
#include <string>

namespace castor::tape::tapeFile {

OsmReadSession::OsmReadSession(tapeserver::drive::DriveInterface& drive,
                               const tapeserver::daemon::VolumeInfo& volInfo,
                               const bool useLbp)
    : ReadSession(drive, volInfo, useLbp) {
  m_drive.rewind();
  m_drive.disableLogicalBlockProtection();

  uint8_t uiLLBPMethod = SCSI::logicBlockProtectionMethod::DoNotUse;
  osm::LABEL osmLabel;

  m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel()),
                         osm::LIMITS::MAXMRECSIZE,
                         "[OsmReadSession::OsmReadSession] - Reading OSM label - part 1");
  m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + osm::LIMITS::MAXMRECSIZE),
                         osm::LIMITS::MAXMRECSIZE,
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
                           osm::LIMITS::MAXMRECSIZE,
                           "[OsmReadSession::OsmReadSession] - Reading OSM label - part 1");
    m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + osm::LIMITS::MAXMRECSIZE),
                           osm::LIMITS::MAXMRECSIZE,
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
