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

namespace castor {
namespace tape {
namespace tapeFile {

OsmReadSession::OsmReadSession(tapeserver::drive::DriveInterface& drive,
                               const tapeserver::daemon::VolumeInfo& volInfo,
                               const bool useLbp) :
ReadSession(drive, volInfo, useLbp) {
  m_drive.rewind();
  m_drive.disableLogicalBlockProtection();

  uint8_t uiLLBPMethod = SCSI::logicBlockProtectionMethod::DoNotUse;
  osm::LABEL osmLabel;

  m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel()), osm::LIMITS::MAXMRECSIZE,
                         "[OsmReadSession::OsmReadSession] - Reading OSM label - part 1");
  m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + osm::LIMITS::MAXMRECSIZE),
                         osm::LIMITS::MAXMRECSIZE, "[OsmReadSession::OsmReadSession] - Reading OSM label - part 2");

  try {
    osmLabel.decode();
    uiLLBPMethod = osmLabel.getLBPMethod();
  }
  catch (const std::exception& osmExc) {
    throw TapeFormatError(osmExc.what());
  }

  switch (uiLLBPMethod) {
    case SCSI::logicBlockProtectionMethod::CRC32C:
      m_detectedLbp = true;
      if (m_useLbp) {
        m_drive.enableCRC32CLogicalBlockProtectionReadOnly();
      }
      else {
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
    m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel()), osm::LIMITS::MAXMRECSIZE,
                           "[OsmReadSession::OsmReadSession] - Reading OSM label - part 1");
    m_drive.readExactBlock(reinterpret_cast<void*>(osmLabel.rawLabel() + osm::LIMITS::MAXMRECSIZE),
                           osm::LIMITS::MAXMRECSIZE, "[OsmReadSession::OsmReadSession] - Reading OSM label - part 2");
    try {
      osmLabel.decode();
    }
    catch (const std::exception& e) {
      throw TapeFormatError(e.what());
    }
    HeaderChecker::checkOSM(osmLabel, volInfo.vid);
  }
}

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
