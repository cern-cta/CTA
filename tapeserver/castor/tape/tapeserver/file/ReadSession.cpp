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

ReadSession::ReadSession(tapeserver::drive::DriveInterface& drive,
                         const tapeserver::daemon::VolumeInfo& volInfo,
                         const bool useLbp) :
m_drive(drive),
m_vid(volInfo.vid),
m_useLbp(useLbp),
m_corrupted(false),
m_locked(false),
m_fseq(1),
m_currentFilePart(PartOfFile::Header),
m_volInfo(volInfo),
m_detectedLbp(false) {
  if (!m_vid.compare("")) {
    throw cta::exception::InvalidArgument();
  }

  if (m_drive.isTapeBlank()) {
    cta::exception::Exception ex;
    ex.getMessage() << "[ReadSession::ReadSession()] - Tape is blank, cannot proceed with constructing the ReadSession";
    throw ex;
  }
}

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
