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

#include "castor/tape/tapeserver/file/LabelSession.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"

namespace castor::tape::tapeFile {

void LabelSession::label(tapeserver::drive::DriveInterface *drive, const std::string &vid, const bool lbp)  {
  VOL1 vol1;
  if (lbp) {
    // we only support crc32c LBP method
    vol1.fill(vid, SCSI::logicBlockProtectionMethod::CRC32C);
  } else {
    vol1.fill(vid, SCSI::logicBlockProtectionMethod::DoNotUse);
  }
  drive->writeBlock(&vol1, sizeof(vol1));
  HDR1PRELABEL prelabel;
  prelabel.fill(vid);
  drive->writeBlock(&prelabel, sizeof(prelabel));
  drive->writeSyncFileMarks(1);
}

} // namespace castor::tape::tapeFile
