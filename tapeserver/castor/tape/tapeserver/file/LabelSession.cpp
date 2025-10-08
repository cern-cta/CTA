/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
