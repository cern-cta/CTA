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
#include "castor/tape/tapeserver/file/EnstoreReadSession.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"

namespace castor::tape::tapeFile {

EnstoreReadSession::EnstoreReadSession(tapeserver::drive::DriveInterface &drive,
  const tapeserver::daemon::VolumeInfo &volInfo, const bool useLbp)
  : ReadSession(drive, volInfo, useLbp) {
  m_drive.rewind();
  m_drive.disableLogicalBlockProtection();
  m_detectedLbp = false;

  VOL1 vol1;

  // If this is an EnstoreLarge tape recycled as Enstore, VOL1 is 240 bytes instead of 80
  // Throw away the end and validate the beggining as a normal VOL1
  size_t blockSize = 256 * 1024;
  char* data = new char[blockSize + 1];
  size_t bytes_read = m_drive.readBlock(data, blockSize);
  if (bytes_read < sizeof(vol1)) {
    delete[] data;
    throw cta::exception::Exception("Too few bytes read from label");
  }
  memcpy(&vol1, data, sizeof(vol1));
  delete[] data;

  // Tapes should have label character 0, but if they were recycled from EnstoreLarge tapes, it could be 3
  try {
    vol1.verify("0");
  } catch (std::exception& e) {
    try {
      vol1.verify("3");
    } catch (std::exception& e) {
      throw TapeFormatError(e.what());
    };
  };
  HeaderChecker::checkVOL1(vol1, volInfo.vid);
  // after which we are at the end of VOL1 header (e.g. beginning of first file)

}

} // namespace castor::tape::tapeFile
