/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <memory>
#include <string>

#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/EnstoreLargeReadSession.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "common/exception/Exception.hpp"

namespace castor::tape::tapeFile {

EnstoreLargeReadSession::EnstoreLargeReadSession(tapeserver::drive::DriveInterface& drive,
                                                 const tapeserver::daemon::VolumeInfo& volInfo,
                                                 const bool useLbp) :
ReadSession(drive, volInfo, useLbp) {
  m_drive.rewind();
  m_drive.disableLogicalBlockProtection();
  m_detectedLbp = false;

  VOL1 vol1;

  // VOL1 on the EnstoreLarge tapes is longer than the existing ones.
  // Throw away the end and validate the beggining as a normal VOL1
  size_t blockSize = 256 * 1024;
  char* data = new char[blockSize + 1];
  size_t bytes_read = m_drive.readBlock(data, blockSize);
  if (bytes_read < sizeof(vol1)) {
    delete[] data;
    throw cta::exception::Exception(std::string(__FUNCTION__) + " failed: Too few bytes read from label");
  }
  memcpy(&vol1, data, sizeof(vol1));
  delete[] data;

  // Tapes should have label character 3, but if they were recycled from CPIO tapes, it could be 0
  try {
    vol1.verify("3");
  } catch (std::exception& e) {
    try {
      vol1.verify("0");
    } catch (std::exception& e) {
      throw TapeFormatError(e.what());
    };
  };
  HeaderChecker::checkVOL1(vol1, volInfo.vid);
  // after which we are at the end of VOL1 header (e.g. beginning of first file)
  m_drive.readFileMark(std::string(__FUNCTION__) + " failed: Reading filemark");
}

}  // namespace castor::tape::tapeFile
