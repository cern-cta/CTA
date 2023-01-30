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

#include <algorithm>
#include <string>

#include "tapeserver/castor/tape/tapeserver/file/Exceptions.hpp"
#include "tapeserver/castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "tapeserver/castor/tape/tapeserver/file/WriteSession.hpp"
#include "tapeserver/castor/tape/tapeserver/file/Structures.hpp"

namespace castor {
namespace tape {
namespace tapeFile {

WriteSession::WriteSession(tapeserver::drive::DriveInterface &drive,
  const tapeserver::daemon::VolumeInfo& volInfo,
  const uint32_t last_fSeq, const bool compression,
  const bool useLbp)
  : m_drive(drive), m_vid(volInfo.vid), m_compressionEnabled(compression),
    m_useLbp(useLbp), m_corrupted(false), m_locked(false),
    m_volInfo(volInfo), m_detectedLbp(false) {
  if (!m_vid.compare("")) {
    throw cta::exception::InvalidArgument();
  }

  if (m_drive.isTapeBlank()) {
    cta::exception::Exception ex;
    ex.getMessage() << "[WriteSession::WriteSession()] - "
                    << "Tape is blank, cannot proceed with constructing the WriteSession";
    throw ex;
  }

  m_drive.rewind();
  m_drive.disableLogicalBlockProtection();
  {
    VOL1 vol1;
    m_drive.readExactBlock(reinterpret_cast<void *>(&vol1), sizeof(vol1),
      "[WriteSession::WriteSession()] - Reading VOL1");
    switch (vol1.getLBPMethod()) {
      case SCSI::logicBlockProtectionMethod::CRC32C:
        m_detectedLbp = true;
        if (m_useLbp) {
          m_drive.enableCRC32CLogicalBlockProtectionReadWrite();
        } else {
          cta::exception::Exception ex;
          ex.getMessage() << "[WriteSession::WriteSession()] - Tape is "
            "labeled with crc32c logical block protection but tapserverd "
            "started without LBP support";
          throw ex;
        }
        break;
      case SCSI::logicBlockProtectionMethod::ReedSolomon:
        throw cta::exception::Exception("In WriteSession::WriteSession(): "
            "ReedSolomon LBP method not supported");
      case SCSI::logicBlockProtectionMethod::DoNotUse:
        m_drive.disableLogicalBlockProtection();
        m_detectedLbp = false;
        break;
      default:
        throw cta::exception::Exception("In WriteSession::WriteSession(): unknown LBP method");
    }
  }

  // from this point the right LBP mode should be set or not set
  m_drive.rewind();
  {
    VOL1 vol1;
    m_drive.readExactBlock(reinterpret_cast<void *>(&vol1), sizeof(vol1),
      "[WriteSession::WriteSession()] - Reading VOL1");
    try {
      vol1.verify();
    } catch (std::exception &e) {
      throw TapeFormatError(e.what());
    }
    HeaderChecker::checkVOL1(vol1, m_vid);  // now we know that we are going to write on the correct tape
  }
  // if the tape is not empty let's move to the last trailer
  if (last_fSeq > 0) {
    // 3 file marks per file but we want to read the last trailer (hence the -1)
    uint32_t dst_filemark = last_fSeq * 3 - 1;
    m_drive.spaceFileMarksForward(dst_filemark);

    EOF1 eof1;
    EOF2 eof2;
    UTL1 utl1;
    m_drive.readExactBlock(reinterpret_cast<void *>(&eof1), sizeof(eof1),
      "[WriteSession::WriteSession] - Reading EOF1");
    m_drive.readExactBlock(reinterpret_cast<void *>(&eof2), sizeof(eof2),
      "[WriteSession::WriteSession] - Reading EOF2");
    m_drive.readExactBlock(reinterpret_cast<void *>(&utl1), sizeof(utl1),
      "[WriteSession::WriteSession] - Reading UTL1");
    // after this we should be where we want, i.e. at the end of the last trailer of the last file on tape
    m_drive.readFileMark("[WriteSession::WriteSession] - Reading file mark at the end of file trailer");

    // the size of the trailers is fine, now let's check each trailer
    try {
      eof1.verify();
      eof2.verify();
      utl1.verify();
    } catch (std::exception & e) {
      throw TapeFormatError(e.what());
    }

    // trailers are valid here, let's see if they contain the right info, i.e. are we in the correct place?
    // we disregard eof1 and eof2 on purpose as they contain no useful information for us now, we now check the fSeq
    // in utl1 (hdr1 also contains fSeq info but it is modulo 10000, therefore useless)
    HeaderChecker::checkUTL1(utl1, last_fSeq);
    m_lastWrittenFSeq = last_fSeq;
  } else {
    // else we are already where we want to be: at the end of the 80 bytes of the VOL1,
    // all ready to write the headers of the first file
    m_lastWrittenFSeq = 0;
  }
  // now we need to get two pieces of information that will end up in the headers and trailers
  // that we will write (siteName, hostName)
  setSiteName();
  setHostName();
}

std::string WriteSession::getLBPMode() {
    if (m_useLbp && m_detectedLbp)
      return "LBP_On";
    else if (!m_useLbp && m_detectedLbp)
      return "LBP_Off_but_present";
    else if (!m_detectedLbp)
      return "LBP_Off";
    throw cta::exception::Exception("In WriteSession::getLBPMode(): unexpected state");
}

void WriteSession::setHostName()  {
  char *hostname_cstr = new char[MAX_UNIX_HOSTNAME_LENGTH];
  cta::exception::Errnum::throwOnMinusOne(gethostname(hostname_cstr, MAX_UNIX_HOSTNAME_LENGTH),
    "Failed gethostname() in WriteFile::setHostName");
  m_hostName = hostname_cstr;
  std::transform(m_hostName.begin(), m_hostName.end(), m_hostName.begin(), ::toupper);
  m_hostName = m_hostName.substr(0, m_hostName.find("."));
  delete[] hostname_cstr;
}

void WriteSession::setSiteName()  {
  std::ifstream resolv;
  resolv.exceptions(std::ifstream::failbit|std::ifstream::badbit);
  try {
    resolv.open("/etc/resolv.conf", std::ifstream::in);
    std::string buf;
    const char * const toFind = "search ";
    while (std::getline(resolv, buf)) {
      if (buf.substr(0, 7) == toFind) {
        m_siteName = buf.substr(7);
        m_siteName = m_siteName.substr(0, m_siteName.find("."));
        std::transform(m_siteName.begin(), m_siteName.end(), m_siteName.begin(), ::toupper);
        break;
      }
    }
    resolv.close();
  }
  catch (const std::ifstream::failure& e) {
    throw cta::exception::Exception(
      std::string("In /etc/resolv.conf : error opening/closing or can't find search domain [") + e.what() + "]");
  }
}

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
