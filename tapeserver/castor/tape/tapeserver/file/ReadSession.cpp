/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <string>

#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/ReadSession.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"

namespace castor::tape::tapeFile {

ReadSession::ReadSession(tapeserver::drive::DriveInterface &drive,
  const tapeserver::daemon::VolumeInfo &volInfo, const bool useLbp)
  : m_drive(drive), m_vid(volInfo.vid), m_useLbp(useLbp), m_corrupted(false),
    m_locked(false), m_fseq(1), m_currentFilePart(PartOfFile::Header), m_volInfo(volInfo),
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

} // namespace castor::tape::tapeFile
