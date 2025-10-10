/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <limits>
#include <memory>
#include <sstream>
#include <string>

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "castor/tape/tapeserver/file/FileReader.hpp"
#include "castor/tape/tapeserver/file/ReadSession.hpp"
#include "scheduler/RetrieveJob.hpp"

namespace castor::tape::tapeFile {

FileReader::FileReader(ReadSession& rs, const cta::RetrieveJob& fileToRecall) :
  m_session(rs), m_positionCommandCode(fileToRecall.positioningMethod), m_LBPMode(rs.getLBPMode()) {
  if (m_session.isCorrupted()) {
    throw SessionCorrupted();
  }
  m_session.lock();
}

FileReader::~FileReader() noexcept {
  if (cta::PositioningMethod::ByFSeq == m_positionCommandCode
    && m_session.getCurrentFilePart() != PartOfFile::Header) {
    m_session.setCorrupted();
  }
  m_session.release();
}

size_t FileReader::getBlockSize()  {
  if (m_currentBlockSize < 1) {
    std::ostringstream ex_str;
    ex_str << "[FileReader::getBlockSize] - Invalid block size: " << m_currentBlockSize;
    throw TapeFormatError(ex_str.str());
  }
  return m_currentBlockSize;
}

std::string FileReader::getLBPMode() {
  return m_LBPMode;
}

void FileReader::position(const cta::RetrieveJob &fileToRecall) {
  if (cta::PositioningMethod::ByBlock == m_positionCommandCode) {
    positionByBlockID(fileToRecall);
  } else if (cta::PositioningMethod::ByFSeq == m_positionCommandCode) {
    positionByFseq(fileToRecall);
  } else {
    throw UnsupportedPositioningMode();
  }
}

} // namespace castor::tape::tapeFile
