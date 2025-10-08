/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/tape/tapeserver/file/CpioFileHeaderStructure.hpp"
#include "castor/tape/tapeserver/file/Exceptions.hpp"

#include <cstring> // memeset
#include <string> // std::string to_string

/**
 * CPIO
 */
size_t castor::tape::tapeFile::CPIO::decode(const uint8_t* puiData, const size_t uiSize) {
  if(uiSize < (CPIO::HEADER + CPIO::PATHLEN)) {
    std::ostringstream ex_str;
    ex_str << "[CPIO::decode] - Invalid data block size: " << uiSize << " " << "the data block size is smaller then CPIO header";
    throw tape::tapeFile::TapeFormatError(ex_str.str());
  }

  /*
   *
   */
  m_strMagic = std::string(6, ' ');
  m_strFid = std::string(CPIO::PATHLEN, ' ');

  /*
  * check for old (octal) or new (HEX) filesize (string) representation
  */
  std::ostringstream strFormat;
  if(puiData[65] == 'H') {
    strFormat << "%06c%06o%06o%06o%06o%06o%06o%06o%011lo%06oH%010lX%" <<  CPIO::PATHLEN - 1 << "s";
    sscanf(reinterpret_cast<const char*>(puiData), strFormat.str().c_str(),
      &m_strMagic[0], &m_uiDev, &m_uiIno, &m_uiMode, &m_uiUid, &m_uiGid,
      &m_uiNlink, &m_uiRdev, reinterpret_cast<uint64_t*>(&m_ulMtime),
      &m_uiNameSize, &m_ui64FileSize, &m_strFid[0]);
  } else {
    strFormat << "%06c%06o%06o%06o%06o%06o%06o%06o%011lo%06o%011lo%" <<  CPIO::PATHLEN - 1 << "s";
    sscanf(reinterpret_cast<const char*>(puiData), strFormat.str().c_str(),
      &m_strMagic[0], &m_uiDev, &m_uiIno, &m_uiMode, &m_uiUid, &m_uiGid,
      &m_uiNlink, &m_uiRdev, reinterpret_cast<uint64_t*>(&m_ulMtime),
      &m_uiNameSize, &m_ui64FileSize, &m_strFid[0]);
  }
  // Trim a file name
  m_strFid.erase(m_strFid.begin() + m_uiNameSize, m_strFid.end());
  if(m_strMagic != ASCIIMAGIC) {
    return 0;
  }

  return CPIO::HEADER + m_uiNameSize;
}
/**
 *
 */
bool castor::tape::tapeFile::CPIO::valid() {
  if(m_strMagic != ASCIIMAGIC) {
    return false;
  }
  if(m_ui64FileSize == 0) {
    return false;
  }
  return true;
}
