/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "castor/tape/tapeserver/file/FileReader.hpp"
#include "common/Constants.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Structures.hpp"

#include <string>

namespace castor::tape::tapeFile {

class CPIO {
public:
  // Limits
  static const size_t PATHLEN = 1024;
  static const size_t HEADER = 76;  // CPIO ASCII header string length
  static const size_t MAXHEADERSIZE = HEADER + PATHLEN;

  const std::string ASCIIMAGIC = "070707";

  std::string m_strMagic = "";
  uint32_t m_uiDev = 0;
  uint32_t m_uiIno = 0;
  uint32_t m_uiMode = 0;
  uint32_t m_uiUid = 0;
  uint32_t m_uiGid = 0;
  uint32_t m_uiNlink = 0;
  uint32_t m_uiRdev = 0;
  uint64_t m_ulMtime = 0;
  uint32_t m_uiNameSize = 0;
  uint64_t m_ui64FileSize = 0;
  std::string m_strFid = "";

  CPIO() = default;
  ~CPIO() = default;

  /**
   * Decode CPIO header from a data block.
   * @param data pointer the the data block
   * @param count size of the data block
   * @return the actual size of CPIO header
   */
  size_t decode(const uint8_t* puiData, const size_t uiSize);
  /**
   * Validate the CPIO header.
   * @return true if the header is valid
   */
  bool valid();

private:
  CPIO(CPIO const&) = default;
  CPIO& operator=(CPIO const&) = default;
};

}  // namespace castor::tape::tapeFile
