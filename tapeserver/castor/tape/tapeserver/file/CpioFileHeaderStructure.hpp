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

#pragma once

#include "castor/tape/tapeserver/file/FileReader.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Structures.hpp"
#include "common/Constants.hpp"

#include <string>
 
namespace castor::tape::tapeFile {

class CPIO {
public:
  // Limits
  static const size_t PATHLEN = 1024;
  static const size_t HEADER = 76;     // CPIO ASCII header string length
  static const size_t MAXHEADERSIZE = HEADER + PATHLEN;

  const std::string ASCIIMAGIC = "070707";
  // const std::string ASCIITRAILER = "TRAILER!!!"; // not used

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
     
} // namespace castor::tape::tapeFile
