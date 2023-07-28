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

#include <string>

#include "tapeserver/castor/tape/tapeserver/SCSI/Structures.hpp"
#include "common/Constants.hpp"

namespace castor {
namespace tape {
/**
 * Namespace managing the reading and writing of files to and from tape.
 */
namespace tapeFile {
/**
 * OSM tape
 */
namespace osm {

struct LIMITS {
public:
  static const uint64_t MVOLMAGIC = 0x070460;
  static const size_t LABELVERSIONLEN = 9;       //!< label version string
  static const size_t VOLNAMELEN = 15;           //!< volume name
  static const size_t CIDLEN = 33;               //!< client id length
  static const size_t MAXMRECSIZE = 32768;       //!< half of label block
  static const size_t MMAXCHK = 2048;

// prevent the generation of default public constructor and destructor
private:
  LIMITS() = default;
  ~LIMITS() = default;
  LIMITS(LIMITS const&) = default;
  LIMITS& operator=(LIMITS const&) = default;
};

class LABEL {
public:
  LABEL();
  ~LABEL() = default;

  inline char* rawLabel() {
    return m_tcRawLabel;
  }

  void decode(); // can throw

  void encode(uint64_t ulCreateTime, uint64_t ulExpireTime, uint64_t ulRecSize, uint64_t ulVolId,
              const std::string& strVolName, const std::string& strOwner, const std::string& strVersion); // can throw

  inline std::string owner() {
    return std::string(m_tcOwner);
  }
  inline std::string version() {
    return std::string(m_tcVersion);
  }
  inline std::string name() const {
    // Returns truncated name of the label
    return std::string(m_tcName, cta::CA_MAXVIDLEN);
  }
  inline uint64_t createTime() {
    return m_ulCreateTime;
  }
  inline uint64_t expireTime() {
    return m_ulExpireTime;
  }
  inline uint64_t recSize() {
    return m_ulRecSize;
  }
  inline uint64_t volId() {
    return m_ulVolId;
  }

  /**
   * @return the logic block protection method
   */
  inline uint8_t getLBPMethod() const {
    // SCSI::logicBlockProtectionMethod::DoNotUse
    // SCSI::logicBlockProtectionMethod::ReedSolomon
    // SCSI::logicBlockProtectionMethod::CRC32C
    return SCSI::logicBlockProtectionMethod::CRC32C;
  }

protected:
  char m_tcOwner[LIMITS::CIDLEN];
  char m_tcVersion[LIMITS::LABELVERSIONLEN];
  char m_tcName[LIMITS::VOLNAMELEN+1];
  char m_tcRawLabel[2 * LIMITS::MAXMRECSIZE];
  uint64_t m_ulCreateTime;
  uint64_t m_ulExpireTime;
  uint64_t m_ulRecSize;
  uint64_t m_ulVolId;
};

} // namespace osm
} // namespace tapeFile
} // namespace tape
} // namespace castor
