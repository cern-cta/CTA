/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "tapeserver/castor/tape/tapeserver/SCSI/Structures.hpp"
#include "common/Constants.hpp"

namespace castor::tape::tapeFile::osm {

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

  char* rawLabel() {
    return m_tcRawLabel;
  }

  void decode(); // can throw

  void encode(uint64_t ulCreateTime, uint64_t ulExpireTime, uint64_t ulRecSize, uint64_t ulVolId,
              const std::string& strVolName, const std::string& strOwner, const std::string& strVersion); // can throw

  std::string owner() {
    return std::string(m_tcOwner);
  }
  std::string version() {
    return std::string(m_tcVersion);
  }
  std::string name() const {
    // Returns truncated name of the label
    return std::string(m_tcName, cta::CA_MAXVIDLEN);
  }
  uint64_t createTime() {
    return m_ulCreateTime;
  }
  uint64_t expireTime() {
    return m_ulExpireTime;
  }
  uint64_t recSize() {
    return m_ulRecSize;
  }
  uint64_t volId() {
    return m_ulVolId;
  }

  /**
   * @return the logical block protection method
   */
  uint8_t getLBPMethod() const {
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

} // namespace castor::tape::tapeFile::osm
