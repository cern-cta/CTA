/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/DRData.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/Requester.hpp"

namespace cta {
namespace common {
namespace dataStructures {

class ArchiveRequest {

public:

  /**
   * Constructor
   */
  ArchiveRequest();

  /**
   * Destructor
   */
  ~ArchiveRequest() throw();

  void setChecksumType(const std::string &checksumType);
  std::string getChecksumType() const;

  void setChecksumValue(const std::string &checksumValue);
  std::string getChecksumValue() const;

  void setCreationLog(const cta::common::dataStructures::EntryLog &creationLog);
  cta::common::dataStructures::EntryLog getCreationLog() const;

  void setDiskpoolName(const std::string &diskpoolName);
  std::string getDiskpoolName() const;

  void setDiskpoolThroughput(const uint64_t diskpoolThroughput);
  uint64_t getDiskpoolThroughput() const;

  void setDrData(const cta::common::dataStructures::DRData &drData);
  cta::common::dataStructures::DRData getDrData() const;

  void setEosFileID(const std::string &eosFileID);
  std::string getEosFileID() const;

  void setFileSize(const uint64_t fileSize);
  uint64_t getFileSize() const;

  void setRequester(const cta::common::dataStructures::Requester &requester);
  cta::common::dataStructures::Requester getRequester() const;

  void setSrcURL(const std::string &srcURL);
  std::string getSrcURL() const;

  void setStorageClass(const std::string &storageClass);
  std::string getStorageClass() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::string m_checksumType;
  bool m_checksumTypeSet;

  std::string m_checksumValue;
  bool m_checksumValueSet;

  cta::common::dataStructures::EntryLog m_creationLog;
  bool m_creationLogSet;

  std::string m_diskpoolName;
  bool m_diskpoolNameSet;

  uint64_t m_diskpoolThroughput;
  bool m_diskpoolThroughputSet;

  cta::common::dataStructures::DRData m_drData;
  bool m_drDataSet;

  std::string m_eosFileID;
  bool m_eosFileIDSet;

  uint64_t m_fileSize;
  bool m_fileSizeSet;

  cta::common::dataStructures::Requester m_requester;
  bool m_requesterSet;

  std::string m_srcURL;
  bool m_srcURLSet;

  std::string m_storageClass;
  bool m_storageClassSet;

}; // class ArchiveRequest

} // namespace dataStructures
} // namespace common
} // namespace cta
