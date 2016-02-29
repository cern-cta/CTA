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

class RetrieveRequest {

public:

  /**
   * Constructor
   */
  RetrieveRequest();

  /**
   * Destructor
   */
  ~RetrieveRequest() throw();

  void setArchiveFileID(const uint64_t archiveFileID);
  uint64_t getArchiveFileID() const;

  void setDiskpoolName(const std::string &diskpoolName);
  std::string getDiskpoolName() const;

  void setDiskpoolThroughput(const uint64_t diskpoolThroughput);
  uint64_t getDiskpoolThroughput() const;

  void setDrData(const cta::common::dataStructures::DRData &drData);
  cta::common::dataStructures::DRData getDrData() const;

  void setDstURL(const std::string &dstURL);
  std::string getDstURL() const;

  void setRequester(const cta::common::dataStructures::Requester &requester);
  cta::common::dataStructures::Requester getRequester() const;

  void setCreationLog(const cta::common::dataStructures::EntryLog &creationLog);
  cta::common::dataStructures::EntryLog getCreationLog() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  uint64_t m_archiveFileID;
  bool m_archiveFileIDSet;

  std::string m_diskpoolName;
  bool m_diskpoolNameSet;

  uint64_t m_diskpoolThroughput;
  bool m_diskpoolThroughputSet;

  cta::common::dataStructures::DRData m_drData;
  bool m_drDataSet;

  std::string m_dstURL;
  bool m_dstURLSet;

  cta::common::dataStructures::Requester m_requester;
  bool m_requesterSet;

  cta::common::dataStructures::EntryLog m_creationLog;
  bool m_creationLogSet;

}; // class RetrieveRequest

} // namespace dataStructures
} // namespace common
} // namespace cta
