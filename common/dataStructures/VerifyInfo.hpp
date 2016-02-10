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

#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/VerifyType.hpp"

namespace cta {
namespace common {
namespace dataStructures {

class VerifyInfo {

public:

  /**
   * Constructor
   */
  VerifyInfo();

  /**
   * Destructor
   */
  ~VerifyInfo() throw();

  void setCreationLog(const cta::common::dataStructures::EntryLog &creationLog);
  cta::common::dataStructures::EntryLog getCreationLog() const;

  void setFilesFailed(const uint64_t filesFailed);
  uint64_t getFilesFailed() const;

  void setFilesToVerify(const uint64_t filesToVerify);
  uint64_t getFilesToVerify() const;

  void setFilesVerified(const uint64_t filesVerified);
  uint64_t getFilesVerified() const;

  void setTag(const std::string &tag);
  std::string getTag() const;

  void setTotalFiles(const uint64_t totalFiles);
  uint64_t getTotalFiles() const;

  void setTotalSize(const uint64_t totalSize);
  uint64_t getTotalSize() const;

  void setVerifyStatus(const std::string &verifyStatus);
  std::string getVerifyStatus() const;

  void setVerifyType(const cta::common::dataStructures::VerifyType &verifyType);
  cta::common::dataStructures::VerifyType getVerifyType() const;

  void setVid(const std::string &vid);
  std::string getVid() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  cta::common::dataStructures::EntryLog m_creationLog;
  bool m_creationLogSet;

  uint64_t m_filesFailed;
  bool m_filesFailedSet;

  uint64_t m_filesToVerify;
  bool m_filesToVerifySet;

  uint64_t m_filesVerified;
  bool m_filesVerifiedSet;

  std::string m_tag;
  bool m_tagSet;

  uint64_t m_totalFiles;
  bool m_totalFilesSet;

  uint64_t m_totalSize;
  bool m_totalSizeSet;

  std::string m_verifyStatus;
  bool m_verifyStatusSet;

  cta::common::dataStructures::VerifyType m_verifyType;
  bool m_verifyTypeSet;

  std::string m_vid;
  bool m_vidSet;

}; // class VerifyInfo

} // namespace dataStructures
} // namespace common
} // namespace cta
