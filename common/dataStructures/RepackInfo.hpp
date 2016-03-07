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
#include "common/dataStructures/RepackType.hpp"

namespace cta {
namespace common {
namespace dataStructures {

class RepackInfo {

public:

  /**
   * Constructor
   */
  RepackInfo();

  /**
   * Destructor
   */
  ~RepackInfo() throw();

  void setCreationLog(const cta::common::dataStructures::EntryLog &creationLog);
  cta::common::dataStructures::EntryLog getCreationLog() const;

  void setErrors(const std::map<uint64_t,std::string> &errors);
  std::map<uint64_t,std::string> getErrors() const;

  void setFilesArchived(const uint64_t filesArchived);
  uint64_t getFilesArchived() const;

  void setFilesFailed(const uint64_t filesFailed);
  uint64_t getFilesFailed() const;

  void setFilesToArchive(const uint64_t filesToArchive);
  uint64_t getFilesToArchive() const;

  void setFilesToRetrieve(const uint64_t filesToRetrieve);
  uint64_t getFilesToRetrieve() const;

  void setRepackStatus(const std::string &repackStatus);
  std::string getRepackStatus() const;

  void setRepackType(const cta::common::dataStructures::RepackType &repackType);
  cta::common::dataStructures::RepackType getRepackType() const;

  void setTag(const std::string &tag);
  std::string getTag() const;

  void setTotalFiles(const uint64_t totalFiles);
  uint64_t getTotalFiles() const;

  void setTotalSize(const uint64_t totalSize);
  uint64_t getTotalSize() const;

  void setVid(const std::string &vid);
  std::string getVid() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  cta::common::dataStructures::EntryLog m_creationLog;
  bool m_creationLogSet;

  std::map<uint64_t,std::string> m_errors;
  bool m_errorsSet;

  uint64_t m_filesArchived;
  bool m_filesArchivedSet;

  uint64_t m_filesFailed;
  bool m_filesFailedSet;

  uint64_t m_filesToArchive;
  bool m_filesToArchiveSet;

  uint64_t m_filesToRetrieve;
  bool m_filesToRetrieveSet;

  std::string m_repackStatus;
  bool m_repackStatusSet;

  cta::common::dataStructures::RepackType m_repackType;
  bool m_repackTypeSet;

  std::string m_tag;
  bool m_tagSet;

  uint64_t m_totalFiles;
  bool m_totalFilesSet;

  uint64_t m_totalSize;
  bool m_totalSizeSet;

  std::string m_vid;
  bool m_vidSet;

}; // class RepackInfo

} // namespace dataStructures
} // namespace common
} // namespace cta
