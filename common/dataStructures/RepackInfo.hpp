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

  void setCreationLog(const cta::dataStructures::EntryLog &creationLog);
  cta::dataStructures::EntryLog getCreationLog() const;

  void setErrors(const std::map<int,std::string> &errors);
  std::map<int,std::string> getErrors() const;

  void setFilesFailed(const uint64_t filesFailed);
  uint64_t getFilesFailed() const;

  void setFilesMigrated(const uint64_t filesMigrated);
  uint64_t getFilesMigrated() const;

  void setFilesToMigr(const uint64_t filesToMigr);
  uint64_t getFilesToMigr() const;

  void setFilesToRecall(const uint64_t filesToRecall);
  uint64_t getFilesToRecall() const;

  void setRepackStatus(const std::string &repackStatus);
  std::string getRepackStatus() const;

  void setRepackType(const cta::dataStructures::RepackType &repackType);
  cta::dataStructures::RepackType getRepackType() const;

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

  cta::dataStructures::EntryLog m_creationLog;
  bool m_creationLogSet;

  std::map<int,std::string> m_errors;
  bool m_errorsSet;

  uint64_t m_filesFailed;
  bool m_filesFailedSet;

  uint64_t m_filesMigrated;
  bool m_filesMigratedSet;

  uint64_t m_filesToMigr;
  bool m_filesToMigrSet;

  uint64_t m_filesToRecall;
  bool m_filesToRecallSet;

  std::string m_repackStatus;
  bool m_repackStatusSet;

  cta::dataStructures::RepackType m_repackType;
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
} // namespace cta
