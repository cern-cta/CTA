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

namespace cta {
namespace common {
namespace dataStructures {

class MountGroup {

public:

  /**
   * Constructor
   */
  MountGroup();

  /**
   * Destructor
   */
  ~MountGroup() throw();

  void setArchive_minBytesQueued(const uint64_t archive_minBytesQueued);
  uint64_t getArchive_minBytesQueued() const;

  void setArchive_minFilesQueued(const uint64_t archive_minFilesQueued);
  uint64_t getArchive_minFilesQueued() const;

  void setArchive_minRequestAge(const uint64_t archive_minRequestAge);
  uint64_t getArchive_minRequestAge() const;

  void setArchive_priority(const uint64_t archive_priority);
  uint64_t getArchive_priority() const;

  void setComment(const std::string &comment);
  std::string getComment() const;

  void setCreationLog(const cta::common::dataStructures::EntryLog &creationLog);
  cta::common::dataStructures::EntryLog getCreationLog() const;

  void setLastModificationLog(const cta::common::dataStructures::EntryLog &lastModificationLog);
  cta::common::dataStructures::EntryLog getLastModificationLog() const;

  void setMaxDrivesAllowed(const uint64_t maxDrivesAllowed);
  uint64_t getMaxDrivesAllowed() const;

  void setName(const std::string &name);
  std::string getName() const;

  void setRetrieve_minBytesQueued(const uint64_t retrieve_minBytesQueued);
  uint64_t getRetrieve_minBytesQueued() const;

  void setRetrieve_minFilesQueued(const uint64_t retrieve_minFilesQueued);
  uint64_t getRetrieve_minFilesQueued() const;

  void setRetrieve_minRequestAge(const uint64_t retrieve_minRequestAge);
  uint64_t getRetrieve_minRequestAge() const;

  void setRetrieve_priority(const uint64_t retrieve_priority);
  uint64_t getRetrieve_priority() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  uint64_t m_archive_minBytesQueued;
  bool m_archive_minBytesQueuedSet;

  uint64_t m_archive_minFilesQueued;
  bool m_archive_minFilesQueuedSet;

  uint64_t m_archive_minRequestAge;
  bool m_archive_minRequestAgeSet;

  uint64_t m_archive_priority;
  bool m_archive_prioritySet;

  std::string m_comment;
  bool m_commentSet;

  cta::common::dataStructures::EntryLog m_creationLog;
  bool m_creationLogSet;

  cta::common::dataStructures::EntryLog m_lastModificationLog;
  bool m_lastModificationLogSet;

  uint64_t m_maxDrivesAllowed;
  bool m_maxDrivesAllowedSet;

  std::string m_name;
  bool m_nameSet;

  uint64_t m_retrieve_minBytesQueued;
  bool m_retrieve_minBytesQueuedSet;

  uint64_t m_retrieve_minFilesQueued;
  bool m_retrieve_minFilesQueuedSet;

  uint64_t m_retrieve_minRequestAge;
  bool m_retrieve_minRequestAgeSet;

  uint64_t m_retrieve_priority;
  bool m_retrieve_prioritySet;

}; // class MountGroup

} // namespace dataStructures
} // namespace common
} // namespace cta
