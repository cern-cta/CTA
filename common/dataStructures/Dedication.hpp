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
namespace dataStructures {

class Dedication {

public:

  /**
   * Constructor
   */
  Dedication();

  /**
   * Destructor
   */
  ~Dedication() throw();

  void setComment(const std::string &comment);
  std::string getComment() const;

  void setCreationLog(const cta::dataStructures::EntryLog &creationLog);
  cta::dataStructures::EntryLog getCreationLog() const;

  void setDriveName(const std::string &driveName);
  std::string getDriveName() const;

  void setFromTimestamp(const uint64_t fromTimestamp);
  uint64_t getFromTimestamp() const;

  void setLastModificationLog(const cta::dataStructures::EntryLog &lastModificationLog);
  cta::dataStructures::EntryLog getLastModificationLog() const;

  void setReadonly(const bool readonly);
  bool getReadonly() const;

  void setTag(const std::string &tag);
  std::string getTag() const;

  void setUntilTimestamp(const uint64_t untilTimestamp);
  uint64_t getUntilTimestamp() const;

  void setUserGroup(const std::string &userGroup);
  std::string getUserGroup() const;

  void setVid(const std::string &vid);
  std::string getVid() const;

  void setWriteonly(const bool writeonly);
  bool getWriteonly() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::string m_comment;
  bool m_commentSet;

  cta::dataStructures::EntryLog m_creationLog;
  bool m_creationLogSet;

  std::string m_driveName;
  bool m_driveNameSet;

  uint64_t m_fromTimestamp;
  bool m_fromTimestampSet;

  cta::dataStructures::EntryLog m_lastModificationLog;
  bool m_lastModificationLogSet;

  bool m_readonly;
  bool m_readonlySet;

  std::string m_tag;
  bool m_tagSet;

  uint64_t m_untilTimestamp;
  bool m_untilTimestampSet;

  std::string m_userGroup;
  bool m_userGroupSet;

  std::string m_vid;
  bool m_vidSet;

  bool m_writeonly;
  bool m_writeonlySet;

}; // class Dedication

} // namespace dataStructures
} // namespace cta
