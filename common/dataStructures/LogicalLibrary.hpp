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

class LogicalLibrary {

public:

  /**
   * Constructor
   */
  LogicalLibrary();

  /**
   * Destructor
   */
  ~LogicalLibrary() throw();

  void setComment(const std::string &comment);
  std::string getComment() const;

  void setCreationLog(const cta::dataStructures::EntryLog &creationLog);
  cta::dataStructures::EntryLog getCreationLog() const;

  void setLastModificationLog(const cta::dataStructures::EntryLog &lastModificationLog);
  cta::dataStructures::EntryLog getLastModificationLog() const;

  void setName(const std::string &name);
  std::string getName() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::string m_comment;
  bool m_commentSet;

  cta::dataStructures::EntryLog m_creationLog;
  bool m_creationLogSet;

  cta::dataStructures::EntryLog m_lastModificationLog;
  bool m_lastModificationLogSet;

  std::string m_name;
  bool m_nameSet;

}; // class LogicalLibrary

} // namespace dataStructures
} // namespace cta
