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

class Tape {

public:

  /**
   * Constructor
   */
  Tape();

  /**
   * Destructor
   */
  ~Tape() throw();

  void setBusy(const bool busy);
  bool getBusy() const;

  void setCapacityInBytes(const uint64_t capacityInBytes);
  uint64_t getCapacityInBytes() const;

  void setComment(const std::string &comment);
  std::string getComment() const;

  void setCreationLog(const cta::dataStructures::EntryLog &creationLog);
  cta::dataStructures::EntryLog getCreationLog() const;

  void setDataOnTapeInBytes(const uint64_t dataOnTapeInBytes);
  uint64_t getDataOnTapeInBytes() const;

  void setDisabled(const bool disabled);
  bool getDisabled() const;

  void setFull(const bool full);
  bool getFull() const;

  void setLastFSeq(const uint64_t lastFSeq);
  uint64_t getLastFSeq() const;

  void setLastModificationLog(const cta::dataStructures::EntryLog &lastModificationLog);
  cta::dataStructures::EntryLog getLastModificationLog() const;

  void setLogicalLibraryName(const std::string &logicalLibraryName);
  std::string getLogicalLibraryName() const;

  void setTapePoolName(const std::string &tapePoolName);
  std::string getTapePoolName() const;

  void setVid(const std::string &vid);
  std::string getVid() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  bool m_busy;
  bool m_busySet;

  uint64_t m_capacityInBytes;
  bool m_capacityInBytesSet;

  std::string m_comment;
  bool m_commentSet;

  cta::dataStructures::EntryLog m_creationLog;
  bool m_creationLogSet;

  uint64_t m_dataOnTapeInBytes;
  bool m_dataOnTapeInBytesSet;

  bool m_disabled;
  bool m_disabledSet;

  bool m_full;
  bool m_fullSet;

  uint64_t m_lastFSeq;
  bool m_lastFSeqSet;

  cta::dataStructures::EntryLog m_lastModificationLog;
  bool m_lastModificationLogSet;

  std::string m_logicalLibraryName;
  bool m_logicalLibraryNameSet;

  std::string m_tapePoolName;
  bool m_tapePoolNameSet;

  std::string m_vid;
  bool m_vidSet;

}; // class Tape

} // namespace dataStructures
} // namespace cta
