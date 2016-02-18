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

#include "common/dataStructures/DriveStatus.hpp"
#include "common/dataStructures/MountType.hpp"

namespace cta {
namespace common {
namespace dataStructures {

class DriveState {

public:

  /**
   * Constructor
   */
  DriveState();

  /**
   * Destructor
   */
  ~DriveState() throw();

  void setBytesTransferedInSession(const uint64_t bytesTransferedInSession);
  uint64_t getBytesTransferedInSession() const;

  void setCurrentStateStartTime(const time_t &currentStateStartTime);
  time_t getCurrentStateStartTime() const;

  void setCurrentTapePool(const std::string &currentTapePool);
  std::string getCurrentTapePool() const;

  void setCurrentVid(const std::string &currentVid);
  std::string getCurrentVid() const;

  void setFilesTransferedInSession(const uint64_t filesTransferedInSession);
  uint64_t getFilesTransferedInSession() const;

  void setLatestBandwidth(const double &latestBandwidth);
  double getLatestBandwidth() const;

  void setLogicalLibrary(const std::string &logicalLibrary);
  std::string getLogicalLibrary() const;

  void setMountType(const cta::common::dataStructures::MountType &mountType);
  cta::common::dataStructures::MountType getMountType() const;

  void setName(const std::string &name);
  std::string getName() const;

  void setHost(const std::string &host);
  std::string getHost() const;

  void setSessionId(const uint64_t sessionId);
  uint64_t getSessionId() const;

  void setSessionStartTime(const time_t &sessionStartTime);
  time_t getSessionStartTime() const;

  void setStatus(const cta::common::dataStructures::DriveStatus &status);
  cta::common::dataStructures::DriveStatus getStatus() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  uint64_t m_bytesTransferedInSession;
  bool m_bytesTransferedInSessionSet;

  time_t m_currentStateStartTime;
  bool m_currentStateStartTimeSet;

  std::string m_currentTapePool;
  bool m_currentTapePoolSet;

  std::string m_currentVid;
  bool m_currentVidSet;

  uint64_t m_filesTransferedInSession;
  bool m_filesTransferedInSessionSet;

  double m_latestBandwidth;
  bool m_latestBandwidthSet;

  std::string m_logicalLibrary;
  bool m_logicalLibrarySet;

  cta::common::dataStructures::MountType m_mountType;
  bool m_mountTypeSet;

  std::string m_name;
  bool m_nameSet;

  std::string m_host;
  bool m_hostSet;

  uint64_t m_sessionId;
  bool m_sessionIdSet;

  time_t m_sessionStartTime;
  bool m_sessionStartTimeSet;

  cta::common::dataStructures::DriveStatus m_status;
  bool m_statusSet;

}; // class DriveState

} // namespace dataStructures
} // namespace common
} // namespace cta
