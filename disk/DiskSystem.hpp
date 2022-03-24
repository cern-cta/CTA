/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/utils/Regex.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/log/LogContext.hpp"
#include "common/optional.hpp"

#include <string>
#include <list>
#include <set>
#include <common/exception/Exception.hpp>


namespace cta { namespace disk {

/**
 * Description of a disk system as defined by operators.
 * Defines:
 *  - a name used an index
 *  - a regular expression allowing matching destination URLs for this disk system
 *  - a query URL that describes a method to query the free space from the filesystem
 *  - a refresh interval (seconds) defining how long do we use a 
 *  - a targeted free space (margin) based on the free space update latency (inherent to the file system and induced by the refresh 
 *  interval), and the expected external bandwidth from sources external to CTA.
 */
CTA_GENERATE_EXCEPTION_CLASS(FetchEosFreeSpaceException);
CTA_GENERATE_EXCEPTION_CLASS(FetchEosFreeSpaceScriptException);

struct DiskSystem {
  std::string name;
  std::string fileRegexp;
  std::string freeSpaceQueryURL;
  time_t refreshInterval;
  uint64_t targetedFreeSpace;
  time_t sleepTime;
  cta::common::dataStructures::EntryLog creationLog;
  cta::common::dataStructures::EntryLog lastModificationLog;
  std::string comment;
  cta::optional<std::string> diskInstanceName;
  cta::optional<std::string> diskInstanceSpaceName;
};

class DiskSystemList: public std::list<DiskSystem> {
  using std::list<DiskSystem>::list;
  
public:
  /** Get the filesystem for a given destination URL */
  std::string getDSName(const std::string &fileURL) const;
  
  /** Get the file system parameters from a file system name */
  const DiskSystem & at(const std::string &name) const;
  
  /** Get the fetch EOS free space script path. This script will be used by the backpressure */
  std::string getFetchEosFreeSpaceScript() const;
  
  /** Sets the fetch EOS free space script path. This script will be used by the backpressure */
  void setFetchEosFreeSpaceScript(const std::string & path);
  
private:
  struct PointerAndRegex {
    PointerAndRegex(const DiskSystem & dsys, const std::string &re): ds(dsys), regex(re) {}
    const DiskSystem & ds;
    utils::Regex regex;
  };
  
  mutable std::list<PointerAndRegex> m_pointersAndRegexes;
  std::string m_fetchEosFreeSpaceScript;
  
};

struct DiskSystemFreeSpace {
  uint64_t freeSpace;
  uint64_t targetedFreeSpace;
  time_t fetchTime;
};

class DiskSystemFreeSpaceListException: public cta::exception::Exception {
public:
  //Key = DiskSystemName
  //Value = exception
  std::map<std::string,cta::exception::Exception> m_failedDiskSystems;
};

class DiskSystemFreeSpaceList: public std::map<std::string, DiskSystemFreeSpace> {
public:
  DiskSystemFreeSpaceList(DiskSystemList &diskSystemList): m_systemList(diskSystemList) {}
  void fetchDiskSystemFreeSpace(const std::set<std::string> &diskSystems, log::LogContext & lc);
  const DiskSystemList &getDiskSystemList() { return m_systemList; }
private:
  DiskSystemList &m_systemList;
  uint64_t fetchEosFreeSpace(const std::string & instanceAddress, const std::string & spaceName,log::LogContext & lc);
  uint64_t fetchConstantFreeSpace(const std::string & instanceAddress, log::LogContext & lc);
  uint64_t fetchEosFreeSpaceWithScript(const std::string & scriptPath, const std::string & jsonInput, log::LogContext &lc);
};

}} // namespace cta::common

