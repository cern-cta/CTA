/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <list>
#include <optional>
#include <set>
#include <string>

#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/Regex.hpp"

namespace cta {
namespace catalogue {

/**
 * Forward declaration.
 */
class Catalogue;

} // namespace catalogue

namespace disk {

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
CTA_GENERATE_EXCEPTION_CLASS(FreeDiskSpaceException);
CTA_GENERATE_EXCEPTION_CLASS(FreeDiskSpaceScriptException);

struct DiskSystem {
  std::string name;
  common::dataStructures::DiskInstanceSpace diskInstanceSpace;
  std::string fileRegexp;
  uint64_t targetedFreeSpace;
  time_t sleepTime;
  cta::common::dataStructures::EntryLog creationLog;
  cta::common::dataStructures::EntryLog lastModificationLog;
  std::string comment;
};

class DiskSystemList: public std::list<DiskSystem> {
  using std::list<DiskSystem>::list;

public:
  /** Get the filesystem for a given destination URL */
  std::string getDSName(const std::string &fileURL) const;

  /** Get the file system parameters from a file system name */
  const DiskSystem & at(const std::string &name) const;

  /** Get the fetch EOS free space script path. This script will be used by the backpressure */
  std::string getExternalFreeDiskSpaceScript() const;

  /** Sets the fetch EOS free space script path. This script will be used by the backpressure */
  void setExternalFreeDiskSpaceScript(const std::string & path);

private:
  struct PointerAndRegex {
    PointerAndRegex(const DiskSystem & dsys, const std::string &re): ds(dsys), regex(re) {}
    const DiskSystem & ds;
    utils::Regex regex;
  };

  mutable std::list<PointerAndRegex> m_pointersAndRegexes;
  std::string m_externalFreeDiskSpaceScript;

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
  void fetchDiskSystemFreeSpace(const std::set<std::string> &diskSystems, cta::catalogue::Catalogue &catalogue, log::LogContext & lc);
  const DiskSystemList &getDiskSystemList() { return m_systemList; }
private:
  DiskSystemList &m_systemList;
  uint64_t fetchFreeDiskSpace(const std::string & instanceAddress, const std::string & spaceName,log::LogContext & lc);
  uint64_t fetchConstantFreeSpace(const std::string & instanceAddress, log::LogContext & lc);
  uint64_t fetchFreeDiskSpaceWithScript(const std::string & scriptPath, const std::string & jsonInput, log::LogContext &lc);
};

}} // namespace cta::disk
