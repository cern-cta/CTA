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

#include "DiskSystem.hpp"

#include <algorithm>
#include "common/exception/Exception.hpp"
#include "common/threading/SubProcess.hpp"
#include "common/exception/Errnum.hpp"
#include "common/utils/utils.hpp"

namespace cta {
namespace disk {

//------------------------------------------------------------------------------
// DiskSystemList::at()
//------------------------------------------------------------------------------
const DiskSystem& DiskSystemList::at(const std::string& name) const {
  auto dsi = std::find_if(begin(), end(), [&](const DiskSystem& ds){ return ds.name == name;});
  if (dsi != end()) return *dsi;
  throw std::out_of_range("In DiskSystemList::at(): name not found.");
}

//------------------------------------------------------------------------------
// DiskSystemList::getFSNAme()
//------------------------------------------------------------------------------
std::string DiskSystemList::getDSNAme(const std::string& fileURL) const {
  // First if the regexes have not been created yet, do so.
  if (m_pointersAndRegexes.empty() && size()) {
    for (const auto &ds: *this) {
      m_pointersAndRegexes.emplace_back(ds, ds.fileRegexp);
    }
  }
  // Try and find the fileURL
  auto pri = std::find_if(m_pointersAndRegexes.begin(), m_pointersAndRegexes.end(), 
      [&](const PointerAndRegex & pr){ return !pr.regex.exec(fileURL).empty(); });
  if (pri != m_pointersAndRegexes.end()) {
    // We found a match. Let's move the pointer and regex to the front so next file will be faster (most likely).
    if (pri != m_pointersAndRegexes.begin())
      m_pointersAndRegexes.splice(m_pointersAndRegexes.begin(), m_pointersAndRegexes, pri);
    return pri->ds.name;
  }
  throw std::out_of_range("In DiskSystemList::getDSNAme(): not match for fileURL");
}

//------------------------------------------------------------------------------
// DiskSystemFreeSpaceList::fetchFileSystemFreeSpace()
//------------------------------------------------------------------------------
void DiskSystemFreeSpaceList::fetchDiskSystemFreeSpace(const std::set<std::string>& diskSystems, log::LogContext & lc) {
  // The real deal: go fetch the file system's free space.
  cta::utils::Regex eosDiskSystem("^eos:(.*):(.*)$");
  // For testing purposes
  cta::utils::Regex constantFreeSpaceDiskSystem("^constantFreeSpace:(.*)");
  //Key = diskSystemName, Value = failureReason
  std::map<std::string, cta::exception::Exception> failedToFetchDiskSystems;
  for (auto const & ds: diskSystems) {
    uint64_t freeSpace = 0;
    try {
      std::vector<std::string> regexResult;
      regexResult = eosDiskSystem.exec(m_systemList.at(ds).freeSpaceQueryURL);
      if (regexResult.size()) {
        freeSpace = fetchEosFreeSpace(regexResult.at(1), regexResult.at(2), lc);
        goto found;
      }
      regexResult = constantFreeSpaceDiskSystem.exec(m_systemList.at(ds).freeSpaceQueryURL);
      if (regexResult.size()) {
        freeSpace = fetchConstantFreeSpace(regexResult.at(1), lc);
        goto found;
      }
      throw cta::disk::FetchEosFreeSpaceException("In DiskSystemFreeSpaceList::fetchDiskSystemFreeSpace(): could not interpret free space query URL.");
    } catch (const cta::disk::FetchEosFreeSpaceException &ex) {
      failedToFetchDiskSystems[ds] = ex;
    }
  found:
    DiskSystemFreeSpace & entry = operator [](ds);
    entry.freeSpace = freeSpace;
    entry.fetchTime = ::time(nullptr);
    entry.targetedFreeSpace = m_systemList.at(ds).targetedFreeSpace;
  }
  if(failedToFetchDiskSystems.size()){
    cta::disk::DiskSystemFreeSpaceListException ex;
    ex.m_failedDiskSystems = failedToFetchDiskSystems;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// DiskSystemFreeSpaceList::fetchFileSystemFreeSpace()
//------------------------------------------------------------------------------
uint64_t DiskSystemFreeSpaceList::fetchEosFreeSpace(const std::string& instanceAddress, const std::string &spaceName, log::LogContext & lc) {
  threading::SubProcess sp("/usr/bin/eos", {"/usr/bin/eos", std::string("root://")+instanceAddress, "space", "ls", "-m"});
  sp.wait();
  try {
    exception::Errnum::throwOnNonZero(sp.exitValue(),
        std::string("In DiskSystemFreeSpaceList::fetchEosFreeSpace(), failed to call \"eos root://") + 
        instanceAddress + " space ls -m\"");
  } catch (exception::Exception & ex) {
    ex.getMessage() << " instanceAddress: " << instanceAddress << " stderr: " << sp.stderr();
    throw cta::disk::FetchEosFreeSpaceException(ex.getMessage().str());
  }
  if (sp.wasKilled()) {
    exception::Exception ex("In DiskSystemFreeSpaceList::fetchEosFreeSpace(): eos space ls -m killed by signal: ");
    ex.getMessage() << utils::toString(sp.killSignal());
    throw cta::disk::FetchEosFreeSpaceException(ex.getMessage().str());
  }
  // Look for the result line for default space.
  std::istringstream spStdoutIss(sp.stdout());
  std::string defaultSpaceLine;
  utils::Regex defaultSpaceRe("^.*name="+spaceName+" .*$");
  do {
    std::string spStdoutLine;
    std::getline(spStdoutIss, spStdoutLine);
    auto res = defaultSpaceRe.exec(spStdoutLine);
    if (res.size()) {
      defaultSpaceLine = res.at(0);
      goto defaultFound;
    }
  } while (!spStdoutIss.eof());
  throw cta::disk::FetchEosFreeSpaceException("In DiskSystemFreeSpaceList::fetchEosFreeSpace(): could not find the \""+spaceName+"\" in the eos space ls -m result.");
  
defaultFound:
  // Look for the parameters in the result line.
  utils::Regex rwSpaceRegex("sum.stat.statfs.capacity\\?configstatus@rw=([0-9]+) ");
  auto rwSpaceRes = rwSpaceRegex.exec(defaultSpaceLine);
  if (rwSpaceRes.empty())
    throw cta::disk::FetchEosFreeSpaceException(
        "In DiskSystemFreeSpaceList::fetchEosFreeSpace(): failed to parse parameter sum.stat.statfs.capacity?configstatus@rw.");
  utils::Regex usedSpaceRegex("sum.stat.statfs.usedbytes=([0-9]+) ");
  auto usedSpaceRes = usedSpaceRegex.exec(sp.stdout());
  if (usedSpaceRes.empty())
    throw cta::disk::FetchEosFreeSpaceException("In DiskSystemFreeSpaceList::fetchEosFreeSpace(): failed to parse parameter sum.stat.statfs.usedbytes.");
  return utils::toUint64(rwSpaceRes.at(1)) - utils::toUint64(usedSpaceRes.at(1));
}

//------------------------------------------------------------------------------
// DiskSystemFreeSpaceList::fetchFileSystemFreeSpace()
//------------------------------------------------------------------------------
uint64_t DiskSystemFreeSpaceList::fetchConstantFreeSpace(const std::string& instanceAddress, log::LogContext & lc) {
  return utils::toUint64(instanceAddress);
}



}} // namespace cta::disk