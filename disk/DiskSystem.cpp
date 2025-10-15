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

#include <algorithm>
#include <map>
#include <string>

#include "catalogue/Catalogue.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "common/json/object/JSONObjectException.hpp"
#include "common/process/threading/SubProcess.hpp"
#include "common/utils/utils.hpp"
#include "disk/DiskSystem.hpp"
#include "disk/JSONDiskSystem.hpp"
#include "disk/JSONFreeSpace.hpp"

namespace cta::disk {

//------------------------------------------------------------------------------
// DiskSystemList::at()
//------------------------------------------------------------------------------
const DiskSystem& DiskSystemList::at(const std::string& name) const {
  {
    auto dsi = std::find_if(begin(), end(), [&name](const DiskSystem &ds) { return ds.name == name; });
    if (dsi != end()) return *dsi;
  }
  throw std::out_of_range("In DiskSystemList::at(): name " + name + " not found.");
}

//------------------------------------------------------------------------------
// DiskSystemList::getDSName()
//------------------------------------------------------------------------------
std::string DiskSystemList::getDSName(const std::string& fileURL) const {
  // First if the regexes have not been created yet, do so.
  if (m_pointersAndRegexes.empty() && size()) {
    for (const auto &ds : *this) {
      m_pointersAndRegexes.emplace_back(ds, ds.fileRegexp);
    }
  }
  // Try and find the fileURL
  {
    auto pri = std::find_if(m_pointersAndRegexes.begin(), m_pointersAndRegexes.end(),
                            [&fileURL](const PointerAndRegex &pr) { return !pr.regex.exec(fileURL).empty(); });
    if (pri != m_pointersAndRegexes.end()) {
      // We found a match. Let's move the pointer and regex to the front so next file will be faster (most likely).
      if (pri != m_pointersAndRegexes.begin())
        m_pointersAndRegexes.splice(m_pointersAndRegexes.begin(), m_pointersAndRegexes, pri);
      return pri->ds.name;
    }
  }
  throw std::out_of_range("In DiskSystemList::getDSNAme(): not match for fileURL");
}

//------------------------------------------------------------------------------
// DiskSystemList::setExternalFreeDiskSpaceScript()
//------------------------------------------------------------------------------
void DiskSystemList::setExternalFreeDiskSpaceScript(const std::string& path) {
  m_externalFreeDiskSpaceScript = path;
}

//------------------------------------------------------------------------------
// DiskSystemList::getExternalFreeDiskSpaceScript()
//------------------------------------------------------------------------------
std::string DiskSystemList::getExternalFreeDiskSpaceScript() const{
  return m_externalFreeDiskSpaceScript;
}

//------------------------------------------------------------------------------
// DiskSystemFreeSpaceList::updateFreeSpaceEntry()
//------------------------------------------------------------------------------
void DiskSystemFreeSpaceList::updateFreeSpaceEntry(const std::string& diskSystemName, uint64_t freeSpace, cta::catalogue::Catalogue &catalogue, bool updateCatalogue){
  DiskSystemFreeSpace & entry = operator[](diskSystemName);
  auto &diskSystem = m_systemList.at(diskSystemName);
  auto &diskInstanceSpace = diskSystem.diskInstanceSpace;
  entry.freeSpace = freeSpace;
  entry.fetchTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  entry.targetedFreeSpace = diskSystem.targetedFreeSpace;

  if (updateCatalogue) {
    catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceFreeSpace(diskInstanceSpace.name,
      diskInstanceSpace.diskInstance, freeSpace);
  }
}

//------------------------------------------------------------------------------
// DiskSystemFreeSpaceList::fetchFileSystemFreeSpace()
//------------------------------------------------------------------------------
void DiskSystemFreeSpaceList::fetchDiskSystemFreeSpace(const std::set<std::string>& diskSystems, cta::catalogue::Catalogue &catalogue, log::LogContext & lc) {
  auto getDiskSystemFreeSpaceQueryURL = [](DiskSystem ds) {
    auto dsURL = ds.diskInstanceSpace.freeSpaceQueryURL;
    // Replace URLS starting in eosSpace with eos:{diskInstanceName}
    if (dsURL.rfind("eosSpace", 0) == 0) {
      dsURL = "eos:" + ds.diskInstanceSpace.diskInstance + dsURL.substr(8);
    }
    return dsURL;
  };
  // The real deal: go fetch the file system's free space.
  cta::utils::Regex eosDiskSystem("^eos:(.*):(.*)$");
  // For testing purposes
  cta::utils::Regex constantFreeSpaceDiskSystem("^constantFreeSpace:(.*)");
  // Key = diskSystemName, Value = failureReason
  std::map<std::string, cta::exception::Exception> failedToFetchDiskSystems;
  cta::log::ScopedParamContainer spc(lc);
  for (auto const & ds : diskSystems) {
    uint64_t freeSpace = 0;
    bool updateCatalogue = false;
    auto &diskSystem = m_systemList.at(ds);
    auto &diskInstanceSpace = diskSystem.diskInstanceSpace;
    try {
      std::vector<std::string> regexResult;
      {
        const auto currentTime = static_cast<uint64_t>(std::chrono::system_clock::to_time_t(
                std::chrono::system_clock::now()));

        if (diskInstanceSpace.lastRefreshTime + diskInstanceSpace.refreshInterval >= currentTime) {
          // use the value in the catalogue, it is still fresh
          freeSpace = diskSystem.diskInstanceSpace.freeSpace;
          updateFreeSpaceEntry(ds, freeSpace, catalogue, updateCatalogue);
          continue;
        }
      }
      updateCatalogue = true;
      const auto &freeSpaceQueryUrl = getDiskSystemFreeSpaceQueryURL(diskSystem);
      regexResult = eosDiskSystem.exec(freeSpaceQueryUrl);
      // eos:ctaeos:default
      if (regexResult.size()) {
        try {
          cta::disk::JSONDiskSystem jsoncDiskSystem(diskSystem);
          std::string diskInstanceName = regexResult.at(1);
          std::string spaceName = regexResult.at(2);
          freeSpace = fetchFreeDiskSpaceWithScript(m_systemList.getExternalFreeDiskSpaceScript(), diskInstanceName, spaceName, jsoncDiskSystem.getJSON(), lc);
          updateFreeSpaceEntry(ds, freeSpace, catalogue, updateCatalogue);
          continue;
        } catch (const cta::disk::FreeDiskSpaceException &ex) {
          updateCatalogue = false; // if we update the catalogue at this point, we will update it with an old value, since we were not able to get a new one
          // but will still reset the last refresh time, so it will be essentially a "false" update; better to not update and let the lastRefreshTime reflect
          // the actual last time an up-to-date value was put in the catalogue
          spc.add("exceptionMsg", ex.getMessageValue());
          spc.add("externalScript", m_systemList.getExternalFreeDiskSpaceScript());
          const std::string errorMsg = "In DiskSystemFreeSpaceList::fetchDiskSystemFreeSpace(), unable to get the free disk space with the script."
            "Script threw runtime exception.";
          lc.log(cta::log::WARNING, errorMsg);
          throw;
        }
      }
      regexResult = constantFreeSpaceDiskSystem.exec(freeSpaceQueryUrl);
      if (regexResult.size()) {
        freeSpace = fetchConstantFreeSpace(regexResult.at(1));
        updateFreeSpaceEntry(ds, freeSpace, catalogue, updateCatalogue);
      }
      else {
        throw cta::disk::FreeDiskSpaceException("In DiskSystemFreeSpaceList::fetchDiskSystemFreeSpace(): could not interpret free space query URL.");
      }
    } catch (const cta::disk::FreeDiskSpaceException &ex) {
      failedToFetchDiskSystems[ds] = ex;
      updateFreeSpaceEntry(ds, freeSpace, catalogue, updateCatalogue);
    }
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
uint64_t DiskSystemFreeSpaceList::fetchConstantFreeSpace(const std::string& instanceAddress) {
  return utils::toUint64(instanceAddress);
}

//------------------------------------------------------------------------------
// DiskSystemFreeSpaceList::fetchFreeDiskSpaceWithScript()
//------------------------------------------------------------------------------
uint64_t DiskSystemFreeSpaceList::fetchFreeDiskSpaceWithScript(const std::string& scriptPath,
                                                               const std::string& diskInstanceName,
                                                               const std::string& spaceName,
                                                               const std::string& jsonInput,
                                                               log::LogContext& lc) {
  std::unique_ptr<cta::threading::SubProcess> sp;
  try {
    sp = std::make_unique<cta::threading::SubProcess>(scriptPath,std::list{scriptPath, diskInstanceName, spaceName},jsonInput);
  }
  // for example, if the executable is not found, this exception will not be caught here - spawning the subprocess will throw an exception
  catch (cta::exception::Errnum & ex) {
    throw cta::disk::FreeDiskSpaceException(ex.getMessage().str());
  }
  catch (cta::exception::Exception & ex) {
    throw cta::disk::FreeDiskSpaceException(ex.what());
  }
  catch (...) {
    throw cta::disk::FreeDiskSpaceException("Error spawning the subprocess to run the free disk space script.");
  }
  sp->wait();
  try {
    std::string errMsg = "In DiskSystemFreeSpaceList::fetchFreeDiskSpaceWithScript(), failed to call \"" + scriptPath;
    exception::Errnum::throwOnNonZero(sp->exitValue(),errMsg);
  } catch (exception::Exception & ex) {
    ex.getMessage() << " scriptPath: " << scriptPath << " stderr: " << sp->stderr();
    throw cta::disk::FreeDiskSpaceException(ex.getMessage().str());
  }
  if (sp->wasKilled()) {
    std::string errMsg = "In DiskSystemFreeSpaceList::fetchFreeDiskSpaceWithScript(): " + scriptPath + " killed by signal: ";
    exception::Exception ex(errMsg);
    ex.getMessage() << utils::toString(sp->killSignal());
    throw cta::disk::FreeDiskSpaceException(ex.getMessage().str());
  }
  //Get the JSON result from stdout and return the free space
  JSONFreeSpace jsonFreeSpace;
  std::istringstream spStdoutIss(sp->stdout());
  std::string stdoutScript = spStdoutIss.str();
  try {
    jsonFreeSpace.buildFromJSON(stdoutScript);
    std::string logMessage = "In DiskSystemFreeSpaceList::fetchFreeDiskSpaceWithScript(), freeSpace returned from the script is: " + std::to_string(jsonFreeSpace.m_freeSpace);
    lc.log(log::DEBUG,logMessage);
    return jsonFreeSpace.m_freeSpace;
  } catch(const cta::exception::JSONObjectException&){
    std::string errMsg = "In DiskSystemFreeSpaceList::fetchFreeDiskSpaceWithScript(): the json received from the script "+ scriptPath +
            " json=" + stdoutScript + " could not be used to get the FreeSpace, the json to receive from the script should have the following format: " +
            jsonFreeSpace.getExpectedJSONToBuildObject() + ".";
    throw cta::disk::FreeDiskSpaceException(errMsg);
  }
}

} // namespace cta::disk
