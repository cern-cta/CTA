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

#include "DiskSystem.hpp"
#include "JSONDiskSystem.hpp"
#include <algorithm>
#include "common/exception/Exception.hpp"
#include "common/threading/SubProcess.hpp"
#include "common/exception/Errnum.hpp"
#include "common/utils/utils.hpp"
#include "JSONFreeSpace.hpp"
#include "common/json/object/JSONObjectException.hpp"

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
// DiskSystemList::getDSName()
//------------------------------------------------------------------------------
std::string DiskSystemList::getDSName(const std::string& fileURL) const {
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
// DiskSystemList::setFetchEosFreeSpaceScript()
//------------------------------------------------------------------------------
void DiskSystemList::setFetchEosFreeSpaceScript(const std::string& path){
  m_fetchEosFreeSpaceScript = path;
}

//------------------------------------------------------------------------------
// DiskSystemList::getFetchEosFreeSpaceScript()
//------------------------------------------------------------------------------
std::string DiskSystemList::getFetchEosFreeSpaceScript() const{
  return m_fetchEosFreeSpaceScript;
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
  //Key = diskSystemName, Value = failureReason
  std::map<std::string, cta::exception::Exception> failedToFetchDiskSystems;
  for (auto const & ds: diskSystems) {
    uint64_t freeSpace = 0;
    bool updateCatalogue = false;
    auto &diskSystem = m_systemList.at(ds);
    auto &diskInstanceSpace = diskSystem.diskInstanceSpace;  
    try {
      std::vector<std::string> regexResult;
      const auto currentTime = static_cast<uint64_t>(time(nullptr));
      if (diskInstanceSpace.lastRefreshTime + diskInstanceSpace.refreshInterval >= currentTime) {
        // use the value in the catalogue, it is still fresh
        freeSpace = diskSystem.diskInstanceSpace.freeSpace;
        goto found;
      }
      updateCatalogue = true;
      const auto &freeSpaceQueryUrl = getDiskSystemFreeSpaceQueryURL(diskSystem);
      regexResult = eosDiskSystem.exec(freeSpaceQueryUrl);
      if (regexResult.size()) {
        //Script, then EOS free space query
        if(!m_systemList.getFetchEosFreeSpaceScript().empty()){
          //Script is provided
          try {
            cta::disk::JSONDiskSystem jsoncDiskSystem(diskSystem);
            freeSpace = fetchEosFreeSpaceWithScript(m_systemList.getFetchEosFreeSpaceScript(),jsoncDiskSystem.getJSON(),lc);
            goto found;
          } catch(const cta::disk::FetchEosFreeSpaceScriptException &ex){
            cta::log::ScopedParamContainer spc(lc);
            spc.add("exceptionMsg",ex.getMessageValue());
            std::string errorMsg = "In DiskSystemFreeSpaceList::fetchDiskSystemFreeSpace(), unable to get the EOS free space with the script " 
                    + m_systemList.getFetchEosFreeSpaceScript() + ". Will run eos space ls -m to fetch the free space for backpressure";
            lc.log(cta::log::INFO,errorMsg);
          }
        }
        freeSpace = fetchEosFreeSpace(regexResult.at(1), regexResult.at(2), lc);
        goto found;
      } 
      regexResult = constantFreeSpaceDiskSystem.exec(freeSpaceQueryUrl);
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

    if (updateCatalogue) {
      catalogue.modifyDiskInstanceSpaceFreeSpace(diskInstanceSpace.name, diskInstanceSpace.diskInstance, freeSpace);   
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
      goto spaceNameFound;
    }
  } while (!spStdoutIss.eof());
  throw cta::disk::FetchEosFreeSpaceException("In DiskSystemFreeSpaceList::fetchEosFreeSpace(): could not find the \""+spaceName+"\" in the eos space ls -m result.");
  
spaceNameFound:
  // Look for the parameters in the result line.
  utils::Regex rwSpaceRegex("sum.stat.statfs.freebytes\\?configstatus@rw=([0-9]+) ");
  auto rwSpaceRes = rwSpaceRegex.exec(defaultSpaceLine);
  if (rwSpaceRes.empty())
    throw cta::disk::FetchEosFreeSpaceException(
        "In DiskSystemFreeSpaceList::fetchEosFreeSpace(): failed to parse parameter sum.stat.statfs.capacity?configstatus@rw.");
  return utils::toUint64(rwSpaceRes.at(1));
}

//------------------------------------------------------------------------------
// DiskSystemFreeSpaceList::fetchFileSystemFreeSpace()
//------------------------------------------------------------------------------
uint64_t DiskSystemFreeSpaceList::fetchConstantFreeSpace(const std::string& instanceAddress, log::LogContext & lc) {
  return utils::toUint64(instanceAddress);
}

//------------------------------------------------------------------------------
// DiskSystemFreeSpaceList::fetchEosFreeSpaceWithScript()
//------------------------------------------------------------------------------
uint64_t DiskSystemFreeSpaceList::fetchEosFreeSpaceWithScript(const std::string& scriptPath, const std::string& jsonInput, log::LogContext& lc){
  cta::threading::SubProcess sp(scriptPath,{scriptPath},jsonInput);
  sp.wait();
  try {
    std::string errMsg = "In DiskSystemFreeSpaceList::fetchEosFreeSpaceWithScript(), failed to call \"" + scriptPath;
    exception::Errnum::throwOnNonZero(sp.exitValue(),errMsg);
  } catch (exception::Exception & ex) {
    ex.getMessage() << " scriptPath: " << scriptPath << " stderr: " << sp.stderr();
    throw cta::disk::FetchEosFreeSpaceScriptException(ex.getMessage().str());
  }
  if (sp.wasKilled()) {
    std::string errMsg = "In DiskSystemFreeSpaceList::fetchEosFreeSpaceWithScript(): " + scriptPath + " killed by signal: ";
    exception::Exception ex(errMsg);
    ex.getMessage() << utils::toString(sp.killSignal());
    throw cta::disk::FetchEosFreeSpaceScriptException(ex.getMessage().str());
  }
  //Get the JSON result from stdout and return the free space
  JSONFreeSpace jsonFreeSpace;
  std::istringstream spStdoutIss(sp.stdout());
  std::string stdoutScript = spStdoutIss.str();
  try {
    jsonFreeSpace.buildFromJSON(stdoutScript);
    std::string logMessage = "In DiskSystemFreeSpaceList::fetchEosFreeSpaceWithScript(), freeSpace returned from the script is: " + std::to_string(jsonFreeSpace.m_freeSpace); 
    lc.log(log::DEBUG,logMessage);
    return jsonFreeSpace.m_freeSpace;
  } catch(const cta::exception::JSONObjectException &ex){
    std::string errMsg = "In DiskSystemFreeSpaceList::fetchEosFreeSpaceWithScript(): the json received from the script "+ scriptPath + 
            " json=" + stdoutScript + " could not be used to get the FreeSpace, the json to receive from the script should have the following format: " +
            jsonFreeSpace.getExpectedJSONToBuildObject() + ".";
    throw cta::disk::FetchEosFreeSpaceScriptException(errMsg);
  }
}

}} // namespace cta::disk