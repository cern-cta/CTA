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

#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbIStreamBuffer.hpp>

#include "cmdline/CtaAdminCmdParse.hpp"
#include "cmdline/standalone_cli_tools/change_storage_class/ChangeStorageClass.hpp"
#include "cmdline/standalone_cli_tools/change_storage_class/JsonFileData.hpp"
#include "cmdline/standalone_cli_tools/common/CatalogueFetch.hpp"
#include "cmdline/standalone_cli_tools/common/CmdLineArgs.hpp"
#include "cmdline/standalone_cli_tools/common/ConnectionConfiguration.hpp"
#include "common/checksum/ChecksumBlob.hpp"
#include "common/exception/CommandLineNotParsed.hpp"
#include "common/exception/GrpcError.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"
#include "CtaFrontendApi.hpp"
#include "eos_grpc_client/GrpcEndpoint.hpp"

// GLOBAL VARIABLES : used to pass information between main thread and stream handler thread

// global synchronisation flag
extern std::atomic<bool> isHeaderSent;
extern std::list<std::string> g_storageClasses;

namespace cta {
namespace cliTool {

class ChangeStorageClassError : public std::runtime_error {
public:
  using runtime_error::runtime_error;
}; // class ChangeStorageClassError

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ChangeStorageClass::ChangeStorageClass(
  std::istream &inStream,
  std::ostream &outStream,
  std::ostream &errStream,
  cta::log::StdoutLogger &log):
  CmdLineTool(inStream, outStream, errStream),
  m_log(log) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
ChangeStorageClass::~ChangeStorageClass() = default;

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int ChangeStorageClass::exceptionThrowingMain(const int argc, char *const *const argv) {
  CmdLineArgs cmdLineArgs(argc, argv, StandaloneCliTool::CTA_CHANGE_STORAGE_CLASS);
  handleArguments(cmdLineArgs);

  auto [serviceProvider, endpointmap] = ConnConfiguration::readAndSetConfiguration(m_log, getUsername(), cmdLineArgs);
  m_serviceProviderPtr = std::move(serviceProvider);
  m_endpointMapPtr = std::move(endpointmap);

  storageClassExists();
  updateStorageClassInEosNamespace();
  updateStorageClassInCatalogue();
  writeSkippedArchiveIdsToFile();
  return 0;
}

//------------------------------------------------------------------------------
// handleArguments
//------------------------------------------------------------------------------
void ChangeStorageClass::handleArguments(const CmdLineArgs &cmdLineArgs) {
  if (cmdLineArgs.m_help) {
    cmdLineArgs.printUsage(std::cout);
    throw exception::UserError("");
  }

  if(cmdLineArgs.m_json) {
    JsonFileData jsonFilaData(cmdLineArgs.m_json.value());
    for(const auto &jsonFileDataObject : jsonFilaData.m_jsonArgumentsCollection) {
      m_archiveFileIds.push_back(jsonFileDataObject.archiveId);
    }
  }

  if (!cmdLineArgs.m_storageClassName) {
    cmdLineArgs.printUsage(std::cout);
    throw exception::UserError("Missing requried option: storage.class.name");
  }

  if (!cmdLineArgs.m_archiveFileId && !cmdLineArgs.m_json) {
    cmdLineArgs.printUsage(std::cout);
    throw exception::UserError("filename or id must be provided");
  }

  m_storageClassName = cmdLineArgs.m_storageClassName.value();

  if (cmdLineArgs.m_archiveFileId) {
    m_archiveFileIds.push_back(cmdLineArgs.m_archiveFileId.value());
  }

  if (cmdLineArgs.m_frequency) {
    m_eosUpdateFrequency = cmdLineArgs.m_frequency.value();
  } else {
    m_eosUpdateFrequency = 100;
  }
}

//------------------------------------------------------------------------------
// fileInFlight
//------------------------------------------------------------------------------
bool ChangeStorageClass::fileInFlight(const google::protobuf::RepeatedField<uint32_t> &locations) const {
  // file is not in flight if fsid==65535
  return std::all_of(std::begin(locations), std::end(locations), [](const int &location) { return location != 65535; });
}


//------------------------------------------------------------------------------
// updateStorageClassInEosNamespace
//------------------------------------------------------------------------------
void ChangeStorageClass::updateStorageClassInEosNamespace() {
  uint64_t requestCounter = 0;
  for(const auto &archiveFileId : m_archiveFileIds) {
    requestCounter++;
    if(requestCounter >= m_eosUpdateFrequency) {
      requestCounter = 0;
      sleep(1);
    }

    const auto [diskInstance, diskFileId] = CatalogueFetch::getInstanceAndFid(archiveFileId, m_serviceProviderPtr, m_log);

    // No files in flight should change storage class
    const bool showJson = false;
    if(const auto md_response = m_endpointMapPtr->getMD(diskInstance, ::eos::rpc::FILE, cta::utils::toUint64(diskFileId), "", showJson);
      fileInFlight(md_response.fmd().locations())) {
        if (md_response.fmd().locations().empty()){
          throw ChangeStorageClassError("Metadata from EOS could not be fetched: disk file id " + diskFileId + " does not exist or authentication is failing");
        }
        m_archiveIdsNotUpdatedInEos.push_back(archiveFileId);
        std::list<cta::log::Param> params;
        params.push_back(cta::log::Param("archiveFileId", archiveFileId));
        m_log(cta::log::WARNING, "File did not change storage class because the file was in flight", params);
        continue;
    }

    const auto path = m_endpointMapPtr->getPath(diskInstance, diskFileId);
    {
      std::list<cta::log::Param> params;
      params.push_back(cta::log::Param("path", path));
      m_log(cta::log::INFO, "Path found", params);
    }

    if(auto status = m_endpointMapPtr->setXAttr(diskInstance, path, "sys.archive.storage_class", m_storageClassName); status.ok()) {
      m_archiveIdsUpdatedInEos.push_back(archiveFileId);
    } else {
      m_archiveIdsNotUpdatedInEos.push_back(archiveFileId);
      std::list<cta::log::Param> params;
      params.push_back(cta::log::Param("archiveFileId", archiveFileId));
      params.push_back(cta::log::Param("error", status.error_message()));
      m_log(cta::log::WARNING, "File did not change storage class because query to EOS failed", params);
    }
  }
}

//------------------------------------------------------------------------------
// storageClassExists
//------------------------------------------------------------------------------
void ChangeStorageClass::storageClassExists() const {
  cta::xrd::Request request;
  const auto admincmd = request.mutable_admincmd();

  request.set_client_cta_version(CTA_VERSION);
  request.set_client_xrootd_ssi_protobuf_interface_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd->set_cmd(cta::admin::AdminCmd::CMD_STORAGECLASS);
  admincmd->set_subcmd(cta::admin::AdminCmd::SUBCMD_LS);

  {
    const auto key = cta::admin::OptionString::STORAGE_CLASS;
    const auto new_opt = admincmd->add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(m_storageClassName);
  }

  cta::xrd::Response response;
  auto stream_future = m_serviceProviderPtr->SendAsync(request, response);

  // Handle responses
  switch(response.type()) {
    using namespace cta::xrd;
    using namespace cta::admin;
    case Response::RSP_SUCCESS:
      // Print message text
      std::cout << response.message_txt();
      // Allow stream processing to commence
      isHeaderSent = true;
      break;
    case Response::RSP_ERR_PROTOBUF:                     throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_USER:                         throw exception::UserError(response.message_txt());
    case Response::RSP_ERR_CTA:                          throw std::runtime_error(response.message_txt());
    default:                                             throw XrdSsiPb::PbException("Invalid response type.");
  }

  // wait until the data stream has been processed before exiting
  stream_future.wait();

  for(const auto &storageClass : g_storageClasses) {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("storageClass", storageClass));
    m_log(cta::log::INFO, "Storage class found", params);
  }

  if (g_storageClasses.empty()){
    throw(exception::UserError("The storage class provided has not been defined."));
  }
}

//------------------------------------------------------------------------------
// updateCatalogue
//------------------------------------------------------------------------------
void ChangeStorageClass::updateStorageClassInCatalogue() const {
  cta::xrd::Request request;

  const auto admincmd = request.mutable_admincmd();

  request.set_client_cta_version(CTA_VERSION);
  request.set_client_xrootd_ssi_protobuf_interface_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd->set_cmd(cta::admin::AdminCmd::CMD_ARCHIVEFILE);
  admincmd->set_subcmd(cta::admin::AdminCmd::SUBCMD_CH);

  {
    const auto key = cta::admin::OptionString::STORAGE_CLASS_NAME;
    const auto new_opt = admincmd->add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(m_storageClassName);
  }

  {
    const auto key = cta::admin::OptionStrList::FILE_ID;
    const auto new_opt = admincmd->add_option_str_list();
    new_opt->set_key(key);
    for (const auto &archiveFileId : m_archiveIdsUpdatedInEos) {
      new_opt->add_item(archiveFileId);
    }
  }

  // Validate the Protocol Buffer
  try {
    cta::admin::validateCmd(*admincmd);
  } catch(std::runtime_error &ex) {
    throw std::runtime_error("Error: Protocol Buffer validation");
  }

  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  m_serviceProviderPtr->Send(request, response);

  // Handle responses
  switch(response.type()) {
    using namespace cta::xrd;
    using namespace cta::admin;
    case Response::RSP_SUCCESS:
      // Print message text
      std::cout << response.message_txt();
      break;
    case Response::RSP_ERR_PROTOBUF:                     throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_USER:                         throw exception::UserError(response.message_txt());
    case Response::RSP_ERR_CTA:                          throw std::runtime_error(response.message_txt());
    default:                                             throw XrdSsiPb::PbException("Invalid response type.");
  }
}

//------------------------------------------------------------------------------
// writeSkippedArchiveIdsToFile
//------------------------------------------------------------------------------
void ChangeStorageClass::writeSkippedArchiveIdsToFile() const {
  const std::filesystem::path filePath = "/tmp/skippedArchiveIds.txt";
  std::ofstream archiveIdFile(filePath);

  if (archiveIdFile.fail()) {
    throw std::runtime_error("Unable to open file " + filePath.string());
  }

  if (archiveIdFile.is_open()) {
    for (const auto& archiveId : m_archiveIdsNotUpdatedInEos) {
      archiveIdFile << "{ archiveId : " << archiveId << " }" << std::endl;
    }
    archiveIdFile.close();
    std::cout << m_archiveIdsNotUpdatedInEos.size() << " files did not update the storage class." << std::endl;
    std::cout << "The skipped archive ids can be found here: " << filePath << std::endl;
  }
}

} // namespace admin
} // namespace cta
