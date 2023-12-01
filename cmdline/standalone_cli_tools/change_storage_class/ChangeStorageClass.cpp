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
#include <unordered_set>

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbIStreamBuffer.hpp>

#include "cmdline/CtaAdminCmdParse.hpp"
#include "cmdline/standalone_cli_tools/change_storage_class/ChangeStorageClass.hpp"
#include "cmdline/standalone_cli_tools/change_storage_class/JsonFileData.hpp"
#include "cmdline/standalone_cli_tools/common/CatalogueFetch.hpp"
#include "cmdline/standalone_cli_tools/common/CmdLineArgs.hpp"
#include "common/checksum/ChecksumBlob.hpp"
#include "common/exception/CommandLineNotParsed.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"
#include "CtaFrontendApi.hpp"

// GLOBAL VARIABLES : used to pass information between main thread and stream handler thread

// global synchronisation flag
extern std::atomic<bool> isHeaderSent;
extern std::list<std::string> g_storageClasses;

namespace cta::cliTool {

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

  auto serviceProvider = setXrootdConfiguration(cmdLineArgs.m_debug);
  m_serviceProviderPtr = std::move(serviceProvider);

  handleArguments(cmdLineArgs);

  storageClassesExist();

  for(const auto& [archiveFileId, storageClass]: m_archiveFileIdsAndStorageClasses) {
    updateStorageClassInCatalogue(archiveFileId, storageClass);
  }
  return 0;
}

std::unique_ptr<XrdSsiPbServiceType> ChangeStorageClass::setXrootdConfiguration(bool debug) const {
  const std::string StreamBufferSize      = "1024";                  //!< Buffer size for Data/Stream Responses
  const std::string DefaultRequestTimeout = "10";                    //!< Default Request Timeout. Can be overridden by
                                                                      //!< XRD_REQUESTTIMEOUT environment variable.
  if (debug) {
    m_log.setLogMask("DEBUG");
  } else {
    m_log.setLogMask("INFO");
  }

  // Set CTA frontend configuration options
  const std::string cli_config_file = "/etc/cta/cta-cli.conf";
  XrdSsiPb::Config cliConfig(cli_config_file, "cta");
  cliConfig.set("resource", "/ctafrontend");
  cliConfig.set("response_bufsize", StreamBufferSize);         // default value = 1024 bytes
  cliConfig.set("request_timeout", DefaultRequestTimeout);     // default value = 10s

  // Allow environment variables to override config file
  cliConfig.getEnv("request_timeout", "XRD_REQUESTTIMEOUT");

  // If XRDDEBUG=1, switch on all logging
  if(getenv("XRDDEBUG")) {
    cliConfig.set("log", "all");
  }
  // If fine-grained control over log level is required, use XrdSsiPbLogLevel
  cliConfig.getEnv("log", "XrdSsiPbLogLevel");

  // If the server is down, we want an immediate failure. Set client retry to a single attempt.
  XrdSsiProviderClient->SetTimeout(XrdSsiProvider::connect_N, 1);
  
  return std::make_unique<XrdSsiPbServiceType>(cliConfig);
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
      if(!validateUserInputFileMetadata(jsonFileDataObject.archiveFileId, jsonFileDataObject.fid, jsonFileDataObject.instance)) {
        throw exception::UserError("Archive id does not match with disk file id or disk instance, are you sure the correct file metadata was provided?");
      }
      m_archiveFileIdsAndStorageClasses.push_back(std::make_pair(jsonFileDataObject.archiveFileId, jsonFileDataObject.storageClass));
    }
  } else {
    if (!cmdLineArgs.m_storageClassName) {
      cmdLineArgs.printUsage(std::cout);
      throw exception::UserError("Missing required option when not providing a path to an input file: storage.class.name");
    }

    if ((!cmdLineArgs.m_archiveFileId || !cmdLineArgs.m_fids || !cmdLineArgs.m_diskInstance)) {
      cmdLineArgs.printUsage(std::cout);
      throw exception::UserError("Archive id, eos file id and disk instance must be provided must be provided");
    }

    m_storageClassName = cmdLineArgs.m_storageClassName.value();

    m_archiveFileIdsAndStorageClasses.push_back(std::make_pair(cmdLineArgs.m_archiveFileId.value(), cmdLineArgs.m_storageClassName.value()));
    if(!validateUserInputFileMetadata(cmdLineArgs.m_archiveFileId.value(), cmdLineArgs.m_fids.value().front(), cmdLineArgs.m_diskInstance.value())) {
      throw exception::UserError("Archive id does not match with disk file id or disk instance, are you sure the correct file metadata was provided?");
    }
  }

}

void ChangeStorageClass::storageClassesExist() const {
  std::unordered_set<std::string> storageClasses;
  for(const auto& [archiveFileId, storageClass] : m_archiveFileIdsAndStorageClasses) {
    storageClasses.insert(storageClass);
  }
  for(const auto& storageClass : storageClasses) {
    storageClassExists(storageClass);
  }
}

//------------------------------------------------------------------------------
// storageClassExists
//------------------------------------------------------------------------------
void ChangeStorageClass::storageClassExists(const std::string& storageClass) const {
  cta::xrd::Request request;
  const auto admincmd = request.mutable_admincmd();

  admincmd->set_client_version(CTA_VERSION);
  admincmd->set_protobuf_tag(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd->set_cmd(cta::admin::AdminCmd::CMD_STORAGECLASS);
  admincmd->set_subcmd(cta::admin::AdminCmd::SUBCMD_LS);

  {
    const auto key = cta::admin::OptionString::STORAGE_CLASS;
    const auto new_opt = admincmd->add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(storageClass);
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
    throw exception::UserError("The storage class " + storageClass + " has not been defined.");
  }
}

//------------------------------------------------------------------------------
// updateCatalogue
//------------------------------------------------------------------------------
void ChangeStorageClass::updateStorageClassInCatalogue(const std::string& archiveFileId, const std::string& storageClass) const {
  cta::xrd::Request request;

  const auto admincmd = request.mutable_admincmd();

  admincmd->set_client_version(CTA_VERSION);
  admincmd->set_protobuf_tag(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd->set_cmd(cta::admin::AdminCmd::CMD_ARCHIVEFILE);
  admincmd->set_subcmd(cta::admin::AdminCmd::SUBCMD_CH);

  {
    const auto key = cta::admin::OptionString::STORAGE_CLASS;
    const auto new_opt = admincmd->add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(storageClass);
  }

  {
    const auto key = cta::admin::OptionStrList::FILE_ID;
    const auto new_opt = admincmd->add_option_str_list();
    new_opt->set_key(key);
    new_opt->add_item(archiveFileId);
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

bool ChangeStorageClass::validateUserInputFileMetadata(const std::string& archiveFileId, const std::string& operatorProvidedFid, const std::string& operatorProvidedInstance) {
  const auto [diskDiskInstance, diskFileId] = CatalogueFetch::getInstanceAndFid(archiveFileId, m_serviceProviderPtr, m_log);
  return ((operatorProvidedInstance == diskDiskInstance) && (operatorProvidedFid == diskFileId));
}

} // namespace cta::cliTool
