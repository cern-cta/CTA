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

#include <stdexcept>
#include <iostream>
#include <map>

#include "cmdline/standalone_cli_tools/common/CatalogueFetch.hpp"
#include "cmdline/standalone_cli_tools/common/ConnectionConfiguration.hpp"
#include "common/CmdLineArgs.hpp"
#include "common/exception/CommandLineNotParsed.hpp"
#include "common/utils/utils.hpp"
#include "CtaFrontendApi.hpp"
#include "version.h"

using namespace cta::cliTool;

const std::string g_config_file = "/etc/cta/cta-cli.conf";

// Global variable from CtaCatalogueFetch
extern std::list<std::string> g_listedVids;

/*
 * Fill a Notification message from the command-line parameters and stdin
 *
 * @param[out]   notification    The protobuf to fill
 * @param[in]    cmdLineArgs     Command line arguments
 * @param[in]    archiveFileId   Archive file id to verify
 */
void fillNotification(cta::eos::Notification &notification, const CmdLineArgs &cmdLineArgs, const std::string &archiveFileId) {
  XrdSsiPb::Config config(g_config_file, "eos");
  for (const auto &conf_option : std::vector<std::string>({ "instance", "requester.user", "requester.group" })) {
    if (!config.getOptionValueStr(conf_option).first) {
      throw std::runtime_error(conf_option + " must be specified in " + g_config_file);
    }
  }
  notification.mutable_wf()->mutable_instance()->set_name(config.getOptionValueStr("instance").second);
  notification.mutable_cli()->mutable_user()->set_username(config.getOptionValueStr("requester.user").second);
  notification.mutable_cli()->mutable_user()->set_groupname(config.getOptionValueStr("requester.group").second);

  if (cmdLineArgs.m_diskInstance) {
    notification.mutable_wf()->mutable_instance()->set_name(cmdLineArgs.m_diskInstance.value());
  }
  if (cmdLineArgs.m_requestUser) {
    notification.mutable_cli()->mutable_user()->set_username(cmdLineArgs.m_requestUser.value());
  }
  if (cmdLineArgs.m_requestGroup) {
    notification.mutable_cli()->mutable_user()->set_groupname(cmdLineArgs.m_requestGroup.value());
  }

  // WF
  notification.mutable_wf()->set_event(cta::eos::Workflow::PREPARE);
  notification.mutable_wf()->set_requester_instance("cta-verify-file");
  notification.mutable_wf()->set_verify_only(true);
  notification.mutable_wf()->set_vid(cmdLineArgs.m_vid.value());


  // Transport
  notification.mutable_transport()->set_dst_url("file://dummy");

  // File
  notification.mutable_file()->set_lpath("dummy");

  // Attribute map type
  using AttrMap= std::map<std::string, std::string>;
  AttrMap xattrs;
  xattrs["sys.archive.file_id"] = archiveFileId;

  for(auto &xattr : xattrs) {
      google::protobuf::MapPair<std::string,std::string> mp(xattr.first, xattr.second);
      notification.mutable_file()->mutable_xattr()->insert(mp);
  }
}

XrdSsiPb::Config getConfig() {
  // Set configuration options
  XrdSsiPb::Config config(g_config_file, "cta");
  config.set("resource", "/ctafrontend");

  // Allow environment variables to override config file
  config.getEnv("request_timeout", "XRD_REQUESTTIMEOUT");

  // If XRDDEBUG=1, switch on all logging
  if (getenv("XRDDEBUG")) {
    config.set("log", "all");
  }
  // If fine-grained control over log level is required, use XrdSsiPbLogLevel
  config.getEnv("log", "XrdSsiPbLogLevel");

  return config;
}

void sendVerifyRequest(const CmdLineArgs &cmdLineArgs, const std::string &archiveFileId, const XrdSsiPb::Config &config) {
  std::string vid;

  // Verify that the Google Protocol Buffer header and linked library versions are compatible
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  cta::xrd::Request request;
  cta::eos::Notification &notification = *(request.mutable_notification());

  // Parse the command line arguments: fill the Notification fields
  fillNotification(notification, cmdLineArgs, archiveFileId);

  // Obtain a Service Provider
  XrdSsiPbServiceType cta_service(config);

  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  cta_service.Send(request, response);

  // Handle responses
  switch(response.type())
  {
    using namespace cta::xrd;

    case Response::RSP_SUCCESS:         std::cout << response.xattr().at("sys.cta.objectstore.id") << std::endl; break;
    case Response::RSP_ERR_PROTOBUF:    throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_CTA:         throw std::runtime_error(response.message_txt());
    case Response::RSP_ERR_USER:        throw std::runtime_error(response.message_txt());
    default:                            throw XrdSsiPb::PbException("Invalid response type.");
  }

  // Delete all global objects allocated by libprotobuf
  google::protobuf::ShutdownProtobufLibrary();
}

/*
 * Checks if the provided vid exists
 */
void vidExists(const std::string &vid, const XrdSsiPb::Config &config) {
  auto serviceProviderPtr = std::make_unique<XrdSsiPbServiceType>(config);
  bool vidExists = CatalogueFetch::vidExists(vid, serviceProviderPtr);

  if(!vidExists) {
    throw std::runtime_error("The provided --vid does not exist in the Catalogue.");
  }
}

/*
 * Sends a Notification to the CTA XRootD SSI server
 */
int exceptionThrowingMain(int argc, char *const *const argv)
{
  using namespace cta::cliTool;

  cta::cliTool::CmdLineArgs cmdLineArgs(argc, argv, StandaloneCliTool::CTA_VERIFY_FILE);

  if(cmdLineArgs.m_help) { cmdLineArgs.printUsage(std::cout); exit(0); }

  std::vector<std::string> archiveFileIds;

  if((!cmdLineArgs.m_archiveFileId && !cmdLineArgs.m_archiveFileIds)) {
    cmdLineArgs.printUsage(std::cout);
    std::cout << "Missing command-line option: --id or --filename must be provided" << std::endl;
    throw std::runtime_error("");
  }

  if(!cmdLineArgs.m_vid) {
    cmdLineArgs.printUsage(std::cout);
    std::cout << "Missing command-line option: --vid must be provided" << std::endl;
    throw std::runtime_error("");
  }

  if(cmdLineArgs.m_archiveFileId) {
    const std::vector<std::string> ids = cta::utils::commaSeparatedStringToVector(cmdLineArgs.m_archiveFileId.value());
    for (const auto &id : ids) {
      archiveFileIds.push_back(id);
    }
  }

  if(cmdLineArgs.m_archiveFileIds) {
    for (const auto &id : cmdLineArgs.m_archiveFileIds.value()) {
      archiveFileIds.push_back(id);
    }
  }

  const XrdSsiPb::Config config = getConfig();

  vidExists(cmdLineArgs.m_vid.value(), config);

  for(const auto &archiveFileId : archiveFileIds) {
    sendVerifyRequest(cmdLineArgs, archiveFileId, config);
  }

  return 0;
}

/*
 * Start here
 */
int main(int argc, char *const *const argv) {
  try {
    return exceptionThrowingMain(argc, argv);
  } catch (XrdSsiPb::PbException &ex) {
    std::cerr << "Error in Google Protocol Buffers: " << ex.what() << std::endl;
  } catch (XrdSsiPb::XrdSsiException &ex) {
    std::cerr << "Error from XRootD SSI Framework: " << ex.what() << std::endl;
  } catch (std::exception &ex) {
    std::cerr << ex.what() << std::endl;
  } catch (...) {
    std::cerr << "Caught an unknown exception" << std::endl;
  }

  return 1;
}
