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

#include "CtaFrontendApi.hpp"
#include "version.h"
#include "CtaCmdOptions.hpp"

const std::string config_file = "/etc/cta/cta-cli.conf";

// Define XRootD SSI Alert message callback
namespace XrdSsiPb {
/*
 * Alert callback.
 *
 * Defines how Alert messages should be logged by EOS (or directed to the User)
 */
template<>
void RequestCallback<cta::xrd::Alert>::operator()(const cta::xrd::Alert &alert)
{
  std::cout << "AlertCallback():" << std::endl;
  XrdSsiPb::Log::DumpProtobuf(XrdSsiPb::Log::PROTOBUF, &alert);
}
} // namespace XrdSsiPb

// Attribute map type
typedef std::map<std::string, std::string> AttrMap;

// Usage exception
const std::runtime_error Usage("Usage: cta-verify-file <archiveFileID> [-v <vid>] "
                               "[-i <instance>] [-r.user <user>] [-r.group <group>]\n"
                               "cta-verify-file <archiveFileID> [--vid <id>] "
                               "[--instance <instance>] [--request.user <user>] [--request.group <group>]");

StringOption option_instance {"--instance", "-i", true};
StringOption option_request_user {"--request.user", "-r.user", true};
StringOption option_request_group {"--request.group", "-r.group", true};
StringOption option_vid {"--vid", "-v", true};

std::vector<StringOption*> verify_options = {&option_instance, &option_request_user, &option_request_group, &option_vid};

std::map<std::string, StringOption*> option_map = {
        {"-i", &option_instance},
        {"--instance", &option_instance},
        {"-r.user", &option_request_user},
        {"--request.user", &option_request_user},
        {"-r.group", &option_request_group},
        {"--request.group", &option_request_group},
        {"-v", &option_vid},
        {"--vid", &option_vid},
};

void validate_cmd() {
    for (auto &it: option_map) {
        auto option = it.second;
        if (!option->is_optional() && !option->is_present()) {
            std::cout << "Error: Option " << option->get_name() << " is mandatory but not present." << std::endl;
            throw Usage;
        }
    }
}

void parse_cmd(const int argc, const char *const *const argv) {
    for (int i = 2; i < argc; i += 2) {
        auto search = option_map.find(argv[i]);
        if (search == option_map.end()) {
            std::cout << "Error: Unknown option: " << argv[i] << std::endl;
            throw Usage;
        }
        auto option = search->second;
        option->set_present();
        option->set_value(argv[i + 1]);
    }
}

/*
 * Fill a Notification message from the command-line parameters and stdin
 *
 * @param[out]   notification    The protobuf to fill
 * @param[in]    argc            Number of arguments passed on the command line
 * @param[in]    argv            Command line arguments array
 */
void fillNotification(cta::eos::Notification &notification, const int argc, const char *const *const argv)
{   
  XrdSsiPb::Config config(config_file, "eos");
  for (const auto &conf_option : std::vector<std::string>({ "instance", "requester.user", "requester.group" })) {
    if (!config.getOptionValueStr(conf_option).first) {
      throw std::runtime_error(conf_option + " must be specified in " + config_file);
    }
  }
  notification.mutable_wf()->mutable_instance()->set_name(config.getOptionValueStr("instance").second);
  notification.mutable_cli()->mutable_user()->set_username(config.getOptionValueStr("requester.user").second);
  notification.mutable_cli()->mutable_user()->set_groupname(config.getOptionValueStr("requester.group").second);

  parse_cmd(argc, argv);
  validate_cmd();

  if (option_instance.is_present()) {
    notification.mutable_wf()->mutable_instance()->set_name(option_instance.get_value());
  }
  if (option_request_user.is_present()) {
    notification.mutable_cli()->mutable_user()->set_username(option_request_user.get_value());  
  }
  if (option_request_group.is_present()) {
    notification.mutable_cli()->mutable_user()->set_groupname(option_request_group.get_value());
  }  

  std::string archiveFileId(argv[1]);

  // WF
  notification.mutable_wf()->set_event(cta::eos::Workflow::PREPARE);
  notification.mutable_wf()->set_requester_instance("cta-verify-file");
  notification.mutable_wf()->set_verify_only(true);
  notification.mutable_wf()->set_vid(option_vid.get_value());

  
  // Transport
  notification.mutable_transport()->set_dst_url("file://dummy");

  // File
  notification.mutable_file()->set_lpath("dummy");

  // eXtended attributes
  AttrMap xattrs;
  xattrs["sys.archive.file_id"] = archiveFileId;

  for(auto &xattr : xattrs) {
      google::protobuf::MapPair<std::string,std::string> mp(xattr.first, xattr.second);
      notification.mutable_file()->mutable_xattr()->insert(mp);
  }
}


/*
 * Sends a Notification to the CTA XRootD SSI server
 */
int exceptionThrowingMain(int argc, const char *const *const argv)
{
  std::string vid;

  if (argc == 1 || argc % 2) { // Since all options need values associated, argc should always be even
    throw Usage;
  }

  // Verify that the Google Protocol Buffer header and linked library versions are compatible
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  cta::xrd::Request request;
  cta::eos::Notification &notification = *(request.mutable_notification());

  // Set client version
  request.set_client_cta_version(CTA_VERSION);
  request.set_client_xrootd_ssi_protobuf_interface_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);

  // Set configuration options
  XrdSsiPb::Config config(config_file, "cta");
  config.set("resource", "/ctafrontend");

  // Allow environment variables to override config file
  config.getEnv("request_timeout", "XRD_REQUESTTIMEOUT");

  // If XRDDEBUG=1, switch on all logging
  if (getenv("XRDDEBUG")) {
    config.set("log", "all");
  }
  // If fine-grained control over log level is required, use XrdSsiPbLogLevel
  config.getEnv("log", "XrdSsiPbLogLevel");

  // Parse the command line arguments: fill the Notification fields
  fillNotification(notification, argc, argv);

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

  return 0;
}


/*
 * Start here
 */
int main(int argc, const char **argv)
{
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
