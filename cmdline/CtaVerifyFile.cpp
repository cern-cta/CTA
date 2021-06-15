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

#include <stdexcept>
#include <iostream>
#include <map>

#include "CtaFrontendApi.hpp"
#include "version.h"

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
const std::runtime_error Usage("Usage: cta-verify-file archiveFileID [vid]");

/*
 * Fill a Notification message from the command-line parameters and stdin
 *
 * @param[out]   notification    The protobuf to fill
 * @param[in]    config          The XrdSsiPb object containing the configuration parameters
 * @param[in]    archiveFileId   The ID of the file to retrieve
 * @param[in]    vid             If non-empty, restrict PREPARE requests to this tape only
 */
void fillNotification(cta::eos::Notification &notification, const std::string &archiveFileId, const std::string &vid)
{
  XrdSsiPb::Config config(config_file, "eos");

  // WF
  notification.mutable_wf()->set_event(cta::eos::Workflow::PREPARE);

  for(auto &conf_option : std::vector<std::string>({ "instance", "requester.user", "requester.group" })) {
    if(!config.getOptionValueStr(conf_option).first) {
      throw std::runtime_error(conf_option + " must be specified in " + config_file);
    }
  }
  notification.mutable_wf()->mutable_instance()->set_name(config.getOptionValueStr("instance").second);
  notification.mutable_wf()->set_requester_instance("cta-verify-file");
  notification.mutable_wf()->set_verify_only(true);
  notification.mutable_wf()->set_vid(vid);

  // CLI
  notification.mutable_cli()->mutable_user()->set_username(config.getOptionValueStr("requester.user").second);
  notification.mutable_cli()->mutable_user()->set_groupname(config.getOptionValueStr("requester.group").second);

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

  if(argc == 3) {
    vid = argv[2];
  } else if(argc != 2) {
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
  if(getenv("XRDDEBUG")) {
    config.set("log", "all");
  }
  // If fine-grained control over log level is required, use XrdSsiPbLogLevel
  config.getEnv("log", "XrdSsiPbLogLevel");

  // Parse the command line arguments: fill the Notification fields
  fillNotification(notification, argv[1], vid);

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

  return 0;
}
