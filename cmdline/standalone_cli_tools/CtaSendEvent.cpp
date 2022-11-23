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
#include <algorithm>
#include <map>

#include <XrdSsiPbLog.hpp>

#include <common/checksum/ChecksumBlobSerDeser.hpp>
#include "CtaFrontendApi.hpp"
#include "version.h"
#include "common/CmdLineArgs.hpp"
#include "common/exception/CommandLineNotParsed.hpp"

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
const std::runtime_error Usage("Usage: eos --json fileinfo /eos/path | cta-send-event CLOSEW|PREPARE "
                               "-i/--eos.instance <instance> [-e/--eos.endpoint <url>] "
                               "-u/--request.user <user> -g/--request.group <group>");

// remove leading spaces and quotes
void ltrim(std::string &s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) {
    return !(std::isspace(ch) || ch == '"');
  }));
}

// remove trailing spaces, quotes and commas
void rtrim(std::string &s) {
  s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) {
    return !(std::isspace(ch) || ch == '"' || ch == ',');
  }).base(), s.end());
}

// parse JSON and fill attribute maps
void parseFileInfo(std::istream &in, AttrMap &attr, AttrMap &xattr)
{
  const int MaxLineLength = 4096;
  char buffer[MaxLineLength];
  bool isXattr(false);

  while(true) {
    in.getline(buffer, MaxLineLength);
    if(in.eof()) break;
    if(in.fail()) {
       throw std::runtime_error("Parse error. Line too long?");
    }

    std::string line(buffer);
    auto sep = line.find_first_of(':');
    std::string key(line.substr(0,sep));
    std::string value(line.substr(sep < line.length() ? sep+1 : line.length()));
    ltrim(key);
    rtrim(key);
    ltrim(value);
    rtrim(value);
    if(key == "xattr") { isXattr = true; continue; }
    if(key == "}") isXattr = false;
    if(value.empty()) continue;

    if(isXattr) {
      xattr[key] = value;
    } else {
      attr[key] = value;
    }
  }
}


/*
 * Fill a Notification message from the command-line parameters and stdin
 *
 * @param[out]   notification    The protobuf to fill
 * @param[in]    config          The XrdSsiPb object containing the configuration parameters
 * @param[in]    wf_command      The workflow command (CLOSEW or PREPARE)
 */
void fillNotification(cta::eos::Notification &notification, const std::string &wf_command,
                      const int argc, char *const *const argv)
{
  cta::cliTool::CmdLineArgs cmdLineArgs(argc, argv, cta::cliTool::StandaloneCliTool::CTA_SEND_EVENT);
  const std::string &eos_instance = cmdLineArgs.m_diskInstance.value();
  const std::string &eos_endpoint = cmdLineArgs.m_eosEndpoint.has_value() ? cmdLineArgs.m_eosEndpoint.value() : "localhost:1095";
  const std::string &requester_user = cmdLineArgs.m_requestUser.value();
  const std::string &requester_group = cmdLineArgs.m_requestGroup.value();

  // Set the event type
  if(wf_command == "CLOSEW") {
    notification.mutable_wf()->set_event(cta::eos::Workflow::CLOSEW);
  } else if(wf_command == "PREPARE") {
    notification.mutable_wf()->set_event(cta::eos::Workflow::PREPARE);
  } else {
    throw Usage;
  }

  // Parse the JSON input on stdin
  AttrMap attr;
  AttrMap xattrs;
  parseFileInfo(std::cin, attr, xattrs);

  std::string accessUrl = "root://" + eos_endpoint + "/" + attr["path"] + "?eos.lfn=fxid:" + attr["fxid"];
  std::string reportUrl = "eosQuery://" + eos_endpoint + "//eos/wfe/passwd?mgm.pcmd=event&mgm.fid=" + attr["fxid"] +
    "&mgm.logid=cta&mgm.event=sync::archived&mgm.workflow=default&mgm.path=/dummy_path&mgm.ruid=0&mgm.rgid=0&cta_archive_file_id=" +
    xattrs["sys.archive.file_id"];
  std::string destUrl = "root://" + eos_endpoint + "/" + attr["path"] + "?eos.lfn=fxid:" + attr["fxid"] +
    "&eos.ruid=0&eos.rgid=0&eos.injection=1&eos.workflow=retrieve_written&eos.space=default";

  // WF
  notification.mutable_wf()->mutable_instance()->set_name(eos_instance);
  notification.mutable_wf()->mutable_instance()->set_url(accessUrl);
  notification.mutable_wf()->set_requester_instance("cta-send-event");

  // CLI
  notification.mutable_cli()->mutable_user()->set_username(requester_user);
  notification.mutable_cli()->mutable_user()->set_groupname(requester_group);

  // Transport
  if(wf_command == "CLOSEW") {
    notification.mutable_transport()->set_report_url(reportUrl);
    notification.mutable_transport()->set_error_report_url("eosQuery://" + eos_endpoint +
      "//eos/wfe/passwd?mgm.pcmd=event&mgm.fid=" + attr["fxid"] + "&mgm.logid=cta" +
      "&mgm.event=sync::archive_failed" +
      "&mgm.workflow=default&mgm.path=/dummy_path&mgm.ruid=0&mgm.rgid=0" +
      "&cta_archive_file_id=" + xattrs["sys.archive.file_id"] +
      "&mgm.errmsg=");
  } else if(wf_command == "PREPARE") {
    notification.mutable_transport()->set_dst_url(destUrl);
    notification.mutable_transport()->set_error_report_url("eosQuery://" + eos_endpoint +
      "//eos/wfe/passwd?mgm.pcmd=event&mgm.fid=" + attr["fxid"] + "&mgm.logid=cta" +
      "&mgm.event=sync::retrieve_failed" +
      "&mgm.workflow=default&mgm.path=/dummy_path&mgm.ruid=0&mgm.rgid=0" +
      "&mgm.errmsg=");
  }

  // File
  notification.mutable_file()->set_fid(std::strtoul(attr["id"].c_str(), nullptr, 0));
  notification.mutable_file()->mutable_owner()->set_uid(std::stoi(attr["uid"]));
  notification.mutable_file()->mutable_owner()->set_gid(std::stoi(attr["gid"]));
  notification.mutable_file()->set_size(std::strtoul(attr["size"].c_str(), nullptr, 0));

  // In principle it's possible to set the full checksum blob with multiple checksums of different types.
  // For now we support only one checksum which is always of type ADLER32.
  auto cs = notification.mutable_file()->mutable_csb()->add_cs();
  if(attr["checksumtype"] == "adler") {
    cs->set_type(cta::common::ChecksumBlob::Checksum::ADLER32);
  }
  cs->set_value(cta::checksum::ChecksumBlob::HexToByteArray(attr["checksumvalue"]));
  notification.mutable_file()->set_lpath(attr["path"]);

  // eXtended attributes
  for(auto &xattr : xattrs) {
    google::protobuf::MapPair<std::string,std::string> mp(xattr.first, xattr.second);
    notification.mutable_file()->mutable_xattr()->insert(mp);
  }
}


/*
 * Sends a Notification to the CTA XRootD SSI server
 */
int exceptionThrowingMain(int argc, char *const *const argv)
{
  if(argc % 2) {
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
  fillNotification(notification, argv[1], argc, argv);

  // Obtain a Service Provider
  XrdSsiPbServiceType cta_service(config);

  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  cta_service.Send(request, response);

  // Handle responses
  switch(response.type())
  {
    using namespace cta::xrd;

    case Response::RSP_SUCCESS:         std::cout << response.message_txt() << std::endl; break;
    case Response::RSP_ERR_PROTOBUF:    throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_CTA:         throw std::runtime_error(response.message_txt());
    case Response::RSP_ERR_USER:        throw std::runtime_error(response.message_txt());
    // ... define other response types in the protocol buffer (e.g. user error)
    default:                            throw XrdSsiPb::PbException("Invalid response type.");
  }

  // Delete all global objects allocated by libprotobuf
  google::protobuf::ShutdownProtobufLibrary();

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
  } catch(cta::exception::CommandLineNotParsed &ex) {
    std::cerr << ex.what() << std::endl;
  } catch (XrdSsiPb::XrdSsiException &ex) {
    std::cerr << "Error from XRootD SSI Framework: " << ex.what() << std::endl;
  } catch (std::exception &ex) {
    std::cerr << "Caught exception: " << ex.what() << std::endl;
  } catch (...) {
    std::cerr << "Caught an unknown exception" << std::endl;
  }

  return 0;
}
