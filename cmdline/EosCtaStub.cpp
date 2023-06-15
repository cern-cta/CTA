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
#include <sstream>
#include <cryptopp/base64.h>

#include <XrdSsiPbLog.hpp>

#include <common/dataStructures/FrontendReturnCode.hpp>
#include <common/checksum/ChecksumBlobSerDeser.hpp>
#include "CtaFrontendApi.hpp"

// Define XRootD SSI Alert message callback

namespace XrdSsiPb {

/*!
 * Alert callback.
 *
 * Defines how Alert messages should be logged by EOS (or directed to the User)
 */

template<>
void RequestCallback<cta::xrd::Alert>::operator()(const cta::xrd::Alert& alert) {
  std::cout << "AlertCallback():" << std::endl;
  XrdSsiPb::Log::DumpProtobuf(XrdSsiPb::Log::PROTOBUF, &alert);
}

}  // namespace XrdSsiPb

//! Usage exception

const std::runtime_error Usage("Usage: cta-wfe-test archive|retrieve|abort|deletearchive [options] [--stderr]");

/*!
 * Fill a Notification message from the command-line parameters
 *
 * @param[out]    notification    The protobuf to fill
 * @param[in]     argval          A base64-encoded blob
 */

void base64Decode(cta::eos::Notification& notification, const std::string& argval) {
  using namespace std;

  // Recovery blob is deprecated, no need to unpack it
  return;

  string base64str(argval.substr(7));  // delete "base64:" from start of string

  // Decode Base64 blob

  string decoded;
  CryptoPP::StringSource ss1(base64str, true, new CryptoPP::Base64Decoder(new CryptoPP::StringSink(decoded)));

  // Split into file metadata and directory metadata parts

  string fmd = decoded.substr(decoded.find("fmd={ ") + 6, decoded.find(" }") - 7);
  string dmd = decoded.substr(decoded.find("dmd={ ") + 6);
  dmd.resize(dmd.size() - 3);  // remove closing space/brace/quote

  // Process file metadata

  stringstream fss(fmd);
  while (!fss.eof()) {
    string key;
    fss >> key;
    size_t eq_pos = key.find("=");
    string val(key.substr(eq_pos + 1));
    key.resize(eq_pos);

    if (key == "fid") {
      notification.mutable_file()->set_fid(stoi(val));
    }
    else if (key == "pid") {
      notification.mutable_file()->set_pid(stoi(val));
    }
    else if (key == "ctime") {
      size_t pt_pos = val.find(".");
      notification.mutable_file()->mutable_ctime()->set_sec(stoi(val.substr(0, pt_pos)));
      notification.mutable_file()->mutable_ctime()->set_nsec(stoi(val.substr(pt_pos + 1)));
    }
    else if (key == "mtime") {
      size_t pt_pos = val.find(".");
      notification.mutable_file()->mutable_mtime()->set_sec(stoi(val.substr(0, pt_pos)));
      notification.mutable_file()->mutable_mtime()->set_nsec(stoi(val.substr(pt_pos + 1)));
    }
    else if (key == "size") {
      notification.mutable_file()->set_size(stoi(val));
    }
    else if (key == "xs") {
      // In principle it's possible to set the full checksum blob with multiple checksums of different
      // types, but this is not currently supported in eos_wfe_stub. It's only possible to set one
      // checksum, which is assumed to be of type ADLER32.
      auto cs = notification.mutable_file()->mutable_csb()->add_cs();
      cs->set_type(cta::common::ChecksumBlob::Checksum::ADLER32);
      cs->set_value(cta::checksum::ChecksumBlob::HexToByteArray(val));
    }
    else if (key == "mode") {
      notification.mutable_file()->set_mode(stoi(val));
    }
    else if (key == "file") {
      notification.mutable_file()->set_lpath(val);
    }
    else {
      std::cerr << "base64Decode(): No match in protobuf for fmd:" << key << '=' << val << std::endl;
    }
  }

  // Process directory metadata

  string xattrn;

  stringstream dss(dmd);
  while (!dss.eof()) {
    string key;
    dss >> key;
    size_t eq_pos = key.find("=");
    string val(key.substr(eq_pos + 1));
    key.resize(eq_pos);

    if (key == "fid") {
      notification.mutable_directory()->set_fid(stoi(val));
    }
    else if (key == "pid") {
      notification.mutable_directory()->set_pid(stoi(val));
    }
    else if (key == "ctime") {
      size_t pt_pos = val.find(".");
      notification.mutable_directory()->mutable_ctime()->set_sec(stoi(val.substr(0, pt_pos)));
      notification.mutable_directory()->mutable_ctime()->set_nsec(stoi(val.substr(pt_pos + 1)));
    }
    else if (key == "mtime") {
      size_t pt_pos = val.find(".");
      notification.mutable_directory()->mutable_mtime()->set_sec(stoi(val.substr(0, pt_pos)));
      notification.mutable_directory()->mutable_mtime()->set_nsec(stoi(val.substr(pt_pos + 1)));
    }
    else if (key == "size") {
      notification.mutable_directory()->set_size(stoi(val));
    }
    else if (key == "mode") {
      notification.mutable_directory()->set_mode(stoi(val));
    }
    else if (key == "file") {
      notification.mutable_directory()->set_lpath(val);
    }
    else if (key == "xattrn") {
      xattrn = val;
    }
    else if (key == "xattrv") {
      // Insert extended attributes

      notification.mutable_directory()->mutable_xattr()->insert(google::protobuf::MapPair<string, string>(xattrn, val));
    }
    else {
      std::cerr << "base64Decode(): No match in protobuf for dmd:" << key << '=' << val << std::endl;
    }
  }
}

/*!
 * Fill a Notification message from the command-line parameters
 *
 * @param[out]    notification    The protobuf to fill
 * @param[in]     argc            The number of command-line arguments
 * @param[in]     argv            The command-line arguments
 */

void fillNotification(cta::eos::Notification& notification, int argc, const char* const* const argv) {
  // First argument must be a valid command specifying which workflow action to execute

  if (argc < 2) {
    throw Usage;
  }

  const std::string wf_command(argv[1]);

  if (wf_command == "archive") {
    notification.mutable_wf()->set_event(cta::eos::Workflow::CLOSEW);
  }
  else if (wf_command == "retrieve") {
    notification.mutable_wf()->set_event(cta::eos::Workflow::PREPARE);
  }
  else if (wf_command == "abort") {
    notification.mutable_wf()->set_event(cta::eos::Workflow::ABORT_PREPARE);
  }
  else if (wf_command == "deletearchive") {
    notification.mutable_wf()->set_event(cta::eos::Workflow::DELETE);
  }
  else {
    throw Usage;
  }

  // Parse the remaining command-line options

  for (int arg = 2; arg < argc; ++arg) {
    const std::string argstr(argv[arg]);

    if (argstr.substr(0, 2) != "--" || argc == ++arg) {
      throw std::runtime_error("Arguments must be provided as --key value pairs");
    }

    const std::string argval(argv[arg]);

    if (argstr == "--instance") {
      notification.mutable_wf()->mutable_instance()->set_name(argval);
    }
    else if (argstr == "--srcurl") {
      notification.mutable_wf()->mutable_instance()->set_url(argval);
    }

    else if (argstr == "--user") {
      notification.mutable_cli()->mutable_user()->set_username(argval);
    }
    else if (argstr == "--group") {
      notification.mutable_cli()->mutable_user()->set_groupname(argval);
    }

    else if (argstr == "--reportURL") {
      notification.mutable_transport()->set_report_url(argval);  // for archive WF
    }
    else if (argstr == "--dsturl") {
      notification.mutable_transport()->set_dst_url(argval);  // for retrieve WF
    }

    else if (argstr == "--diskid") {
      notification.mutable_file()->set_fid(std::stoi(argval));
    }
    else if (argstr == "--diskfileowner") {
      notification.mutable_file()->mutable_owner()->set_uid(std::stoi(argval));
    }
    else if (argstr == "--diskfilegroup") {
      notification.mutable_file()->mutable_owner()->set_gid(std::stoi(argval));
    }
    else if (argstr == "--size") {
      notification.mutable_file()->set_size(std::stoi(argval));
    }
    else if (argstr == "--checksumvalue") {
      // In principle it's possible to set the full checksum blob with multiple checksums of different
      // types, but this is not currently supported in eos_wfe_stub. It's only possible to set one
      // checksum, which is assumed to be of type ADLER32.
      auto cs = notification.mutable_file()->mutable_csb()->add_cs();
      cs->set_type(cta::common::ChecksumBlob::Checksum::ADLER32);
      cs->set_value(cta::checksum::ChecksumBlob::HexToByteArray(argval));
    }
    else if (argstr == "--diskfilepath") {
      notification.mutable_file()->set_lpath(argval);
    }
    else if (argstr == "--storageclass") {
      google::protobuf::MapPair<std::string, std::string> sc("sys.archive.storage_class", argval);
      notification.mutable_file()->mutable_xattr()->insert(sc);
    }
    else if (argstr == "--id") {
      google::protobuf::MapPair<std::string, std::string> id("sys.archive.file_id", argval);
      notification.mutable_file()->mutable_xattr()->insert(id);
    }
    else {
      throw std::runtime_error("Unrecognised key " + argstr);
    }
  }
}

/*!
 * Sends a Notification to the CTA XRootD SSI server
 *
 * @param    argc[in]    The number of command-line arguments
 * @param    argv[in]    The command-line arguments
 */

int exceptionThrowingMain(int argc, const char* const* const argv) {
  // Verify that the Google Protocol Buffer header and linked library versions are compatible
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  cta::xrd::Request request;
  cta::eos::Notification& notification = *(request.mutable_notification());

  // Parse the command line arguments: fill the Notification fields
  fillNotification(notification, argc, argv);

  // Set configuration options
  XrdSsiPb::Config config("/etc/cta/cta-cli.conf", "cta");
  config.set("resource", "/ctafrontend");

  // Allow environment variables to override config file
  config.getEnv("request_timeout", "XRD_REQUESTTIMEOUT");

  // If XRDDEBUG=1, switch on all logging
  if (getenv("XRDDEBUG")) {
    config.set("log", "all");
  }
  // If fine-grained control over log level is required, use XrdSsiPbLogLevel
  config.getEnv("log", "XrdSsiPbLogLevel");

  // Obtain a Service Provider
  XrdSsiPbServiceType cta_service(config);

  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  cta_service.Send(request, response);

  // Handle responses
  switch (response.type()) {
    using namespace cta::xrd;

    case Response::RSP_SUCCESS:
      std::cout << response.message_txt() << std::endl;
      break;
    case Response::RSP_ERR_PROTOBUF:
      throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_CTA:
      throw std::runtime_error(response.message_txt());
    case Response::RSP_ERR_USER:
      throw std::runtime_error(response.message_txt());
    // ... define other response types in the protocol buffer (e.g. user error)
    default:
      throw XrdSsiPb::PbException("Invalid response type.");
  }

  // Delete all global objects allocated by libprotobuf
  google::protobuf::ShutdownProtobufLibrary();

  return 0;
}

/*!
 * Start here
 *
 * @param    argc[in]    The number of command-line arguments
 * @param    argv[in]    The command-line arguments
 */

int main(int argc, const char** argv) {
  try {
    return exceptionThrowingMain(argc, argv);
  }
  catch (XrdSsiPb::PbException& ex) {
    std::cerr << "Error in Google Protocol Buffers: " << ex.what() << std::endl;
  }
  catch (XrdSsiPb::XrdSsiException& ex) {
    std::cerr << "Error from XRootD SSI Framework: " << ex.what() << std::endl;
  }
  catch (std::exception& ex) {
    std::cerr << "Caught exception: " << ex.what() << std::endl;
  }
  catch (...) {
    std::cerr << "Caught an unknown exception" << std::endl;
  }

  return cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry;
}
