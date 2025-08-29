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
#include <sstream>
#include <iostream>
#include <fstream>

#include "CtaAdminGrpcCmd.hpp"
#include <cmdline/CtaAdminTextFormatter.hpp>
#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "callback_api/CtaAdminClientReadReactor.hpp"

static std::string file2string(std::string filename){
    std::ifstream as_stream(filename);
    std::ostringstream as_string;
    as_string << as_stream.rdbuf();
    return as_string.str();
}

namespace cta::admin {

// Implement the send() method here, by wrapping the Admin rpc call
void CtaAdminGrpcCmd::send(const CtaAdminParsedCmd& parsedCmd, cta::common::Config& config, const std::string& config_file) const {
  const auto &request = parsedCmd.getRequest();
  // Validate the Protocol Buffer
  try {
    validateCmd(request.admincmd());
  } catch (std::runtime_error& ex) {
    parsedCmd.throwUsage(ex.what());
  }

  // now construct the Admin call
  // get a client stub in order to make it
  grpc::ClientContext context;
  grpc::Status status;
  std::shared_ptr<grpc::ChannelCredentials> credentials;
  grpc::SslCredentialsOptions ssl_options;

  auto endpoint = config.getOptionValueStr("cta.endpoint");
  if (!endpoint.has_value()) {
    std::cout << "Configuration error: cta.endpoint missing from " + config_file << std::endl;
    throw std::runtime_error("Configuration error: cta.endpoint missing from " + config_file);
  }
  auto tls = config.getOptionValueBool("grpc.tls.enabled").value_or(false);
  auto caCert = config.getOptionValueStr("grpc.tls.chain_cert_path");

  if (tls) {
    if (caCert) {
      std::string caCertContents = file2string(caCert.value());
      ssl_options.pem_root_certs = caCertContents;
    } else {
      ssl_options.pem_root_certs = "";
    }
    credentials = grpc::SslCredentials(ssl_options);
  } else {
    credentials = grpc::InsecureChannelCredentials();
  }

  if (!isStreamCmd(request.admincmd())) {
    cta::xrd::Response response;

    // need method to obtain credentials - configuration will tell me which credentials to use
    std::unique_ptr<cta::xrd::CtaRpc::Stub> client_stub = cta::xrd::CtaRpc::NewStub(grpc::CreateChannel(endpoint.value(), credentials));

    // do all the filling in of the command to send
    status = client_stub->Admin(&context, request, &response);

    if (!status.ok()) {
      switch (status.error_code()) {
        case ::grpc::StatusCode::INVALID_ARGUMENT:
          throw cta::exception::UserError(status.error_message());
        case ::grpc::StatusCode::FAILED_PRECONDITION:
          throw cta::exception::Exception(status.error_message());
        case ::grpc::StatusCode::UNKNOWN:
        default:
          throw std::runtime_error(status.error_message());
      }
    }
  }
  else {
    // insecure channel credentials won't work anymore, need TLS
    std::unique_ptr<cta::xrd::CtaRpcStream::Stub> client_stub = cta::xrd::CtaRpcStream::NewStub(grpc::CreateChannel(endpoint.value(), credentials));
    // Also create a ClientReadReactor instance to handle the command
    try {
      auto client_reactor = CtaAdminClientReadReactor(client_stub.get(), parsedCmd);
      status = client_reactor.Await();
      if (!status.ok()) {
        std::cout << "gRPC call failed. Error code: " + std::to_string(status.error_code()) + " Error message: " + status.error_message() << std::endl;
      }
      // close the json delimiter, open is done inside command execution
      if (parsedCmd.isJson()) {
        std::cout << CtaAdminParsedCmd::jsonCloseDelim();
      }
    } catch (std::exception &ex) {
      // what to do in catch? Maybe print an error?
      std::cout << "An exception was thrown in CtaAdminClientReactor: " << ex.what() << std::endl;
    }
  }
}

}  // namespace cta::admin

/*!
* Start here
*
* @param    argc[in]    The number of command-line arguments
* @param    argv[in]    The command-line arguments
*/

int main(int argc, const char** argv) {
  using namespace cta::admin;

  try {
    // Parse the command line arguments
    CtaAdminParsedCmd parsedCmd(argc, argv);
    // get the grpc endpoint from the config? but for now, use
    std::string config_file = parsedCmd.getConfigFilePath();
    cta::common::Config config(config_file);

    CtaAdminGrpcCmd cmd;
    // Send the protocol buffer
    cmd.send(parsedCmd, config, config_file);

    // Delete all global objects allocated by libprotobuf
    google::protobuf::ShutdownProtobufLibrary();

    return 0;
  } catch (cta::exception::UserError& ex) {
    std::cerr << ex.getMessageValue() << std::endl;
    return 2;
  } catch (cta::exception::Exception& ex) {
    std::cerr << ex.getMessageValue() << std::endl;
  } catch (std::runtime_error& ex) {
    std::cerr << ex.what() << std::endl;
  } catch (std::exception& ex) {
    std::cerr << "Caught exception: " << ex.what() << std::endl;
  } catch (...) {
    std::cerr << "Caught an unknown exception" << std::endl;
  }

  return 1;
}
