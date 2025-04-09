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

#include "CtaAdminGrpcCmd.hpp"
#include <cmdline/CtaAdminTextFormatter.hpp>
#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "common/config/Config.hpp"
#include "callback_api/CtaAdminClientReadReactor.hpp"

// This will be renamed to CtaAdminCmdGrpc

namespace cta::admin {


// Implement the send() method here, by wrapping the Admin rpc call
void CtaAdminGrpcCmd::send(const CtaAdminParsedCmd& parsedCmd, std::string endpoint) const {
  const auto &request = parsedCmd.getRequest();
  // Validate the Protocol Buffer
  try {
    validateCmd(request.admincmd());
  }
  catch (std::runtime_error& ex) {
    std::cout << "In the catch block of the send() command, caught a runtime error, about to throw usage" << std::endl;
    parsedCmd.throwUsage(ex.what());
  }

  // now construct the Admin call
  // get a client stub in order to make it
  grpc::ClientContext context;
  grpc::Status status;

  if (!isStreamCmd(request.admincmd())) {
    cta::xrd::Response response;

    std::unique_ptr<cta::xrd::CtaRpc::Stub> client_stub = cta::xrd::CtaRpc::NewStub(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));

    // do all the filling in of the command to send
    status = client_stub->Admin(&context, request, &response);
    // then check the response result, if it's not an error response we are good to continue
    if (!status.ok()) {
      std::cout << "gRPC call failed. Error code: " + std::to_string(status.error_code()) + " Error message: " + status.error_message() << std::endl;
      std::cout << "The response message text is " + response.message_txt() << std::endl;
      switch (response.type()) {
        case cta::xrd::Response::RSP_ERR_PROTOBUF:
        case cta::xrd::Response::RSP_ERR_CTA:
        case cta::xrd::Response::RSP_ERR_USER:
        default:
        throw std::runtime_error(response.message_txt()); // for some reason, the message_txt() is empty even though we have set it???
        // apparently, gRPC makes no guarantees that this will be filled in in case of failure
        break;
      }
    }
  }
  else {
    // insecure channel credentials won't work anymore, need TLS
    std::unique_ptr<cta::xrd::CtaRpcStream::Stub> client_stub = cta::xrd::CtaRpcStream::NewStub(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));
    // Also create a ClientReadReactor instance to handle the command
    // we could also have the switch-case logic inside the ClientReadReactor?
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
      std::cout << "An exception was thrown in CtaAdminClientReactor" << std::endl;
      throw ex;
    }
  }
}

} // namespace cta::admin



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
    auto endpoint = config.getOptionValueStr("cta.endpoint");
    if (!endpoint.has_value()) {
      std::cout << "Configuration error: cta.endpoint missing from " + config_file << std::endl;
      throw std::runtime_error("Configuration error: cta.endpoint missing from " + config_file);
    }
    CtaAdminGrpcCmd cmd;
    // Send the protocol buffer
    cmd.send(parsedCmd, endpoint.value());

    // Delete all global objects allocated by libprotobuf
    google::protobuf::ShutdownProtobufLibrary();

    return 0;
  } catch (std::runtime_error& ex) {
    std::cerr << ex.what() << std::endl;
  } catch (...) {
    std::cerr << "Caught some exception" << std::endl;
  }

 return 1;
}
