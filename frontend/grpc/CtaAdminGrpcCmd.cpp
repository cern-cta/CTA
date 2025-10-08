/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
*/

#include <filesystem>
#include <sstream>
#include <iostream>

#include "CtaAdminGrpcCmd.hpp"
#include <cmdline/CtaAdminTextFormatter.hpp>
#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "common/config/Config.hpp"
#include "callback_api/CtaAdminClientReadReactor.hpp"

namespace cta::admin {

// Implement the send() method here, by wrapping the Admin rpc call
void CtaAdminGrpcCmd::send(const CtaAdminParsedCmd& parsedCmd, std::string endpoint) const {
  const auto& request = parsedCmd.getRequest();
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

  if (!isStreamCmd(request.admincmd())) {
    cta::xrd::Response response;

    std::unique_ptr<cta::xrd::CtaRpc::Stub> client_stub =
      cta::xrd::CtaRpc::NewStub(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));

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
  } else {
    std::unique_ptr<cta::xrd::CtaRpcStream::Stub> client_stub =
      cta::xrd::CtaRpcStream::NewStub(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));
    // Create a ClientReadReactor instance to handle the command
    auto client_reactor = CtaAdminClientReadReactor(client_stub.get(), parsedCmd);
    status = client_reactor.Await();
    if (!status.ok()) {
      throw cta::exception::UserError(status.error_message());
    }
    // close the json delimiter, open is done inside command execution
    if (parsedCmd.isJson()) {
      std::cout << CtaAdminParsedCmd::jsonCloseDelim();
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
    auto endpoint = config.getOptionValueStr("cta.endpoint");
    if (!endpoint.has_value()) {
      throw std::runtime_error("Configuration error: cta.endpoint missing from " + config_file);
    }
    CtaAdminGrpcCmd cmd;
    // Send the protocol buffer
    cmd.send(parsedCmd, endpoint.value());

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
