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

// #include "frontend/common/AdminCmd.hpp"
#include "CtaAdminCmdStreaming.hpp"
#include <cmdline/CtaAdminTextFormatter.hpp>
#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "CtaAdminClientReadReactor.hpp"
#include "cmdline/CtaAdminCmdParser.hpp"


namespace cta::admin {

// Implement the send() method here, by wrapping the Admin rpc call
void CtaAdminCmdStreaming::send(const CtaAdminParsedCmd& parsedCmd) {

  std::cout << "In the send() method of cta-admin-grpc-stream" << std::endl;
  const auto &request = parsedCmd.getRequest();
  // Validate the Protocol Buffer
  try {
    cta::admin::validateCmd(request.admincmd());
  }
  catch (std::runtime_error& ex) {
    parsedCmd.throwUsage(ex.what());
  }

  // now make the appropriate async rpc call
  // get a client stub in order to make it
  grpc::ClientContext context;
  grpc::Status status;
  cta::xrd::Response response;
  // get the grpc endpoint from the config? but for now, use 
  std::string endpoint("cta-frontend:10955");
  // I guess here we do not need the CtaRpc class, we need the CtaRpcStream class, or a new one, or whatever
  std::unique_ptr<cta::xrd::CtaRpcStream::Stub> client_stub = cta::xrd::CtaRpcStream::NewStub(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));
  // Also create a ClientReadReactor instance to handle the command

  // we could also have the switch-case logic inside the ClientReadReactor?
  try {
    auto client_reactor = CtaAdminClientReadReactor(client_stub.get(), &request);
    status = client_reactor.Await();
    if (status.ok()) {
      std::cout << "rpc call succeeded!" << std::endl;
    } else {
      std::cout << "gRPC call failed. Error code: " + std::to_string(status.error_code()) + " Error message: " + status.error_message() << std::endl;
    }
  } catch (std::exception &ex) {
    // what to do in catch? Maybe print an error?
    std::cout << "An exception was thrown in CtaAdminClientReactor" << std::endl;
    throw ex;
  }
  
  // then check the response result, if it's not an error response we are good to continue
  // if (!status.ok()) {
  //   switch (response.type()) {
  //     case cta::xrd::Response::RSP_ERR_PROTOBUF:
  //     case cta::xrd::Response::RSP_ERR_CTA:
  //     case cta::xrd::Response::RSP_ERR_USER:
  //     default:
  //      throw std::runtime_error(response.message_txt());
  //      break;
  //   }
  // }
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
    CtaAdminParsedCmd parsedCmd(argc, argv); // this will throw the usage, which is runtime_error
    CtaAdminCmdStreaming cmd;
    // Send the protocol buffer
    cmd.send(parsedCmd);

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

// I need to keep:
// exceptionThrowingMain
// code that parses the command
// code that creates a client stub and then sends the Admin rpc call to the server (we expect the server to be up and running and ready to serve the requests, at a known address)
// figure out later how to notify of the server address, I guess via the config is a good way

// Which server will be serving these commands?
// I can in the beginning create a grpc server of my own, perhaps the best solution for now but later I should have the same server
// as the rest of the commands implement this.