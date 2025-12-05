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

#include "CtaAdminGrpcCmd.hpp"
#include <cmdline/CtaAdminTextFormatter.hpp>
#include "tapeserver/daemon/common/TapedConfiguration.hpp"
#include "callback_api/CtaAdminClientReadReactor.hpp"
#include "AsyncClient.hpp"
#include "ClientNegotiationRequestHandler.hpp"
#include "utils.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/FileLogger.hpp"
#include "common/utils/Base64.hpp"
#include "common/utils/utils.hpp"

#include <cstdlib>  // for getenv

namespace cta::admin {

void CtaAdminGrpcCmd::setupJwtAuthenticatedAdminCall(grpc::ClientContext& context, const std::string& token_path) {
  // read the token from the path
  std::string token_contents = cta::utils::file2string(token_path);

  context.AddMetadata("authorization", "Bearer " + token_contents);
}

void CtaAdminGrpcCmd::setupKrb5AuthenticatedAdminCall(std::shared_ptr<grpc::Channel> spChannelNegotiation,
                                                      grpc::ClientContext& context,
                                                      const std::string& GSS_SPN,
                                                      cta::log::FileLogger& log) {
  // First do a negotiation call to obtain a kerberos token, which will be attached to the call metadata
  // Storage for the KRB token
  std::string strToken {""};
  // Encoded token to be send as part of metadata
  std::string strEncodedToken {""};
  cta::frontend::grpc::client::AsyncClient<cta::xrd::Negotiation> clientNeg(log, spChannelNegotiation);
  try {
    strToken = clientNeg.exe<cta::frontend::grpc::client::NegotiationRequestHandler>(GSS_SPN)->token();
    /*
     * TODO: ???
     *        Move encoder to client::NegotiationRequestHandler
     *        or server::NegotiationRequestHandler
     *        or KerberosAuthenticator
     *       ???
     */
    strEncodedToken = cta::utils::base64encode(strToken);

  } catch (const cta::exception::Exception& e) {
    /*
     * In case of any problems with the negotiation service,
     * log and stop the execution
     */
    cta::log::LogContext lc(log);
    lc.log(cta::log::CRIT,
           "In cta::frontend::grpc::client::CtaAdminGrpcCmdDeprecated::exe(): Problem with the negotiation service.");
    throw;  // rethrow
  }

  // Attach the Kerberos token directly to the ClientContext as per-call metadata
  // This is similar to how JWT authentication works, but uses a different metadata key
  context.AddMetadata("authorization", "Negotiate " + strEncodedToken);
}

// Implement the send() method here, by wrapping the Admin rpc call
void CtaAdminGrpcCmd::send(const CtaAdminParsedCmd& parsedCmd, const std::string& config_file) {
  cta::common::Config config(config_file);
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
  std::shared_ptr<grpc::ChannelCredentials> credentials;
  grpc::SslCredentialsOptions ssl_options;

  auto endpoint = config.getOptionValueStr("cta.endpoint");
  if (!endpoint.has_value()) {
    std::cout << "Configuration error: cta.endpoint missing from " + config_file << std::endl;
    throw std::runtime_error("Configuration error: cta.endpoint missing from " + config_file);
  }
  auto tls = config.getOptionValueBool("grpc.tls.enabled").value_or(true);
  auto caCert = config.getOptionValueStr("grpc.tls.chain_cert_path");

  if (tls) {
    if (caCert) {
      std::string caCertContents = cta::utils::file2string(caCert.value());
      ssl_options.pem_root_certs = caCertContents;
    } else {
      ssl_options.pem_root_certs = "";
    }
    credentials = grpc::SslCredentials(ssl_options);
  } else {
    credentials = grpc::InsecureChannelCredentials();
  }

  // gRPC stream server
  std::string strGrpcHost = "cta-frontend-grpc";
  const std::string GRPC_SERVER = endpoint.value();
  // Service name
  const std::string GSS_SPN = "cta/" + strGrpcHost;
  // Create a channel to the KRB-GSI negotiation service
  std::shared_ptr<::grpc::Channel> spChannel {::grpc::CreateChannel(GRPC_SERVER, credentials)};
  cta::log::FileLogger log(GRPC_SERVER, "cta-admin-grpc", "/var/log/cta-admin-grpc.log", cta::log::INFO);
  cta::log::LogContext lc(log);

  // Determine authentication method: env variable overrides config, default to krb5
  std::string auth_method;
  const char* auth_method_env = std::getenv("CTA_ADMIN_GRPC_AUTH_METHOD");
  if (auth_method_env != nullptr) {
    // Environment variable takes precedence
    auth_method = auth_method_env;
  } else {
    // Check config file, default to krb5 if not specified
    auth_method = config.getOptionValueStr("grpc.cta_admin_auth_method").value_or("");
    if (auth_method.empty()) {
      lc.log(cta::log::DEBUG,
             "Authentication method not specified either in config or with environment variable "
             "CTA_ADMIN_GRPC_AUTH_METHOD, using Kerberos to authenticate!");
      auth_method = "krb5";
    }
  }

  // Validate and process the authentication method
  if (tls) {
    if (auth_method == "jwt") {
      // Read JWT token path from config, with default fallback
      std::string token_path = config.getOptionValueStr("grpc.jwt_token_path").value_or("");
      if (token_path.empty()) {
        throw cta::exception::UserError("jwt authentication specified but no token provided");
      }
      setupJwtAuthenticatedAdminCall(context, token_path);
    } else if (auth_method == "krb5") {
      setupKrb5AuthenticatedAdminCall(spChannel, context, GSS_SPN, log);
    } else {
      throw cta::exception::UserError("Unrecognized authentication method '" + auth_method + "' specified");
    }
  }  // do not attach call credentials if using unencrypted connection

  if (!isStreamCmd(request.admincmd())) {
    cta::xrd::Response response;

    // need method to obtain credentials - configuration will tell me which credentials to use
    std::unique_ptr<cta::xrd::CtaRpc::Stub> client_stub = cta::xrd::CtaRpc::NewStub(spChannel);

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
    // insecure channel credentials won't work anymore, need TLS
    std::unique_ptr<cta::xrd::CtaRpcStream::Stub> client_stub = cta::xrd::CtaRpcStream::NewStub(spChannel);
    // Also create a ClientReadReactor instance to handle the command
    try {
      auto client_reactor = CtaAdminClientReadReactor(context, client_stub.get(), parsedCmd);
      status = client_reactor.Await();
      if (!status.ok()) {
        std::cout << "gRPC call failed. Error code: " + std::to_string(status.error_code()) +
                       " Error message: " + status.error_message()
                  << std::endl;
      }
      // close the json delimiter, open is done inside command execution
      if (parsedCmd.isJson()) {
        std::cout << CtaAdminParsedCmd::jsonCloseDelim();
      }
    } catch (std::exception& ex) {
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

    CtaAdminGrpcCmd cmd;
    // Send the protocol buffer
    cmd.send(parsedCmd, config_file);

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
