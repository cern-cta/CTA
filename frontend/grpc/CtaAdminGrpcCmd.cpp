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
#include "KerberosAuthenticator.hpp"
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

std::shared_ptr<::grpc::Channel>
CtaAdminGrpcCmd::setupKrb5AuthenticatedAdminCall(std::shared_ptr<grpc::Channel> spChannelNegotiation,
                                                 grpc::ClientContext& context,
                                                 const std::string& GRPC_SERVER,
                                                 const std::string& GSS_SPN,
                                                 std::shared_ptr<grpc::ChannelCredentials> credentials,
                                                 cta::log::FileLogger& log) {
  // first do a negotiation call to obtain a kerberos token, which will be attached to the call metadata
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
  /*
    * Channel arguments can be overriden e.g: 
    * ::grpc::ChannelArguments channelArgs;ClientContext
    * channelArgs.SetString(GRPC_SSL_TARGET_NAME_OVERRIDE_ARG, "ok.org");
    * std::shared_ptr<::grpc::Channel> spChannel {::grpc::CreateCustomChannel("0.0.0.0:17017", spCredentials, channelArgs)};
    */
  //--- ref: https://grpc.io/docs/guides/auth/#credential-types
  /*
    * Credentials can be of two types:
    * - Channel credentials, which are attached to a Channel, such as SSL credentials.
    * - Call credentials, which are attached to a call (or ClientContext in C++).
    * CompositeChannelCredentials associates a ChannelCredentials and a CallCredentials
    * to create a new ChannelCredentials
    */
  /*
    * Individual CallCredentials can also be composed using CompositeCallCredentials.
    * The resulting CallCredentials when used in a call will trigger the sending of
    * the authentication data associated with the two CallCredentials.
    * !!! It dose not work with InsecureChannelCredentials !!!
    */
  //---
  /*
    * Call credentails can be set manually in a client's conspCallCredentialstext
    * e.g.:
    *   m_ctx.set_credentials(spCallCredentials);
    */
  std::shared_ptr<::grpc::CallCredentials> spCallCredentials =
    ::grpc::MetadataCredentialsFromPlugin(std::unique_ptr<::grpc::MetadataCredentialsPlugin>(
      new cta::frontend::grpc::client::KerberosAuthenticator(strEncodedToken)));
  std::shared_ptr<::grpc::ChannelCredentials> spCompositeCredentials =
    ::grpc::CompositeChannelCredentials(credentials, spCallCredentials);
  std::shared_ptr<::grpc::Channel> spChannel {::grpc::CreateChannel(GRPC_SERVER, spCompositeCredentials)};
  return spChannel;
}

// Implement the send() method here, by wrapping the Admin rpc call
void CtaAdminGrpcCmd::send(const CtaAdminParsedCmd& parsedCmd,
                           const cta::common::Config& config,
                           const std::string& config_file) {
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
  auto tls = config.getOptionValueBool("grpc.tls.enabled").value_or(false);
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
  std::shared_ptr<::grpc::Channel> spChannelNegotiation {::grpc::CreateChannel(GRPC_SERVER, credentials)};
  cta::log::FileLogger log(GRPC_SERVER, "cta-admin-grpc", "/var/log/cta-admin-grpc.log", cta::log::DEBUG);
  cta::log::LogContext lc(log);
  std::shared_ptr<::grpc::Channel> spChannel;

  // read from the env variable which auth method should be used
  const char* auth_method = std::getenv("CTA_ADMIN_GRPC_AUTH_METHOD");
  if (auth_method == NULL) {
    lc.log(cta::log::WARNING,
           "Environment variable CTA_ADMIN_GRPC_AUTH_METHOD not set, using Kerberos to authenticate!");
    std::cout << "using kerberos authentication";
    spChannel = setupKrb5AuthenticatedAdminCall(spChannelNegotiation, context, GRPC_SERVER, GSS_SPN, credentials, log);
  } else {
    if (strcmp("jwt", auth_method) == 0) {
      // TODO read token path from config
      // for now, hard-coded
      std::string token_path = "/etc/grid-security/jwt-token-grpc";
      // std::cout << "using token authentication" << std::endl;
      setupJwtAuthenticatedAdminCall(context, token_path);
      spChannel = spChannelNegotiation;
    } else if (strcmp("krb5", auth_method) == 0) {
      // std::cout << "using kerberos authentication";
      spChannel =
        setupKrb5AuthenticatedAdminCall(spChannelNegotiation, context, GRPC_SERVER, GSS_SPN, credentials, log);
    } else {
      // WARN that we are falling back to Kerberos
      lc.log(cta::log::WARNING,
             "Unrecognized authentication method set in CTA_ADMIN_GRPC_AUTH_METHOD, using Kerberos!");
      spChannel =
        setupKrb5AuthenticatedAdminCall(spChannelNegotiation, context, GRPC_SERVER, GSS_SPN, credentials, log);
    }
  }

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
