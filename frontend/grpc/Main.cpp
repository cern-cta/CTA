/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2021 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FrontendGrpcService.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "common/auth/jwt/JwtAuthManager.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "rdbms/Login.hpp"
#include "version.hpp"
#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

#include "NegotiationService.hpp"
#include "TokenStorage.hpp"
#include "callback_api/CtaAdminServer.hpp"
#include "common/utils/utils.hpp"

#include <future>
#include <getopt.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <set>
#include <thread>

using namespace cta;
using namespace cta::common;
using namespace cta::frontend::grpc;

constexpr std::string_view defaultPort = "17017";

const std::string help = "Usage: cta-frontend-grpc [options]\n"
                         "\n"
                         "where options can be:\n"
                         "\n"
                         "\t--config <config-file>, -c        \tConfiguration file\n"
                         "\t--grpc-config <grpc-config-file>  \tgRPC configuration file (TOML)"
                         "\t--version, -v                     \tprint version and exit\n"
                         "\t--help, -h                        \tprint this help and exit\n";

const static struct option long_options[] = {
  {"config",      required_argument, nullptr, 'c'},
  {"grpc-config", required_argument, nullptr, 'a'},
  {"help",        no_argument,       nullptr, 'h'},
  {"version",     no_argument,       nullptr, 'v'},
  {nullptr,       0,                 nullptr, 0  }
};

[[noreturn]] void printHelpAndExit(int rc) {
  std::cout << help << std::endl;
  exit(rc);
}

[[noreturn]] void printVersionAndExit() {
  std::cout << "cta-frontend-grpc version: " << CTA_VERSION << std::endl;
  exit(0);
}

void JwksCacheRefreshLoop(std::weak_ptr<cta::auth::JwtAuthManager> weakAuthManager,
                          std::future<void> shouldStopThread,
                          int cacheRefreshInterval,
                          const log::LogContext& lc) {
  log::LogContext threadLc(lc);
  threadLc.log(log::INFO, "Detached JWKS cache refresh thread started");

  while (shouldStopThread.wait_for(std::chrono::seconds(cacheRefreshInterval)) == std::future_status::timeout) {
    auto jwtConfig = weakAuthManager.lock();
    if (!jwtConfig) {
      threadLc.log(log::INFO, "jwtAuthManager no longer exists, exiting JWKS cache refresh thread");
      break;  // Cache destroyed
    }
    time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    threadLc.log(log::INFO, "Updating the JWKS cache");
    jwtConfig->updateCache(now);
  }
  threadLc.log(log::INFO, "Detached JWKS cache refresh thread ended");
}

int main(const int argc, char* const* const argv) {
  std::string configFile("/etc/cta/cta-frontend.conf");
  std::string grpcConfigFile("/etc/cta/cta-frontend-grpc.toml");

  int c;
  int optionIndex = 0;
  const std::string shortHostName = utils::getShortHostname();
  std::string port;

  while ((c = getopt_long(argc, argv, "c:a:hv", long_options, &optionIndex)) != -1) {
    switch (c) {
      case 'h':
        printHelpAndExit(0);
        break;
      case 'v':
        printVersionAndExit();
        break;
      case 'c':
        configFile = optarg;
        break;
      case 'a':
        grpcConfigFile = optarg;
        break;
      default:
        printHelpAndExit(1);
    }
  }

  // Initialize frontend service first to get configuration
  auto frontendService = std::make_shared<cta::frontend::FrontendService>(configFile, grpcConfigFile);
  auto grpcConfig = frontendService->getGrpcConfig();

  // get the log context
  log::LogContext lc = frontendService->getLogContext();

  const auto jwtConfig = grpcConfig->auth.jwt;

  std::shared_ptr<cta::auth::JwtAuthManager> jwtAuthManager;
  std::optional<std::jthread> cacheRefreshThread;
  std::promise<void> shouldStopThreadPromise;

  if (jwtConfig.has_value() && jwtConfig->enabled) {
    // Build the shared JWK cache
    auto jwksFetcher {std::make_unique<cta::auth::CurlJwksFetcher>(jwtConfig->jwks_total_timeout)};
    jwtAuthManager = std::make_shared<cta::auth::JwtAuthManager>(std::move(jwksFetcher),
                                                                 jwtConfig->jwks_uri,
                                                                 jwtConfig->pub_key_timeout,
                                                                 jwtConfig->expected_issuer,
                                                                 jwtConfig->expected_audience,
                                                                 jwtConfig->min_generation,
                                                                 jwtConfig->revoke_list_path,
                                                                 frontendService->getLogContext());

    {
      log::ScopedParamContainer spc(lc);
      spc.add("jwks_uri", jwtConfig->jwks_uri)
        .add("total_timeout", std::to_string(jwtConfig->jwks_total_timeout))
        .add("key_timeout", std::to_string(jwtConfig->pub_key_timeout))
        .add("cache_refresh_interval", std::to_string(jwtConfig->cache_refresh_interval));
      lc.log(log::INFO, std::string("JWT authentication enabled"));
    }

    std::weak_ptr<cta::auth::JwtAuthManager> weakAuthManager {jwtAuthManager};
    std::future<void> shouldStopThreadFuture {shouldStopThreadPromise.get_future()};

    // Set up the refresh thread
    lc.log(log::INFO, "Starting the cache refresh thread for JWKS cache");
    cacheRefreshThread = std::jthread(JwksCacheRefreshLoop,
                                      weakAuthManager,
                                      std::move(shouldStopThreadFuture),
                                      jwtConfig->cache_refresh_interval,
                                      std::cref(lc));
  }

  // Setup TokenStorage for Kerberos authentication
  cta::frontend::grpc::server::TokenStorage tokenStorage;

  // Initialize RPC service with shared frontend service and cache
  frontend::grpc::CtaRpcImpl svc(frontendService, jwtAuthManager, tokenStorage);

  lc.log(log::INFO, "Starting cta-frontend-grpc");

  std::string serverAddress("0.0.0.0:" + std::to_string(grpcConfig->grpc.port));

  // start gRPC service

  ServerBuilder builder;

  std::shared_ptr<grpc::ServerCredentials> creds;

  // get number of threads
  int threads = grpcConfig->grpc.number_of_threads.value_or(8 * std::thread::hardware_concurrency());
  grpc_ssl_client_certificate_request_type cert_request_type = GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE;

  using namespace grpc::experimental;

  lc.log(log::INFO, "Using gRPC over TLS");
  if (frontendService->getOperationMode() == cta::frontend::OperationMode::WFE
      && frontendService->usesAuthMethod(cta::frontend::AuthMethod::MTLS)) {
    // mTLS requires a check on the client certificate
    cert_request_type = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
    lc.log(log::INFO, "Using mutual TLS to authenticate WFE client requests");
  }

  IdentityKeyCertPair cert;
  auto tlsConfig = grpcConfig->grpc.tls;

  auto keyFile = tlsConfig.server_key_path;
  cert.private_key = cta::utils::readFileAsString(tlsConfig.server_key_path);

  auto certFile = tlsConfig.server_cert_path;
  cert.certificate_chain = cta::utils::readFileAsString(certFile);

  std::shared_ptr<StaticDataCertificateProvider> provider;
  {
    log::ScopedParamContainer spc(lc);
    spc.add("tls_key", keyFile).add("tls_cert", certFile);

    // if we have a CA certificate chain, use it
    if (auto caChain = tlsConfig.chain_cert_path; caChain.has_value()) {
      spc.add("tls_chain", caChain.value());
      provider = std::make_shared<StaticDataCertificateProvider>(cta::utils::readFileAsString(caChain.value()),
                                                                 std::vector {cert});
    } else {
      spc.add("tls_chain", "<no chain file>");
      provider = std::make_shared<StaticDataCertificateProvider>(std::vector {cert});
    }
    lc.log(log::INFO, "TLS configuration loaded");
  }

  TlsServerCredentialsOptions tlsOptions {provider};
  tlsOptions.set_cert_request_type(cert_request_type);
  tlsOptions.watch_root_certs();
  tlsOptions.watch_identity_key_cert_pairs();
  creds = TlsServerCredentials(tlsOptions);

  // enable health checking, needed by CI
  grpc::EnableDefaultHealthCheckService(true);
  // add reflection
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(serverAddress, creds);

  // fixed the number of request threads
  ResourceQuota quota;
  quota.SetMaxThreads(threads);
  lc.log(log::INFO, "Using " + std::to_string(threads) + " request processing threads");
  builder.SetResourceQuota(quota);

  std::optional<std::unique_ptr<cta::frontend::grpc::server::NegotiationService>> negotiationService;

  // If we're in Admin Command mode and using Kerberos, we need to set up the negotiation service for Kerberos authentication
  if (frontendService->getOperationMode() != cta::frontend::OperationMode::WFE
      && frontendService->usesAuthMethod(cta::frontend::AuthMethod::KERBEROS)) {
    // Get Kerberos configuration - .value() is safe because we've checked that kerberos is enabled
    auto kerberosConfig = grpcConfig->auth.kerberos.value();
    std::string keytabPath = kerberosConfig.keytab_path;
    std::string servicePrincipal = kerberosConfig.service_principal;

    lc.log(log::INFO,
           "Using Kerberos authentication with keytab '" + keytabPath + "' and service principal '" + servicePrincipal
             + "'");

    // Create completion queue that will be used only for the Kerberos negotiation service
    std::unique_ptr<::grpc::ServerCompletionQueue> negCq = builder.AddCompletionQueue();

    // Create negotiation service
    negotiationService = std::make_unique<cta::frontend::grpc::server::NegotiationService>(lc.logger(),
                                                                                           tokenStorage,
                                                                                           std::move(negCq),
                                                                                           keytabPath,
                                                                                           servicePrincipal,
                                                                                           1 /* threads */);

    // Register negotiation service on main builder
    builder.RegisterService(&negotiationService.value()->getService());
  }

  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&svc);

  lc.log(log::DEBUG, "Instance name is: '" + frontendService->getInstanceName() + "'");

  frontend::grpc::CtaRpcStreamImpl streamSvc(frontendService->getCatalogue(),
                                             frontendService->getScheduler(),
                                             frontendService->getSchedDb(),
                                             frontendService->getInstanceName(),
                                             frontendService->getCatalogueConnString(),
                                             frontendService->getMissingFileCopiesMinAgeSecs(),
                                             frontendService->getLogContext(),
                                             frontendService->getAuthMethods(),
                                             jwtAuthManager,
                                             tokenStorage);
  builder.RegisterService(&streamSvc);

  // add reflection
  // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  // Build and start server
  std::unique_ptr<Server> server(builder.BuildAndStart());

  // if we're using Kerberos, we need to start the negotiation service processing loop after the server is built
  if (negotiationService.has_value()) {
    // Start negotiation service processing (after server is built)
    negotiationService.value()->startProcessing();
  }

  lc.log(cta::log::INFO, "Listening on socket address: " + serverAddress);
  server->Wait();
  lc.logEvent(log::INFO, "Exiting cta-frontend-grpc", semconv::log::EventNameValues::kProgramExiting);
  if (cacheRefreshThread.has_value()) {
    // if we ever receive a shutdown, or want to handle termination of the frontend gracefully,
    // add the following line:
    shouldStopThreadPromise.set_value();
    if (cacheRefreshThread->joinable()) {
      cacheRefreshThread->join();
    }
  }
}
