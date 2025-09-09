/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @copyright      Copyright(C) 2021 DESY
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
#include "FrontendGrpcService.hpp"
#include "version.h"

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/StdoutLogger.hpp"
#include "rdbms/Login.hpp"
#include "common/JwkCache.hpp"
#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

#include <getopt.h>
#include <fstream>
#include "callback_api/CtaAdminServer.hpp" // for the impl of the streaming service
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "TokenStorage.hpp" // for Kerberos authentication - we'll wipe the storage periodically
#include "ServiceKerberosAuthProcessor.hpp"
#include "AsyncServer.hpp"
#include "ServerNegotiationRequestHandler.hpp"

// #include <grpc++/reflection.h>
#include <thread>
#include <future>

using namespace cta;
using namespace cta::common;
using namespace cta::frontend::grpc;
using cta::frontend::grpc::server::ServiceKerberosAuthProcessor;

std::string help =
    "Usage: cta-frontend-grpc [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--config <config-file>, -c   \tConfiguration file\n"
    "\t--version, -v                \tprint version and exit\n"
    "\t--help, -h                   \tprint this help and exit\n";

static struct option long_options[] = {
  { "config",        required_argument, nullptr, 'c' },
  { "help",          no_argument,       nullptr, 'h' },
  { "version",       no_argument,       nullptr, 'v' },
  { nullptr,         0,                 nullptr, 0   }
};

void printHelpAndExit(int rc) {
    std::cout << help << std::endl;
    exit(rc);
}

void printVersionAndExit() {
    std::cout << "cta-frontend-grpc version: " << CTA_VERSION << std::endl;
    exit(0);
}

std::string file2string(std::string filename){
    std::ifstream as_stream(filename);
    std::ostringstream as_string;
    as_string << as_stream.rdbuf();
    return as_string.str();
}

void JwksCacheRefreshLoop(std::weak_ptr<JwkCache> weakCache,
                          std::future<void> shouldStopThread,
                          int cacheRefreshInterval,
                          const log::LogContext& lc) {
  log::LogContext threadLc(lc);
  threadLc.log(log::INFO, "Detached JWKS cache refresh thread started");

  while (shouldStopThread.wait_for(std::chrono::seconds(cacheRefreshInterval)) == std::future_status::timeout) {
    auto cache = weakCache.lock();
    if (!cache) {
      threadLc.log(log::INFO, "JwkCache no longer exists, exiting JWKS cache refresh thread");
      break;  // Cache destroyed
    }
    time_t now = time(nullptr);
    threadLc.log(log::INFO, "Updating the JWKS cache");
    cache->updateCache(now);
  }
  threadLc.log(log::INFO, "Detached JWKS cache refresh thread ended");
}

int main(const int argc, char *const *const argv) {

    std::string config_file("/etc/cta/cta-frontend-grpc.conf");

    char c;
    int option_index = 0;
    const std::string shortHostName = utils::getShortHostname();
    bool useTLS = false;
    std::string port;

    while( (c = getopt_long(argc, argv, "c:hv", long_options, &option_index)) != EOF) {

        switch(c) {
            case 'h':
                printHelpAndExit(0);
                break;
            case 'v':
                printVersionAndExit();
                break;
            case 'c':
                config_file = optarg;
                break;
            default:
                printHelpAndExit(1);
        }
    }

    // Initialize catalogue, scheduler, logContext
    frontend::grpc::CtaRpcImpl svc(config_file);
    // get the log context
    log::LogContext lc = svc.getFrontendService().getLogContext();
    auto jwkCache = svc.getPubkeyCache();
    std::weak_ptr<JwkCache> weakCache = jwkCache;
    std::promise<void> shouldStopThreadPromise;
    std::future<void> shouldStopThreadFuture = shouldStopThreadPromise.get_future();
    std::thread cacheRefreshThread;
    // if token authentication is specified, then also start the refresh thread, otherwise no point in doing this
    if (svc.getFrontendService().getJwtAuth()) {
        lc.log(log::INFO, "Starting the cache refresh thread for JWKS cache");
        cacheRefreshThread = std::thread(JwksCacheRefreshLoop,
                                         weakCache,
                                         std::move(shouldStopThreadFuture),
                                         svc.getFrontendService().getCacheRefreshInterval().value_or(600),
                                         std::cref(lc));
    }

    // use castor config to avoid dependency on xroot-ssi
    // Configuration config(config_file);

    lc.log(log::INFO, "Starting cta-frontend-grpc- " + std::string(CTA_VERSION));

    // try to update port from config
    if (svc.getFrontendService().getPort().has_value())
        port = svc.getFrontendService().getPort().value();
    else
    {
        port = "17017";
        // also set the member value
    }

    std::string server_address("0.0.0.0:" + port);

    // start gRPC service

    ServerBuilder builder;

    std::shared_ptr<grpc::ServerCredentials> creds;

    // read TLS value from config
    useTLS = svc.getFrontendService().getTls();

    // get number of threads
    int threads = svc.getFrontendService().getThreads().value_or(8 * std::thread::hardware_concurrency());
    

    if (useTLS) {
        lc.log(log::INFO, "Using gRPC over TLS");
        grpc::SslServerCredentialsOptions tls_options(GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE);
        grpc::SslServerCredentialsOptions::PemKeyCertPair cert;

        if (!svc.getFrontendService().getTlsKey().has_value()) {
            lc.log(log::WARNING, "TLS specified but TLS key is not defined. Using gRPC over plaintext socket instead");
            creds = grpc::InsecureServerCredentials();
        }
        else if (!svc.getFrontendService().getTlsCert().has_value()) {
            lc.log(log::WARNING, "TLS specified but TLS key is not defined. Using gRPC over plaintext socket instead");
            creds = grpc::InsecureServerCredentials();
        }
        else {
            auto key_file = svc.getFrontendService().getTlsKey().value();
            lc.log(log::INFO, "TLS service key file: " + key_file);
            cert.private_key = file2string(key_file);

            auto cert_file = svc.getFrontendService().getTlsCert().value();
            lc.log(log::INFO, "TLS service certificate file: " + cert_file);
            cert.cert_chain = file2string(cert_file);

            auto ca_chain = svc.getFrontendService().getTlsChain();
            if (ca_chain.has_value()) {
                lc.log(log::INFO, "TLS CA chain file: " + ca_chain.value());
                tls_options.pem_root_certs = file2string(ca_chain.value());
            } else {
                lc.log(log::INFO, "TLS CA chain file not defined ...");
                tls_options.pem_root_certs = "";
            }
            tls_options.pem_key_cert_pairs.emplace_back(std::move(cert));

            creds = grpc::SslServerCredentials(tls_options);
        }
    } else {
        lc.log(log::INFO, "Using gRPC over plaintext socket");
        creds = grpc::InsecureServerCredentials();
    }

    // enable health checking, needed by CI
    grpc::EnableDefaultHealthCheckService(true);
    // add reflection
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, creds);

    // fixed the number of request threads
    ResourceQuota quota;
    quota.SetMaxThreads(threads);
    lc.log(log::INFO, "Using " + std::to_string(threads) + " request processing threads");
    builder.SetResourceQuota(quota);

    // Setup TokenStorage for Kerberos authentication
    cta::frontend::grpc::server::TokenStorage tokenStorage;
    
    // Get Kerberos configuration from environment or use defaults
    std::string strKeytab;
    std::string strService;

    strKeytab = svc.getFrontendService().getKeytab().value_or("");
    
    if (strKeytab.empty()) {
        strKeytab = "/etc/cta/cta-frontend.keytab"; // default path
    }

    strService = "cta/" + shortHostName; // default service principal
    
    lc.log(log::INFO, "Using keytab: " + strKeytab);
    lc.log(log::INFO, "Using service principal: " + strService);
    
    // setup a completion queue that will be used only for the Kerberos authentication
    std::unique_ptr<::grpc::ServerCompletionQueue> cq;
    cq = builder.AddCompletionQueue();

    // Create AsyncServer with separate builder for internal use, but register services on main builder
    std::unique_ptr<::grpc::ServerBuilder> separateBuilder = std::make_unique<::grpc::ServerBuilder>();
    
    cta::frontend::grpc::server::AsyncServer negotiationServer(lc, svc.getFrontendService().getCatalogue(),
                                                    tokenStorage, std::move(separateBuilder), std::move(cq));
    // Register services & run
    try {
        // SERVICE: Negotiation - register on main builder for single server
        negotiationServer.registerServiceOnBuilder<cta::xrd::Negotiation::AsyncService>(builder);
        try {
        negotiationServer
            .registerHandler<cta::frontend::grpc::server::NegotiationRequestHandler, cta::xrd::Negotiation::AsyncService>(
            strKeytab, strService);
        } catch(const cta::exception::Exception &ex) {
        log::ScopedParamContainer params(lc);
        params.add("handler", "NegotiationRequestHandler");
        lc.log(cta::log::ERR, "In frontend/grpc/Main.cpp: Error while registering handler.");
        throw;
        }
        // std::shared_ptr<ServiceKerberosAuthProcessor> spAuthProcessor = std::make_shared<ServiceKerberosAuthProcessor>(tokenStorage);
        // server.startServerAndRun(spServerCredentials, spAuthProcessor);
    } catch(const cta::exception::Exception& e) {
        log::ScopedParamContainer params(lc);
        params.add("exceptionMessage", e.getMessageValue());
        lc.log(cta::log::CRIT, "In cta::frontend::grpc::server::FrontendCmd::main(): Got an exception.");
        return 1;
    } catch(const std::exception& e) {
        log::ScopedParamContainer params(lc);
        params.add("exceptionMessage", e.what());
        lc.log(cta::log::CRIT, "In cta::frontend::grpc::server::FrontendCmd::main(): Got an exception.");
        return 1;
    }
 
    // Setup main service authentication processor
    std::shared_ptr<ServiceKerberosAuthProcessor> kerberosAuthProcessor = 
        std::make_shared<ServiceKerberosAuthProcessor>(tokenStorage);
    creds->SetAuthMetadataProcessor(kerberosAuthProcessor);
    // Kerberos authenticates all admin commands - non-streaming commands that are made through the Admin rpc
    // and streaming commands made through GenericAdminStream

    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&svc);
    
    frontend::grpc::CtaRpcStreamImpl streamSvc(svc.getFrontendService().getCatalogue(),
                                               svc.getFrontendService().getScheduler(),
                                               svc.getFrontendService().getSchedDb(),
                                               svc.getFrontendService().getCatalogueConnString(),
                                               svc.getFrontendService().getInstanceName(),
                                               svc.getFrontendService().getMissingFileCopiesMinAgeSecs(),
                                               svc.getFrontendService().getLogContext());
    builder.RegisterService(&streamSvc);
    // add reflection
    // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    std::unique_ptr <Server> server(builder.BuildAndStart());

    // Start AsyncServer processing threads without taking server ownership
    negotiationServer.startProcessingThreads(kerberosAuthProcessor);
    
    lc.log(cta::log::INFO, "Listening on socket address: " + server_address);
    server->Wait();

    // if we ever receive a shutdown, or want to handle termination of the frontend gracefully,
    // add the following line:
    shouldStopThreadPromise.set_value();
    if (cacheRefreshThread.joinable())
        cacheRefreshThread.join();
}
