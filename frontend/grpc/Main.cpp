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
#include "FrontendGRpcSvc.h"
#include "version.h"

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/StdoutLogger.hpp"
#include "rdbms/Login.hpp"
#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

#include <getopt.h>
#include <fstream>

using namespace cta;
using namespace cta::common;
// using namespace cta::frontend::grpc;

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

    // get number of threads, maybe consider setting it too if unset?
    int threads = 8 * std::thread::hardware_concurrency();
    

    if (useTLS) {
        lc.log(log::INFO, "Using gRPC over TLS");
        grpc::SslServerCredentialsOptions tls_options;
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

    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, creds);

    // fixed the number of request threads
    ResourceQuota quota;
    quota.SetMaxThreads(threads);
    lc.log(log::INFO, "Using " + std::to_string(threads) + " request processing threads");
    builder.SetResourceQuota(quota);

    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&svc);

    std::unique_ptr <Server> server(builder.BuildAndStart());

    lc.log(cta::log::INFO, "Listening on socket address: " + server_address);
    server->Wait();
}
