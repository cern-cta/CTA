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
#include "common/Configuration.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/StdoutLogger.hpp"
#include "rdbms/Login.hpp"
#ifdef CTA_PGSCHED
#include "scheduler/PostgresSchedDB/PostgresSchedDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

#include <getopt.h>
#include <fstream>

using namespace cta;
using namespace cta::common;

std::string help =
    "Usage: cta-frontend-grpc [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--threads <N>, -c <N>    \tnumber of threads to process concurrent requests, defaults to 8x#CPUs\n"
    "\t--port <port>, -p <port> \tTCP port number to use, defaults to 17017\n"
    "\t--log-header, -n         \tadd hostname and timestamp to log outputs, default\n"
    "\t--no-log-header, -s      \tdon't add hostname and timestamp to log outputs\n"
    "\t--tls, -t                \tenable Transport Layer Security (TLS)\n"
    "\t--version, -v            \tprint version and exit\n"
    "\t--help, -h               \tprint this help and exit\n";

static struct option long_options[] = {
  { "threads",       required_argument, nullptr, 'c' },
  { "port",          required_argument, nullptr, 'p' },
  { "log-header",    no_argument,       nullptr, 'n' },
  { "no-log-header", no_argument,       nullptr, 's' },
  { "help",          no_argument,       nullptr, 'h' },
  { "version",       no_argument,       nullptr, 'v' },
  { "tls",           no_argument,       nullptr, 't' },
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

    std::string port = "17017";

    char c;
    bool shortHeader = false;
    int option_index = 0;
    const std::string shortHostName = utils::getShortHostname();
    bool useTLS = false;
    int threads = 8 * std::thread::hardware_concurrency();

    while( (c = getopt_long(argc, argv, "c:p:nshv", long_options, &option_index)) != EOF) {

        switch(c) {
            case 'p':
                port = std::string(optarg);
                break;
            case 'n':
                shortHeader = false;
                break;
            case 's':
                shortHeader = true;
                break;
            case 'h':
                printHelpAndExit(0);
                break;
            case 'v':
                printVersionAndExit();
                break;
            case 't':
                useTLS = true;
                break;
            case 'c':
                threads = std::atoi(optarg);
                break;
            default:
                printHelpAndExit(1);
        }
    }

    log::StdoutLogger logger(shortHostName, "cta-frontend-grpc", shortHeader);
    log::LogContext lc(logger);

    lc.log(log::INFO, "Starting cta-frontend-grpc- " + std::string(CTA_VERSION));

    // use castor config to avoid dependency on xroot-ssi
    Configuration config("/etc/cta/cta.conf");

    std::string server_address("0.0.0.0:" + port);

    // Initialise the Catalogue
    std::string catalogueConfigFile = "/etc/cta/cta-catalogue.conf";
    const rdbms::Login catalogueLogin = rdbms::Login::parseFile(catalogueConfigFile);

    const uint64_t nbArchiveFileListingConns = 2;
    auto catalogueFactory = catalogue::CatalogueFactoryFactory::create(logger, catalogueLogin,
                                                                       10,
                                                                       nbArchiveFileListingConns);
    auto catalogue = catalogueFactory->create();
    try {
        catalogue->Schema()->ping();
        lc.log(log::INFO, "Connected to catalog " + catalogue->Schema()->getSchemaVersion().
            getSchemaVersion<std::string>());
    } catch (cta::exception::Exception &ex) {
        lc.log(cta::log::CRIT, ex.getMessageValue());
        exit(1);
    }

    // Initialise the Scheduler
    auto backed = config.getConfEntString("ObjectStore", "BackendPath");
    lc.log(log::INFO, "Using scheduler backend: " + backed);

    auto sInit = std::make_unique<SchedulerDBInit_t>("Frontend", backed, logger);
    auto scheddb = sInit->getSchedDB(*catalogue, logger);
    scheddb->initConfig();
    auto scheduler = std::make_unique<cta::Scheduler>(*catalogue, *scheddb, 5, 2*1000*1000);

    CtaRpcImpl svc(&logger, catalogue, scheduler);

    // start gRPC service

    ServerBuilder builder;

    std::shared_ptr<grpc::ServerCredentials> creds;

    if (useTLS) {
        lc.log(log::INFO, "Using gRPC over TLS");
        grpc::SslServerCredentialsOptions tls_options;
        grpc::SslServerCredentialsOptions::PemKeyCertPair cert;

        auto key_file = config.getConfEntString("gRPC", "TlsKey");
        lc.log(log::INFO, "TLS service key file: " + key_file);
        cert.private_key = file2string(key_file);

        auto cert_file = config.getConfEntString("gRPC", "TlsCert");
        lc.log(log::INFO, "TLS service certificate file: " + cert_file);
        cert.cert_chain = file2string(cert_file);

        auto ca_chain = config.getConfEntString("gRPC", "TlsChain", "");
        if (!ca_chain.empty()) {
            lc.log(log::INFO, "TLS CA chain file: " + ca_chain);
            tls_options.pem_root_certs = file2string(ca_chain);
        } else {
            lc.log(log::INFO, "TLS CA chain file not defined ...");
            tls_options.pem_root_certs = "";
        }
        tls_options.pem_key_cert_pairs.emplace_back(std::move(cert));

        creds = grpc::SslServerCredentials(tls_options);
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
