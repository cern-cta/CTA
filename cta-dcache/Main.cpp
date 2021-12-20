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

#include "catalogue/CatalogueFactoryFactory.hpp"
#include <common/Configuration.hpp>
#include "rdbms/Login.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/make_unique.hpp"
#include "scheduler/OStoreDB/OStoreDBInit.hpp"

#include <getopt.h>

using namespace cta;
using namespace cta::common;

std::string help =
        "Usage: cta-dcache [options]\n"
        "\n"
        "where options can be:\n"
        "\n"
        "\t--port <port>, -p <port>:\tTCP port number to use, defaults to 17017\n"
        "\t--log-header, -n         \tadd hostname and timestamp to log outputs, default\n"
        "\t--no-log-header, -s      \tdon't add hostname and timestamp to log outputs\n"
        "\t--version, -v            \tprint version and exit.\n"
        "\t--help, -h               \tprint this help and exit\n";

static struct option long_options[] =
        {
                {"port", required_argument, 0, 'p'},
                {"log-header", no_argument, 0, 'n'},
                {"no-log-header", no_argument, 0, 's'},
                {"help", no_argument, 0, 'h'},
                { "version", no_argument, 0, 'v'},
                {0, 0, 0, 0}
        };

void printHelpAndExit(int rc) {
    std::cout << help << std::endl;
    exit(rc);
}

void printVersionAndExit() {
    std::cout << "cta-dcache version: " << CTA_VERSION << std::endl;
    exit(0);
}

int main(const int argc, char *const *const argv) {

    std::string port = "17017";

    char c;
    bool shortHeader = false;
    int option_index = 0;
    const std::string shortHostName = utils::getShortHostname();

    while( (c = getopt_long(argc, argv, "p:nshv", long_options, &option_index)) != EOF) {

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
            default:
                printHelpAndExit(1);
        }
    }


    log::StdoutLogger logger(shortHostName, "cta-dcache", shortHeader);
    log::LogContext lc(logger);

    lc.log(log::INFO, "Starting " + CTA_DCACHE_VERSION);

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
        catalogue->ping();
        lc.log(log::INFO, "Connected to catalog " + catalogue->getSchemaVersion().getSchemaVersion<std::string>());
    } catch (cta::exception::Exception &ex) {
        lc.log(cta::log::CRIT, ex.getMessageValue());
        exit(1);
    }

    // Initialise the Scheduler
    auto backed = config.getConfEntString("ObjectStore", "BackendPath");
    lc.log(log::INFO, "Using scheduler backend: " + backed);

    auto sInit = cta::make_unique<SchedulerDBInit_t>("Frontend", backed, logger);
    auto scheddb = sInit->getSchedDB(*catalogue, logger);
    scheddb->setBottomHalfQueueSize(25000);
    auto scheduler = cta::make_unique<cta::Scheduler>(*catalogue, *scheddb, 5, 2*1000*1000);

    CtaRpcImpl svc(&logger, catalogue, scheduler);

    // start gRPC service

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&svc);

    std::unique_ptr <Server> server(builder.BuildAndStart());

    lc.log(cta::log::INFO, "Listening on socket address: " + server_address);
    server->Wait();
}
