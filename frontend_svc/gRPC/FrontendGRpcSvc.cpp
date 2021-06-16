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

Status CtaRpcImpl::Version(::grpc::ServerContext *context, const ::google::protobuf::Empty *request,
                           ::cta::admin::Version *response) {
    response->set_cta_version(CTA_VERSION);
    response->set_xrootd_ssi_protobuf_interface_version("gPRC-frontend v0.0.1");
    return Status::OK;
}

void CtaRpcImpl::run(const std::string server_address) {

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(this);
    std::unique_ptr <Server> server(builder.BuildAndStart());

    m_log->log(cta::log::INFO, "Listening on socket address: " + server_address);
    server->Wait();
}

CtaRpcImpl::CtaRpcImpl(cta::log::LogContext *lc, std::unique_ptr<cta::catalogue::Catalogue> &catalogue):
    m_catalogue(std::move(catalogue)) {
    m_log = lc;
}

using namespace cta;

int main(const int argc, char *const *const argv) {

    std::unique_ptr <cta::log::Logger> logger;
    logger.reset(new log::StdoutLogger("cta-dev", "cta-grpc-frontend"));

    log::LogContext lc(*logger);

    std::string server_address("0.0.0.0:17017");

    // Initialise the Catalogue
    std::string catalogueConfigFile = "/etc/cta/cta-catalogue.conf";
    const rdbms::Login catalogueLogin = rdbms::Login::parseFile(catalogueConfigFile);

    const uint64_t nbArchiveFileListingConns = 2;
    auto catalogueFactory = catalogue::CatalogueFactoryFactory::create(*logger, catalogueLogin,
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

    CtaRpcImpl svc(&lc, catalogue);
    svc.run(server_address);

}