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

Status CtaRpcImpl::Archive(::grpc::ServerContext* context, const ::cta::dcache::rpc::ArchiveRequest* request, ::cta::dcache::rpc::ArchiveResponse* response) {

    m_log->log(cta::log::INFO, "Archive request");

    const std::string storageClass = request->file().storageclass();
    m_log->log(cta::log::DEBUG, "Archive request for storageClass: " + storageClass);

    cta::common::dataStructures::RequesterIdentity requester;
    requester.name = request->cli().user().username();
    requester.group = request->cli().user().groupname();

    auto instance = request->instance().name();

    try {
        uint64_t archiveFileId = m_scheduler->checkAndGetNextArchiveFileId(instance, storageClass, requester, *m_log);

        cta::common::dataStructures::ArchiveRequest archiveRequest;
        cta::checksum::ProtobufToChecksumBlob(request->file().csb(), archiveRequest.checksumBlob);
        archiveRequest.diskFileInfo.owner_uid = 1;
        archiveRequest.diskFileInfo.gid = 1;
        archiveRequest.diskFileInfo.path = "/" + request->file().fid();
        archiveRequest.diskFileID = request->file().fid();
        archiveRequest.fileSize = request->file().size();
        archiveRequest.requester.name = request->cli().user().username();
        archiveRequest.requester.group = request->cli().user().groupname();
        archiveRequest.storageClass = storageClass;
        archiveRequest.srcURL = request->transport().dst_url();
        archiveRequest.archiveReportURL = request->transport().report_url() + "?archiveid=" + std::to_string(archiveFileId);
        archiveRequest.archiveErrorReportURL = request->transport().error_report_url();
        archiveRequest.creationLog.host = context->peer();
        archiveRequest.creationLog.username = instance;
        archiveRequest.creationLog.time = time(nullptr);

        std::string reqId = m_scheduler->queueArchiveWithGivenId(archiveFileId, instance, archiveRequest, *m_log);
        m_log->log(cta::log::INFO, "Archive request for storageClass: " + storageClass
            + " archiveFileId: " + std::to_string(archiveFileId)
            + "RequestID: " + reqId);

        response->set_fid(archiveFileId);
        response->set_reqid(reqId);

    } catch (cta::exception::Exception &ex) {
        m_log->log(cta::log::CRIT, ex.getMessageValue());
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
    }

    return Status::OK;
}


Status CtaRpcImpl::Delete(::grpc::ServerContext* context, const ::cta::dcache::rpc::DeleteRequest* request, ::google::protobuf::Empty* response) {

    m_log->log(cta::log::DEBUG, "Delete request");
    auto instance = request->instance().name();
    // Unpack message
    cta::common::dataStructures::DeleteArchiveRequest deleteRequest;
    deleteRequest.requester.name    = request->cli().user().username();
    deleteRequest.requester.group   = request->cli().user().groupname();

    deleteRequest.diskFilePath          = "/" + request->file().fid();
    deleteRequest.diskFileId = request->file().fid();
    deleteRequest.diskInstance = instance;

    deleteRequest.archiveFileID = request->archiveid();

    // Delete the file from the catalogue or from the objectstore if archive request is created
    cta::utils::Timer t;
    try {
        deleteRequest.archiveFile = m_catalogue->getArchiveFileById(deleteRequest.archiveFileID);
    } catch (cta::exception::Exception &ex){
        // TODO add logging
    }
    m_scheduler->deleteArchive(instance, deleteRequest, *m_log);

    m_log->log(cta::log::INFO, "archive file deleted.");

    return Status::OK;
}

Status CtaRpcImpl::Retrieve(::grpc::ServerContext* context, const ::cta::dcache::rpc::RetrieveRequest* request, ::cta::dcache::rpc::RetrieveResponse *response) {


    const std::string storageClass = request->file().storageclass();
    m_log->log(cta::log::DEBUG, "Retrieve request for storageClass: " + storageClass);

    auto instance = request->instance().name();

    // Unpack message
    cta::common::dataStructures::RetrieveRequest retrieveRequest;
    retrieveRequest.requester.name         = request->cli().user().username();
    retrieveRequest.requester.group        = request->cli().user().groupname();
    retrieveRequest.dstURL                 = request->transport().dst_url();
    retrieveRequest.errorReportURL         = request->transport().error_report_url();
    retrieveRequest.diskFileInfo.owner_uid = 1;
    retrieveRequest.diskFileInfo.gid       = 1;
    retrieveRequest.diskFileInfo.path      = "/" + request->file().fid();
    retrieveRequest.creationLog.host       = context->peer();
    retrieveRequest.creationLog.username   = instance;
    retrieveRequest.creationLog.time       = time(nullptr);
    retrieveRequest.isVerifyOnly           = false;

    retrieveRequest.archiveFileID = request->archiveid();

    cta::utils::Timer t;

    // Queue the request
    std::string reqId = m_scheduler->queueRetrieve(instance, retrieveRequest, *m_log);
    m_log->log(cta::log::INFO, "Retrieve request for storageClass: " + storageClass
                               + " archiveFileId: " + std::to_string(retrieveRequest.archiveFileID)
                               + "RequestID: " + reqId);

    response->set_reqid(reqId);
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

CtaRpcImpl::CtaRpcImpl(cta::log::LogContext *lc, std::unique_ptr<cta::catalogue::Catalogue> &catalogue, std::unique_ptr <cta::Scheduler> &scheduler):
    m_catalogue(std::move(catalogue)), m_scheduler(std::move(scheduler)) {
    m_log = lc;
}

using namespace cta;
using namespace cta::common;

int main(const int argc, char *const *const argv) {

    std::unique_ptr <cta::log::Logger> logger = std::unique_ptr<cta::log::Logger>(new log::StdoutLogger("cta-dev", "cta-grpc-frontend", true));
    log::LogContext lc(*logger);

    // use castor config to avoid dependency on xroot-ssi
    Configuration config("/etc/cta/cta.conf");

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


    // Initialise the Scheduler
    auto backed = config.getConfEntString("ObjectStore", "BackendPath");
    lc.log(log::INFO, "Using scheduler backend: " + backed);

    auto sInit = cta::make_unique<SchedulerDBInit_t>("Frontend", backed, *logger);
    auto scheddb = sInit->getSchedDB(*catalogue, *logger);
    scheddb->setBottomHalfQueueSize(25000);
    auto scheduler = cta::make_unique<cta::Scheduler>(*catalogue, *scheddb, 5, 2*1000*1000);

    CtaRpcImpl svc(&lc, catalogue, scheduler);
    svc.run(server_address);
}
