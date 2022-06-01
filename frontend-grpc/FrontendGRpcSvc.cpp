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

#include "common/log/LogLevel.hpp"
#include <common/checksum/ChecksumBlobSerDeser.hpp>

Status CtaRpcImpl::Version(::grpc::ServerContext *context, const ::google::protobuf::Empty *request,
                           ::cta::admin::Version *response) {
    response->set_cta_version(CTA_VERSION);
    return Status::OK;
}

Status CtaRpcImpl::Archive(::grpc::ServerContext* context, const ::cta::dcache::rpc::ArchiveRequest* request, ::cta::dcache::rpc::ArchiveResponse* response) {

    cta::log::LogContext lc(*m_log);
    cta::log::ScopedParamContainer sp(lc);

    lc.log(cta::log::INFO, "Archive request");

    sp.add("remoteHost", context->peer());
    sp.add("request", "archive");

    const std::string storageClass = request->file().storageclass();
    if (storageClass.empty()) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
    }

    lc.log(cta::log::DEBUG, "Archive request for storageClass: " + storageClass);

    cta::common::dataStructures::RequesterIdentity requester;
    requester.name = request->cli().user().username();
    requester.group = request->cli().user().groupname();

    // check validate request args
    if (request->instance().name().empty()) {
        lc.log(cta::log::WARNING, "CTA instance is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
    }

    if (request->cli().user().username().empty()) {
        lc.log(cta::log::WARNING, "CTA username is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
    }

    if (request->cli().user().groupname().empty()) {
        lc.log(cta::log::WARNING, "CTA groupname is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
    }

    auto instance = request->instance().name();
    sp.add("instance", instance);
    sp.add("username", request->cli().user().username());
    sp.add("groupname", request->cli().user().groupname());

    sp.add("storageClass", storageClass);
    sp.add("fileID", request->file().fid());

    try {
        uint64_t archiveFileId = m_scheduler->checkAndGetNextArchiveFileId(instance, storageClass, requester, lc);
        sp.add("archiveID", archiveFileId);

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

        std::string reqId = m_scheduler->queueArchiveWithGivenId(archiveFileId, instance, archiveRequest, lc);
        sp.add("reqId", reqId);

        lc.log(cta::log::INFO, "Archive request for storageClass: " + storageClass
            + " archiveFileId: " + std::to_string(archiveFileId)
            + " RequestID: " + reqId);

        response->set_fid(archiveFileId);
        response->set_reqid(reqId);

    } catch (cta::exception::Exception &ex) {
        lc.log(cta::log::CRIT, ex.getMessageValue());
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
    }

    return Status::OK;
}


Status CtaRpcImpl::Delete(::grpc::ServerContext* context, const ::cta::dcache::rpc::DeleteRequest* request, ::google::protobuf::Empty* response) {

    cta::log::LogContext lc(*m_log);
    cta::log::ScopedParamContainer sp(lc);

    sp.add("remoteHost", context->peer());

    lc.log(cta::log::DEBUG, "Delete request");
    sp.add("request", "delete");

    // check validate request args
    if (request->instance().name().empty()) {
        lc.log(cta::log::WARNING, "CTA instance is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
    }

    if (request->cli().user().username().empty()) {
        lc.log(cta::log::WARNING, "CTA username is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
    }

    if (request->cli().user().groupname().empty()) {
        lc.log(cta::log::WARNING, "CTA groupname is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
    }

    if (request->archiveid() == 0) {
        lc.log(cta::log::WARNING, "Invalid archive file id");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
    }

    auto instance = request->instance().name();
    // Unpack message
    cta::common::dataStructures::DeleteArchiveRequest deleteRequest;
    deleteRequest.requester.name    = request->cli().user().username();
    deleteRequest.requester.group   = request->cli().user().groupname();

    sp.add("instance", instance);
    sp.add("username", request->cli().user().username());
    sp.add("groupname", request->cli().user().groupname());
    sp.add("fileID", request->file().fid());


    deleteRequest.diskFilePath          = "/" + request->file().fid();
    deleteRequest.diskFileId = request->file().fid();
    deleteRequest.diskInstance = instance;

    // remove pending scheduler entry, if any
    deleteRequest.archiveFileID = request->archiveid();
    if (!request->reqid().empty()) {
        deleteRequest.address = request->reqid();
    }

    // Delete the file from the catalogue or from the objectstore if archive request is created
    cta::utils::Timer t;
    try {
        deleteRequest.archiveFile = m_catalogue->getArchiveFileById(deleteRequest.archiveFileID);
    } catch (cta::exception::Exception &ex){
        lc.log(cta::log::WARNING, "Deleted file is not in catalog.");
    }
    m_scheduler->deleteArchive(instance, deleteRequest, lc);

    lc.log(cta::log::INFO, "archive file deleted.");

    return Status::OK;
}

Status CtaRpcImpl::Retrieve(::grpc::ServerContext* context, const ::cta::dcache::rpc::RetrieveRequest* request, ::cta::dcache::rpc::RetrieveResponse *response) {


    cta::log::LogContext lc(*m_log);
    cta::log::ScopedParamContainer sp(lc);

    sp.add("remoteHost", context->peer());
    sp.add("request", "retrieve");

    const std::string storageClass = request->file().storageclass();
    if (storageClass.empty()) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
    }

    lc.log(cta::log::DEBUG, "Retrieve request for storageClass: " + storageClass);

    // check validate request args
    if (request->instance().name().empty()) {
        lc.log(cta::log::WARNING, "CTA instance is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
    }

    if (request->cli().user().username().empty()) {
        lc.log(cta::log::WARNING, "CTA username is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
    }

    if (request->cli().user().groupname().empty()) {
        lc.log(cta::log::WARNING, "CTA groupname is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
    }

    if (request->archiveid() == 0) {
        lc.log(cta::log::WARNING, "Invalid archive file id");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
    }

    auto instance = request->instance().name();

    sp.add("instance", instance);
    sp.add("username", request->cli().user().username());
    sp.add("groupname", request->cli().user().groupname());
    sp.add("storageClass", storageClass);
    sp.add("archiveID", request->archiveid());
    sp.add("fileID", request->file().fid());

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
    sp.add("archiveID", request->archiveid());
    sp.add("fileID", request->file().fid());

    cta::utils::Timer t;

    // Queue the request
    try {
        std::string reqId = m_scheduler->queueRetrieve(instance, retrieveRequest, lc);
        sp.add("reqId", reqId);
        lc.log(cta::log::INFO, "Retrieve request for storageClass: " + storageClass
                               + " archiveFileId: " + std::to_string(retrieveRequest.archiveFileID)
                               + " RequestID: " + reqId);

        response->set_reqid(reqId);
    } catch (cta::exception::Exception &ex){
        lc.log(cta::log::CRIT, ex.getMessageValue());
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
    }
    return Status::OK;
}

Status CtaRpcImpl::CancelRetrieve(::grpc::ServerContext* context, const ::cta::dcache::rpc::CancelRetrieveRequest* request, ::google::protobuf::Empty* response) {

    cta::log::LogContext lc(*m_log);
    cta::log::ScopedParamContainer sp(lc);

    sp.add("remoteHost", context->peer());

    lc.log(cta::log::DEBUG, "CancelRetrieve request");
    sp.add("request", "cancel");

    // check validate request args
    if (request->instance().name().empty()) {
        lc.log(cta::log::WARNING, "CTA instance is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
    }

    if (request->cli().user().username().empty()) {
        lc.log(cta::log::WARNING, "CTA username is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
    }

    if (request->cli().user().groupname().empty()) {
        lc.log(cta::log::WARNING, "CTA groupname is not set");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
    }

    if (request->fid() == 0) {
        lc.log(cta::log::WARNING, "Invalid archive file id");
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
    }

    auto instance = request->instance().name();
    // Unpack message
    cta::common::dataStructures::CancelRetrieveRequest cancelRequest;
    cancelRequest.requester.name    = request->cli().user().username();
    cancelRequest.requester.group   = request->cli().user().groupname();
    cancelRequest.archiveFileID = request->fid();
    cancelRequest.retrieveRequestId = request->reqid();

    sp.add("instance", instance);
    sp.add("username", request->cli().user().username());
    sp.add("groupname", request->cli().user().groupname());
    sp.add("fileID", request->fid());
    sp.add("schedulerJobID", request->reqid());

    m_scheduler->abortRetrieve(instance, cancelRequest, lc);

    lc.log(cta::log::INFO, "retrieve request canceled.");

    return Status::OK;
}

CtaRpcImpl::CtaRpcImpl(cta::log::Logger *logger, std::unique_ptr<cta::catalogue::Catalogue> &catalogue, std::unique_ptr <cta::Scheduler> &scheduler):
    m_catalogue(std::move(catalogue)), m_scheduler(std::move(scheduler)) {
    m_log = logger;
}
