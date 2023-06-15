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

#include "catalogue/Catalogue.hpp"
#include "common/log/LogLevel.hpp"
#include <common/checksum/ChecksumBlobSerDeser.hpp>

Status CtaRpcImpl::Version(::grpc::ServerContext* context,
                           const ::google::protobuf::Empty* request,
                           ::cta::admin::Version* response) {
  response->set_cta_version(CTA_VERSION);
  return Status::OK;
}

/*
 * Validate the storage class and issue the archive ID which should be used for the Archive request
 */
Status CtaRpcImpl::Create(::grpc::ServerContext* context,
                          const ::cta::frontend::rpc::SchedulerRequest* request,
                          ::cta::frontend::rpc::CreateResponse* response) {
  cta::log::LogContext lc(*m_log);
  cta::log::ScopedParamContainer sp(lc);

  lc.log(cta::log::INFO, "Create");

  try {
    auto& instance = request->md().wf().instance().name();
    auto& storageClass = request->md().file().storage_class();
    cta::common::dataStructures::RequesterIdentity requester;
    requester.name = request->md().cli().user().username();
    requester.group = request->md().cli().user().groupname();
    uint64_t archiveFileId = m_scheduler->checkAndGetNextArchiveFileId(instance, storageClass, requester, lc);
    response->set_archive_file_id(archiveFileId);
  }
  catch (cta::exception::Exception& ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
  }

  return Status::OK;
}

Status CtaRpcImpl::Archive(::grpc::ServerContext* context,
                           const ::cta::frontend::rpc::SchedulerRequest* request,
                           ::cta::frontend::rpc::ArchiveResponse* response) {
  cta::log::LogContext lc(*m_log);
  cta::log::ScopedParamContainer sp(lc);

  lc.log(cta::log::INFO, "Archive request");

  sp.add("remoteHost", context->peer());
  sp.add("request", "archive");

  const std::string storageClass = request->md().file().storage_class();
  if (storageClass.empty()) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
  }

  lc.log(cta::log::DEBUG, "Archive request for storageClass: " + storageClass);

  cta::common::dataStructures::RequesterIdentity requester;
  requester.name = request->md().cli().user().username();
  requester.group = request->md().cli().user().groupname();

  // check validate request args
  if (request->md().wf().instance().name().empty()) {
    lc.log(cta::log::WARNING, "CTA instance is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
  }

  if (request->md().cli().user().username().empty()) {
    lc.log(cta::log::WARNING, "CTA username is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
  }

  if (request->md().cli().user().groupname().empty()) {
    lc.log(cta::log::WARNING, "CTA groupname is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
  }

  if (!request->md().file().owner().uid()) {
    lc.log(cta::log::WARNING, "File's owner uid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner uid can't be zero");
  }

  if (!request->md().file().owner().gid()) {
    lc.log(cta::log::WARNING, "File's owner gid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner gid can't be zero");
  }

  if (request->md().file().lpath().empty()) {
    lc.log(cta::log::WARNING, "File's path can't be empty");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's path can't be empty");
  }

  auto instance = request->md().wf().instance().name();
  sp.add("instance", instance);
  sp.add("username", request->md().cli().user().username());
  sp.add("groupname", request->md().cli().user().groupname());

  sp.add("storageClass", storageClass);
  sp.add("fileID", request->md().file().disk_file_id());

  try {
    auto archiveFileId = request->md().file().archive_file_id();
    sp.add("archiveID", archiveFileId);

    cta::common::dataStructures::ArchiveRequest archiveRequest;
    cta::checksum::ProtobufToChecksumBlob(request->md().file().csb(), archiveRequest.checksumBlob);
    archiveRequest.diskFileInfo.owner_uid = request->md().file().owner().uid();
    archiveRequest.diskFileInfo.gid = request->md().file().owner().gid();
    archiveRequest.diskFileInfo.path = request->md().file().lpath();
    archiveRequest.diskFileID = request->md().file().disk_file_id();
    archiveRequest.fileSize = request->md().file().size();
    archiveRequest.requester.name = request->md().cli().user().username();
    archiveRequest.requester.group = request->md().cli().user().groupname();
    archiveRequest.storageClass = storageClass;
    archiveRequest.srcURL = request->md().transport().dst_url();
    archiveRequest.archiveReportURL = request->md().transport().report_url();
    archiveRequest.archiveErrorReportURL = request->md().transport().error_report_url();
    archiveRequest.creationLog.host = context->peer();
    archiveRequest.creationLog.username = instance;
    archiveRequest.creationLog.time = time(nullptr);

    std::string reqId = m_scheduler->queueArchiveWithGivenId(archiveFileId, instance, archiveRequest, lc);
    sp.add("reqId", reqId);

    lc.log(cta::log::INFO, "Archive request for storageClass: " + storageClass +
                             " archiveFileId: " + std::to_string(archiveFileId) + " RequestID: " + reqId);

    response->set_objectstore_id(reqId);
  }
  catch (cta::exception::Exception& ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
  }

  return Status::OK;
}

Status CtaRpcImpl::Delete(::grpc::ServerContext* context,
                          const ::cta::frontend::rpc::SchedulerRequest* request,
                          ::google::protobuf::Empty* response) {
  cta::log::LogContext lc(*m_log);
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());

  lc.log(cta::log::DEBUG, "Delete request");
  sp.add("request", "delete");

  // check validate request args
  if (request->md().wf().instance().name().empty()) {
    lc.log(cta::log::WARNING, "CTA instance is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
  }

  if (request->md().cli().user().username().empty()) {
    lc.log(cta::log::WARNING, "CTA username is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
  }

  if (request->md().cli().user().groupname().empty()) {
    lc.log(cta::log::WARNING, "CTA groupname is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
  }

  if (request->md().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  if (!request->md().file().owner().uid()) {
    lc.log(cta::log::WARNING, "File's owner uid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner uid can't be zero");
  }

  if (!request->md().file().owner().gid()) {
    lc.log(cta::log::WARNING, "File's owner gid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner gid can't be zero");
  }

  if (request->md().file().lpath().empty()) {
    lc.log(cta::log::WARNING, "File's path can't be empty");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's path can't be empty");
  }

  auto instance = request->md().wf().instance().name();
  // Unpack message
  cta::common::dataStructures::DeleteArchiveRequest deleteRequest;
  deleteRequest.requester.name = request->md().cli().user().username();
  deleteRequest.requester.group = request->md().cli().user().groupname();

  sp.add("instance", instance);
  sp.add("username", request->md().cli().user().username());
  sp.add("groupname", request->md().cli().user().groupname());
  sp.add("fileID", request->md().file().disk_file_id());

  deleteRequest.diskFilePath = request->md().file().lpath();
  deleteRequest.diskFileId = request->md().file().disk_file_id();
  deleteRequest.diskInstance = instance;

  // remove pending scheduler entry, if any
  deleteRequest.archiveFileID = request->md().file().archive_file_id();
  if (!request->objectstore_id().empty()) {
    deleteRequest.address = request->objectstore_id();
  }

  // Delete the file from the catalogue or from the objectstore if archive request is created
  cta::utils::Timer t;
  try {
    deleteRequest.archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(deleteRequest.archiveFileID);
  }
  catch (cta::exception::Exception& ex) {
    lc.log(cta::log::WARNING, "Deleted file is not in catalog.");
  }
  m_scheduler->deleteArchive(instance, deleteRequest, lc);

  lc.log(cta::log::INFO, "archive file deleted.");

  return Status::OK;
}

Status CtaRpcImpl::Retrieve(::grpc::ServerContext* context,
                            const ::cta::frontend::rpc::SchedulerRequest* request,
                            ::cta::frontend::rpc::RetrieveResponse* response) {
  cta::log::LogContext lc(*m_log);
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());
  sp.add("request", "retrieve");

  const std::string storageClass = request->md().file().storage_class();
  if (storageClass.empty()) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
  }

  lc.log(cta::log::DEBUG, "Retrieve request for storageClass: " + storageClass);

  // check validate request args
  if (request->md().wf().instance().name().empty()) {
    lc.log(cta::log::WARNING, "CTA instance is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
  }

  if (request->md().cli().user().username().empty()) {
    lc.log(cta::log::WARNING, "CTA username is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
  }

  if (request->md().cli().user().groupname().empty()) {
    lc.log(cta::log::WARNING, "CTA groupname is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
  }

  if (request->md().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  if (!request->md().file().owner().uid()) {
    lc.log(cta::log::WARNING, "File's owner uid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner uid can't be zero");
  }

  if (!request->md().file().owner().gid()) {
    lc.log(cta::log::WARNING, "File's owner gid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner gid can't be zero");
  }

  if (request->md().file().lpath().empty()) {
    lc.log(cta::log::WARNING, "File's path can't be empty");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's path can't be empty");
  }

  auto instance = request->md().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->md().cli().user().username());
  sp.add("groupname", request->md().cli().user().groupname());
  sp.add("storageClass", storageClass);
  sp.add("archiveID", request->md().file().archive_file_id());
  sp.add("fileID", request->md().file().disk_file_id());

  // Unpack message
  cta::common::dataStructures::RetrieveRequest retrieveRequest;
  retrieveRequest.requester.name = request->md().cli().user().username();
  retrieveRequest.requester.group = request->md().cli().user().groupname();
  retrieveRequest.dstURL = request->md().transport().dst_url();
  retrieveRequest.errorReportURL = request->md().transport().error_report_url();
  retrieveRequest.diskFileInfo.owner_uid = request->md().file().owner().uid();
  retrieveRequest.diskFileInfo.gid = request->md().file().owner().gid();
  retrieveRequest.diskFileInfo.path = request->md().file().lpath();
  retrieveRequest.creationLog.host = context->peer();
  retrieveRequest.creationLog.username = instance;
  retrieveRequest.creationLog.time = time(nullptr);
  retrieveRequest.isVerifyOnly = false;

  retrieveRequest.archiveFileID = request->md().file().archive_file_id();
  sp.add("archiveID", request->md().file().archive_file_id());
  sp.add("fileID", request->md().file().disk_file_id());

  cta::utils::Timer t;

  // Queue the request
  try {
    std::string reqId = m_scheduler->queueRetrieve(instance, retrieveRequest, lc);
    sp.add("reqId", reqId);
    lc.log(cta::log::INFO, "Retrieve request for storageClass: " + storageClass + " archiveFileId: " +
                             std::to_string(retrieveRequest.archiveFileID) + " RequestID: " + reqId);

    response->set_objectstore_id(reqId);
  }
  catch (cta::exception::Exception& ex) {
    lc.log(cta::log::CRIT, ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
  }
  return Status::OK;
}

Status CtaRpcImpl::CancelRetrieve(::grpc::ServerContext* context,
                                  const ::cta::frontend::rpc::SchedulerRequest* request,
                                  ::google::protobuf::Empty* response) {
  cta::log::LogContext lc(*m_log);
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());

  lc.log(cta::log::DEBUG, "CancelRetrieve request");
  sp.add("request", "cancel");

  // check validate request args
  if (request->md().wf().instance().name().empty()) {
    lc.log(cta::log::WARNING, "CTA instance is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
  }

  if (request->md().cli().user().username().empty()) {
    lc.log(cta::log::WARNING, "CTA username is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
  }

  if (request->md().cli().user().groupname().empty()) {
    lc.log(cta::log::WARNING, "CTA groupname is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
  }

  if (!request->md().file().archive_file_id()) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  auto instance = request->md().wf().instance().name();
  // Unpack message
  cta::common::dataStructures::CancelRetrieveRequest cancelRequest;
  cancelRequest.requester.name = request->md().cli().user().username();
  cancelRequest.requester.group = request->md().cli().user().groupname();
  cancelRequest.archiveFileID = request->md().file().archive_file_id();
  cancelRequest.retrieveRequestId = request->objectstore_id();

  sp.add("instance", instance);
  sp.add("username", request->md().cli().user().username());
  sp.add("groupname", request->md().cli().user().groupname());
  sp.add("fileID", request->md().file().archive_file_id());
  sp.add("schedulerJobID", request->objectstore_id());

  m_scheduler->abortRetrieve(instance, cancelRequest, lc);

  lc.log(cta::log::INFO, "retrieve request canceled.");

  return Status::OK;
}

CtaRpcImpl::CtaRpcImpl(cta::log::Logger* logger,
                       std::unique_ptr<cta::catalogue::Catalogue>& catalogue,
                       std::unique_ptr<cta::Scheduler>& scheduler) :
m_catalogue(std::move(catalogue)),
m_scheduler(std::move(scheduler)) {
  m_log = logger;
}
