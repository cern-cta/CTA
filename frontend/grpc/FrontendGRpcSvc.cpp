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
#include "frontend/common/WorkflowEvent.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "frontend/common/FrontendService.hpp"

/*
 * Validate the storage class and issue the archive ID which should be used for the Archive request
 */
Status
CtaRpcImpl::Create(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc = m_frontendService->getLogContext();
  cta::log::ScopedParamContainer sp(lc);

  lc.log(cta::log::INFO, "Create");

  try {
    cta::eos::Client client = request->notification().cli();
    cta::common::dataStructures::SecurityIdentity clientIdentity(client.sec().name(), cta::utils::getShortHostname(),
                                                                 client.sec().host(), client.sec().prot());
    cta::frontend::WorkflowEvent wfe(*m_frontendService, clientIdentity, request->notification());
    *response = wfe.process();
  } catch (cta::exception::Exception &ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
  }

  return Status::OK;
}

Status
CtaRpcImpl::Archive(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc = m_frontendService->getLogContext();
  cta::log::ScopedParamContainer sp(lc);

  lc.log(cta::log::INFO, "Archive request");

  sp.add("remoteHost", context->peer());
  sp.add("request", "archive");

  const std::string storageClass = request->notification().file().storage_class();
  if (storageClass.empty()) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
  }

  lc.log(cta::log::DEBUG, "Archive request for storageClass: " + storageClass);

  cta::common::dataStructures::RequesterIdentity requester;
  requester.name = request->notification().cli().user().username();
  requester.group = request->notification().cli().user().groupname();

  // check validate request args
  if (request->notification().wf().instance().name().empty()) {
    lc.log(cta::log::WARNING, "CTA instance is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
  }

  if (request->notification().cli().user().username().empty()) {
    lc.log(cta::log::WARNING, "CTA username is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
  }

  if (request->notification().cli().user().groupname().empty()) {
    lc.log(cta::log::WARNING, "CTA groupname is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
  }

  if (!request->notification().file().owner().uid()) {
    lc.log(cta::log::WARNING, "File's owner uid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner uid can't be zero");
  }

  if (!request->notification().file().owner().gid()) {
    lc.log(cta::log::WARNING, "File's owner gid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner gid can't be zero");
  }

  if (request->notification().file().lpath().empty()) {
    lc.log(cta::log::WARNING, "File's path can't be empty");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's path can't be empty");
  }

  auto instance = request->notification().wf().instance().name();
  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());

  sp.add("storageClass", storageClass);
  sp.add("fileID", request->notification().file().disk_file_id());

  try {
    cta::eos::Client client = request->notification().cli();
    cta::common::dataStructures::SecurityIdentity clientIdentity(client.sec().name(), cta::utils::getShortHostname(),
                                                                 client.sec().host(), client.sec().prot());
    cta::frontend::WorkflowEvent wfe(*m_frontendService, clientIdentity, request->notification());
    *response = wfe.process();
  }
  catch (cta::exception::Exception& ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
  }

  return Status::OK;
}

Status
CtaRpcImpl::Delete(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc = m_frontendService->getLogContext();
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());

  lc.log(cta::log::DEBUG, "Delete request");
  sp.add("request", "delete");

  // check validate request args
  if (request->notification().wf().instance().name().empty()) {
    lc.log(cta::log::WARNING, "CTA instance is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
  }

  if (request->notification().cli().user().username().empty()) {
    lc.log(cta::log::WARNING, "CTA username is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
  }

  if (request->notification().cli().user().groupname().empty()) {
    lc.log(cta::log::WARNING, "CTA groupname is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
  }

  if (request->notification().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  if (!request->notification().file().owner().uid()) {
    lc.log(cta::log::WARNING, "File's owner uid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner uid can't be zero");
  }

  if (!request->notification().file().owner().gid()) {
    lc.log(cta::log::WARNING, "File's owner gid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner gid can't be zero");
  }

  if (request->notification().file().lpath().empty()) {
    lc.log(cta::log::WARNING, "File's path can't be empty");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's path can't be empty");
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("fileID", request->notification().file().disk_file_id());

  // done with validation, now add the workflow processing logic
  try {
    cta::eos::Client client = request->notification().cli();
    cta::common::dataStructures::SecurityIdentity clientIdentity(client.sec().name(), cta::utils::getShortHostname(),
                                                                 client.sec().host(), client.sec().prot());
    cta::frontend::WorkflowEvent wfe(*m_frontendService, clientIdentity, request->notification());
    *response = wfe.process();
    lc.log(cta::log::INFO, "archive file deleted.");
  }
  catch (cta::exception::Exception& ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
  }
  return Status::OK;
}

Status
CtaRpcImpl::Retrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc = m_frontendService->getLogContext();
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());
  sp.add("request", "retrieve");

  const std::string storageClass = request->notification().file().storage_class();
  if (storageClass.empty()) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
  }

  lc.log(cta::log::DEBUG, "Retrieve request for storageClass: " + storageClass);

  // check validate request args
  if (request->notification().wf().instance().name().empty()) {
    lc.log(cta::log::WARNING, "CTA instance is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
  }

  if (request->notification().cli().user().username().empty()) {
    lc.log(cta::log::WARNING, "CTA username is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
  }

  if (request->notification().cli().user().groupname().empty()) {
    lc.log(cta::log::WARNING, "CTA groupname is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
  }

  if (request->notification().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  if (!request->notification().file().owner().uid()) {
    lc.log(cta::log::WARNING, "File's owner uid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner uid can't be zero");
  }

  if (!request->notification().file().owner().gid()) {
    lc.log(cta::log::WARNING, "File's owner gid can't be zero");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's owner gid can't be zero");
  }

  if (request->notification().file().lpath().empty()) {
    lc.log(cta::log::WARNING, "File's path can't be empty");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "File's path can't be empty");
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("storageClass", storageClass);
  sp.add("archiveID", request->notification().file().archive_file_id());
  sp.add("fileID", request->notification().file().disk_file_id());

  try {
    cta::eos::Client client = request->notification().cli();
    cta::common::dataStructures::SecurityIdentity clientIdentity(client.sec().name(), cta::utils::getShortHostname(),
                                                                 client.sec().host(), client.sec().prot());
    cta::frontend::WorkflowEvent wfe(*m_frontendService, clientIdentity, request->notification());
    *response = wfe.process();
  }
  catch (cta::exception::Exception& ex) {
    lc.log(cta::log::CRIT, ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
  }
  return Status::OK;
}

Status CtaRpcImpl::CancelRetrieve(::grpc::ServerContext* context,
                                  const cta::xrd::Request* request,
                                  cta::xrd::Response* response) {
  cta::log::LogContext lc = m_frontendService->getLogContext();
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());

  lc.log(cta::log::DEBUG, "CancelRetrieve request");
  sp.add("request", "cancel");

  // check validate request args
  if (request->notification().wf().instance().name().empty()) {
    lc.log(cta::log::WARNING, "CTA instance is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA instance is not set.");
  }

  if (request->notification().cli().user().username().empty()) {
    lc.log(cta::log::WARNING, "CTA username is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA username is not set.");
  }

  if (request->notification().cli().user().groupname().empty()) {
    lc.log(cta::log::WARNING, "CTA groupname is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "CTA groupname is not set.");
  }

  if (!request->notification().file().archive_file_id()) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("fileID", request->notification().file().archive_file_id());
  sp.add("schedulerJobID", request->notification().file().archive_file_id());

  // field verification done, now try to call the process method
  try {
    cta::eos::Client client = request->notification().cli();
    cta::common::dataStructures::SecurityIdentity clientIdentity(client.sec().name(), cta::utils::getShortHostname(),
                                                                 client.sec().host(), client.sec().prot());
    cta::frontend::WorkflowEvent wfe(*m_frontendService, clientIdentity, request->notification());
    *response = wfe.process();
    lc.log(cta::log::INFO, "retrieve request canceled.");
  }
  catch (cta::exception::Exception& ex) {
    lc.log(cta::log::CRIT, ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
  }
  return Status::OK;

  return Status::OK;
}

/* initialize the frontend service
 * this iniitalizes the catalogue, scheduler, logger
 * and makes the rpc calls available through this class
 */
CtaRpcImpl::CtaRpcImpl(const std::string& config)
    : m_frontendService(std::make_unique<cta::frontend::FrontendService>(config)) {}