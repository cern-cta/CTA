/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "WorkflowEvent.hpp"

#include "catalogue/Catalogue.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/utils/StringConversions.hpp"
#include "grpc-server/PbException.hpp"
#include "grpc-server/server/RequestTracker.hpp"
#include "protobuf/common/ChecksumBlobSerDeser.hpp"
#include "telemetry/metrics/FrontendMetrics.hpp"

#include <opentelemetry/context/runtime_context.h>
#include <optional>

namespace cta::workflow_api {

namespace {

/// Throw an exception for empty protocol buffer strings
void checkIsNotEmptyString(const std::string& value, const std::string& name) {
  if (value.empty()) {
    throw exception::PbException("Protocol buffer field " + name + " is an empty string.");
  }
}

}  // namespace

namespace proto_notifications {

/// Validate and extract the identity of the user who triggered the event
common::dataStructures::RequesterIdentity getRequester(const eos::Notification& event) {
  checkIsNotEmptyString(event.cli().user().username(), "m_event.cli.user.username");
  checkIsNotEmptyString(event.cli().user().groupname(), "m_event.cli.user.groupname");

  common::dataStructures::RequesterIdentity requester;
  requester.name = event.cli().user().username();
  requester.group = event.cli().user().groupname();
  return requester;
}

/// Extract the archive file ID. Legacy disk systems may be sending it as an extended attribute
/// instead of explicitly. Older files may still use the legacy `CTA_ArchiveFileId`
uint64_t getArchiveFileId(const eos::Notification& event) {
  // try the first-class attribute which comes in the Notification
  if (const uint64_t archiveFileId = event.file().archive_file_id(); archiveFileId != 0) {
    return archiveFileId;
  }

  // look for the extended attribute
  const auto& xattrs = event.file().xattr();
  auto archiveFileIdItor = xattrs.find("sys.archive.file_id");
  if (archiveFileIdItor == xattrs.end()) {
    // if we don't find `sys.archive.file_id`, try the legacy version
    archiveFileIdItor = xattrs.find("CTA_ArchiveFileId");
  }

  // if we don't find anything, throw an exception
  if (archiveFileIdItor == xattrs.end()) {
    throw exception::PbException(Workflow_EventType_Name(event.wf().event())
                                 + ": Failed to find the extended attribute named sys.archive.file_id");
  }

  const std::string& archiveFileIdStr = archiveFileIdItor->second;
  const auto archiveFileId = utils::tryToUint64(archiveFileIdStr);
  if (!archiveFileId || archiveFileId.value() == 0) {
    throw exception::PbException("Invalid archiveFileID " + archiveFileIdStr);
  }
  return archiveFileId.value();
}

/// Extract the scheduler request ID. The first-class attribute takes priority; if it is empty, fall back to the given
/// extended attribute
///
/// @return std::nullopt if neither is present
std::optional<std::string> getRequestId(const eos::Notification& event, const std::string& xattrKey) {
  if (const std::string& requestId = event.file().request_objectstore_id(); !requestId.empty()) {
    return requestId;
  }
  if (const auto requestIdItor = event.file().xattr().find(xattrKey); requestIdItor != event.file().xattr().end()) {
    return requestIdItor->second;
  }
  return std::nullopt;
}

}  // namespace proto_notifications

using proto_notifications::getArchiveFileId;
using proto_notifications::getRequester;
using proto_notifications::getRequestId;

WorkflowEvent::WorkflowEvent(const service_resources::ServiceResources& serviceResources,
                             const workflow_api::WorkflowApiConfig& config,
                             const common::dataStructures::SecurityIdentity& clientIdentity,
#ifdef CTA_PGSCHED
                             FrontendBatchingLayer& batchingLayer,
#endif
                             const eos::Notification& event)
    : m_event(event),
      m_cliIdentity(clientIdentity),
      m_catalogue(serviceResources.getCatalogue()),
      m_scheduler(serviceResources.getScheduler()),
#ifdef CTA_PGSCHED
      m_batchingLayer(&batchingLayer),
#endif
      m_lc(serviceResources.getLogContext()),
      m_verificationMountPolicy(config.requests.prepare.verification_mount_policy),
      m_zeroLengthFilesDisallowed(config.requests.archive.zero_length_files_forbidden),
      m_zeroLengthFilesDisallowedExceptions {config.requests.archive.zero_length_vo_exceptions.begin(),
                                             config.requests.archive.zero_length_vo_exceptions.end()} {
  m_lc.push({"user", m_cliIdentity.username});

  // Log event before processing. This corresponds to the entry in WFE.log in EOS.
  const std::string& eventTypeName = Workflow_EventType_Name(event.wf().event());
  const std::string& eosInstanceName = event.wf().instance().name();
  const std::string& diskFilePath = event.file().lpath();
  const std::string& diskFileId = event.file().disk_file_id();
  log::ScopedParamContainer params(m_lc);
  params.add("eventType", eventTypeName)
    .add("eosInstance", eosInstanceName)
    .add("diskFilePath", diskFilePath)
    .add("diskFileId", diskFileId);

  m_lc.log(log::INFO, "In WorkflowEvent::WorkflowEvent(): received event.");

  // Validate that instance name in key used to authenticate == instance name in protocol buffer
  if (m_cliIdentity.username != event.wf().instance().name()) {
    // Special case:
    // Allow KRB5 authentication for CLOSEW and PREPARE events, to allow operators to use a command line
    // tool to resubmit failed archive or prepare requests. This is NOT permitted for DELETE events as we
    // don't want files removed from the catalogue to be left in the disk namespace.
    if (m_cliIdentity.authProtocol == common::dataStructures::SecurityIdentity::Protocol::KRB5
        && (event.wf().event() == eos::Workflow::CLOSEW || event.wf().event() == eos::Workflow::PREPARE)) {
      m_scheduler.authorizeAdmin(m_cliIdentity, m_lc);
      m_cliIdentity.username = event.wf().instance().name();
    } else {
      throw exception::PbException("Instance name \"" + event.wf().instance().name()
                                   + "\" does not match key identifier \"" + m_cliIdentity.username + "\"");
    }
  }
  // Refuse any workflow events for files in /eos/INSTANCE_NAME/proc/
  const std::string& longInstanceName = event.wf().instance().name();
  const bool longInstanceNameStartsWithEos = (0 == longInstanceName.find("eos"));
  const std::string shortInstanceName = longInstanceNameStartsWithEos ? longInstanceName.substr(3) : longInstanceName;
  if (shortInstanceName.empty()) {
    std::ostringstream msg;
    msg << "Short instance name is an empty string: instance=" << longInstanceName;
    throw exception::PbException(msg.str());
  }
  const std::string procFullPath = std::string("/eos/") + shortInstanceName + "/proc/";
  if (event.file().lpath().find(procFullPath) == 0) {
    std::ostringstream msg;
    msg << "Cannot process a workflow event for a file in " << procFullPath << " instance=" << longInstanceName
        << " event=" << Workflow_EventType_Name(event.wf().event()) << " lpath=" << event.file().lpath();
    throw exception::PbException(msg.str());
  }
}

xrd::Response WorkflowEvent::process() {
  cta::grpc_server::RequestTracker requestTracker(Workflow_EventType_Name(m_event.wf().event()),
                                                  m_cliIdentity.username);
  xrd::Response response;

  try {
    switch (m_event.wf().event()) {
      using namespace cta::eos;

      case Workflow::OPENW:
        processOPENW(response);
        break;
      case Workflow::CREATE:
        processCREATE(response);
        break;
      case Workflow::CLOSEW:
        processCLOSEW(response);
        break;
      case Workflow::PREPARE:
        processPREPARE(response);
        break;
      case Workflow::ABORT_PREPARE:
        processABORT_PREPARE(response);
        break;
      case Workflow::DELETE:
        processDELETE(response);
        break;
      case Workflow::UPDATE_FID:
        processUPDATE_FID(response);
        break;
      default:
        throw exception::PbException("Workflow event " + Workflow_EventType_Name(m_event.wf().event())
                                     + " is not implemented.");
    }
  } catch (exception::UserError&) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kUserError);
    throw;
  } catch (...) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    throw;
  }

  return response;
}

void WorkflowEvent::processOPENW(xrd::Response& response) {
  // Create a log entry
  m_lc.log(log::INFO, "In WorkflowEvent::processOPENW(): ignoring OPENW event.");

  // Set response type
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processCREATE(xrd::Response& response) {
  // Validate received protobuf and unpack message
  const auto requester = getRequester(m_event);
  if (m_event.file().owner().uid() == 0) {
    throw exception::PbException("CREATE: file owner uid must be non-zero (archive files cannot be owned by root)");
  }

  utils::Timer t;
  uint64_t archiveFileId;
  // Check the standalone storage class attribute
  std::string storageClassStr = m_event.file().storage_class();
  if (storageClassStr.empty()) {
    throw exception::PbException("CREATE: storage class is not set.");
  }
  if (storageClassStr == "fail_on_closew_test") {
    archiveFileId = std::numeric_limits<uint64_t>::max();
  } else {
    archiveFileId = m_scheduler.checkAndGetNextArchiveFileId(m_cliIdentity.username, storageClassStr, requester, m_lc);
  }

  // Create a log entry
  log::ScopedParamContainer params(m_lc);
  params.add("diskFileId", m_event.file().disk_file_id())
    .add("diskFilePath", m_event.file().lpath())
    .add("fileId", archiveFileId)
    .add("schedulerTime", t.secs());
  m_lc.log(log::INFO, "In WorkflowEvent::processCREATE(): assigning new archive file ID.");

  // Set ArchiveFileId
  response.mutable_xattr()->insert(
    google::protobuf::MapPair<std::string, std::string>("sys.archive.file_id", std::to_string(archiveFileId)));
  response.set_archive_file_id(std::to_string(archiveFileId));

  // Set response type
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processCLOSEW(xrd::Response& response) {
  // Validate received protobuf
  const auto requester = getRequester(m_event);
  checkIsNotEmptyString(m_event.file().lpath(), "m_event.file.lpath");
  checkIsNotEmptyString(m_event.wf().instance().url(), "m_event.wf.instance.url");
  checkIsNotEmptyString(m_event.transport().report_url(), "m_event.transport.report_url");
  checkIsNotEmptyString(m_event.wf().instance().name(), "m_event.wf.instance.name");

  // Check storage class attribute in the first-class attributes
  std::string storageClassStr = m_event.file().storage_class();
  if (storageClassStr.empty()) {
    throw exception::PbException("CLOSEW: storage class is not set.");
  }

  // For testing: this storage class will always fail
  if (storageClassStr == "fail_on_closew_test") {
    throw exception::UserError("File is in fail_on_closew_test storage class, which always fails.");
  }

  {
    auto storageClass = m_catalogue.StorageClass()->getStorageClass(storageClassStr);
    // Disallow archival of files above the specified limit
    if (storageClass.vo.maxFileSize && m_event.file().size() > storageClass.vo.maxFileSize) {
      throw exception::UserError("Archive request rejected: file size (" + std::to_string(m_event.file().size())
                                 + " bytes) exceeds maximum allowed size ("
                                 + std::to_string(storageClass.vo.maxFileSize) + " bytes)");
    }
    // Check that the file is not 0-length
    if ((m_event.file().size() == 0) && m_zeroLengthFilesDisallowed
        && !m_zeroLengthFilesDisallowedExceptions.count(storageClass.vo.name)) {
      throw exception::UserError("Archival of 0-length files not allowed.");
    }
  }

  common::dataStructures::ArchiveRequest request;
  checksum::ProtobufToChecksumBlob(m_event.file().csb(), request.checksumBlob);
  request.diskFileInfo.owner_uid = m_event.file().owner().uid();
  request.diskFileInfo.gid = m_event.file().owner().gid();
  request.diskFileInfo.path = m_event.file().lpath();
  request.diskFileID = m_event.file().disk_file_id();
  request.fileSize = m_event.file().size();
  request.requester = requester;
  request.srcURL = m_event.wf().instance().url();
  request.storageClass = storageClassStr;
  request.archiveReportURL = m_event.transport().report_url();
  request.archiveErrorReportURL = m_event.transport().error_report_url();
  request.creationLog.host = m_cliIdentity.host;
  request.creationLog.username = m_cliIdentity.username;
  request.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

  log::ScopedParamContainer params(m_lc);
  params.add("requesterInstance", m_event.wf().requester_instance());
  std::string logMessage = "In WorkflowEvent::processCLOSEW(): ";

  // The first-class attribute is expected to be set by dCache; EOS only sets the xattr
  uint64_t archiveFileId = 0;
  try {
    archiveFileId = getArchiveFileId(m_event);
  } catch (exception::PbException& ex) {
    m_lc.log(log::INFO, logMessage + ex.getMessageValue());
    throw;
  }
  params.add("fileId", archiveFileId);

  utils::Timer t;

  if (request.fileSize > 0) {
    // Queue the request
    std::string archiveRequestAddr =
#ifdef CTA_PGSCHED
      m_batchingLayer->queueArchiveWithGivenId(archiveFileId, m_cliIdentity.username, request, m_lc);
#else
      m_scheduler.queueArchiveWithGivenId(archiveFileId, m_cliIdentity.username, request, m_lc);
#endif
    logMessage += "queued file for archive.";
    params.add("schedulerTime", t.secs());
    params.add("archiveRequestId", archiveRequestAddr);

    // Add archive request reference to response as an extended attribute
    response.mutable_xattr()->insert(
      google::protobuf::MapPair<std::string, std::string>("sys.cta.objectstore.id", archiveRequestAddr));
    // also add the request_objectstore_id (expected by the dCache/gRPC frontend)
    response.set_request_objectstore_id(archiveRequestAddr);
  } else {
    logMessage += "ignoring zero-length file.";
  }

  // Create a log entry
  m_lc.log(log::INFO, logMessage);

  // Set response type
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processPREPARE(xrd::Response& response) {
  // Validate received protobuf
  const auto requester = getRequester(m_event);
  checkIsNotEmptyString(m_event.file().lpath(), "m_event.file.lpath");
  checkIsNotEmptyString(m_event.transport().dst_url(), "m_event.transport.dst_url");

  // Unpack message
  common::dataStructures::RetrieveRequest request;
  request.requester = requester;
  request.dstURL = m_event.transport().dst_url();
  request.retrieveReportURL = m_event.transport().report_url();
  request.errorReportURL = m_event.transport().error_report_url();
  request.diskFileInfo.owner_uid = m_event.file().owner().uid();
  request.diskFileInfo.gid = m_event.file().owner().gid();
  request.diskFileInfo.path = m_event.file().lpath();
  request.creationLog.host = m_cliIdentity.host;
  request.creationLog.username = m_cliIdentity.username;
  request.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  request.isVerifyOnly = m_event.wf().verify_only();
  if (request.isVerifyOnly) {
    if (m_verificationMountPolicy.empty()) {
      throw exception::UserError("No verification mount policy is configured.");
    }
    request.mountPolicy = m_verificationMountPolicy;
  }

  // Vid is for tape verification use case (for dual-copy files) so normally is not specified
  if (!m_event.wf().vid().empty()) {
    request.vid = m_event.wf().vid();
  }

  request.archiveFileID = getArchiveFileId(m_event);

  // Activity value is a string. The parameter might be present or not.
  if (m_event.file().xattr().contains("activity")) {
    request.activity = m_event.file().xattr().at("activity");
  }

  utils::Timer t;

  // Queue the request
  std::string retrieveReqId =
#ifdef CTA_PGSCHED
    m_batchingLayer->queueRetrieve(m_cliIdentity.username, request, m_lc);
#else
    m_scheduler.queueRetrieve(m_cliIdentity.username, request, m_lc);
#endif

  // Create a log entry
  log::ScopedParamContainer params(m_lc);
  params.add("fileId", request.archiveFileID)
    .add("schedulerTime", t.secs())
    .add("isVerifyOnly", request.isVerifyOnly)
    .add("retrieveReqId", retrieveReqId);
  if (static_cast<bool>(request.activity)) {
    params.add("activity", request.activity.value());
  }
  m_lc.log(log::INFO, "In WorkflowEvent::processPREPARE(): queued file for retrieve.");

  // Set response type and add retrieve request reference as an extended attribute.
  response.mutable_xattr()->insert(
    google::protobuf::MapPair<std::string, std::string>("sys.cta.objectstore.id", retrieveReqId));
  // also set it in the first-class citizen attribute
  response.set_request_objectstore_id(retrieveReqId);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processABORT_PREPARE(xrd::Response& response) {
  // Validate received protobuf and unpack message
  common::dataStructures::CancelRetrieveRequest request;
  request.requester = getRequester(m_event);
  request.archiveFileID = getArchiveFileId(m_event);

  auto retrieveRequestId = getRequestId(m_event, "sys.cta.objectstore.id");
  if (!retrieveRequestId) {
    throw exception::PbException("ABORT_PREPARE: Failed to find the extended attribute named sys.cta.objectstore.id");
  }
  request.retrieveRequestId = std::move(*retrieveRequestId);

  utils::Timer t;

  // Queue the request
  m_scheduler.abortRetrieve(m_cliIdentity.username, request, m_lc);

  // Create a log entry
  log::ScopedParamContainer params(m_lc);
  params.add("fileId", request.archiveFileID)
    .add("schedulerTime", t.secs())
    .add("retrieveRequestId", request.retrieveRequestId)
    .add("diskFilePath", utils::midEllipsis(m_event.file().lpath(), 100));
  m_lc.log(log::INFO, "In WorkflowEvent::processABORT_PREPARE(): canceled retrieve request.");

  // Set response type and remove reference to retrieve request in EOS extended attributes.
  response.mutable_xattr()->insert(google::protobuf::MapPair<std::string, std::string>("sys.cta.objectstore.id", ""));
  // reset request_objectstore_id for consistency with xattrs
  response.set_request_objectstore_id("");
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processDELETE(xrd::Response& response) {
  // Validate received protobuf
  const auto requester = getRequester(m_event);
  checkIsNotEmptyString(m_event.file().lpath(), "m_event.file.lpath");

  // Unpack message
  common::dataStructures::DeleteArchiveRequest request;
  if (!m_event.file().csb().cs().empty()) {
    checksum::ChecksumBlob csb;
    checksum::ProtobufToChecksumBlob(m_event.file().csb(), csb);
    request.checksumBlob = csb;
  }

  // Log with file size
  if (m_event.file().size() != 0) {
    request.diskFileSize = m_event.file().size();
  }
  request.requester = requester;
  request.diskFilePath = m_event.file().lpath();
  request.diskFileId = m_event.file().disk_file_id();
  request.diskInstance = m_cliIdentity.username;
  request.archiveFileID = getArchiveFileId(m_event);
  // Address of the ArchiveRequest, if the file is still queued for archival
  if (auto archiveRequestAddr = getRequestId(m_event, "sys.cta.archive.objectstore.id");
      archiveRequestAddr && !archiveRequestAddr->empty()) {
    request.address = std::move(*archiveRequestAddr);
  }

  // Delete the file from the catalogue or from the objectstore if archive request is created
  utils::Timer t;
  log::TimingList tl;
  try {
    request.archiveFile = m_catalogue.ArchiveFile()->getArchiveFileById(request.archiveFileID);
    tl.insertAndReset("catalogueGetArchiveFileByIdTime", t);
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer spc(m_lc);
    spc.add("fileId", request.archiveFileID);
    spc.add("catalogueError", ex.getMessage().str());
    m_lc.log(log::WARNING,
             "Received an exception when trying to get archive file by id. Ignoring request to delete archive file.");
  }

  m_scheduler.deleteArchive(m_cliIdentity.username, request, m_lc);
  tl.insertAndReset("schedulerTime", t);
  // Create a log entry
  log::ScopedParamContainer params(m_lc);
  params.add("fileId", request.archiveFileID)
    .add("address", (request.address ? request.address.value() : "null"))
    .add("filePath", request.diskFilePath);
  tl.addToLog(params);
  m_lc.log(log::INFO, "In WorkflowEvent::processDELETE(): archive file deleted.");

  // Set response type
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processUPDATE_FID(xrd::Response& response) {
  // Validate received protobuf
  checkIsNotEmptyString(m_event.file().lpath(), "m_event.file.lpath");

  // Unpack message
  const std::string& diskInstance = m_cliIdentity.username;
  const std::string& diskFilePath = m_event.file().lpath();
  const std::string diskFileId = m_event.file().disk_file_id();
  const uint64_t archiveFileId = getArchiveFileId(m_event);

  // Update the disk file ID
  utils::Timer t;
  m_catalogue.ArchiveFile()->updateDiskFileId(archiveFileId, diskInstance, diskFileId);

  // Create a log entry
  log::ScopedParamContainer params(m_lc);
  params.add("fileId", archiveFileId)
    .add("schedulerTime", t.secs())
    .add("diskInstance", diskInstance)
    .add("diskFilePath", diskFilePath)
    .add("diskFileId", diskFileId);
  m_lc.log(log::INFO, "In WorkflowEvent::processUPDATE_FID(): updated disk file ID.");

  // Set response type
  response.set_type(xrd::Response::RSP_SUCCESS);
}

}  // namespace cta::workflow_api
