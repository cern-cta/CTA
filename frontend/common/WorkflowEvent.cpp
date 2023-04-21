/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2023 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "PbException.hpp"
#include "WorkflowEvent.hpp"

namespace cta {
namespace frontend {

WorkflowEvent::WorkflowEvent(const frontend::FrontendService& frontendService,
  const common::dataStructures::SecurityIdentity& clientIdentity,
  const eos::Notification& event) :
  m_event(event),
  m_cliIdentity(clientIdentity),
  m_catalogue(frontendService.getCatalogue()),
  m_scheduler(frontendService.getScheduler()),
  m_lc(frontendService.getLogContext()),
  m_verificationMountPolicy(frontendService.getVerificationMountPolicy())
{
  m_lc.pushOrReplace({"user", m_cliIdentity.username + "@" + m_cliIdentity.host});

  // Log event before processing. This corresponds to the entry in WFE.log in EOS.
  {
    const std::string& eventTypeName = Workflow_EventType_Name(event.wf().event());
    const std::string& eosInstanceName = event.wf().instance().name();
    const std::string& diskFilePath = event.file().lpath();
    const std::string& diskFileId = std::to_string(event.file().fid());
    log::ScopedParamContainer params(m_lc);
    params.add("eventType", eventTypeName)
          .add("eosInstance", eosInstanceName)
          .add("diskFilePath", diskFilePath)
          .add("diskFileId", diskFileId);
    m_lc.log(log::INFO, "In WorkflowEvent::WorkflowEvent(): received event.");
  }
  // Validate that instance name in key used to authenticate == instance name in protocol buffer
  if(m_cliIdentity.username != event.wf().instance().name()) {
    // Special case:
    // Allow KRB5 authentication for CLOSEW and PREPARE events, to allow operators to use a command line
    // tool to resubmit failed archive or prepare requests. This is NOT permitted for DELETE events as we
    // don't want files removed from the catalogue to be left in the disk namespace.
    if(m_cliIdentity.authProtocol == common::dataStructures::SecurityIdentity::Protocol::KRB5 &&
      (event.wf().event() == eos::Workflow::CLOSEW ||
       event.wf().event() == eos::Workflow::PREPARE)) {
      m_scheduler.authorizeAdmin(m_cliIdentity, m_lc);
      m_cliIdentity.username = event.wf().instance().name();
    } else {
      throw exception::PbException("Instance name \"" + event.wf().instance().name() +
        "\" does not match key identifier \"" + m_cliIdentity.username + "\"");
    }
  }
  // Refuse any workflow events for files in /eos/INSTANCE_NAME/proc/
  {
    const std::string& longInstanceName = event.wf().instance().name();
    const bool longInstanceNameStartsWithEos = (0 == longInstanceName.find("eos"));
    const std::string shortInstanceName =
      longInstanceNameStartsWithEos ? longInstanceName.substr(3) : longInstanceName;
    if(shortInstanceName.empty()) {
      std::ostringstream msg;
      msg << "Short instance name is an empty string: instance=" << longInstanceName;
      throw exception::PbException(msg.str());
    }
    const std::string procFullPath = std::string("/eos/") + shortInstanceName + "/proc/";
    if(event.file().lpath().find(procFullPath) == 0) {
      std::ostringstream msg;
      msg << "Cannot process a workflow event for a file in " << procFullPath << " instance=" << longInstanceName
          << " event=" << Workflow_EventType_Name(event.wf().event()) << " lpath=" << event.file().lpath();
      throw exception::PbException(msg.str());
    }
  }
}

xrd::Response WorkflowEvent::process() {
  xrd::Response response;

  switch(m_event.wf().event()) {
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
      throw exception::PbException("Workflow event " + Workflow_EventType_Name(m_event.wf().event()) +
        " is not implemented.");
  }
  return response;
}

void WorkflowEvent::processOPENW(xrd::Response& response) {
  // Create a log entry
  log::ScopedParamContainer params(m_lc);
  m_lc.log(log::INFO, "In WorkflowEvent::processOPENW(): ignoring OPENW event.");

  // Set response type
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processCREATE(xrd::Response& response) {
  // Validate received protobuf
  checkIsNotEmptyString(m_event.cli().user().username(),  "m_event.cli.user.username");
  checkIsNotEmptyString(m_event.cli().user().groupname(), "m_event.cli.user.groupname");

  // Unpack message
  common::dataStructures::RequesterIdentity requester;
  requester.name  = m_event.cli().user().username();
  requester.group = m_event.cli().user().groupname();

  auto storageClassItor = m_event.file().xattr().find("sys.archive.storage_class");
  if(m_event.file().xattr().end() == storageClassItor) {
    // Fall back to old xattr format
    storageClassItor = m_event.file().xattr().find("CTA_StorageClass");
    if(m_event.file().xattr().end() == storageClassItor) {
      throw exception::PbException(std::string(__FUNCTION__) + ": sys.archive.storage_class extended attribute is not set");
    }
  }
  const std::string storageClass = storageClassItor->second;
  if(storageClass.empty()) {
    throw exception::PbException(std::string(__FUNCTION__) + ": sys.archive.storage_class extended attribute is set to an empty string");
  }

  utils::Timer t;
  uint64_t archiveFileId;

  // For testing, this storage class will always fail on CLOSEW. Allow it to pass CREATE and don't allocate an archive Id from the pool.
  if(storageClassItor->second == "fail_on_closew_test") {
    archiveFileId = std::numeric_limits<uint64_t>::max();
  } else {
    archiveFileId = m_scheduler.checkAndGetNextArchiveFileId(m_cliIdentity.username, storageClass, requester, m_lc);
  }

  // Create a log entry
  log::ScopedParamContainer params(m_lc);
  params.add("diskFileId", std::to_string(m_event.file().fid()))
        .add("diskFilePath", m_event.file().lpath())
        .add("fileId", archiveFileId)
        .add("schedulerTime", t.secs());
  m_lc.log(log::INFO, "In WorkflowEvent::processCREATE(): assigning new archive file ID.");

  // Set ArchiveFileId in xattrs
  response.mutable_xattr()->insert(google::protobuf::MapPair<std::string,std::string>("sys.archive.file_id", std::to_string(archiveFileId)));

  // Set the storage class in xattrs
  response.mutable_xattr()->insert(google::protobuf::MapPair<std::string,std::string>("sys.archive.storage_class", storageClass));

  // Set response type
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processCLOSEW(xrd::Response& response) {
  // Validate received protobuf
  checkIsNotEmptyString(m_event.cli().user().username(),    "m_event.cli.user.username");
  checkIsNotEmptyString(m_event.cli().user().groupname(),   "m_event.cli.user.groupname");
  checkIsNotEmptyString(m_event.file().lpath(),             "m_event.file.lpath");
  checkIsNotEmptyString(m_event.wf().instance().url(),      "m_event.wf.instance.url");
  checkIsNotEmptyString(m_event.transport().report_url(),   "m_event.transport.report_url");

  // Unpack message
  const auto storageClassItor = m_event.file().xattr().find("sys.archive.storage_class");
  if(m_event.file().xattr().end() == storageClassItor) {
    throw exception::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.storage_class");
  }

  // For testing: this storage class will always fail
  if(storageClassItor->second == "fail_on_closew_test") {
     throw exception::UserError("File is in fail_on_closew_test storage class, which always fails.");
  }

  auto storageClass = m_catalogue.StorageClass()->getStorageClass(storageClassItor->second);

  // Disallow archival of files above the specified limit
  if(storageClass.vo.maxFileSize && m_event.file().size() > storageClass.vo.maxFileSize) {
     throw exception::UserError("Archive request rejected: file size (" + std::to_string(m_event.file().size()) +
                                " bytes) exceeds maximum allowed size (" + std::to_string(storageClass.vo.maxFileSize) + " bytes)");
  }

  common::dataStructures::ArchiveRequest request;
  checksum::ProtobufToChecksumBlob(m_event.file().csb(), request.checksumBlob);
  request.diskFileInfo.owner_uid = m_event.file().owner().uid();
  request.diskFileInfo.gid       = m_event.file().owner().gid();
  request.diskFileInfo.path      = m_event.file().lpath();
  request.diskFileID             = std::to_string(m_event.file().fid());
  request.fileSize               = m_event.file().size();
  request.requester.name         = m_event.cli().user().username();
  request.requester.group        = m_event.cli().user().groupname();
  request.srcURL                 = m_event.wf().instance().url();
  request.storageClass           = storageClassItor->second;
  request.archiveReportURL       = m_event.transport().report_url();
  request.archiveErrorReportURL  = m_event.transport().error_report_url();
  request.creationLog.host       = m_cliIdentity.host;
  request.creationLog.username   = m_cliIdentity.username;
  request.creationLog.time       = time(nullptr);

  log::ScopedParamContainer params(m_lc);
  params.add("requesterInstance", m_event.wf().requester_instance());
  std::string logMessage = "In WorkflowEvent::processCLOSEW(): ";

  // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
  // must be converted to a valid uint64_t
  const auto archiveFileIdItor = m_event.file().xattr().find("sys.archive.file_id");
  if(m_event.file().xattr().end() == archiveFileIdItor) {
    logMessage += "sys.archive.file_id is not present in extended attributes";
    m_lc.log(log::INFO, logMessage);
    throw exception::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
  }
  const std::string archiveFileIdStr = archiveFileIdItor->second;
  uint64_t archiveFileId = 0;
  if((archiveFileId = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
  {
     params.add("sys.archive.file_id", archiveFileIdStr);
     logMessage += "sys.archive.file_id is not a positive integer";
     m_lc.log(log::INFO, logMessage);
     throw exception::PbException("Invalid archiveFileID " + archiveFileIdStr);
  }
  params.add("fileId", archiveFileId);

  utils::Timer t;

  if(request.fileSize > 0) {
    // Queue the request
    std::string archiveRequestAddr = m_scheduler.queueArchiveWithGivenId(archiveFileId, m_cliIdentity.username, request, m_lc);
    logMessage += "queued file for archive.";
    params.add("schedulerTime", t.secs());
    params.add("archiveRequestId", archiveRequestAddr);

    // Add archive request reference to response as an extended attribute
    response.mutable_xattr()->insert(google::protobuf::MapPair<std::string,std::string>("sys.cta.objectstore.id", archiveRequestAddr));
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
  checkIsNotEmptyString(m_event.cli().user().username(),    "m_event.cli.user.username");
  checkIsNotEmptyString(m_event.cli().user().groupname(),   "m_event.cli.user.groupname");
  checkIsNotEmptyString(m_event.file().lpath(),             "m_event.file.lpath");
  checkIsNotEmptyString(m_event.transport().dst_url(),      "m_event.transport.dst_url");

  // Unpack message
  common::dataStructures::RetrieveRequest request;
  request.requester.name         = m_event.cli().user().username();
  request.requester.group        = m_event.cli().user().groupname();
  request.dstURL                 = m_event.transport().dst_url();
  request.errorReportURL         = m_event.transport().error_report_url();
  request.diskFileInfo.owner_uid = m_event.file().owner().uid();
  request.diskFileInfo.gid       = m_event.file().owner().gid();
  request.diskFileInfo.path      = m_event.file().lpath();
  request.creationLog.host       = m_cliIdentity.host;
  request.creationLog.username   = m_cliIdentity.username;
  request.creationLog.time       = time(nullptr);
  request.isVerifyOnly           = m_event.wf().verify_only();
  if (request.isVerifyOnly) {
     request.mountPolicy = m_verificationMountPolicy;
  }

  // Vid is for tape verification use case (for dual-copy files) so normally is not specified
  if(!m_event.wf().vid().empty()) {
    request.vid = m_event.wf().vid();
  }

  // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which must be
  // converted to a valid uint64_t
  auto archiveFileIdItor = m_event.file().xattr().find("sys.archive.file_id");
  if(m_event.file().xattr().end() == archiveFileIdItor) {
    // Fall back to the old xattr format
    archiveFileIdItor = m_event.file().xattr().find("CTA_ArchiveFileId");
    if(m_event.file().xattr().end() == archiveFileIdItor) {
      throw exception::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
    }
  }
  const std::string archiveFileIdStr = archiveFileIdItor->second;
  if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
  {
     throw exception::PbException("Invalid archiveFileID " + archiveFileIdStr);
  }

  // Activity value is a string. The parameter might be present or not.
  if(m_event.file().xattr().find("activity") != m_event.file().xattr().end()) {
    request.activity = m_event.file().xattr().at("activity");
  }

  utils::Timer t;

  // Queue the request
  std::string retrieveReqId = m_scheduler.queueRetrieve(m_cliIdentity.username, request, m_lc);

  // Create a log entry
  log::ScopedParamContainer params(m_lc);
  params.add("fileId", request.archiveFileID)
        .add("schedulerTime", t.secs())
        .add("isVerifyOnly", request.isVerifyOnly)
        .add("retrieveReqId", retrieveReqId);
  if(static_cast<bool>(request.activity)) {
    params.add("activity", request.activity.value());
  }
  m_lc.log(log::INFO, "In WorkflowEvent::processPREPARE(): queued file for retrieve.");

  // Set response type and add retrieve request reference as an extended attribute.
  response.mutable_xattr()->insert(google::protobuf::MapPair<std::string,std::string>("sys.cta.objectstore.id", retrieveReqId));
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processABORT_PREPARE(xrd::Response& response) {
  // Validate received protobuf
  checkIsNotEmptyString(m_event.cli().user().username(),    "m_event.cli.user.username");
  checkIsNotEmptyString(m_event.cli().user().groupname(),   "m_event.cli.user.groupname");

  // Unpack message
  common::dataStructures::CancelRetrieveRequest request;
  request.requester.name   = m_event.cli().user().username();
  request.requester.group  = m_event.cli().user().groupname();

  // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which must be
  // converted to a valid uint64_t
  auto archiveFileIdItor = m_event.file().xattr().find("sys.archive.file_id");
  if(m_event.file().xattr().end() == archiveFileIdItor) {
    // Fall back to the old xattr format
    archiveFileIdItor = m_event.file().xattr().find("CTA_ArchiveFileId");
    if(m_event.file().xattr().end() == archiveFileIdItor) {
      throw exception::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
    }
  }
  const std::string archiveFileIdStr = archiveFileIdItor->second;
  if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
  {
     throw exception::PbException("Invalid archiveFileID " + archiveFileIdStr);
  }

  // The request Id should be stored as an extended attribute
  const auto retrieveRequestIdItor = m_event.file().xattr().find("sys.cta.objectstore.id");
  if(m_event.file().xattr().end() == retrieveRequestIdItor) {
    throw exception::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.cta.objectstore.id");
  }
  const std::string retrieveRequestId = retrieveRequestIdItor->second;
  request.retrieveRequestId = retrieveRequestId;

  // Queue the request
  m_scheduler.abortRetrieve(m_cliIdentity.username, request, m_lc);

  utils::Timer t;

  // Create a log entry
  log::ScopedParamContainer params(m_lc);
  params.add("fileId", request.archiveFileID)
        .add("schedulerTime", t.secs())
        .add("retrieveRequestId", request.retrieveRequestId)
        .add("diskFilePath", utils::midEllipsis(request.diskFileInfo.path, 100));
  m_lc.log(log::INFO, "In WorkflowEvent::processABORT_PREPARE(): canceled retrieve request.");

  // Set response type and remove reference to retrieve request in EOS extended attributes.
  response.mutable_xattr()->insert(google::protobuf::MapPair<std::string,std::string>("sys.cta.objectstore.id", ""));
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processDELETE(xrd::Response& response) {
  // Validate received protobuf
  checkIsNotEmptyString(m_event.cli().user().username(),    "m_event.cli.user.username");
  checkIsNotEmptyString(m_event.cli().user().groupname(),   "m_event.cli.user.groupname");
  checkIsNotEmptyString(m_event.file().lpath(),             "m_event.file.lpath");

  // Unpack message
  common::dataStructures::DeleteArchiveRequest request;
  request.requester.name    = m_event.cli().user().username();
  request.requester.group   = m_event.cli().user().groupname();

  std::string lpath         = m_event.file().lpath();
  request.diskFilePath          = lpath;
  request.diskFileId = std::to_string(m_event.file().fid());
  request.diskInstance = m_cliIdentity.username;
  // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
  // must be converted to a valid uint64_t
  auto archiveFileIdItor = m_event.file().xattr().find("sys.archive.file_id");
  if(m_event.file().xattr().end() == archiveFileIdItor) {
    // Fall back to the old xattr format
    archiveFileIdItor = m_event.file().xattr().find("CTA_ArchiveFileId");
    if(m_event.file().xattr().end() == archiveFileIdItor) {
      throw exception::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
    }
  }
  const std::string archiveFileIdStr = archiveFileIdItor->second;
  if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
  {
     throw exception::PbException("Invalid archiveFileID " + archiveFileIdStr);
  }

  auto archiveRequestAddrItor = m_event.file().xattr().find("sys.cta.archive.objectstore.id");
  if(archiveRequestAddrItor != m_event.file().xattr().end()){
    //We have the ArchiveRequest's objectstore address.
    std::string objectstoreAddress = archiveRequestAddrItor->second;
    if(!objectstoreAddress.empty()){
     request.address = archiveRequestAddrItor->second;
    }
  }

  // Delete the file from the catalogue or from the objectstore if archive request is created
  utils::Timer t;
  log::TimingList tl;
  try {
    request.archiveFile = m_catalogue.ArchiveFile()->getArchiveFileById(request.archiveFileID);
    tl.insertAndReset("catalogueGetArchiveFileByIdTime",t);
  } catch (exception::Exception &ex){
   log::ScopedParamContainer spc(m_lc);
   spc.add("fileId", request.archiveFileID);
   m_lc.log(log::DEBUG, "Ignoring request to delete archive file from the catalogue, because it does not exist");
  }
  m_scheduler.deleteArchive(m_cliIdentity.username, request, m_lc);
  tl.insertAndReset("schedulerTime",t);
  // Create a log entry
  log::ScopedParamContainer params(m_lc);
  params.add("fileId", request.archiveFileID)
        .add("address", (request.address ? request.address.value() : "null"))
        .add("filePath",request.diskFilePath);
  tl.addToLog(params);
  m_lc.log(log::INFO, "In WorkflowEvent::processDELETE(): archive file deleted.");

  // Set response type
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void WorkflowEvent::processUPDATE_FID(xrd::Response& response) {
  // Validate received protobuf
  checkIsNotEmptyString(m_event.file().lpath(),  "m_event.file.lpath");

  // Unpack message
  const std::string &diskInstance = m_cliIdentity.username;
  const std::string &diskFilePath = m_event.file().lpath();
  const std::string diskFileId = std::to_string(m_event.file().fid());

  // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which must be
  // converted to a valid uint64_t
  auto archiveFileIdItor = m_event.file().xattr().find("sys.archive.file_id");
  if(m_event.file().xattr().end() == archiveFileIdItor) {
    // Fall back to the old xattr format
    archiveFileIdItor = m_event.file().xattr().find("CTA_ArchiveFileId");
    if(m_event.file().xattr().end() == archiveFileIdItor) {
      throw exception::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
    }
  }
  const std::string archiveFileIdStr = archiveFileIdItor->second;
  const uint64_t archiveFileId = strtoul(archiveFileIdStr.c_str(), nullptr, 10);
  if(0 == archiveFileId) {
     throw exception::PbException("Invalid archiveFileID " + archiveFileIdStr);
  }

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

}} // namespace cta::frontend
