/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          XRootD EOS Notification handler
 * @copyright      Copyright 2019 CERN
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

#include <XrdSsiPbException.hpp>
using XrdSsiPb::PbException;

#include <cmdline/CtaAdminCmdParse.hpp>
#include "XrdSsiCtaRequestMessage.hpp"
#include "XrdCtaAdminLs.hpp"
#include "XrdCtaArchiveFileLs.hpp"
#include "XrdCtaArchiveRouteLs.hpp"
#include "XrdCtaDriveLs.hpp"
#include "XrdCtaFailedRequestLs.hpp"
#include "XrdCtaGroupMountRuleLs.hpp"
#include "XrdCtaListPendingQueue.hpp"
#include "XrdCtaLogicalLibraryLs.hpp"
#include "XrdCtaMountPolicyLs.hpp"
#include "XrdCtaRepackLs.hpp"
#include "XrdCtaRequesterMountRuleLs.hpp"
#include "XrdCtaShowQueues.hpp"
#include "XrdCtaTapeLs.hpp"
#include "XrdCtaTapeFileLs.hpp"
#include "XrdCtaStorageClassLs.hpp"
#include "XrdCtaTapePoolLs.hpp"
#include "XrdCtaDiskSystemLs.hpp"
#include "XrdCtaVirtualOrganizationLs.hpp"

namespace cta {
namespace xrd {

// Codes to change colours for console output (when sending a response to cta-admin)
const char* const TEXT_RED    = "\x1b[31;1m";
const char* const TEXT_NORMAL = "\x1b[0m\n";


/*
 * Convert AdminCmd <Cmd, SubCmd> pair to an integer so that it can be used in a switch statement
 */
constexpr unsigned int cmd_pair(cta::admin::AdminCmd::Cmd cmd, cta::admin::AdminCmd::SubCmd subcmd) {
   return (cmd << 16) + subcmd;
}


void RequestMessage::process(const cta::xrd::Request &request, cta::xrd::Response &response, XrdSsiStream* &stream)
{
   // Branch on the Request payload type

   switch(request.request_case())
   {
      using namespace cta::xrd;

      case Request::kAdmincmd: {
         // Validate that the Kerberos user is an authorized CTA Admin user
         if(m_protocol != Protocol::KRB5) {
            throw cta::exception::UserError("[ERROR] Admin commands must be authenticated using the Kerberos 5 protocol.");
         }
         m_scheduler.authorizeAdmin(m_cliIdentity, m_lc);

         cta::utils::Timer t;

         // Validate the Protocol Buffer and import options into maps
         importOptions(request.admincmd());

         // Map the <Cmd, SubCmd> to a method
         switch(cmd_pair(request.admincmd().cmd(), request.admincmd().subcmd())) {
            using namespace cta::admin;

            case cmd_pair(AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_ADD):
               processAdmin_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_CH):
               processAdmin_Ch(response);
               break;
            case cmd_pair(AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_RM):
               processAdmin_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_LS):
               processAdmin_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_ARCHIVEFILE, AdminCmd::SUBCMD_LS):
               processArchiveFile_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_ADD):
               processArchiveRoute_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_CH):
               processArchiveRoute_Ch(response);
               break;
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_RM):
               processArchiveRoute_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_LS):
               processArchiveRoute_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_UP):
               processDrive_Up(response);
               break;
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_DOWN):
               processDrive_Down(response);
               break;
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_LS):
               processDrive_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_RM):
               processDrive_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_FAILEDREQUEST, AdminCmd::SUBCMD_LS):
               processFailedRequest_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_ADD):
               processGroupMountRule_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_CH):
               processGroupMountRule_Ch(response);
               break;
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_RM):
               processGroupMountRule_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_LS):
               processGroupMountRule_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_LISTPENDINGARCHIVES, AdminCmd::SUBCMD_NONE):
               processListPendingArchives(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_LISTPENDINGRETRIEVES, AdminCmd::SUBCMD_NONE):
               processListPendingRetrieves(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_ADD):
               processLogicalLibrary_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_CH):
               processLogicalLibrary_Ch(response);
               break;
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_RM):
               processLogicalLibrary_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_LS):
               processLogicalLibrary_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_ADD):
               processMountPolicy_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_CH):
               processMountPolicy_Ch(response);
               break;
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_RM):
               processMountPolicy_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_LS):
               processMountPolicy_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ADD):
               processRepack_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_RM):
               processRepack_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_LS):
               processRepack_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ERR):
               processRepack_Err(response);
               break;
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_ADD):
               processRequesterMountRule_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_CH):
               processRequesterMountRule_Ch(response);
               break;
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_RM):
               processRequesterMountRule_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_LS):
               processRequesterMountRule_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_SHOWQUEUES, AdminCmd::SUBCMD_NONE):
               processShowQueues(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_ADD):
               processStorageClass_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_CH):
               processStorageClass_Ch(response);
               break;
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_RM):
               processStorageClass_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_LS):
               processStorageClass_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_ADD):
               processTape_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_CH):
               processTape_Ch(response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_RM):
               processTape_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_RECLAIM):
               processTape_Reclaim(response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LS):
               processTape_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LABEL):
               processTape_Label(response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPEFILE, AdminCmd::SUBCMD_LS):
               processTapeFile_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_ADD):
               processTapePool_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_CH):
               processTapePool_Ch(response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_RM):
               processTapePool_Rm(response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_LS):
               processTapePool_Ls(response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_LS):
               processDiskSystem_Ls(response, stream);
               break;   
            case cmd_pair(AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_ADD):
               processDiskSystem_Add(response);
               break;
            case cmd_pair(AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_RM):
               processDiskSystem_Rm(response);
               break;  
            case cmd_pair(AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_CH):
               processDiskSystem_Ch(response);
               break;
           case cmd_pair(AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_ADD):
               processVirtualOrganization_Add(response);
               break;
           case cmd_pair(AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_CH):
               processVirtualOrganization_Ch(response);
               break;
           case cmd_pair(AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_RM):
               processVirtualOrganization_Rm(response);
               break;
           case cmd_pair(AdminCmd::CMD_VIRTUALORGANIZATION,AdminCmd::SUBCMD_LS):
               processVirtualOrganization_Ls(response, stream);
               break;
            default:
               throw PbException("Admin command pair <" +
                     AdminCmd_Cmd_Name(request.admincmd().cmd()) + ", " +
                     AdminCmd_SubCmd_Name(request.admincmd().subcmd()) +
                     "> is not implemented.");
            } // end switch

            // Log the admin command
            logAdminCmd(__FUNCTION__, request.admincmd(), t);
         } // end case Request::kAdmincmd
         break;

      case Request::kNotification:
         // Validate that instance name in SSS key and instance name in Protocol buffer match
         if(m_cliIdentity.username != request.notification().wf().instance().name()) {
            throw PbException("Instance name \"" + request.notification().wf().instance().name() +
                              "\" does not match key identifier \"" + m_cliIdentity.username + "\"");
         }

         // Map the Workflow Event to a method
         switch(request.notification().wf().event()) {
            using namespace cta::eos;

            case Workflow::OPENW:
               processOPENW (request.notification(), response);
               break;
            case Workflow::CREATE:
               processCREATE (request.notification(), response);
               break;
            case Workflow::CLOSEW:
               processCLOSEW (request.notification(), response);
               break;
            case Workflow::PREPARE:
               processPREPARE(request.notification(), response);
               break;
            case Workflow::ABORT_PREPARE:
               processABORT_PREPARE(request.notification(), response);
               break;
            case Workflow::DELETE:
               processDELETE (request.notification(), response);
               break;

            default:
               throw PbException("Workflow event " +
                     Workflow_EventType_Name(request.notification().wf().event()) +
                     " is not implemented.");
         }
         break;

      case Request::REQUEST_NOT_SET:
         throw PbException("Request message has not been set.");

      default:
         throw PbException("Unrecognized Request message. "
                           "Possible Protocol Buffer version mismatch between client and server.");
   }
}



// EOS Workflow commands

void RequestMessage::processOPENW(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Create a log entry

   cta::log::ScopedParamContainer params(m_lc);
   m_lc.log(cta::log::INFO, "In RequestMessage::processOPENW(): ignoring OPENW event.");

   // Set response type

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processCREATE(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Validate received protobuf
   checkIsNotEmptyString(notification.cli().user().username(),  "notification.cli.user.username");
   checkIsNotEmptyString(notification.cli().user().groupname(), "notification.cli.user.groupname");

   // Unpack message
   cta::common::dataStructures::RequesterIdentity requester;
   requester.name  = notification.cli().user().username();
   requester.group = notification.cli().user().groupname();

   auto storageClassItor = notification.file().xattr().find("sys.archive.storage_class");
   if(notification.file().xattr().end() == storageClassItor) {
     // Fall back to old xattr format
     storageClassItor = notification.file().xattr().find("CTA_StorageClass");
     if(notification.file().xattr().end() == storageClassItor) {
       throw PbException(std::string(__FUNCTION__) + ": sys.archive.storage_class extended attribute is not set");
     }
   }
   const std::string storageClass = storageClassItor->second;
   if(storageClass.empty()) {
     throw PbException(std::string(__FUNCTION__) + ": sys.archive.storage_class extended attribute is set to an empty string");
   }

   cta::utils::Timer t;
   uint64_t archiveFileId;

   // For testing, this storage class will always fail on CLOSEW. Allow it to pass CREATE and don't allocate an archive Id from the pool.
   if(storageClassItor->second == "fail_on_closew_test") {
     archiveFileId = std::numeric_limits<uint64_t>::max();
   } else {
     archiveFileId = m_scheduler.checkAndGetNextArchiveFileId(m_cliIdentity.username, storageClass, requester, m_lc);
   }

   // Create a log entry
   cta::log::ScopedParamContainer params(m_lc);
   params.add("diskFileId", std::to_string(notification.file().fid()))
         .add("diskFilePath", notification.file().lpath())
         .add("fileId", archiveFileId)
         .add("schedulerTime", t.secs());
   m_lc.log(cta::log::INFO, "In RequestMessage::processCREATE(): assigning new archive file ID.");

   // Set ArchiveFileId in xattrs
   response.mutable_xattr()->insert(google::protobuf::MapPair<std::string,std::string>("sys.archive.file_id", std::to_string(archiveFileId)));
   
   // Set the storage class in xattrs
   response.mutable_xattr()->insert(google::protobuf::MapPair<std::string,std::string>("sys.archive.storage_class", storageClass));

   // Set response type
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processCLOSEW(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Validate received protobuf
   checkIsNotEmptyString(notification.cli().user().username(),    "notification.cli.user.username");
   checkIsNotEmptyString(notification.cli().user().groupname(),   "notification.cli.user.groupname");
   checkIsNotEmptyString(notification.file().lpath(),             "notification.file.lpath");
   checkIsNotEmptyString(notification.wf().instance().url(),      "notification.wf.instance.url");
   checkIsNotEmptyString(notification.transport().report_url(),   "notification.transport.report_url");

   // Unpack message
   const auto storageClassItor = notification.file().xattr().find("sys.archive.storage_class");
   if(notification.file().xattr().end() == storageClassItor) {
     throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.storage_class");
   }

   // For testing: this storage class will always fail
   if(storageClassItor->second == "fail_on_closew_test") {
      throw PbException("File is in fail_on_closew_test storage class, which always fails.");
   }

   // Disallow archival of files above the specified limit
   if(notification.file().size() > m_archiveFileMaxSize) {
      throw exception::UserError("Archive request rejected: file size (" + std::to_string(notification.file().size()) +
                                 " bytes) exceeds maximum allowed size (" + std::to_string(m_archiveFileMaxSize) + " bytes)");
   }

   cta::common::dataStructures::ArchiveRequest request;
   checksum::ProtobufToChecksumBlob(notification.file().csb(), request.checksumBlob);
   request.diskFileInfo.owner_uid = notification.file().owner().uid();
   request.diskFileInfo.gid       = notification.file().owner().gid();
   request.diskFileInfo.path      = notification.file().lpath();
   request.diskFileID             = std::to_string(notification.file().fid());
   request.fileSize               = notification.file().size();
   request.requester.name         = notification.cli().user().username();
   request.requester.group        = notification.cli().user().groupname();
   request.srcURL                 = notification.wf().instance().url();
   request.storageClass           = storageClassItor->second;
   request.archiveReportURL       = notification.transport().report_url();
   request.archiveErrorReportURL  = notification.transport().error_report_url();
   request.creationLog.host       = m_cliIdentity.host;
   request.creationLog.username   = m_cliIdentity.username;
   request.creationLog.time       = time(nullptr);

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
   // must be converted to a valid uint64_t
   const auto archiveFileIdItor = notification.file().xattr().find("sys.archive.file_id");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   uint64_t archiveFileId = 0;
   if((archiveFileId = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   cta::utils::Timer t;

   // Queue the request
   m_scheduler.queueArchiveWithGivenId(archiveFileId, m_cliIdentity.username, request, m_lc);

   // Create a log entry
   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", archiveFileId);
   params.add("schedulerTime", t.secs());
   params.add("requesterInstance", notification.wf().requester_instance());
   m_lc.log(cta::log::INFO, "In RequestMessage::processCLOSEW(): queued file for archive.");

   // Set response type
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processPREPARE(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Validate received protobuf
   checkIsNotEmptyString(notification.cli().user().username(),    "notification.cli.user.username");
   checkIsNotEmptyString(notification.cli().user().groupname(),   "notification.cli.user.groupname");
   checkIsNotEmptyString(notification.file().lpath(),             "notification.file.lpath");
   checkIsNotEmptyString(notification.transport().dst_url(),      "notification.transport.dst_url");

   // Unpack message
   cta::common::dataStructures::RetrieveRequest request;
   request.requester.name         = notification.cli().user().username();
   request.requester.group        = notification.cli().user().groupname();
   request.dstURL                 = notification.transport().dst_url();
   request.errorReportURL         = notification.transport().error_report_url();
   request.diskFileInfo.owner_uid = notification.file().owner().uid();
   request.diskFileInfo.gid       = notification.file().owner().gid();
   request.diskFileInfo.path      = notification.file().lpath();
   request.creationLog.host       = m_cliIdentity.host;
   request.creationLog.username   = m_cliIdentity.username;
   request.creationLog.time       = time(nullptr);

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which must be
   // converted to a valid uint64_t
   auto archiveFileIdItor = notification.file().xattr().find("sys.archive.file_id");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     // Fall back to the old xattr format
     archiveFileIdItor = notification.file().xattr().find("CTA_ArchiveFileId");
     if(notification.file().xattr().end() == archiveFileIdItor) {
       throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
     }
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw PbException("Invalid archiveFileID " + archiveFileIdStr);
   }
   
   // Activity value is a string. The parameter might be present or not.
   if(notification.file().xattr().find("activity") != notification.file().xattr().end()) {
     request.activity = notification.file().xattr().at("activity");
   }

   cta::utils::Timer t;

   // Queue the request
   std::string retrieveReqId = m_scheduler.queueRetrieve(m_cliIdentity.username, request, m_lc);

   // Create a log entry
   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", request.archiveFileID).add("schedulerTime", t.secs())
         .add("retrieveReqId", retrieveReqId);
   if(static_cast<bool>(request.activity)) {
     params.add("activity", request.activity.value());
   }
   m_lc.log(cta::log::INFO, "In RequestMessage::processPREPARE(): queued file for retrieve.");

   // Set response type and add retrieve request reference as an extended attribute.
   response.mutable_xattr()->insert(google::protobuf::MapPair<std::string,std::string>("sys.cta.objectstore.id", retrieveReqId));
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processABORT_PREPARE(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Validate received protobuf
   checkIsNotEmptyString(notification.cli().user().username(),    "notification.cli.user.username");
   checkIsNotEmptyString(notification.cli().user().groupname(),   "notification.cli.user.groupname");

   // Unpack message
   cta::common::dataStructures::CancelRetrieveRequest request;
   request.requester.name   = notification.cli().user().username();
   request.requester.group  = notification.cli().user().groupname();

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which must be
   // converted to a valid uint64_t
   auto archiveFileIdItor = notification.file().xattr().find("sys.archive.file_id");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     // Fall back to the old xattr format
     archiveFileIdItor = notification.file().xattr().find("CTA_ArchiveFileId");
     if(notification.file().xattr().end() == archiveFileIdItor) {
       throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
     }
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw PbException("Invalid archiveFileID " + archiveFileIdStr);
   }
   
   // The request Id should be stored as an extended attribute
   const auto retrieveRequestIdItor = notification.file().xattr().find("sys.cta.objectstore.id");
   if(notification.file().xattr().end() == retrieveRequestIdItor) {
     throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.cta.objectstore.id");
   }
   const std::string retrieveRequestId = retrieveRequestIdItor->second;
   request.retrieveRequestId = retrieveRequestId;

   // Queue the request
   m_scheduler.abortRetrieve(m_cliIdentity.username, request, m_lc);

   cta::utils::Timer t;

   // Create a log entry
   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", request.archiveFileID)
         .add("schedulerTime", t.secs())
         .add("retrieveRequestId", request.retrieveRequestId)
         .add("diskFilePath", cta::utils::midEllipsis(request.diskFileInfo.path, 100));
   m_lc.log(cta::log::INFO, "In RequestMessage::processABORT_PREPARE(): canceled retrieve request.");

   // Set response type and remove reference to retrieve request in EOS extended attributes.
   response.mutable_xattr()->insert(google::protobuf::MapPair<std::string,std::string>("sys.cta.objectstore.id", ""));
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDELETE(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Validate received protobuf
   checkIsNotEmptyString(notification.cli().user().username(),    "notification.cli.user.username");
   checkIsNotEmptyString(notification.cli().user().groupname(),   "notification.cli.user.groupname");

   // Unpack message
   cta::common::dataStructures::DeleteArchiveRequest request;
   request.requester.name    = notification.cli().user().username();
   request.requester.group   = notification.cli().user().groupname();

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
   // must be converted to a valid uint64_t

   auto archiveFileIdItor = notification.file().xattr().find("sys.archive.file_id");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     // Fall back to the old xattr format
     archiveFileIdItor = notification.file().xattr().find("CTA_ArchiveFileId");
     if(notification.file().xattr().end() == archiveFileIdItor) {
       throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
     }
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   // Delete the file from the catalogue
   cta::utils::Timer t;
   m_scheduler.deleteArchive(m_cliIdentity.username, request, m_lc);

   // Create a log entry
   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", request.archiveFileID).add("schedulerTime", t.secs());
   m_lc.log(cta::log::INFO, "In RequestMessage::processDELETE(): archive file deleted.");

   // Set response type
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



// Admin commands

void RequestMessage::logAdminCmd(const std::string &function, const cta::admin::AdminCmd &admincmd, cta::utils::Timer &t)
{
   using namespace cta::admin;

   std::string log_msg = "In RequestMessage::" + function + "(): Admin command succeeded: ";

   // Reverse lookup of strings corresponding to <command,subcommand> pair
   for(auto cmd_it = cmdLookup.begin(); cmd_it != cmdLookup.end(); ++cmd_it) {
      // Return the matching long string (length > 3)
      if(admincmd.cmd() == cmd_it->second && cmd_it->first.length() > 3) {
         log_msg += cmd_it->first + ' ';
         break;
      }
   }
   for(auto subcmd_it = subcmdLookup.begin(); subcmd_it != subcmdLookup.end(); ++subcmd_it) {
      if(admincmd.subcmd() == subcmd_it->second) {
         log_msg += subcmd_it->first;
         break;
      }
   }

   // Add the log message
   cta::log::ScopedParamContainer params(m_lc);
   params.add("adminTime", t.secs());
   m_lc.log(cta::log::INFO, log_msg);
}



void RequestMessage::processAdmin_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = getRequired(OptionString::USERNAME);
   auto &comment  = getRequired(OptionString::COMMENT);

   m_catalogue.createAdminUser(m_cliIdentity, username, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = getRequired(OptionString::USERNAME);
   auto &comment  = getRequired(OptionString::COMMENT);

   m_catalogue.modifyAdminUserComment(m_cliIdentity, username, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = getRequired(OptionString::USERNAME);

   m_catalogue.deleteAdminUser(username);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new AdminLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::ADMIN_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveFile_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new ArchiveFileLsStream(*this, m_catalogue, m_scheduler);

  // Set correct column headers
  response.set_show_header(has_flag(OptionBoolean::SUMMARY) ? HeaderType::ARCHIVEFILE_LS_SUMMARY
                                                            : HeaderType::ARCHIVEFILE_LS);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &scn      = getRequired(OptionString::STORAGE_CLASS);
   auto &cn       = getRequired(OptionUInt64::COPY_NUMBER);
   auto &tapepool = getRequired(OptionString::TAPE_POOL);
   auto &comment  = getRequired(OptionString::COMMENT);

   m_catalogue.createArchiveRoute(m_cliIdentity, scn, cn, tapepool, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &scn      = getRequired(OptionString::STORAGE_CLASS);
   auto &cn       = getRequired(OptionUInt64::COPY_NUMBER);
   auto  tapepool = getOptional(OptionString::TAPE_POOL);
   auto  comment  = getOptional(OptionString::COMMENT);

   if(comment) {
      m_catalogue.modifyArchiveRouteComment(m_cliIdentity, scn, cn, comment.value());
   }
   if(tapepool) {
      m_catalogue.modifyArchiveRouteTapePoolName(m_cliIdentity, scn, cn, tapepool.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &scn = getRequired(OptionString::STORAGE_CLASS);
   auto &cn  = getRequired(OptionUInt64::COPY_NUMBER);

   m_catalogue.deleteArchiveRoute(scn, cn);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new ArchiveRouteLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::ARCHIVEROUTE_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Up(cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::string cmdlineOutput = setDriveState('^' + getRequired(OptionString::DRIVE) + '$', Up);

   response.set_message_txt(cmdlineOutput);
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Down(cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::string cmdlineOutput = setDriveState('^' + getRequired(OptionString::DRIVE) + '$', Down);

   response.set_message_txt(cmdlineOutput);
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new DriveLsStream(*this, m_catalogue, m_scheduler, m_cliIdentity, m_lc);

  response.set_show_header(HeaderType::DRIVE_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   auto regex = getRequired(OptionString::DRIVE);
   regex = '^' + regex + '$';
   cta::utils::Regex driveNameRegex(regex.c_str());
   auto driveStates = m_scheduler.getDriveStates(m_cliIdentity, m_lc);
   bool drivesFound = false;
   for(auto driveState: driveStates)
   {
      const auto regexResult = driveNameRegex.exec(driveState.driveName);
      if(!regexResult.empty())
      {
         if(driveState.driveStatus == cta::common::dataStructures::DriveStatus::Down     ||
            driveState.driveStatus == cta::common::dataStructures::DriveStatus::Shutdown ||
            driveState.driveStatus == cta::common::dataStructures::DriveStatus::Unknown  ||
            has_flag(OptionBoolean::FORCE))
         {
            m_scheduler.removeDrive(m_cliIdentity, driveState.driveName, m_lc);
            cmdlineOutput << "Drive " << driveState.driveName << " removed"
                          << (has_flag(OptionBoolean::FORCE) ? " (forced)." : ".") << std::endl;            
         } else {
            cmdlineOutput << "Drive " << driveState.driveName << " in state "
                          << cta::common::dataStructures::toString(driveState.driveStatus)
                          << " and force is not set (skipped)." << std::endl;
         }
         drivesFound = true;
      }
   }

   if(!drivesFound) {
      cmdlineOutput << "No drives match \"" << regex << "\". No drives were removed." << std::endl;
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processFailedRequest_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  stream = new FailedRequestLsStream(*this, m_catalogue, m_scheduler, m_scheddb, m_lc);

  // Display the correct column headers
  response.set_show_header(has_flag(OptionBoolean::SUMMARY) ? HeaderType::FAILEDREQUEST_LS_SUMMARY
                                                            : HeaderType::FAILEDREQUEST_LS);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &mountpolicy = getRequired(OptionString::MOUNT_POLICY);
   auto &in          = getRequired(OptionString::INSTANCE);
   auto &name        = getRequired(OptionString::USERNAME);
   auto &comment     = getRequired(OptionString::COMMENT);

   m_catalogue.createRequesterGroupMountRule(m_cliIdentity, mountpolicy, in, name, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in          = getRequired(OptionString::INSTANCE);
   auto &name        = getRequired(OptionString::USERNAME);
   auto  mountpolicy = getOptional(OptionString::MOUNT_POLICY);
   auto  comment     = getOptional(OptionString::COMMENT);

   if(comment) {
      m_catalogue.modifyRequesterGroupMountRuleComment(m_cliIdentity, in, name, comment.value());
   }
   if(mountpolicy) {
      m_catalogue.modifyRequesterGroupMountRulePolicy(m_cliIdentity, in, name, mountpolicy.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in   = getRequired(OptionString::INSTANCE);
   auto &name = getRequired(OptionString::USERNAME);

   m_catalogue.deleteRequesterGroupMountRule(in, name);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  stream = new GroupMountRuleLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::GROUPMOUNTRULE_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processListPendingArchives(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new ListPendingQueueStream<OStoreDB::ArchiveQueueItor_t>(*this, m_catalogue, m_scheduler, m_scheddb);

  // Display the correct column headers
  response.set_show_header(has_flag(OptionBoolean::EXTENDED) ? HeaderType::LISTPENDINGARCHIVES
                                                             : HeaderType::LISTPENDINGARCHIVES_SUMMARY);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processListPendingRetrieves(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new ListPendingQueueStream<OStoreDB::RetrieveQueueItor_t>(*this, m_catalogue, m_scheduler, m_scheddb);

  // Display correct column headers
  response.set_show_header(has_flag(OptionBoolean::EXTENDED) ? HeaderType::LISTPENDINGRETRIEVES
                                                             : HeaderType::LISTPENDINGRETRIEVES_SUMMARY);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name      = getRequired(OptionString::LOGICAL_LIBRARY);
   auto isDisabled = getOptional(OptionBoolean::DISABLED);
   auto &comment   = getRequired(OptionString::COMMENT);

   m_catalogue.createLogicalLibrary(m_cliIdentity, name, isDisabled ? isDisabled.value() : false, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name     = getRequired(OptionString::LOGICAL_LIBRARY);
   auto  disabled = getOptional(OptionBoolean::DISABLED);
   auto  comment  = getOptional(OptionString::COMMENT);

   if(disabled) {
      m_catalogue.setLogicalLibraryDisabled(m_cliIdentity, name, disabled.value());
   }
   if(comment) {
      m_catalogue.modifyLogicalLibraryComment(m_cliIdentity, name, comment.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name = getRequired(OptionString::LOGICAL_LIBRARY);

   m_catalogue.deleteLogicalLibrary(name);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new LogicalLibraryLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::LOGICALLIBRARY_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &group                 = getRequired(OptionString::MOUNT_POLICY);
   auto &archivepriority       = getRequired(OptionUInt64::ARCHIVE_PRIORITY);
   auto &minarchiverequestage  = getRequired(OptionUInt64::MIN_ARCHIVE_REQUEST_AGE);
   auto &retrievepriority      = getRequired(OptionUInt64::RETRIEVE_PRIORITY);
   auto &minretrieverequestage = getRequired(OptionUInt64::MIN_RETRIEVE_REQUEST_AGE);
   auto &maxdrivesallowed      = getRequired(OptionUInt64::MAX_DRIVES_ALLOWED);
   auto &comment               = getRequired(OptionString::COMMENT);

   m_catalogue.createMountPolicy(m_cliIdentity, group, archivepriority, minarchiverequestage, retrievepriority,
                                 minretrieverequestage, maxdrivesallowed, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &group                 = getRequired(OptionString::MOUNT_POLICY);
   auto  archivepriority       = getOptional(OptionUInt64::ARCHIVE_PRIORITY);
   auto  minarchiverequestage  = getOptional(OptionUInt64::MIN_ARCHIVE_REQUEST_AGE);
   auto  retrievepriority      = getOptional(OptionUInt64::RETRIEVE_PRIORITY);
   auto  minretrieverequestage = getOptional(OptionUInt64::MIN_RETRIEVE_REQUEST_AGE);
   auto  maxdrivesallowed      = getOptional(OptionUInt64::MAX_DRIVES_ALLOWED);
   auto  comment               = getOptional(OptionString::COMMENT);

   if(archivepriority) {
      m_catalogue.modifyMountPolicyArchivePriority(m_cliIdentity, group, archivepriority.value());
   }
   if(minarchiverequestage) {
      m_catalogue.modifyMountPolicyArchiveMinRequestAge(m_cliIdentity, group, minarchiverequestage.value());
   }
   if(retrievepriority) {
      m_catalogue.modifyMountPolicyRetrievePriority(m_cliIdentity, group, retrievepriority.value());
   }
   if(minretrieverequestage) {
      m_catalogue.modifyMountPolicyRetrieveMinRequestAge(m_cliIdentity, group, minretrieverequestage.value());
   }
   if(maxdrivesallowed) {
      m_catalogue.modifyMountPolicyMaxDrivesAllowed(m_cliIdentity, group, maxdrivesallowed.value());
   }
   if(comment) {
      m_catalogue.modifyMountPolicyComment(m_cliIdentity, group, comment.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &group = getRequired(OptionString::MOUNT_POLICY);

   m_catalogue.deleteMountPolicy(group);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new MountPolicyLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::MOUNTPOLICY_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   // VIDs can be provided as a single option or as a list
   std::vector<std::string> vid_list;
   std::string bufferURL;

   auto vidl = getOptional(OptionStrList::VID);
   if(vidl) vid_list = vidl.value();
   auto vid = getOptional(OptionString::VID);
   if(vid) vid_list.push_back(vid.value());

   if(vid_list.empty()) {
      throw cta::exception::UserError("Must specify at least one vid, using --vid or --vidfile options");
   }
   
   auto buff = getOptional(OptionString::BUFFERURL);
   if (buff){
     //The buffer is provided by the user
     bufferURL = buff.value();
   }
   else {
     //Buffer is not provided by the user, try to get the one from the configuration file
     if(m_repackBufferURL){
       bufferURL = m_repackBufferURL.value();
     } else {
       //Buffer is neither provided by the user, neither provided by the frontend configuration file, exception
       throw cta::exception::UserError("Must specify the buffer URL using --bufferurl option or using the frontend configuration file.");
     }
   }
   
   typedef common::dataStructures::MountPolicy MountPolicy;
   MountPolicy mountPolicy = MountPolicy::s_defaultMountPolicyForRepack;
   
   auto mountPolicyProvidedByUserOpt = getOptional(OptionString::MOUNT_POLICY);
   if(mountPolicyProvidedByUserOpt){
     //The user specified a mount policy name for this repack request
     std::string mountPolicyProvidedByUser = mountPolicyProvidedByUserOpt.value();
     //Get the mountpolicy from the catalogue
     typedef std::list<common::dataStructures::MountPolicy> MountPolicyList;
     MountPolicyList mountPolicies = m_catalogue.getMountPolicies();
     MountPolicyList::const_iterator repackMountPolicyItor = std::find_if(mountPolicies.begin(),mountPolicies.end(),[mountPolicyProvidedByUser](const common::dataStructures::MountPolicy & mp){
       return mp.name == mountPolicyProvidedByUser;
     });
     if(repackMountPolicyItor != mountPolicies.end()){
       //The mount policy exists
       mountPolicy = *repackMountPolicyItor;
     } else {
       //The mount policy does not exist, throw a user error
       throw cta::exception::UserError("The mount policy name provided does not match any existing mount policy.");
     }
   }

   // Expand, repack, or both ?
   cta::common::dataStructures::RepackInfo::Type type;

   if(has_flag(OptionBoolean::JUSTADDCOPIES) && has_flag(OptionBoolean::JUSTMOVE)) {
      throw cta::exception::UserError("--justaddcopies and --justmove are mutually exclusive");
   } else if(has_flag(OptionBoolean::JUSTADDCOPIES)) {
      type = cta::common::dataStructures::RepackInfo::Type::AddCopiesOnly;
   } else if(has_flag(OptionBoolean::JUSTMOVE)) {
      type = cta::common::dataStructures::RepackInfo::Type::MoveOnly;
   } else {
      type = cta::common::dataStructures::RepackInfo::Type::MoveAndAddCopies;
   }
   
   bool forceDisabledTape = has_flag(OptionBoolean::DISABLED);

   // Process each item in the list
   for(auto it = vid_list.begin(); it != vid_list.end(); ++it) {
      m_scheduler.queueRepack(m_cliIdentity, *it, bufferURL, type, mountPolicy, forceDisabledTape, m_lc);
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = getRequired(OptionString::VID);

   m_scheduler.cancelRepack(m_cliIdentity, vid, m_lc);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;
  
  auto vid = getOptional(OptionString::VID);

  // Create a XrdSsi stream object to return the results
  stream = new RepackLsStream(m_scheduler, vid);

  response.set_show_header(HeaderType::REPACK_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Err(cta::xrd::Response &response)
{
  using namespace cta::admin;

  response.set_message_txt("Not implemented");
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &mountpolicy = getRequired(OptionString::MOUNT_POLICY);
   auto &in          = getRequired(OptionString::INSTANCE);
   auto &name        = getRequired(OptionString::USERNAME);
   auto &comment     = getRequired(OptionString::COMMENT);

   m_catalogue.createRequesterMountRule(m_cliIdentity, mountpolicy, in, name, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in          = getRequired(OptionString::INSTANCE);
   auto &name        = getRequired(OptionString::USERNAME);
   auto  comment     = getOptional(OptionString::COMMENT);
   auto  mountpolicy = getOptional(OptionString::MOUNT_POLICY);

   if(comment) {
      m_catalogue.modifyRequesteMountRuleComment(m_cliIdentity, in, name, comment.value());
   }
   if(mountpolicy) {
      m_catalogue.modifyRequesterMountRulePolicy(m_cliIdentity, in, name, mountpolicy.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in   = getRequired(OptionString::INSTANCE);
   auto &name = getRequired(OptionString::USERNAME);

   m_catalogue.deleteRequesterMountRule(in, name);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  stream = new RequesterMountRuleLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::REQUESTERMOUNTRULE_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processShowQueues(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new ShowQueuesStream(*this, m_catalogue, m_scheduler, m_lc);

  response.set_show_header(HeaderType::SHOWQUEUES);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   common::dataStructures::StorageClass storageClass;

   storageClass.diskInstance = getRequired(OptionString::INSTANCE);
   storageClass.name         = getRequired(OptionString::STORAGE_CLASS);
   storageClass.nbCopies     = getRequired(OptionUInt64::COPY_NUMBER);
   storageClass.comment      = getRequired(OptionString::COMMENT);
   storageClass.vo.name      = getRequired(OptionString::VO);

   m_catalogue.createStorageClass(m_cliIdentity, storageClass);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &scn     = getRequired(OptionString::STORAGE_CLASS);
   auto  comment = getOptional(OptionString::COMMENT);
   auto  cn      = getOptional(OptionUInt64::COPY_NUMBER);
   auto vo       = getOptional(OptionString::VO);

   if(comment) {
      m_catalogue.modifyStorageClassComment(m_cliIdentity, scn, comment.value());
   }
   if(cn) {
      m_catalogue.modifyStorageClassNbCopies(m_cliIdentity, scn, cn.value());
   }
   if(vo){
     m_catalogue.modifyStorageClassVo(m_cliIdentity,scn,vo.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &scn = getRequired(OptionString::STORAGE_CLASS);

   m_catalogue.deleteStorageClass(scn);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new StorageClassLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::STORAGECLASS_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid            = getRequired(OptionString::VID);
   auto &mediaType      = getRequired(OptionString::MEDIA_TYPE);
   auto &vendor         = getRequired(OptionString::VENDOR);
   auto &logicallibrary = getRequired(OptionString::LOGICAL_LIBRARY);
   auto &tapepool       = getRequired(OptionString::TAPE_POOL);
   auto &capacity       = getRequired(OptionUInt64::CAPACITY);
   auto &disabled       = getRequired(OptionBoolean::DISABLED);
   auto &full           = getRequired(OptionBoolean::FULL);
   auto &readOnly       = getRequired(OptionBoolean::READ_ONLY);
   auto  comment        = getOptional(OptionString::COMMENT);

   m_catalogue.createTape(m_cliIdentity, vid, mediaType, vendor, logicallibrary, tapepool, capacity, disabled, full, readOnly, comment ? comment.value() : "-");

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid               = getRequired(OptionString::VID);
   auto  mediaType         = getOptional(OptionString::MEDIA_TYPE);
   auto  vendor            = getOptional(OptionString::VENDOR);
   auto  logicallibrary    = getOptional(OptionString::LOGICAL_LIBRARY);
   auto  tapepool          = getOptional(OptionString::TAPE_POOL);
   auto  capacity          = getOptional(OptionUInt64::CAPACITY);
   auto  comment           = getOptional(OptionString::COMMENT);
   auto  encryptionkeyName = getOptional(OptionString::ENCRYPTION_KEY_NAME);
   auto  disabled          = getOptional(OptionBoolean::DISABLED);
   auto  full              = getOptional(OptionBoolean::FULL);
   auto  readOnly          = getOptional(OptionBoolean::READ_ONLY);

   if(mediaType) {
      m_catalogue.modifyTapeMediaType(m_cliIdentity, vid, mediaType.value());
   }
   if(vendor) {
      m_catalogue.modifyTapeVendor(m_cliIdentity, vid, vendor.value());
   }
   if(logicallibrary) {
      m_catalogue.modifyTapeLogicalLibraryName(m_cliIdentity, vid, logicallibrary.value());
   }
   if(tapepool) {
      m_catalogue.modifyTapeTapePoolName(m_cliIdentity, vid, tapepool.value());
   }
   if(capacity) {
      m_catalogue.modifyTapeCapacityInBytes(m_cliIdentity, vid, capacity.value());
   }
   if(comment) {
      m_catalogue.modifyTapeComment(m_cliIdentity, vid, comment.value());
   }
   if(encryptionkeyName) {
      m_catalogue.modifyTapeEncryptionKeyName(m_cliIdentity, vid, encryptionkeyName.value());
   }
   if(disabled) {
      m_catalogue.setTapeDisabled(m_cliIdentity, vid, disabled.value());
   }
   if(full) {
      m_catalogue.setTapeFull(m_cliIdentity, vid, full.value());
   }
   if(readOnly) {
      m_catalogue.setTapeReadOnly(m_cliIdentity, vid, readOnly.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = getRequired(OptionString::VID);

   m_catalogue.deleteTape(vid);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Reclaim(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = getRequired(OptionString::VID);

   m_catalogue.reclaimTape(m_cliIdentity, vid);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}


void RequestMessage::processTape_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new TapeLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::TAPE_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}


void RequestMessage::processTape_Label(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid   = getRequired(OptionString::VID);
   auto  force = getOptional(OptionBoolean::FORCE);

   m_scheduler.queueLabel(m_cliIdentity, vid, force ? force.value() : false);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}


void RequestMessage::processTapeFile_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  auto isLookupNamespace = getOptional(OptionBoolean::LOOKUP_NAMESPACE);

  if(isLookupNamespace && isLookupNamespace.value()) {
    // Get a stream including filename lookup in the namespace
    stream = new TapeFileLsStream(*this, m_catalogue, m_scheduler, m_namespaceMap);
  } else {
    // Get a stream without namespace lookup
    stream = new TapeFileLsStream(*this, m_catalogue, m_scheduler);
  }

  response.set_show_header(HeaderType::TAPEFILE_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}


void RequestMessage::processTapePool_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name      = getRequired(OptionString::TAPE_POOL);
   auto &vo        = getRequired(OptionString::VO);
   auto &ptn       = getRequired(OptionUInt64::PARTIAL_TAPES_NUMBER);
   auto &comment   = getRequired(OptionString::COMMENT);
   auto &encrypted = getRequired(OptionBoolean::ENCRYPTED);
   auto  supply    = getOptional(OptionString::SUPPLY);

   m_catalogue.createTapePool(m_cliIdentity, name, vo, ptn, encrypted, supply, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name      = getRequired(OptionString::TAPE_POOL);
   auto  vo        = getOptional(OptionString::VO);
   auto  ptn       = getOptional(OptionUInt64::PARTIAL_TAPES_NUMBER);
   auto  comment   = getOptional(OptionString::COMMENT);
   auto  encrypted = getOptional(OptionBoolean::ENCRYPTED);
   auto  supply    = getOptional(OptionString::SUPPLY);

   if(comment) {
      m_catalogue.modifyTapePoolComment(m_cliIdentity, name, comment.value());
   }
   if(vo) {
      m_catalogue.modifyTapePoolVo(m_cliIdentity, name, vo.value());
   }
   if(ptn) {
      m_catalogue.modifyTapePoolNbPartialTapes(m_cliIdentity, name, ptn.value());
   }
   if(encrypted) {
      m_catalogue.setTapePoolEncryption(m_cliIdentity, name, encrypted.value());
   }
   if(supply) {
      m_catalogue.modifyTapePoolSupply(m_cliIdentity, name, supply.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name = getRequired(OptionString::TAPE_POOL);

   m_catalogue.deleteTapePool(name);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new TapePoolLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::TAPEPOOL_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDiskSystem_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new DiskSystemLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::DISKSYSTEM_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskSystem_Add(cta::xrd::Response &response)
{
  using namespace cta::admin;

  const auto &name              = getRequired(OptionString::DISK_SYSTEM);
  const auto &fileRegexp        = getRequired(OptionString::FILE_REGEXP);
  const auto &freeSpaceQueryURL = getRequired(OptionString::FREE_SPACE_QUERY_URL);
  const auto &refreshInterval   = getRequired(OptionUInt64::REFRESH_INTERVAL);
  const auto &targetedFreeSpace = getRequired(OptionUInt64::TARGETED_FREE_SPACE);
  const auto &sleepTime         = getRequired(OptionUInt64::SLEEP_TIME);
  const auto &comment           = getRequired(OptionString::COMMENT);
   
  m_catalogue.createDiskSystem(m_cliIdentity, name, fileRegexp, freeSpaceQueryURL,
    refreshInterval, targetedFreeSpace, sleepTime, comment);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskSystem_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   const auto &name              = getRequired(OptionString::DISK_SYSTEM);
   const auto &fileRegexp        = getOptional(OptionString::FILE_REGEXP);
   const auto &freeSpaceQueryURL = getOptional(OptionString::FREE_SPACE_QUERY_URL);
   const auto  refreshInterval   = getOptional(OptionUInt64::REFRESH_INTERVAL);
   const auto  targetedFreeSpace = getOptional(OptionUInt64::TARGETED_FREE_SPACE);
   const auto  sleepTime         = getOptional(OptionUInt64::SLEEP_TIME);
   const auto  comment           = getOptional(OptionString::COMMENT);
   
   if(comment) {
      m_catalogue.modifyDiskSystemComment(m_cliIdentity, name, comment.value());
   }
   if(fileRegexp) {
      m_catalogue.modifyDiskSystemFileRegexp(m_cliIdentity, name, fileRegexp.value());
   }
   if(freeSpaceQueryURL) {
      m_catalogue.modifyDiskSystemFreeSpaceQueryURL(m_cliIdentity, name, freeSpaceQueryURL.value());
   }
   if (sleepTime) {
     m_catalogue.modifyDiskSystemSleepTime(m_cliIdentity, name, sleepTime.value());
   }
   if(refreshInterval) {
      m_catalogue.modifyDiskSystemRefreshInterval(m_cliIdentity, name, refreshInterval.value());
   }
   if(targetedFreeSpace) {
      m_catalogue.modifyDiskSystemTargetedFreeSpace(m_cliIdentity, name, targetedFreeSpace.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskSystem_Rm(cta::xrd::Response &response)
{
  using namespace cta::admin;

  const auto &name = getRequired(OptionString::DISK_SYSTEM);

  m_catalogue.deleteDiskSystem(name);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processVirtualOrganization_Add(cta::xrd::Response &response){
  using namespace cta::admin;

  const auto &name = getRequired(OptionString::VO);
  const auto &comment = getRequired(OptionString::COMMENT);
  
  cta::common::dataStructures::VirtualOrganization vo;
  vo.name = name;
  vo.comment = comment;
  
  m_catalogue.createVirtualOrganization(m_cliIdentity,vo);
  
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processVirtualOrganization_Ch(cta::xrd::Response &response){
  using namespace cta::admin;

  const auto &name = getRequired(OptionString::VO);
  const auto &comment = getRequired(OptionString::COMMENT);
  
  m_catalogue.modifyVirtualOrganizationComment(m_cliIdentity,name,comment);
  
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processVirtualOrganization_Rm(cta::xrd::Response& response) {
  using namespace cta::admin;
  
  const auto &name = getRequired(OptionString::VO);
  
  m_catalogue.deleteVirtualOrganization(name);
  
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processVirtualOrganization_Ls(cta::xrd::Response &response, XrdSsiStream * & stream){
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new VirtualOrganizationLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::VIRTUALORGANIZATION_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}


std::string RequestMessage::setDriveState(const std::string &regex, DriveState drive_state)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   cta::utils::Regex driveNameRegex(regex.c_str());

   auto driveStates = m_scheduler.getDriveStates(m_cliIdentity, m_lc);
   bool is_found = false;

   for(auto driveState: driveStates)
   {
      const auto regexResult = driveNameRegex.exec(driveState.driveName);
      if(!regexResult.empty())
      {
         is_found = true;

         m_scheduler.setDesiredDriveState(m_cliIdentity, driveState.driveName, drive_state == Up,
                                          has_flag(OptionBoolean::FORCE), m_lc);

         cmdlineOutput << "Drive " << driveState.driveName << " set "
                       << (drive_state == Up ? "Up" : "Down")
                       << (has_flag(OptionBoolean::FORCE) ? " (forced)" : "")
                       << "." << std::endl;
      }
   }
   if (!is_found) {
      cmdlineOutput << "No drives match \"" << regex << "\". No action was taken." << std::endl;
   }

   return cmdlineOutput.str();
}


void RequestMessage::importOptions(const cta::admin::AdminCmd &admincmd)
{
   // Validate the Protocol Buffer
   validateCmd(admincmd);

   // Import Boolean options
   for(auto opt_it = admincmd.option_bool().begin(); opt_it != admincmd.option_bool().end(); ++opt_it) {
      m_option_bool.insert(std::make_pair(opt_it->key(), opt_it->value()));
   }

   // Import UInt64 options
   for(auto opt_it = admincmd.option_uint64().begin(); opt_it != admincmd.option_uint64().end(); ++opt_it) {
      m_option_uint64.insert(std::make_pair(opt_it->key(), opt_it->value()));
   }

   // Import String options
   for(auto opt_it = admincmd.option_str().begin(); opt_it != admincmd.option_str().end(); ++opt_it) {
      m_option_str.insert(std::make_pair(opt_it->key(), opt_it->value()));
   }

   // Import String List options
   for(auto opt_it = admincmd.option_str_list().begin(); opt_it != admincmd.option_str_list().end(); ++opt_it) {
      std::vector<std::string> items;
      for(auto item_it = opt_it->item().begin(); item_it != opt_it->item().end(); ++item_it) {
         items.push_back(*item_it);
      }
      m_option_str_list.insert(std::make_pair(opt_it->key(), items));
   }
}

}} // namespace cta::xrd
