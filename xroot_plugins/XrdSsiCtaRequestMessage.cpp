/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include <limits>
#include <sstream>
#include <string>

#include <XrdSsiPbException.hpp>

#include "catalogue/CreateMountPolicyAttributes.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "cmdline/CtaAdminCmdParse.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/utils/Regex.hpp"

#include "xroot_plugins/XrdCtaActivityMountRuleLs.hpp"
#include "xroot_plugins/XrdCtaAdminLs.hpp"
#include "xroot_plugins/XrdCtaArchiveRouteLs.hpp"
#include "xroot_plugins/XrdCtaChangeStorageClass.hpp"
#include "xroot_plugins/XrdCtaDiskInstanceLs.hpp"
#include "xroot_plugins/XrdCtaDiskInstanceSpaceLs.hpp"
#include "xroot_plugins/XrdCtaDiskSystemLs.hpp"
#include "xroot_plugins/XrdCtaDriveLs.hpp"
#include "xroot_plugins/XrdCtaFailedRequestLs.hpp"
#include "xroot_plugins/XrdCtaGroupMountRuleLs.hpp"
#include "xroot_plugins/XrdCtaLogicalLibraryLs.hpp"
#include "xroot_plugins/XrdCtaMediaTypeLs.hpp"
#include "xroot_plugins/XrdCtaMountPolicyLs.hpp"
#include "xroot_plugins/XrdCtaRecycleTapeFileLs.hpp"
#include "xroot_plugins/XrdCtaRepackLs.hpp"
#include "xroot_plugins/XrdCtaRequesterMountRuleLs.hpp"
#include "xroot_plugins/XrdCtaShowQueues.hpp"
#include "xroot_plugins/XrdCtaStorageClassLs.hpp"
#include "xroot_plugins/XrdCtaTapeFileLs.hpp"
#include "xroot_plugins/XrdCtaTapeLs.hpp"
#include "xroot_plugins/XrdCtaTapePoolLs.hpp"
#include "xroot_plugins/XrdCtaVersion.hpp"
#include "xroot_plugins/XrdCtaVirtualOrganizationLs.hpp"
#include "xroot_plugins/XrdSsiCtaRequestMessage.hpp"

namespace cta {
namespace xrd {

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

         // Check that the user is authorized
         m_scheduler.authorizeAdmin(m_cliIdentity, m_lc);

         cta::utils::Timer t;

         // Validate the Protocol Buffer and import options into maps
         importOptions(request.admincmd());

         m_client_versions.ctaVersion = request.client_cta_version();
         m_client_versions.xrootdSsiProtoIntVersion = request.client_xrootd_ssi_protobuf_interface_version();

         try {
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
               case cmd_pair(AdminCmd::CMD_DRIVE,AdminCmd::SUBCMD_CH):
                  processDrive_Ch(response);
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
               case cmd_pair(AdminCmd::CMD_FAILEDREQUEST, AdminCmd::SUBCMD_RM):
                  processFailedRequest_Rm(response);
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
               case cmd_pair(AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_ADD):
                  processMediaType_Add(response);
                  break;
               case cmd_pair(AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_CH):
                  processMediaType_Ch(response);
                  break;
               case cmd_pair(AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_RM):
                  processMediaType_Rm(response);
                  break;
               case cmd_pair(AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_LS):
                  processMediaType_Ls(response, stream);
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
               case cmd_pair(AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_ADD):
                  processActivityMountRule_Add(response);
                  break;
               case cmd_pair(AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_CH):
                  processActivityMountRule_Ch(response);
                  break;
               case cmd_pair(AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_RM):
                  processActivityMountRule_Rm(response);
                  break;
               case cmd_pair(AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_LS):
                  processActivityMountRule_Ls(response, stream);
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
               case cmd_pair(AdminCmd::CMD_TAPEFILE, AdminCmd::SUBCMD_LS):
                  processTapeFile_Ls(response, stream);
                  break;
               case cmd_pair(AdminCmd::CMD_TAPEFILE, AdminCmd::SUBCMD_RM):
                  processTapeFile_Rm(response);
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
               case cmd_pair(AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_LS):
                  processDiskInstance_Ls(response, stream);
                  break;
               case cmd_pair(AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_ADD):
                  processDiskInstance_Add(response);
                  break;
               case cmd_pair(AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_RM):
                  processDiskInstance_Rm(response);
                  break;
               case cmd_pair(AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_CH):
                  processDiskInstance_Ch(response);
                  break;
                case cmd_pair(AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_LS):
                  processDiskInstanceSpace_Ls(response, stream);
                  break;
               case cmd_pair(AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_ADD):
                  processDiskInstanceSpace_Add(response);
                  break;
               case cmd_pair(AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_RM):
                  processDiskInstanceSpace_Rm(response);
                  break;
               case cmd_pair(AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_CH):
                  processDiskInstanceSpace_Ch(response);
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
               case cmd_pair(AdminCmd::CMD_VERSION, AdminCmd::SUBCMD_NONE):
                  processVersion(response, stream);
                  break;
               case cmd_pair(AdminCmd::CMD_RECYCLETAPEFILE, AdminCmd::SUBCMD_LS):
                  processRecycleTapeFile_Ls(response,stream);
                  break;
               case cmd_pair(AdminCmd::CMD_RECYCLETAPEFILE, AdminCmd::SUBCMD_RESTORE):
                  processRecycleTapeFile_Restore(response);
                  break;
               case cmd_pair(AdminCmd::CMD_ARCHIVEFILE, AdminCmd::SUBCMD_CH):
                  processChangeStorageClass(response);
                  break;

               default:
                  throw XrdSsiPb::PbException("Admin command pair <" +
                        AdminCmd_Cmd_Name(request.admincmd().cmd()) + ", " +
                        AdminCmd_SubCmd_Name(request.admincmd().subcmd()) +
                        "> is not implemented.");
            } // end switch
            
            // Log the admin command
            logAdminCmd(__FUNCTION__, "success", "", request.admincmd(), t);
         } catch(XrdSsiPb::PbException &ex) {
            logAdminCmd(__FUNCTION__, "failure", ex.what(), request.admincmd(), t);
            throw ex;
         } catch(cta::exception::UserError &ex) {
            logAdminCmd(__FUNCTION__, "failure", ex.getMessageValue(), request.admincmd(), t);
            throw ex;
         } catch(cta::exception::Exception &ex) {
            logAdminCmd(__FUNCTION__, "failure", ex.what(), request.admincmd(), t);
            throw ex;
         } catch(std::runtime_error &ex) {
            logAdminCmd(__FUNCTION__, "failure", ex.what(), request.admincmd(), t);
            throw ex;
         }
         break;
         } // end case Request::kAdmincmd

      case Request::kNotification:
         // Log event before processing, same log as in WFE.log on eos side
         {
            const std::string &eventTypeName = Workflow_EventType_Name(request.notification().wf().event());
            const std::string &eosInstanceName = request.notification().wf().instance().name();
            const std::string &diskFilePath = request.notification().file().lpath();
            const std::string &diskFileId = std::to_string(request.notification().file().fid());
            cta::log::ScopedParamContainer params(m_lc);
            params.add("eventType", eventTypeName)
                  .add("eosInstance", eosInstanceName)
                  .add("diskFilePath", diskFilePath)
                  .add("diskFileId", diskFileId);
            m_lc.log(cta::log::INFO, "In RequestMessage::process(): processing SSI event");
            
         }
         // Validate that instance name in key used to authenticate matches instance name in Protocol buffer
         if(m_cliIdentity.username != request.notification().wf().instance().name()) {
            // Special case: allow KRB5 authentication for CLOSEW and PREPARE events, to allow operators
            // to use a command line tool to resubmit failed archive or prepare requests. This is NOT
            // permitted for DELETE events as we don't want files removed from the catalogue to be left
            // in the EOS namespace.
            if(m_protocol == Protocol::KRB5 &&
               (request.notification().wf().event() == cta::eos::Workflow::CLOSEW ||
                request.notification().wf().event() == cta::eos::Workflow::PREPARE)) {
               m_scheduler.authorizeAdmin(m_cliIdentity, m_lc);
               m_cliIdentity.username = request.notification().wf().instance().name();
            } else {
               throw XrdSsiPb::PbException("Instance name \"" + request.notification().wf().instance().name() +
                                 "\" does not match key identifier \"" + m_cliIdentity.username + "\"");
            }
         }

         // Refuse any workflow events for files in /eos/INSTANCE_NAME/proc/
         {
           const std::string &longInstanceName = request.notification().wf().instance().name();
           const bool longInstanceNameStartsWithEos = 0 == longInstanceName.find("eos");
           const std::string shortInstanceName =
             longInstanceNameStartsWithEos ? longInstanceName.substr(3) : longInstanceName;
           if(shortInstanceName.empty()) {
             std::ostringstream msg;
             msg << "Short instance name is an empty string: instance=" << longInstanceName;
             throw XrdSsiPb::PbException(msg.str());
           }
           const std::string procFullPath = std::string("/eos/") + shortInstanceName + "/proc/";
           if(request.notification().file().lpath().find(procFullPath) == 0) {
             std::ostringstream msg;
             msg << "Cannot process a workflow event for a file in " << procFullPath << " instance=" << longInstanceName
               << " event=" << Workflow_EventType_Name(request.notification().wf().event()) << " lpath=" <<
               request.notification().file().lpath();
             throw XrdSsiPb::PbException(msg.str());
           }
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
            case Workflow::UPDATE_FID:
               processUPDATE_FID (request.notification(), response);
               break;

            default:
               throw XrdSsiPb::PbException("Workflow event " +
                     Workflow_EventType_Name(request.notification().wf().event()) +
                     " is not implemented.");
         }
         break;

      case Request::REQUEST_NOT_SET:
         throw XrdSsiPb::PbException("Request message has not been set.");

      default:
         throw XrdSsiPb::PbException("Unrecognized Request message. "
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
       throw XrdSsiPb::PbException(std::string(__FUNCTION__) + ": sys.archive.storage_class extended attribute is not set");
     }
   }
   const std::string storageClass = storageClassItor->second;
   if(storageClass.empty()) {
     throw XrdSsiPb::PbException(std::string(__FUNCTION__) + ": sys.archive.storage_class extended attribute is set to an empty string");
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
     throw XrdSsiPb::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.storage_class");
   }

   // For testing: this storage class will always fail
   if(storageClassItor->second == "fail_on_closew_test") {
      throw cta::exception::UserError("File is in fail_on_closew_test storage class, which always fails.");
   }

   auto storageClass = m_catalogue.getStorageClass(storageClassItor->second);

   // Disallow archival of files above the specified limit
   if(storageClass.vo.maxFileSize && notification.file().size() > storageClass.vo.maxFileSize) {
      throw exception::UserError("Archive request rejected: file size (" + std::to_string(notification.file().size()) +
                                 " bytes) exceeds maximum allowed size (" + std::to_string(storageClass.vo.maxFileSize) + " bytes)");
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

   cta::log::ScopedParamContainer params(m_lc);
   params.add("requesterInstance", notification.wf().requester_instance());
   std::string logMessage = "In RequestMessage::processCLOSEW(): ";

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
   // must be converted to a valid uint64_t
   const auto archiveFileIdItor = notification.file().xattr().find("sys.archive.file_id");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     logMessage += "sys.archive.file_id is not present in extended attributes";
     m_lc.log(cta::log::INFO, logMessage);
     throw XrdSsiPb::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   uint64_t archiveFileId = 0;
   if((archiveFileId = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      params.add("sys.archive.file_id", archiveFileIdStr);
      logMessage += "sys.archive.file_id is not a positive integer";
      m_lc.log(cta::log::INFO, logMessage);
      throw XrdSsiPb::PbException("Invalid archiveFileID " + archiveFileIdStr);
   }
   params.add("fileId", archiveFileId);

   cta::utils::Timer t;

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
   m_lc.log(cta::log::INFO, logMessage);

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
   request.isVerifyOnly           = notification.wf().verify_only();
   if (request.isVerifyOnly) {
      request.mountPolicy = m_verificationMountPolicy;
   }

   // Vid is for tape verification use case (for dual-copy files) so normally is not specified
   if(!notification.wf().vid().empty()) {
     request.vid = notification.wf().vid();
   }

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which must be
   // converted to a valid uint64_t
   auto archiveFileIdItor = notification.file().xattr().find("sys.archive.file_id");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     // Fall back to the old xattr format
     archiveFileIdItor = notification.file().xattr().find("CTA_ArchiveFileId");
     if(notification.file().xattr().end() == archiveFileIdItor) {
       throw XrdSsiPb::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
     }
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw XrdSsiPb::PbException("Invalid archiveFileID " + archiveFileIdStr);
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
   params.add("fileId", request.archiveFileID)
         .add("schedulerTime", t.secs())
         .add("isVerifyOnly", request.isVerifyOnly)
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
       throw XrdSsiPb::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
     }
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw XrdSsiPb::PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   // The request Id should be stored as an extended attribute
   const auto retrieveRequestIdItor = notification.file().xattr().find("sys.cta.objectstore.id");
   if(notification.file().xattr().end() == retrieveRequestIdItor) {
     throw XrdSsiPb::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.cta.objectstore.id");
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
   checkIsNotEmptyString(notification.file().lpath(),             "notification.file.lpath");

   // Unpack message
   cta::common::dataStructures::DeleteArchiveRequest request;
   request.requester.name    = notification.cli().user().username();
   request.requester.group   = notification.cli().user().groupname();

   std::string lpath         = notification.file().lpath();
   uint64_t diskFileId       = notification.file().fid();
   request.diskFilePath          = lpath;
   request.diskFileId = std::to_string(diskFileId);
   request.diskInstance = m_cliIdentity.username;
   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
   // must be converted to a valid uint64_t
   auto archiveFileIdItor = notification.file().xattr().find("sys.archive.file_id");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     // Fall back to the old xattr format
     archiveFileIdItor = notification.file().xattr().find("CTA_ArchiveFileId");
     if(notification.file().xattr().end() == archiveFileIdItor) {
       throw XrdSsiPb::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
     }
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw XrdSsiPb::PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   auto archiveRequestAddrItor = notification.file().xattr().find("sys.cta.archive.objectstore.id");
   if(archiveRequestAddrItor != notification.file().xattr().end()){
     //We have the ArchiveRequest's objectstore address.
     std::string objectstoreAddress = archiveRequestAddrItor->second;
     if(!objectstoreAddress.empty()){
      request.address = archiveRequestAddrItor->second;
     }
   }

   // Delete the file from the catalogue or from the objectstore if archive request is created
   cta::utils::Timer t;
   cta::log::TimingList tl;
   try {
     request.archiveFile = m_catalogue.getArchiveFileById(request.archiveFileID);
     tl.insertAndReset("catalogueGetArchiveFileByIdTime",t);
   } catch (cta::exception::Exception &ex){
    log::ScopedParamContainer spc(m_lc);
    spc.add("fileId", request.archiveFileID);
    m_lc.log(log::WARNING, "Ignoring request to delete archive file because it does not exist in the catalogue");
   }
   m_scheduler.deleteArchive(m_cliIdentity.username, request, m_lc);
   tl.insertAndReset("schedulerTime",t);
   // Create a log entry
   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", request.archiveFileID)
         .add("address", (request.address ? request.address.value() : "null"))
         .add("filePath",request.diskFilePath);
   tl.addToLog(params);
   m_lc.log(cta::log::INFO, "In RequestMessage::processDELETE(): archive file deleted.");

   // Set response type
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processUPDATE_FID(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Validate received protobuf
   checkIsNotEmptyString(notification.file().lpath(),  "notification.file.lpath");

   // Unpack message
   const std::string &diskInstance = m_cliIdentity.username;
   const std::string &diskFilePath = notification.file().lpath();
   const std::string diskFileId = std::to_string(notification.file().fid());

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which must be
   // converted to a valid uint64_t
   auto archiveFileIdItor = notification.file().xattr().find("sys.archive.file_id");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     // Fall back to the old xattr format
     archiveFileIdItor = notification.file().xattr().find("CTA_ArchiveFileId");
     if(notification.file().xattr().end() == archiveFileIdItor) {
       throw XrdSsiPb::PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named sys.archive.file_id");
     }
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   const uint64_t archiveFileId = strtoul(archiveFileIdStr.c_str(), nullptr, 10);
   if(0 == archiveFileId) {
      throw XrdSsiPb::PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   // Update the disk file ID
   cta::utils::Timer t;
   m_catalogue.updateDiskFileId(archiveFileId, diskInstance, diskFileId);

   // Create a log entry
   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", archiveFileId)
         .add("schedulerTime", t.secs())
         .add("diskInstance", diskInstance)
         .add("diskFilePath", diskFilePath)
         .add("diskFileId", diskFileId);
   m_lc.log(cta::log::INFO, "In RequestMessage::processUPDATE_FID(): updated disk file ID.");

   // Set response type
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



// Admin commands

void RequestMessage::logAdminCmd(const std::string &function, const std::string &status, const std::string &reason,
const cta::admin::AdminCmd &admincmd, cta::utils::Timer &t)
{
   using namespace cta::admin;

   cta::log::ScopedParamContainer params(m_lc);

   std::string log_msg = "In RequestMessage::" + function + "(): Admin command succeeded: ";

   // Reverse lookup of strings corresponding to <command,subcommand> pair
   for(auto cmd_it = cmdLookup.begin(); cmd_it != cmdLookup.end(); ++cmd_it) {
      // Return the matching long string (length > 3)
      if(admincmd.cmd() == cmd_it->second && cmd_it->first.length() > 3) {
         params.add("command", cmd_it->first);
         break;
      }
   }
   for(auto subcmd_it = subcmdLookup.begin(); subcmd_it != subcmdLookup.end(); ++subcmd_it) {
      if(admincmd.subcmd() == subcmd_it->second) {
         params.add("subcommand", subcmd_it->first);
         break;
      }
   }

   params.add("status", status);

   if (reason != "") {
      params.add("reason", reason);
   }


   // Log options passed from the command line
   std::pair<cta::admin::AdminCmd::Cmd, cta::admin::AdminCmd::SubCmd> cmd_key(admincmd.cmd(), admincmd.subcmd());
   std::vector<cta::admin::Option> cmd_options = cta::admin::cmdOptions.at(cmd_key);

   for (auto &cmd_option: cmd_options) {
      bool has_option = false;
      auto lookup_key = cmd_option.get_key();
      // Lookup if command line option was used in the command
      switch (cmd_option.get_type()) {
         case cta::admin::Option::option_t::OPT_FLAG: //Treat flag options as bool options
         case cta::admin::Option::option_t::OPT_BOOL:
            {
               auto bool_key = cta::admin::boolOptions.at(lookup_key);
               auto opt_value = getOptional(bool_key, &has_option);
               if (has_option) {
                  auto descriptor = cta::admin::OptionBoolean::Key_descriptor();
                  auto bool_key_name  = descriptor->FindValueByNumber(bool_key)->name();
                  params.add(bool_key_name, opt_value.value() ? "true" : "false");
               }
               break;
            }
         case cta::admin::Option::option_t::OPT_UINT:
            {
               auto int_key = cta::admin::uint64Options.at(lookup_key);
               auto opt_value = getOptional(int_key, &has_option);
               if (has_option) {
                  auto descriptor = cta::admin::OptionUInt64::Key_descriptor();
                  auto int_key_name  = descriptor->FindValueByNumber(int_key)->name();
                  params.add(int_key_name, opt_value.value());
               }
               break;
            }
         case cta::admin::Option::option_t::OPT_CMD: //Treat command options as string options
         case cta::admin::Option::option_t::OPT_STR:
            {
               auto string_key = cta::admin::strOptions.at(lookup_key);
               auto opt_value = getOptional(string_key, &has_option);
               if (has_option) {
                  auto descriptor = cta::admin::OptionString::Key_descriptor();
                  auto string_key_name  = descriptor->FindValueByNumber(string_key)->name();
                  params.add(string_key_name, opt_value.value());
               }
               break;
            }
         case cta::admin::Option::option_t::OPT_STR_LIST:
            {
               auto string_list_key = cta::admin::strListOptions.at(lookup_key);
               auto opt_value = getOptional(string_list_key, &has_option);
               if (has_option) {
                  auto descriptor = cta::admin::OptionStrList::Key_descriptor();
                  auto string_list_key_name  = descriptor->FindValueByNumber(string_list_key)->name();
                  params.add(string_list_key_name, cmd_option.get_help_text());
               }
               break;
            }
      }
   }

   params.add("adminTime", t.secs());
   params.add("user", m_cliIdentity.username + "@" + m_cliIdentity.host);
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

   std::optional<std::string> reason = getOptional(OptionString::REASON);
   cta::common::dataStructures::DesiredDriveState desiredDS;
   desiredDS.up = true;
   desiredDS.forceDown = false;
   desiredDS.reason = reason;
   if(!desiredDS.reason){
     //If reason not provided while setting the drive up, we delete it, so we set it to an empty string
     desiredDS.reason = "";
   }

   std::string cmdlineOutput = setDriveState('^' + getRequired(OptionString::DRIVE) + '$', desiredDS);

   response.set_message_txt(cmdlineOutput);
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Down(cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::string reason = getRequired(OptionString::REASON);
   if(utils::trimString(reason).empty()) {
     throw cta::exception::UserError("You must provide a reason in order to set the drive down");
   }
   cta::common::dataStructures::DesiredDriveState desiredDS;
   desiredDS.up = false;
   desiredDS.forceDown = has_flag(OptionBoolean::FORCE);
   desiredDS.reason = reason;

   std::string cmdlineOutput = setDriveState('^' + getRequired(OptionString::DRIVE) + '$', desiredDS);

   response.set_message_txt(cmdlineOutput);
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDrive_Ch(cta::xrd::Response & response) {
  using namespace cta::admin;

  std::string comment = getRequired(OptionString::COMMENT);
  if(utils::trimString(comment).empty()) {
    throw cta::exception::UserError("You must provide a comment to change it.");
  }

  cta::common::dataStructures::DesiredDriveState desiredDS;
  desiredDS.comment = comment;

  std::string cmdlineOutput = setDriveState('^' + getRequired(OptionString::DRIVE) + '$', desiredDS);

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

  const auto tapeDriveNames = m_catalogue.getTapeDriveNames();
  bool drivesFound = false;

  for (auto tapeDriveName : tapeDriveNames)
  {
    const auto regexResult = driveNameRegex.exec(tapeDriveName);
    if (!regexResult.empty())
    {
      const auto tapeDrive = m_catalogue.getTapeDrive(tapeDriveName).value();

      if (tapeDrive.driveStatus == cta::common::dataStructures::DriveStatus::Down     ||
          tapeDrive.driveStatus == cta::common::dataStructures::DriveStatus::Shutdown ||
          tapeDrive.driveStatus == cta::common::dataStructures::DriveStatus::Unknown  ||
          has_flag(OptionBoolean::FORCE))
      {
        m_scheduler.removeDrive(m_cliIdentity, tapeDriveName, m_lc);
        cmdlineOutput << "Drive " << tapeDriveName << " removed"
                      << (has_flag(OptionBoolean::FORCE) ? " (forced)." : ".") << std::endl;
      } else {
        cmdlineOutput << "Drive " << tapeDriveName << " in state "
                      << cta::common::dataStructures::toString(tapeDrive.driveStatus)
                      << " and force is not set (skipped)." << std::endl;
      }
      drivesFound = true;
    }
  }

  if (!drivesFound) {
    cmdlineOutput << "No drives match \"" << regex << "\". No drives were removed." << std::endl;
  }

  response.set_message_txt(cmdlineOutput.str());
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processFailedRequest_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  stream = new FailedRequestLsStream(*this, m_catalogue, m_scheduler, m_service.getSchedDb(), m_lc);

  // Display the correct column headers
  response.set_show_header(has_flag(OptionBoolean::SUMMARY) ? HeaderType::FAILEDREQUEST_LS_SUMMARY
                                                            : HeaderType::FAILEDREQUEST_LS);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processFailedRequest_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &objectId = getRequired(OptionString::OBJECTID);

   m_scheduler.deleteFailed(objectId, m_lc);

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

   auto &name             = getRequired(OptionString::LOGICAL_LIBRARY);
   auto  disabled         = getOptional(OptionBoolean::DISABLED);
   auto  comment          = getOptional(OptionString::COMMENT);
   auto  disabledReason   = getOptional(OptionString::DISABLED_REASON);

   if(disabled) {
      m_catalogue.setLogicalLibraryDisabled(m_cliIdentity, name, disabled.value());
      if ((!disabled.value()) && (!disabledReason)) {
         //if enabling the tape and the reason is not specified in the command, erase the reason
         m_catalogue.modifyLogicalLibraryDisabledReason(m_cliIdentity, name, "");
      }
   }
   if(comment) {
      m_catalogue.modifyLogicalLibraryComment(m_cliIdentity, name, comment.value());
   }
   if (disabledReason) {
      m_catalogue.modifyLogicalLibraryDisabledReason(m_cliIdentity, name, disabledReason.value());
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

  const auto disabled = getOptional(OptionBoolean::DISABLED);

  // Create a XrdSsi stream object to return the results
  stream = new LogicalLibraryLsStream(*this, m_catalogue, m_scheduler, disabled);

  response.set_show_header(HeaderType::LOGICALLIBRARY_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMediaType_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   // Bounds check unsigned integer options less than 64-bits in width
   const auto nbWraps = getOptional(OptionUInt64::NUMBER_OF_WRAPS);
   const auto primaryDensityCode = getOptional(OptionUInt64::PRIMARY_DENSITY_CODE);
   const auto secondaryDensityCode = getOptional(OptionUInt64::SECONDARY_DENSITY_CODE);
   if(nbWraps && nbWraps.value() > std::numeric_limits<uint32_t>::max()) {
     exception::UserError ex;
     ex.getMessage() << "Number of wraps cannot be larger than " << std::numeric_limits<uint32_t>::max() << ": value="
       << nbWraps.value();
     throw ex;
   }
   if(primaryDensityCode && primaryDensityCode.value() > std::numeric_limits<uint8_t>::max()) {
     exception::UserError ex;
     ex.getMessage() << "Primary density code cannot be larger than " << (uint16_t)(std::numeric_limits<uint8_t>::max())
       << ": value=" << primaryDensityCode.value();
     throw ex;
   }
   if(secondaryDensityCode && secondaryDensityCode.value() > std::numeric_limits<uint8_t>::max()) {
     exception::UserError ex;
     ex.getMessage() << "Secondary density code cannot be larger than " << (uint16_t)(std::numeric_limits<uint8_t>::max())
       << ": value=" << secondaryDensityCode.value();
     throw ex;
   }

   catalogue::MediaType mediaType;
   mediaType.name                 = getRequired(OptionString::MEDIA_TYPE);
   mediaType.cartridge            = getRequired(OptionString::CARTRIDGE);
   mediaType.capacityInBytes      = getRequired(OptionUInt64::CAPACITY);
   if(primaryDensityCode)  mediaType.primaryDensityCode = primaryDensityCode.value();
   if(secondaryDensityCode) mediaType.secondaryDensityCode = secondaryDensityCode.value();
   if (nbWraps) mediaType.nbWraps = nbWraps.value();
   mediaType.minLPos              = getOptional(OptionUInt64::MIN_LPOS);
   mediaType.maxLPos              = getOptional(OptionUInt64::MAX_LPOS);
   mediaType.comment              = getRequired(OptionString::COMMENT);
   m_catalogue.createMediaType(m_cliIdentity, mediaType);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMediaType_Ch(cta::xrd::Response &response)
{
  using namespace cta::admin;

  auto &mediaTypeName = getRequired(OptionString::MEDIA_TYPE);
  auto cartridge = getOptional(OptionString::CARTRIDGE);
  auto primaryDensityCode = getOptional(OptionUInt64::PRIMARY_DENSITY_CODE);
  auto secondaryDensityCode = getOptional(OptionUInt64::SECONDARY_DENSITY_CODE);
  auto nbWraps = getOptional(OptionUInt64::NUMBER_OF_WRAPS);
  auto minlpos = getOptional(OptionUInt64::MIN_LPOS);
  auto maxlpos = getOptional(OptionUInt64::MAX_LPOS);
  auto comment = getOptional(OptionString::COMMENT);

  // Bounds check unsigned integer options less than 64-bits in width
  if(nbWraps && nbWraps.value() > std::numeric_limits<uint32_t>::max()) {
    exception::UserError ex;
    ex.getMessage() << "Number of wraps cannot be larger than " << std::numeric_limits<uint32_t>::max() << ": value="
      << nbWraps.value();
    throw ex;
  }
  if(primaryDensityCode && primaryDensityCode.value() > std::numeric_limits<uint8_t>::max()) {
    exception::UserError ex;
    ex.getMessage() << "Primary density code cannot be larger than " << (uint16_t)(std::numeric_limits<uint8_t>::max())
      << ": value=" << primaryDensityCode.value();
    throw ex;
  }
  if(secondaryDensityCode && secondaryDensityCode.value() > std::numeric_limits<uint8_t>::max()) {
    exception::UserError ex;
    ex.getMessage() << "Secondary density code cannot be larger than " << (uint16_t)(std::numeric_limits<uint8_t>::max())
      << ": value=" << secondaryDensityCode.value();
    throw ex;
  }

  if(cartridge){
    m_catalogue.modifyMediaTypeCartridge(m_cliIdentity,mediaTypeName,cartridge.value());
  }
  if(primaryDensityCode){
    m_catalogue.modifyMediaTypePrimaryDensityCode(m_cliIdentity,mediaTypeName,primaryDensityCode.value());
  }
  if(secondaryDensityCode){
    m_catalogue.modifyMediaTypeSecondaryDensityCode(m_cliIdentity,mediaTypeName,secondaryDensityCode.value());
  }
  if(nbWraps){
    std::optional<uint32_t> newNbWraps = nbWraps.value();
    m_catalogue.modifyMediaTypeNbWraps(m_cliIdentity,mediaTypeName,newNbWraps);
  }
  if(minlpos){
    m_catalogue.modifyMediaTypeMinLPos(m_cliIdentity, mediaTypeName, minlpos);
  }
  if(maxlpos){
    m_catalogue.modifyMediaTypeMaxLPos(m_cliIdentity,mediaTypeName,maxlpos);
  }
  if(comment){
    m_catalogue.modifyMediaTypeComment(m_cliIdentity,mediaTypeName,comment.value());
  }
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMediaType_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   const auto &mtn = getRequired(OptionString::MEDIA_TYPE);

   m_catalogue.deleteMediaType(mtn);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMediaType_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new MediaTypeLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::MEDIATYPE_LS);
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
   auto &comment               = getRequired(OptionString::COMMENT);

   cta::catalogue::CreateMountPolicyAttributes mountPolicy;
   mountPolicy.name = group;
   mountPolicy.archivePriority = archivepriority;
   mountPolicy.minArchiveRequestAge = minarchiverequestage;
   mountPolicy.retrievePriority = retrievepriority;
   mountPolicy.minRetrieveRequestAge = minretrieverequestage;
   mountPolicy.comment = comment;

   m_catalogue.createMountPolicy(m_cliIdentity, mountPolicy);

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

  auto mountPolicyProvidedByUser = getRequired(OptionString::MOUNT_POLICY);

  //Get the mountpolicy from the catalogue
  common::dataStructures::MountPolicy mountPolicy;
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

  auto buff = getOptional(OptionString::BUFFERURL);
  if (buff){
    //The buffer is provided by the user
    bufferURL = buff.value();
  } else {
    //Buffer is not provided by the user, try to get the one from the configuration file
    if(m_repackBufferURL){
      bufferURL = m_repackBufferURL.value();
    } else {
      //Buffer is neither provided by the user, neither provided by the frontend configuration file, exception
      throw cta::exception::UserError("Must specify the buffer URL using --bufferurl option or using the frontend configuration file.");
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

  if (forceDisabledTape) {
   //repacks on a disabled tape must be from a mount policy whose name starts with repack
   if (mountPolicy.name.rfind("repack", 0) != 0) { 
      throw cta::exception::UserError("--disabledtape requires a mount policy whose name starts with repack");
   }
  }

  bool noRecall = has_flag(OptionBoolean::NO_RECALL);

  // Process each item in the list
  for(auto it = vid_list.begin(); it != vid_list.end(); ++it) {
    SchedulerDatabase::QueueRepackRequest repackRequest(*it,bufferURL,type,mountPolicy,forceDisabledTape, noRecall);
    m_scheduler.queueRepack(m_cliIdentity, repackRequest, m_lc);
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
  stream = new RepackLsStream(m_scheduler, m_catalogue, vid);

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




void RequestMessage::processActivityMountRule_Add(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &mountpolicy   = getRequired(OptionString::MOUNT_POLICY);
   auto &in            = getRequired(OptionString::INSTANCE);
   auto &name          = getRequired(OptionString::USERNAME);
   auto &comment       = getRequired(OptionString::COMMENT);
   auto &activityRegex = getRequired(OptionString::ACTIVITY_REGEX);

   m_catalogue.createRequesterActivityMountRule(m_cliIdentity, mountpolicy, in, name, activityRegex, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processActivityMountRule_Ch(cta::xrd::Response &response)
{
   
   using namespace cta::admin;

   auto &in            = getRequired(OptionString::INSTANCE);
   auto &name          = getRequired(OptionString::USERNAME);
   auto &activityRegex = getRequired(OptionString::ACTIVITY_REGEX);
   auto  comment       = getOptional(OptionString::COMMENT);
   auto  mountpolicy   = getOptional(OptionString::MOUNT_POLICY);

   if(comment) {
      m_catalogue.modifyRequesterActivityMountRuleComment(m_cliIdentity, in, name, activityRegex, comment.value());
   }
   if(mountpolicy) {
      m_catalogue.modifyRequesterActivityMountRulePolicy(m_cliIdentity, in, name, activityRegex, mountpolicy.value());
   }
   
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processActivityMountRule_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in   = getRequired(OptionString::INSTANCE);
   auto &name = getRequired(OptionString::USERNAME);
   auto &activityRegex = getRequired(OptionString::ACTIVITY_REGEX);

   m_catalogue.deleteRequesterActivityMountRule(in, name, activityRegex);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processActivityMountRule_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  stream = new ActivityMountRuleLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::ACTIVITYMOUNTRULE_LS);
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
   auto &full           = getRequired(OptionBoolean::FULL);
   auto state           = getOptional(OptionString::STATE);
   auto stateReason     = getOptional(OptionString::REASON);
   auto comment         = getOptional(OptionString::COMMENT);

   cta::catalogue::CreateTapeAttributes tape;
   tape.vid = vid;
   tape.mediaType = mediaType;
   tape.vendor = vendor;
   tape.logicalLibraryName = logicallibrary;
   tape.tapePoolName = tapepool;
   tape.full = full;
   tape.comment = comment ? comment.value() : "";
   if(!state){
     //By default, the state of the tape will be ACTIVE
     tape.state = common::dataStructures::Tape::ACTIVE;
   } else {
     //State has been provided by the user, assign it. Will throw an exception if the state provided does not exist.
     tape.state = common::dataStructures::Tape::stringToState(state.value());
   }
   tape.stateReason = stateReason;
   m_catalogue.createTape(m_cliIdentity, tape);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid                = getRequired(OptionString::VID);
   auto  mediaType          = getOptional(OptionString::MEDIA_TYPE);
   auto  vendor             = getOptional(OptionString::VENDOR);
   auto  logicallibrary     = getOptional(OptionString::LOGICAL_LIBRARY);
   auto  tapepool           = getOptional(OptionString::TAPE_POOL);
   auto  comment            = getOptional(OptionString::COMMENT);
   auto  encryptionkeyName  = getOptional(OptionString::ENCRYPTION_KEY_NAME);
   auto  full               = getOptional(OptionBoolean::FULL);
   auto  state              = getOptional(OptionString::STATE);
   auto  stateReason        = getOptional(OptionString::REASON);
   auto  dirty              = getOptional(OptionBoolean::DIRTY_BIT);
   auto  verificationStatus = getOptional(OptionString::VERIFICATION_STATUS);

   if(mediaType) {
      if (m_catalogue.getNbFilesOnTape(vid) != 0) {
         response.set_type(cta::xrd::Response::RSP_ERR_CTA);
         return;
      }
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
   if(comment) {
      if(comment.value().empty()){
        //If the comment is an empty string, the user meant to delete it
        comment = std::nullopt;
      }
      m_catalogue.modifyTapeComment(m_cliIdentity, vid, comment);
   }
   if(encryptionkeyName) {
      m_catalogue.modifyTapeEncryptionKeyName(m_cliIdentity, vid, encryptionkeyName.value());
   }
   if(full) {
      m_catalogue.setTapeFull(m_cliIdentity, vid, full.value());
   }
   if(state){
     auto stateEnumValue = common::dataStructures::Tape::stringToState(state.value());
     m_catalogue.modifyTapeState(m_cliIdentity,vid,stateEnumValue,stateReason);
   }
   if (dirty) {
      m_catalogue.setTapeDirty(m_cliIdentity, vid, dirty.value());
   }
   if (verificationStatus) {
      m_catalogue.modifyTapeVerificationStatus(m_cliIdentity, vid, verificationStatus.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = getRequired(OptionString::VID);

   if (m_scheduler.isBeingRepacked(vid)) {
     throw cta::exception::UserError("Cannot delete tape " + vid + " because there is a repack for that tape");
   }  
   m_catalogue.deleteTape(vid);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Reclaim(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = getRequired(OptionString::VID);

   m_catalogue.reclaimTape(m_cliIdentity, vid, m_lc);

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

void RequestMessage::processTapeFile_Rm(cta::xrd::Response &response)
{
  using namespace cta::admin;
  auto &vid           = getRequired(OptionString::VID);
  auto &reason        = getRequired(OptionString::REASON);
  auto archiveFileId  = getOptional(OptionUInt64::ARCHIVE_FILE_ID);
  auto instance       = getOptional(OptionString::INSTANCE);
  auto diskFileId     = getOptional(OptionString::FXID);

  cta::catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.vid = vid;
  
  if (archiveFileId) {
    searchCriteria.archiveFileId = archiveFileId.value();
  }
  if (diskFileId) {
    auto fid = strtol(diskFileId.value().c_str(), nullptr, 16);
    if(fid < 1 || fid == LONG_MAX) {
      throw cta::exception::UserError(diskFileId.value() + " is not a valid file ID");
    }
    searchCriteria.diskFileIds = std::vector<std::string>();
    searchCriteria.diskFileIds.value().push_back(std::to_string(fid));
  }
  if (instance) {
    searchCriteria.diskInstance = instance.value();
  }
  
  auto archiveFile = m_catalogue.getArchiveFileForDeletion(searchCriteria);
  grpc::EndpointMap endpoints(m_namespaceMap);
  auto diskFilePath = endpoints.getPath(archiveFile.diskInstance, archiveFile.diskFileId);
  archiveFile.diskFileInfo.path = diskFilePath;

  m_catalogue.deleteTapeFileCopy(archiveFile, reason);
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
  const auto &diskInstance      = getRequired(OptionString::DISK_INSTANCE);
  const auto &diskInstanceSpace = getRequired(OptionString::DISK_INSTANCE_SPACE);
  const auto &fileRegexp        = getRequired(OptionString::FILE_REGEXP);
  const auto &targetedFreeSpace = getRequired(OptionUInt64::TARGETED_FREE_SPACE);
  const auto &sleepTime         = getRequired(OptionUInt64::SLEEP_TIME);
  const auto &comment           = getRequired(OptionString::COMMENT);

  m_catalogue.createDiskSystem(m_cliIdentity, name,diskInstance, diskInstanceSpace, fileRegexp, targetedFreeSpace, sleepTime, comment);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskSystem_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   const auto &name                  = getRequired(OptionString::DISK_SYSTEM);
   const auto &fileRegexp            = getOptional(OptionString::FILE_REGEXP);
   const auto  targetedFreeSpace     = getOptional(OptionUInt64::TARGETED_FREE_SPACE);
   const auto  sleepTime             = getOptional(OptionUInt64::SLEEP_TIME);
   const auto  comment               = getOptional(OptionString::COMMENT);
   const auto  diskInstanceName      = getOptional(OptionString::DISK_INSTANCE);
   const auto  diskInstanceSpaceName = getOptional(OptionString::DISK_INSTANCE_SPACE);

   if(comment) {
      m_catalogue.modifyDiskSystemComment(m_cliIdentity, name, comment.value());
   }
   if(fileRegexp) {
      m_catalogue.modifyDiskSystemFileRegexp(m_cliIdentity, name, fileRegexp.value());
   }
   if (sleepTime) {
     m_catalogue.modifyDiskSystemSleepTime(m_cliIdentity, name, sleepTime.value());
   }
   if(targetedFreeSpace) {
      m_catalogue.modifyDiskSystemTargetedFreeSpace(m_cliIdentity, name, targetedFreeSpace.value());
   }
   if (diskInstanceName) {
      m_catalogue.modifyDiskSystemDiskInstanceName(m_cliIdentity, name, diskInstanceName.value());
   }
   if (diskInstanceSpaceName) {
      m_catalogue.modifyDiskSystemDiskInstanceSpaceName(m_cliIdentity, name, diskInstanceSpaceName.value());
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

void RequestMessage::processDiskInstance_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new DiskInstanceLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::DISKINSTANCE_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskInstance_Add(cta::xrd::Response &response)
{
  using namespace cta::admin;

  const auto &name              = getRequired(OptionString::DISK_INSTANCE);
  const auto &comment           = getRequired(OptionString::COMMENT);

  m_catalogue.createDiskInstance(m_cliIdentity, name, comment);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskInstance_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   const auto &name              = getRequired(OptionString::DISK_INSTANCE);
   const auto comment            = getOptional(OptionString::COMMENT);

   if(comment) {
      m_catalogue.modifyDiskInstanceComment(m_cliIdentity, name, comment.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskInstance_Rm(cta::xrd::Response &response)
{
  using namespace cta::admin;

  const auto &name = getRequired(OptionString::DISK_INSTANCE);

  m_catalogue.deleteDiskInstance(name);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskInstanceSpace_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  // Create a XrdSsi stream object to return the results
  stream = new DiskInstanceSpaceLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(HeaderType::DISKINSTANCESPACE_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskInstanceSpace_Add(cta::xrd::Response &response)
{
  using namespace cta::admin;

  const auto &name              = getRequired(OptionString::DISK_INSTANCE_SPACE);
  const auto &diskInstance      = getRequired(OptionString::DISK_INSTANCE);
  const auto &comment           = getRequired(OptionString::COMMENT);
  const auto &freeSpaceQueryURL = getRequired(OptionString::FREE_SPACE_QUERY_URL);
  const auto refreshInterval    = getRequired(OptionUInt64::REFRESH_INTERVAL);
   
  m_catalogue.createDiskInstanceSpace(m_cliIdentity, name, diskInstance, freeSpaceQueryURL, refreshInterval, comment);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskInstanceSpace_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   const auto &name              = getRequired(OptionString::DISK_INSTANCE_SPACE);
   const auto &diskInstance      = getRequired(OptionString::DISK_INSTANCE);
   const auto comment            = getOptional(OptionString::COMMENT);
   const auto &freeSpaceQueryURL = getOptional(OptionString::FREE_SPACE_QUERY_URL);
   const auto refreshInterval    = getOptional(OptionUInt64::REFRESH_INTERVAL);
   
   if(comment) {
      m_catalogue.modifyDiskInstanceSpaceComment(m_cliIdentity, name, diskInstance, comment.value());
   }
   if(freeSpaceQueryURL) {
      m_catalogue.modifyDiskInstanceSpaceQueryURL(m_cliIdentity, name, diskInstance, freeSpaceQueryURL.value());
   }
   if(refreshInterval) {
      m_catalogue.modifyDiskInstanceSpaceRefreshInterval(m_cliIdentity, name, diskInstance, refreshInterval.value());
   }
   
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskInstanceSpace_Rm(cta::xrd::Response &response)
{
  using namespace cta::admin;

  const auto &name         = getRequired(OptionString::DISK_INSTANCE_SPACE);
  const auto &diskInstance = getRequired(OptionString::DISK_INSTANCE);

  m_catalogue.deleteDiskInstanceSpace(name, diskInstance);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processVirtualOrganization_Add(cta::xrd::Response &response){
  using namespace cta::admin;

  const auto &name = getRequired(OptionString::VO);
  const auto &readMaxDrives = getRequired(OptionUInt64::READ_MAX_DRIVES);
  const auto &writeMaxDrives = getRequired(OptionUInt64::WRITE_MAX_DRIVES);
  const auto &comment = getRequired(OptionString::COMMENT);
  const auto &maxFileSizeOpt = getOptional(OptionUInt64::MAX_FILE_SIZE);
  const auto &diskInstanceName = getRequired(OptionString::DISK_INSTANCE);

  cta::common::dataStructures::VirtualOrganization vo;
  vo.name = name;
  vo.readMaxDrives = readMaxDrives;
  vo.writeMaxDrives = writeMaxDrives;
  vo.comment = comment;
  vo.diskInstanceName = diskInstanceName;

  if (maxFileSizeOpt) {
    vo.maxFileSize = maxFileSizeOpt.value();
  } else {
    vo.maxFileSize = m_archiveFileMaxSize;
  }

  m_catalogue.createVirtualOrganization(m_cliIdentity,vo);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processVirtualOrganization_Ch(cta::xrd::Response &response){
  using namespace cta::admin;

  const auto &name = getRequired(OptionString::VO);
  const auto readMaxDrives = getOptional(OptionUInt64::READ_MAX_DRIVES);
  const auto writeMaxDrives = getOptional(OptionUInt64::WRITE_MAX_DRIVES);
  const auto comment = getOptional(OptionString::COMMENT);
  const auto maxFileSize = getOptional(OptionUInt64::MAX_FILE_SIZE);
  const auto diskInstanceName = getOptional(OptionString::DISK_INSTANCE);

  if(comment)
    m_catalogue.modifyVirtualOrganizationComment(m_cliIdentity,name,comment.value());

  if(readMaxDrives)
    m_catalogue.modifyVirtualOrganizationReadMaxDrives(m_cliIdentity,name,readMaxDrives.value());

  if(writeMaxDrives)
    m_catalogue.modifyVirtualOrganizationWriteMaxDrives(m_cliIdentity,name,writeMaxDrives.value());

  if(maxFileSize)
    m_catalogue.modifyVirtualOrganizationMaxFileSize(m_cliIdentity,name,maxFileSize.value());

  if(diskInstanceName) 
    m_catalogue.modifyVirtualOrganizationDiskInstanceName(m_cliIdentity, name, diskInstanceName.value());

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


std::string RequestMessage::setDriveState(const std::string &regex, const cta::common::dataStructures::DesiredDriveState & desiredDriveState)
{
  using namespace cta::admin;

  std::stringstream cmdlineOutput;

  cta::utils::Regex driveNameRegex(regex.c_str());

  const auto tapeDriveNames = m_catalogue.getTapeDriveNames();
  bool is_found = false;

  for(auto tapeDriveName: tapeDriveNames)
  {
    const auto regexResult = driveNameRegex.exec(tapeDriveName);
    if(!regexResult.empty())
    {
      is_found = true;
      m_scheduler.setDesiredDriveState(m_cliIdentity, tapeDriveName, desiredDriveState, m_lc);

      cmdlineOutput << "Drive " << tapeDriveName << ": set ";
      if(!desiredDriveState.comment){
        cmdlineOutput << (desiredDriveState.up ? "Up" : "Down")
                    << (desiredDriveState.forceDown ? " (forced)" : "")
                    << "." << std::endl;
      } else {
        //We modified the comment so we will output that the comment was changed
        cmdlineOutput << "a new comment."
                      << std::endl;
      }
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

void RequestMessage::processVersion(cta::xrd::Response &response, XrdSsiStream * & stream){
  using namespace cta::admin;

  stream = new VersionStream(*this,m_catalogue,m_scheduler,m_catalogue_conn_string);

  response.set_show_header(HeaderType::VERSION_CMD);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processRecycleTapeFile_Ls(cta::xrd::Response &response, XrdSsiStream * & stream) {
  using namespace cta::admin;

  stream = new RecycleTapeFileLsStream(*this,m_catalogue,m_scheduler);

  response.set_show_header(HeaderType::RECYLETAPEFILE_LS);
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processRecycleTapeFile_Restore(cta::xrd::Response& response) {
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
   using namespace cta::admin;

  bool has_any = false;
  cta::catalogue::RecycleTapeFileSearchCriteria searchCriteria;

  searchCriteria.vid = getOptional(OptionString::VID, &has_any);
  auto diskFileId = getRequired(OptionString::FXID);
  
  auto fid = strtol(diskFileId.c_str(), nullptr, 16);
  if(fid < 1 || fid == LONG_MAX) {
    throw cta::exception::UserError(diskFileId + " is not a valid file ID");
  }

  // Disk instance on its own does not give a valid set of search criteria (no &has_any)
  searchCriteria.diskInstance = getOptional(OptionString::INSTANCE);
  searchCriteria.archiveFileId = getOptional(OptionUInt64::ARCHIVE_FILE_ID, &has_any);
  // Copy number on its own does not give a valid set of search criteria (no &has_any)
  searchCriteria.copynb = getOptional(OptionUInt64::COPY_NUMBER);

  if(!has_any){
    throw cta::exception::UserError("Must specify at least one of the following search options: vid, fxid, fxidfile or archiveFileId");
  }
  m_catalogue.restoreFileInRecycleLog(searchCriteria, std::to_string(fid));
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processChangeStorageClass(cta::xrd::Response& response) {
   try {
      using namespace cta::admin;

      std::string newStorageClassName = getRequired(OptionString::STORAGE_CLASS_NAME);
      auto archiveFileIds = getRequired(OptionStrList::FILE_ID);

      XrdCtaChangeStorageClass xrdCtaChangeStorageClass(m_catalogue, m_lc);
      xrdCtaChangeStorageClass.updateCatalogue(archiveFileIds, newStorageClassName);
      response.set_type(cta::xrd::Response::RSP_SUCCESS);
   } catch(exception::UserError &ue) {
      response.set_message_txt(ue.getMessage().str());
      response.set_type(Response::RSP_ERR_USER);
   }
}

}} // namespace cta::xrd
