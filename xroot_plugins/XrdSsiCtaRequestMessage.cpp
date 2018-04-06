/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          XRootD EOS Notification handler
 * @copyright      Copyright 2017 CERN
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

#include <iomanip> // for setw

#include "common/utils/utils.hpp"
#include <common/utils/Regex.hpp>

#include <XrdSsiPbException.hpp>
using XrdSsiPb::PbException;

#include <cmdline/CtaAdminCmdParse.hpp>
#include "XrdCtaArchiveFileLs.hpp"
#include "XrdSsiCtaRequestMessage.hpp"



namespace cta {
namespace xrd {

/*
 * Convert AdminCmd <Cmd, SubCmd> pair to an integer so that it can be used in a switch statement
 */
constexpr unsigned int cmd_pair(cta::admin::AdminCmd::Cmd cmd, cta::admin::AdminCmd::SubCmd subcmd) {
   return (cmd << 16) + subcmd;
}



/*
 * Helper function to convert time to string
 */
static std::string timeToString(const time_t &time)
{
   std::string timeString(ctime(&time));
   timeString.resize(timeString.size()-1); //remove newline
   return timeString;
}



/*
 * Helper function to convert bytes to Mb and return the result as a string
 */
static std::string bytesToMbString(uint64_t bytes)
{
   long double mBytes = static_cast<long double>(bytes)/(1000.0*1000.0);

   std::ostringstream oss;
   oss << std::setprecision(2) << std::fixed << mBytes;
   return oss.str();
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
               processAdmin_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_CH):
               processAdmin_Ch(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_RM):
               processAdmin_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_LS):
               processAdmin_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ADMINHOST, AdminCmd::SUBCMD_ADD):
               processAdminHost_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ADMINHOST, AdminCmd::SUBCMD_CH):
               processAdminHost_Ch(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ADMINHOST, AdminCmd::SUBCMD_RM):
               processAdminHost_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ADMINHOST, AdminCmd::SUBCMD_LS):
               processAdminHost_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ARCHIVEFILE, AdminCmd::SUBCMD_LS):
               processArchiveFile_Ls(request.admincmd(), response, stream);
               break;
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_ADD):
               processArchiveRoute_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_CH):
               processArchiveRoute_Ch(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_RM):
               processArchiveRoute_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_LS):
               processArchiveRoute_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_UP):
               processDrive_Up(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_DOWN):
               processDrive_Down(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_LS):
               processDrive_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_RM):
               processDrive_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_ADD):
               processGroupMountRule_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_CH):
               processGroupMountRule_Ch(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_RM):
               processGroupMountRule_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_LS):
               processGroupMountRule_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_LISTPENDINGARCHIVES, AdminCmd::SUBCMD_NONE):
               processListPendingArchives(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_LISTPENDINGRETRIEVES, AdminCmd::SUBCMD_NONE):
               processListPendingRetrieves(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_ADD):
               processLogicalLibrary_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_CH):
               processLogicalLibrary_Ch(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_RM):
               processLogicalLibrary_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_LS):
               processLogicalLibrary_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_ADD):
               processMountPolicy_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_CH):
               processMountPolicy_Ch(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_RM):
               processMountPolicy_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_LS):
               processMountPolicy_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ADD):
               processRepack_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_RM):
               processRepack_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_LS):
               processRepack_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ERR):
               processRepack_Err(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_ADD):
               processRequesterMountRule_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_CH):
               processRequesterMountRule_Ch(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_RM):
               processRequesterMountRule_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_LS):
               processRequesterMountRule_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_SHRINK, AdminCmd::SUBCMD_NONE):
               processShrink(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_SHOWQUEUES, AdminCmd::SUBCMD_NONE):
               processShowQueues(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_ADD):
               processStorageClass_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_CH):
               processStorageClass_Ch(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_RM):
               processStorageClass_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_LS):
               processStorageClass_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_ADD):
               processTape_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_CH):
               processTape_Ch(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_RM):
               processTape_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_RECLAIM):
               processTape_Reclaim(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LS):
               processTape_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LABEL):
               processTape_Label(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_ADD):
               processTapePool_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_CH):
               processTapePool_Ch(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_RM):
               processTapePool_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_LS):
               processTapePool_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TEST, AdminCmd::SUBCMD_READ):
               processTest_Read(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TEST, AdminCmd::SUBCMD_WRITE):
               processTest_Write(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_TEST, AdminCmd::SUBCMD_WRITE_AUTO):
               processTest_WriteAuto(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_VERIFY, AdminCmd::SUBCMD_ADD):
               processVerify_Add(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_VERIFY, AdminCmd::SUBCMD_RM):
               processVerify_Rm(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_VERIFY, AdminCmd::SUBCMD_LS):
               processVerify_Ls(request.admincmd(), response);
               break;
            case cmd_pair(AdminCmd::CMD_VERIFY, AdminCmd::SUBCMD_ERR):
               processVerify_Err(request.admincmd(), response);
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
   cta::common::dataStructures::UserIdentity originator;
   originator.name  = notification.cli().user().username();
   originator.group = notification.cli().user().groupname();

   cta::utils::Timer t;

   const auto storageClassItor = notification.file().xattr().find("CTA_StorageClass");
   if(notification.file().xattr().end() == storageClassItor) {
     throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named CTA_StorageClass");
   }
   const std::string storageClass = storageClassItor->second;
   const uint64_t archiveFileId = m_scheduler.checkAndGetNextArchiveFileId(m_cliIdentity.username, storageClass, originator, m_lc);

   // Create a log entry
   cta::log::ScopedParamContainer params(m_lc);
   params.add("diskFileId", std::to_string(notification.file().fid()))
         .add("diskFilePath", notification.file().lpath())
         .add("archiveFileId", archiveFileId)
         .add("catalogueTime", t.secs());
   m_lc.log(cta::log::INFO, "In RequestMessage::processCREATE(): assigning new archive file ID.");

   // Set ArchiveFileId in xattrs
   response.mutable_xattr()->insert(google::protobuf::MapPair<std::string,std::string>("CTA_ArchiveFileId", std::to_string(archiveFileId)));

   // Set response type
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processCLOSEW(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Validate received protobuf
   checkIsNotEmptyString(notification.cli().user().username(),    "notification.cli.user.username");
   checkIsNotEmptyString(notification.cli().user().groupname(),   "notification.cli.user.groupname");
   checkIsNotEmptyString(notification.file().owner().username(),  "notification.file.owner.username");
   checkIsNotEmptyString(notification.file().owner().groupname(), "notification.file.owner.groupname");
   checkIsNotEmptyString(notification.file().lpath(),             "notification.file.lpath");
   checkIsNotEmptyString(notification.wf().instance().url(),      "notification.wf.instance.url");
   checkIsNotEmptyString(notification.transport().report_url(),   "notification.transport.report_url");

   // Unpack message
   cta::common::dataStructures::UserIdentity originator;
   originator.name    = notification.cli().user().username();
   originator.group   = notification.cli().user().groupname();

   cta::common::dataStructures::DiskFileInfo diskFileInfo;
   diskFileInfo.owner = notification.file().owner().username();
   diskFileInfo.group = notification.file().owner().groupname();
   diskFileInfo.path  = notification.file().lpath();

   // Recovery blob is deprecated. EOS will fill in metadata fields in the protocol buffer
   // and we need to decide what will be stored in the database.
   diskFileInfo.recoveryBlob = "deprecated";

   cta::utils::Timer t;

   std::string checksumtype(notification.file().cks().type());
   if(checksumtype == "adler") checksumtype = "ADLER32";   // replace this with an enum!

   std::string checksumvalue("0X" + notification.file().cks().value());
   cta::utils::toUpper(checksumvalue);    // replace this with a number!

   const auto storageClassItor = notification.file().xattr().find("CTA_StorageClass");
   if(notification.file().xattr().end() == storageClassItor) {
     throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named CTA_StorageClass");
   }

   cta::common::dataStructures::ArchiveRequest request;
   request.checksumType         = checksumtype;
   request.checksumValue        = checksumvalue;
   request.diskFileInfo         = diskFileInfo;
   request.diskFileID           = std::to_string(notification.file().fid());
   request.fileSize             = notification.file().size();
   request.requester            = originator;
   request.srcURL               = notification.wf().instance().url();
   request.storageClass         = storageClassItor->second;
   request.archiveReportURL     = notification.transport().report_url();
   request.creationLog.host     = m_cliIdentity.host;
   request.creationLog.username = m_cliIdentity.username;
   request.creationLog.time     = time(nullptr);

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
   // must be converted to a valid uint64_t

   const auto archiveFileIdItor = notification.file().xattr().find("CTA_ArchiveFileId");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named CTA_ArchiveFileId");
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   uint64_t archiveFileId = 0;
   if((archiveFileId = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   // Queue the request
   m_scheduler.queueArchiveWithGivenId(archiveFileId, m_cliIdentity.username, request, m_lc);

   // Create a log entry
   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", archiveFileId).add("catalogueTime", t.secs());
   m_lc.log(cta::log::INFO, "In RequestMessage::processCLOSEW(): queued file for archive.");

   // Set response type
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processPREPARE(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Validate received protobuf
   checkIsNotEmptyString(notification.cli().user().username(),    "notification.cli.user.username");
   checkIsNotEmptyString(notification.cli().user().groupname(),   "notification.cli.user.groupname");
   checkIsNotEmptyString(notification.file().owner().username(),  "notification.file.owner.username");
   checkIsNotEmptyString(notification.file().owner().groupname(), "notification.file.owner.groupname");
   checkIsNotEmptyString(notification.file().lpath(),             "notification.file.lpath");
   checkIsNotEmptyString(notification.transport().dst_url(),      "notification.transport.dst_url");

   // Unpack message
   cta::common::dataStructures::UserIdentity originator;
   originator.name              = notification.cli().user().username();
   originator.group             = notification.cli().user().groupname();

   cta::common::dataStructures::DiskFileInfo diskFileInfo;
   diskFileInfo.owner           = notification.file().owner().username();
   diskFileInfo.group           = notification.file().owner().groupname();
   diskFileInfo.path            = notification.file().lpath();

   // Recovery blob is deprecated. EOS will fill in metadata fields in the protocol buffer
   // and we need to decide what will be stored in the database.
   diskFileInfo.recoveryBlob = "deprecated";

   cta::common::dataStructures::RetrieveRequest request;
   request.requester            = originator;
   request.dstURL               = notification.transport().dst_url();
   request.diskFileInfo         = diskFileInfo;
   request.creationLog.host     = m_cliIdentity.host;
   request.creationLog.username = m_cliIdentity.username;
   request.creationLog.time     = time(nullptr);

   cta::utils::Timer t;

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which must be
   // converted to a valid uint64_t
   const auto archiveFileIdItor = notification.file().xattr().find("CTA_ArchiveFileId");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named CTA_ArchiveFileId");
   }
   const std::string archiveFileIdStr = archiveFileIdItor->second;
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   // Queue the request
   m_scheduler.queueRetrieve(m_cliIdentity.username, request, m_lc);

   // Create a log entry
   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", request.archiveFileID).add("catalogueTime", t.secs());
   m_lc.log(cta::log::INFO, "In RequestMessage::processPREPARE(): queued file for retrieve.");

   // Set response type
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDELETE(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Validate received protobuf
   checkIsNotEmptyString(notification.cli().user().username(),    "notification.cli.user.username");
   checkIsNotEmptyString(notification.cli().user().groupname(),   "notification.cli.user.groupname");

   // Unpack message
   cta::common::dataStructures::UserIdentity originator;
   originator.name          = notification.cli().user().username();
   originator.group         = notification.cli().user().groupname();

   cta::common::dataStructures::DeleteArchiveRequest request;
   request.requester        = originator;

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
   // must be converted to a valid uint64_t

   const auto archiveFileIdItor = notification.file().xattr().find("CTA_ArchiveFileId");
   if(notification.file().xattr().end() == archiveFileIdItor) {
     throw PbException(std::string(__FUNCTION__) + ": Failed to find the extended attribute named CTA_ArchiveFileId");
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
   params.add("fileId", request.archiveFileID).add("catalogueTime", t.secs());
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
   params.add("catalogueTime", t.secs());
   m_lc.log(cta::log::INFO, log_msg);
}



void RequestMessage::processAdmin_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = getRequired(OptionString::USERNAME);
   auto &comment  = getRequired(OptionString::COMMENT);

   m_catalogue.createAdminUser(m_cliIdentity, username, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = getRequired(OptionString::USERNAME);
   auto &comment  = getRequired(OptionString::COMMENT);

   m_catalogue.modifyAdminUserComment(m_cliIdentity, username, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = getRequired(OptionString::USERNAME);

   m_catalogue.deleteAdminUser(username);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::AdminUser> list= m_catalogue.getAdminUsers();

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "user","c.user","c.host","c.time","m.user","m.host","m.time","comment"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->name);
         addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
         currentRow.push_back(it->comment);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdminHost_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &hostname = getRequired(OptionString::HOSTNAME);
   auto &comment  = getRequired(OptionString::COMMENT);

   // Validate that the hostname is a valid IPv4 or IPv6 address, or a Fully-Qualified Domain Name
   if(!utils::isValidIPAddress(hostname)) {
     utils::assertIsFQDN(hostname);
   }

   m_catalogue.createAdminHost(m_cliIdentity, hostname, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdminHost_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &hostname = getRequired(OptionString::HOSTNAME);
   auto &comment  = getRequired(OptionString::COMMENT);

   m_catalogue.modifyAdminHostComment(m_cliIdentity, hostname, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdminHost_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &hostname = getRequired(OptionString::HOSTNAME);

   m_catalogue.deleteAdminHost(hostname);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdminHost_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::AdminHost> list= m_catalogue.getAdminHosts();

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "hostname","c.user","c.host","c.time","m.user","m.host","m.time","comment"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);
      for(auto it = list.cbegin(); it != list.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->name);
         addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
         currentRow.push_back(it->comment);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveFile_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response, XrdSsiStream* &stream)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;
   cta::catalogue::TapeFileSearchCriteria searchCriteria;

   if(!has_flag(OptionBoolean::ALL))
   {
      bool has_any = false; // set to true if at least one optional option is set

      // Get the search criteria from the optional options

      searchCriteria.archiveFileId  = getOptional(OptionUInt64::ARCHIVE_FILE_ID, &has_any);
      searchCriteria.tapeFileCopyNb = getOptional(OptionUInt64::COPY_NUMBER,     &has_any);
      searchCriteria.diskFileId     = getOptional(OptionString::DISKID,          &has_any);
      searchCriteria.vid            = getOptional(OptionString::VID,             &has_any);
      searchCriteria.tapePool       = getOptional(OptionString::TAPE_POOL,       &has_any);
      searchCriteria.diskFileUser   = getOptional(OptionString::OWNER,           &has_any);
      searchCriteria.diskFileGroup  = getOptional(OptionString::GROUP,           &has_any);
      searchCriteria.storageClass   = getOptional(OptionString::STORAGE_CLASS,   &has_any);
      searchCriteria.diskFilePath   = getOptional(OptionString::PATH,            &has_any);
      searchCriteria.diskInstance   = getOptional(OptionString::INSTANCE,        &has_any);

      if(!has_any) {
         throw cta::exception::UserError("Must specify at least one search option, or --all");
      }
   }

   if(has_flag(OptionBoolean::SUMMARY)) {
      // Probably we should use a data response here, to prevent timeouts while calculating the summary statistics
      cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue.getTapeFileSummary(searchCriteria);
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = { "total number of files","total size" };
      std::vector<std::string> row = {std::to_string(static_cast<unsigned long long>(summary.totalFiles)),
                                      std::to_string(static_cast<unsigned long long>(summary.totalBytes))};
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);
      responseTable.push_back(row);
      cmdlineOutput << formatResponse(responseTable);
   } else {
      // Create a XrdSsi stream object to return the results
      stream = new ArchiveFileLsStream(m_catalogue.getArchiveFiles(searchCriteria));

      // Send the column headers in the metadata
      if(has_flag(OptionBoolean::SHOW_HEADER)) {
         const char* const TEXT_RED    = "\x1b[31;1m";
         const char* const TEXT_NORMAL = "\x1b[0m\n";

         cmdlineOutput << TEXT_RED <<
         std::setfill(' ') << std::setw(7)  << std::right << "id"             << ' ' <<
         std::setfill(' ') << std::setw(7)  << std::right << "copy no"        << ' ' <<
         std::setfill(' ') << std::setw(7)  << std::right << "vid"            << ' ' <<
         std::setfill(' ') << std::setw(7)  << std::right << "fseq"           << ' ' <<
         std::setfill(' ') << std::setw(8)  << std::right << "block id"       << ' ' <<
         std::setfill(' ') << std::setw(8)  << std::right << "instance"       << ' ' <<
         std::setfill(' ') << std::setw(7)  << std::right << "disk id"        << ' ' <<
         std::setfill(' ') << std::setw(12) << std::right << "size"           << ' ' <<
         std::setfill(' ') << std::setw(13) << std::right << "checksum type"  << ' ' <<
         std::setfill(' ') << std::setw(14) << std::right << "checksum value" << ' ' <<
         std::setfill(' ') << std::setw(13) << std::right << "storage class"  << ' ' <<
         std::setfill(' ') << std::setw(8)  << std::right << "owner"          << ' ' <<
         std::setfill(' ') << std::setw(8)  << std::right << "group"          << ' ' <<
         std::setfill(' ') << std::setw(13) << std::right << "creation time"  << ' ' <<
                                                             "path"
         << TEXT_NORMAL;
      }
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in       = getRequired(OptionString::INSTANCE);
   auto &scn      = getRequired(OptionString::STORAGE_CLASS);
   auto &cn       = getRequired(OptionUInt64::COPY_NUMBER);
   auto &tapepool = getRequired(OptionString::TAPE_POOL);
   auto &comment  = getRequired(OptionString::COMMENT);

   m_catalogue.createArchiveRoute(m_cliIdentity, in, scn, cn, tapepool, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in       = getRequired(OptionString::INSTANCE);
   auto &scn      = getRequired(OptionString::STORAGE_CLASS);
   auto &cn       = getRequired(OptionUInt64::COPY_NUMBER);
   auto  tapepool = getOptional(OptionString::TAPE_POOL);
   auto  comment  = getOptional(OptionString::COMMENT);

   if(comment) {
      m_catalogue.modifyArchiveRouteComment(m_cliIdentity, in, scn, cn, comment.value());
   }
   if(tapepool) {
      m_catalogue.modifyArchiveRouteTapePoolName(m_cliIdentity, in, scn, cn, tapepool.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in  = getRequired(OptionString::INSTANCE);
   auto &scn = getRequired(OptionString::STORAGE_CLASS);
   auto &cn  = getRequired(OptionUInt64::COPY_NUMBER);

   m_catalogue.deleteArchiveRoute(in, scn, cn);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::ArchiveRoute> list= m_catalogue.getArchiveRoutes();

   if(!list.empty()) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "instance","storage class","copy number","tapepool","c.user","c.host","c.time","m.user","m.host","m.time","comment"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->diskInstanceName);
         currentRow.push_back(it->storageClassName);
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->copyNb)));
         currentRow.push_back(it->tapePoolName);
         addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
         currentRow.push_back(it->comment);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Up(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::string cmdlineOutput = setDriveState('^' + getRequired(OptionString::DRIVE) + '$', Up);

   response.set_message_txt(cmdlineOutput);
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Down(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::string cmdlineOutput = setDriveState('^' + getRequired(OptionString::DRIVE) + '$', Down);

   response.set_message_txt(cmdlineOutput);
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   // Dump all drives unless we specified a drive
   bool hasRegex   = false;
   bool driveFound = false;

   auto driveRegexOpt = getOptional(OptionString::DRIVE, &hasRegex);
   std::string driveRegexStr = hasRegex ? '^' + driveRegexOpt.value() + '$' : ".";
   utils::Regex driveRegex(driveRegexStr.c_str());

   auto driveStates = m_scheduler.getDriveStates(m_cliIdentity, m_lc);
   if (!driveStates.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> headers = {
         "library","drive","host","desired","request","status","since","vid","tapepool","files",
         "MBytes","MB/s","session","age"
      };
      responseTable.push_back(headers);

      typedef decltype(*driveStates.begin()) dStateVal_t;
      driveStates.sort([](const dStateVal_t &a, const dStateVal_t &b){ return a.driveName < b.driveName; });

      for (auto ds: driveStates)
      {
         if(!driveRegex.has_match(ds.driveName)) continue;
         driveFound = true;

         std::vector<std::string> currentRow;
         currentRow.push_back(ds.logicalLibrary);
         currentRow.push_back(ds.driveName);
         currentRow.push_back(ds.host);
         currentRow.push_back(ds.desiredDriveState.up ? "Up" : "Down");
         currentRow.push_back(cta::common::dataStructures::toString(ds.mountType));
         currentRow.push_back(cta::common::dataStructures::toString(ds.driveStatus));

         // print the time spent in the current state
         unsigned long long drive_time = time(nullptr);

         switch(ds.driveStatus) {
            using namespace cta::common::dataStructures;

            case DriveStatus::Probing:           drive_time -= ds.probeStartTime;    break;
            case DriveStatus::Up:
            case DriveStatus::Down:              drive_time -= ds.downOrUpStartTime; break;
            case DriveStatus::Starting:          drive_time -= ds.startStartTime;    break;
            case DriveStatus::Mounting:          drive_time -= ds.mountStartTime;    break;
            case DriveStatus::Transferring:      drive_time -= ds.transferStartTime; break;
            case DriveStatus::CleaningUp:        drive_time -= ds.cleanupStartTime;  break;
            case DriveStatus::Unloading:         drive_time -= ds.unloadStartTime;   break;
            case DriveStatus::Unmounting:        drive_time -= ds.unmountStartTime;  break;
            case DriveStatus::DrainingToDisk:    drive_time -= ds.drainingStartTime; break;
            case DriveStatus::Shutdown:          drive_time -= ds.shutdownTime;      break;
            case DriveStatus::Unknown:           break;
         }
         currentRow.push_back(ds.driveStatus == cta::common::dataStructures::DriveStatus::Unknown ? "-" :
            std::to_string(drive_time));

         currentRow.push_back(ds.currentVid == "" ? "-" : ds.currentVid);
         currentRow.push_back(ds.currentTapePool == "" ? "-" : ds.currentTapePool);

         switch (ds.driveStatus) {
            case cta::common::dataStructures::DriveStatus::Transferring:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>(ds.filesTransferredInSession)));
               currentRow.push_back(bytesToMbString(ds.bytesTransferredInSession));
               currentRow.push_back(bytesToMbString(ds.latestBandwidth));
               break;
            default:
               currentRow.push_back("-");
               currentRow.push_back("-");
               currentRow.push_back("-");
         }
         switch(ds.driveStatus) {
            case cta::common::dataStructures::DriveStatus::Up:
            case cta::common::dataStructures::DriveStatus::Down:
            case cta::common::dataStructures::DriveStatus::Unknown:
               currentRow.push_back("-");
               break;
            default:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>(ds.sessionId)));
         }
         currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.lastUpdateTime))));
         responseTable.push_back(currentRow);
      }

      if (hasRegex && !driveFound) {
         throw cta::exception::UserError(std::string("No such drive: ") + driveRegexOpt.value());
      }

      m_option_bool[OptionBoolean::SHOW_HEADER] = true;
      cmdlineOutput<< formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
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



void RequestMessage::processGroupMountRule_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &mountpolicy = getRequired(OptionString::MOUNT_POLICY);
   auto &in          = getRequired(OptionString::INSTANCE);
   auto &name        = getRequired(OptionString::USERNAME);
   auto &comment     = getRequired(OptionString::COMMENT);

   m_catalogue.createRequesterGroupMountRule(m_cliIdentity, mountpolicy, in, name, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
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



void RequestMessage::processGroupMountRule_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in   = getRequired(OptionString::INSTANCE);
   auto &name = getRequired(OptionString::USERNAME);

   m_catalogue.deleteRequesterGroupMountRule(in, name);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::RequesterGroupMountRule> list = m_catalogue.getRequesterGroupMountRules();

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "instance","group","policy","c.user","c.host","c.time","m.user","m.host","m.time","comment"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++)
      {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->diskInstance);
         currentRow.push_back(it->name);
         currentRow.push_back(it->mountPolicy);
         addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
         currentRow.push_back(it->comment);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processListPendingArchives(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;
   std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> > result;

   auto tapepool = getOptional(OptionString::TAPE_POOL);

   if(tapepool) {
      std::list<cta::common::dataStructures::ArchiveJob> list = m_scheduler.getPendingArchiveJobs(tapepool.value(), m_lc);
      if(!list.empty()) result[tapepool.value()] = list;
   } else {
     result = m_scheduler.getPendingArchiveJobs(m_lc);
   }

   if(!result.empty())
   {
      if(has_flag(OptionBoolean::EXTENDED))
      {
         std::vector<std::vector<std::string>> responseTable;
         std::vector<std::string> header = {
            "tapepool","id","storage class","copy no.","disk id","instance","checksum type",
            "checksum value","size","user","group","path"
         };
         if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
         for(auto it = result.cbegin(); it != result.cend(); it++) {
            for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++)
            {
               std::vector<std::string> currentRow;
               currentRow.push_back(it->first);
               currentRow.push_back(std::to_string(static_cast<unsigned long long>(jt->archiveFileID)));
               currentRow.push_back(jt->request.storageClass);
               currentRow.push_back(std::to_string(static_cast<unsigned long long>(jt->copyNumber)));
               currentRow.push_back(jt->request.diskFileID);
               currentRow.push_back(jt->instanceName);
               currentRow.push_back(jt->request.checksumType);
               currentRow.push_back(jt->request.checksumValue);         
               currentRow.push_back(std::to_string(static_cast<unsigned long long>(jt->request.fileSize)));
               currentRow.push_back(jt->request.requester.name);
               currentRow.push_back(jt->request.requester.group);
               currentRow.push_back(jt->request.diskFileInfo.path);
               responseTable.push_back(currentRow);
            }
         }
         cmdlineOutput << formatResponse(responseTable);
      } else {
         std::vector<std::vector<std::string>> responseTable;
         std::vector<std::string> header = { "tapepool","total files","total size" };
         if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);
         for(auto it = result.cbegin(); it != result.cend(); it++) {
            std::vector<std::string> currentRow;
            currentRow.push_back(it->first);
            currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->second.size())));
            uint64_t size = 0;
            for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++) {
               size += jt->request.fileSize;
            }
            currentRow.push_back(std::to_string(static_cast<unsigned long long>(size)));
            responseTable.push_back(currentRow);
         }
         cmdlineOutput << formatResponse(responseTable);
      }
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processListPendingRetrieves(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > result;

   auto vid = getOptional(OptionString::VID);

   if(vid) {
      std::list<cta::common::dataStructures::RetrieveJob> list = m_scheduler.getPendingRetrieveJobs(vid.value(), m_lc);
      if(!list.empty()) result[vid.value()] = list;
   } else {
      result = m_scheduler.getPendingRetrieveJobs(m_lc);
   }

   if(!result.empty())
   {
      std::vector<std::vector<std::string>> responseTable;

      if(has_flag(OptionBoolean::EXTENDED)) {
         std::vector<std::string> header = {"vid","id","copy no.","fseq","block id","size","user","group","path"};
         if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
         for(auto it = result.cbegin(); it != result.cend(); it++)
         {
            for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++)
            {
               std::vector<std::string> currentRow;
               currentRow.push_back(it->first);
               currentRow.push_back(std::to_string(static_cast<unsigned long long>(jt->request.archiveFileID)));
               cta::common::dataStructures::ArchiveFile file = m_catalogue.getArchiveFileById(jt->request.archiveFileID);
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((jt->tapeCopies.at(it->first).first))));
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((jt->tapeCopies.at(it->first).second.fSeq))));
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((jt->tapeCopies.at(it->first).second.blockId))));
               currentRow.push_back(std::to_string(static_cast<unsigned long long>(file.fileSize)));
               currentRow.push_back(jt->request.requester.name);
               currentRow.push_back(jt->request.requester.group);
               currentRow.push_back(jt->request.diskFileInfo.path);
               responseTable.push_back(currentRow);
            }
         }
      } else {
         std::vector<std::string> header = {"vid","total files","total size"};
         if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
         for(auto it = result.cbegin(); it != result.cend(); it++)
         {
            std::vector<std::string> currentRow;
            currentRow.push_back(it->first);
            currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->second.size())));
            uint64_t size = 0;
            for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++)
            {
               cta::common::dataStructures::ArchiveFile file = m_catalogue.getArchiveFileById(jt->request.archiveFileID);
               size += file.fileSize;
            }
            currentRow.push_back(std::to_string(static_cast<unsigned long long>(size)));
            responseTable.push_back(currentRow);
         }
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name    = getRequired(OptionString::LOGICAL_LIBRARY);
   auto &comment = getRequired(OptionString::COMMENT);

   m_catalogue.createLogicalLibrary(m_cliIdentity, name, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name    = getRequired(OptionString::LOGICAL_LIBRARY);
   auto &comment = getRequired(OptionString::COMMENT);

   m_catalogue.modifyLogicalLibraryComment(m_cliIdentity, name, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name = getRequired(OptionString::LOGICAL_LIBRARY);

   m_catalogue.deleteLogicalLibrary(name);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::LogicalLibrary> list = m_catalogue.getLogicalLibraries();

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "name","c.user","c.host","c.time","m.user","m.host","m.time","comment"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->name);
         addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
         currentRow.push_back(it->comment);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
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



void RequestMessage::processMountPolicy_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
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



void RequestMessage::processMountPolicy_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &group = getRequired(OptionString::MOUNT_POLICY);

   m_catalogue.deleteMountPolicy(group);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::MountPolicy> list= m_catalogue.getMountPolicies();

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "mount policy","a.priority","a.minAge","r.priority","r.minAge","max drives","c.user","c.host","c.time","m.user","m.host","m.time","comment"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++)
      {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->name);
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->archivePriority)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->archiveMinRequestAge)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->retrievePriority)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->retrieveMinRequestAge)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->maxDrivesAllowed)));
         addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
         currentRow.push_back(it->comment);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   // VIDs can be provided as a single option or as a list
   std::vector<std::string> vid_list;

   auto vidl = getOptional(OptionStrList::VID);
   if(vidl) vid_list = vidl.value();
   auto vid = getOptional(OptionString::VID);
   if(vid) vid_list.push_back(vid.value());

   if(vid_list.empty()) {
      throw cta::exception::UserError("Must specify at least one vid, using --vid or --vidfile options");
   }

   // Expand, repack, or both ?
   cta::common::dataStructures::RepackType type;

   if(has_flag(OptionBoolean::JUSTEXPAND) && has_flag(OptionBoolean::JUSTREPACK)) {
      throw cta::exception::UserError("--justexpand and --justrepack are mutually exclusive");
   } else if(has_flag(OptionBoolean::JUSTEXPAND)) {
      type = cta::common::dataStructures::RepackType::justexpand;
   } else if(has_flag(OptionBoolean::JUSTREPACK)) {
      type = cta::common::dataStructures::RepackType::justrepack;
   } else {
      type = cta::common::dataStructures::RepackType::expandandrepack;
   }

   // Process each item in the list
   for(auto it = vid_list.begin(); it != vid_list.end(); ++it) {
      m_scheduler.queueRepack(m_cliIdentity, *it, type);
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = getRequired(OptionString::VID);

   m_scheduler.cancelRepack(m_cliIdentity, vid);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   auto vid = getOptional(OptionString::VID);

   std::list<cta::common::dataStructures::RepackInfo> list;

   if(!vid) {      
      list = m_scheduler.getRepacks(m_cliIdentity);
   } else {
      list.push_back(m_scheduler.getRepack(m_cliIdentity, vid.value()));
   }

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "vid","files","size","type","to retrieve","to archive","failed","archived","status","name","host","time"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++)
      {
         const std::map<common::dataStructures::RepackType, std::string> type_s = {
            { common::dataStructures::RepackType::expandandrepack, "expandandrepack" },
            { common::dataStructures::RepackType::justexpand,      "justexpand" },
            { common::dataStructures::RepackType::justrepack,      "justrepack" }
         };

         std::vector<std::string> currentRow;
         currentRow.push_back(it->vid);
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->totalFiles)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->totalSize)));
         currentRow.push_back(type_s.at(it->repackType));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->filesToRetrieve)));//change names
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->filesToArchive)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->filesFailed)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->filesArchived)));
         currentRow.push_back(it->repackStatus);
         currentRow.push_back(it->creationLog.username);
         currentRow.push_back(it->creationLog.host);        
         currentRow.push_back(timeToString(it->creationLog.time));
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Err(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   auto &vid = getRequired(OptionString::VID);

   cta::common::dataStructures::RepackInfo info = m_scheduler.getRepack(m_cliIdentity, vid);

   if(!info.errors.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = { "fseq","error message" };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = info.errors.cbegin(); it != info.errors.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->first)));
         currentRow.push_back(it->second);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &mountpolicy = getRequired(OptionString::MOUNT_POLICY);
   auto &in          = getRequired(OptionString::INSTANCE);
   auto &name        = getRequired(OptionString::USERNAME);
   auto &comment     = getRequired(OptionString::COMMENT);

   m_catalogue.createRequesterMountRule(m_cliIdentity, mountpolicy, in, name, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
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



void RequestMessage::processRequesterMountRule_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in   = getRequired(OptionString::INSTANCE);
   auto &name = getRequired(OptionString::USERNAME);

   m_catalogue.deleteRequesterMountRule(in, name);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::RequesterMountRule> list = m_catalogue.getRequesterMountRules();

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "instance","username","policy","c.user","c.host","c.time","m.user","m.host","m.time","comment"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->diskInstance);
         currentRow.push_back(it->name);
         currentRow.push_back(it->mountPolicy);
         addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
         currentRow.push_back(it->comment);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processShrink(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &tapepool = getRequired(OptionString::TAPE_POOL);

   m_scheduler.shrink(m_cliIdentity, tapepool);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processShowQueues(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   auto queuesAndMounts = m_scheduler.getQueuesAndMountSummaries(m_lc);

   if(!queuesAndMounts.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "type","tapepool","logical library","vid","files queued","MBytes queued","oldest age","priority",
         "min age","max drives","cur. mounts","cur. files","cur. MBytes","MB/s","next mounts","tapes capacity",
         "files on tapes","MBytes on tapes","full tapes","empty tapes","disabled tapes","writables tapes"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);
      for (auto & q: queuesAndMounts)
      {
         const uint64_t MBytes = 1000 * 1000;

         std::vector<std::string> currentRow;
         currentRow.push_back(common::dataStructures::toString(q.mountType));
         currentRow.push_back(q.tapePool);
         currentRow.push_back(q.logicalLibrary);
         currentRow.push_back(q.vid);
         currentRow.push_back(std::to_string(q.filesQueued));
         currentRow.push_back(bytesToMbString(q.bytesQueued));
         currentRow.push_back(std::to_string(q.oldestJobAge));
         if (common::dataStructures::MountType::Archive == q.mountType) {
            currentRow.push_back(std::to_string(q.mountPolicy.archivePriority));
            currentRow.push_back(std::to_string(q.mountPolicy.archiveMinRequestAge));
            currentRow.push_back(std::to_string(q.mountPolicy.maxDrivesAllowed));
         } else if (common::dataStructures::MountType::Retrieve == q.mountType) {
            currentRow.push_back(std::to_string(q.mountPolicy.retrievePriority));
            currentRow.push_back(std::to_string(q.mountPolicy.retrieveMinRequestAge));
            currentRow.push_back(std::to_string(q.mountPolicy.maxDrivesAllowed));
         } else {
            currentRow.push_back("-");
            currentRow.push_back("-");
            currentRow.push_back("-");
         }
         currentRow.push_back(std::to_string(q.currentMounts));
         currentRow.push_back(std::to_string(q.currentFiles));
         currentRow.push_back(bytesToMbString(q.currentBytes));
         currentRow.push_back(bytesToMbString(q.latestBandwidth));
         currentRow.push_back(std::to_string(q.nextMounts));
         currentRow.push_back(std::to_string(q.tapesCapacity/MBytes));
         currentRow.push_back(std::to_string(q.filesOnTapes));
         currentRow.push_back(bytesToMbString(q.dataOnTapes));
         currentRow.push_back(std::to_string(q.fullTapes));
         currentRow.push_back(std::to_string(q.emptyTapes));
         currentRow.push_back(std::to_string(q.disabledTapes));
         currentRow.push_back(std::to_string(q.writableTapes));
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   common::dataStructures::StorageClass storageClass;

   storageClass.diskInstance = getRequired(OptionString::INSTANCE);
   storageClass.name         = getRequired(OptionString::STORAGE_CLASS);
   storageClass.nbCopies     = getRequired(OptionUInt64::COPY_NUMBER);
   storageClass.comment      = getRequired(OptionString::COMMENT);

   m_catalogue.createStorageClass(m_cliIdentity, storageClass);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in      = getRequired(OptionString::INSTANCE);
   auto &scn     = getRequired(OptionString::STORAGE_CLASS);
   auto  comment = getOptional(OptionString::COMMENT);
   auto  cn      = getOptional(OptionUInt64::COPY_NUMBER);

   if(comment) {
      m_catalogue.modifyStorageClassComment(m_cliIdentity, in, scn, comment.value());
   }
   if(cn) {
      m_catalogue.modifyStorageClassNbCopies(m_cliIdentity, in, scn, cn.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in  = getRequired(OptionString::INSTANCE);
   auto &scn = getRequired(OptionString::STORAGE_CLASS);

   m_catalogue.deleteStorageClass(in, scn);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::StorageClass> list = m_catalogue.getStorageClasses();

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "instance","storage class","number of copies","c.user","c.host","c.time","m.user","m.host","m.time","comment"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->diskInstance);
         currentRow.push_back(it->name);
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->nbCopies)));
         addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
         currentRow.push_back(it->comment);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid            = getRequired(OptionString::VID);
   auto &logicallibrary = getRequired(OptionString::LOGICAL_LIBRARY);
   auto &tapepool       = getRequired(OptionString::TAPE_POOL);
   auto &capacity       = getRequired(OptionUInt64::CAPACITY);
   auto &disabled       = getRequired(OptionBoolean::DISABLED);
   auto &full           = getRequired(OptionBoolean::FULL);
   auto  comment        = getOptional(OptionString::COMMENT);

   m_catalogue.createTape(m_cliIdentity, vid, logicallibrary, tapepool, capacity, disabled, full, comment ? comment.value() : "-");

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid            = getRequired(OptionString::VID);
   auto  logicallibrary = getOptional(OptionString::LOGICAL_LIBRARY);
   auto  tapepool       = getOptional(OptionString::TAPE_POOL);
   auto  capacity       = getOptional(OptionUInt64::CAPACITY);
   auto  comment        = getOptional(OptionString::COMMENT);
   auto  encryptionkey  = getOptional(OptionString::ENCRYPTION_KEY);
   auto  disabled       = getOptional(OptionBoolean::DISABLED);
   auto  full           = getOptional(OptionBoolean::FULL);

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
   if(encryptionkey) {
      m_catalogue.modifyTapeEncryptionKey(m_cliIdentity, vid, encryptionkey.value());
   }
   if(disabled) {
      m_catalogue.setTapeDisabled(m_cliIdentity, vid, disabled.value());
   }
   if(full) {
      m_catalogue.setTapeFull(m_cliIdentity, vid, full.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = getRequired(OptionString::VID);

   m_catalogue.deleteTape(vid);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Reclaim(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = getRequired(OptionString::VID);

   m_catalogue.reclaimTape(m_cliIdentity, vid);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   cta::catalogue::TapeSearchCriteria searchCriteria;

   if(!has_flag(OptionBoolean::ALL))
   {
      bool has_any = false; // set to true if at least one optional option is set

      // Get the search criteria from the optional options

      searchCriteria.disabled        = getOptional(OptionBoolean::DISABLED,       &has_any);
      searchCriteria.full            = getOptional(OptionBoolean::FULL,           &has_any);
      searchCriteria.lbp             = getOptional(OptionBoolean::LBP,            &has_any);
      searchCriteria.capacityInBytes = getOptional(OptionUInt64::CAPACITY,        &has_any);
      searchCriteria.logicalLibrary  = getOptional(OptionString::LOGICAL_LIBRARY, &has_any);
      searchCriteria.tapePool        = getOptional(OptionString::TAPE_POOL,       &has_any);
      searchCriteria.vid             = getOptional(OptionString::VID,             &has_any);

      if(!has_any) {
         throw cta::exception::UserError("Must specify at least one search option, or --all");
      }
   }

   std::list<cta::common::dataStructures::Tape> list= m_catalogue.getTapes(searchCriteria);

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "vid","logical library","tapepool","encription key","capacity","occupancy","last fseq",
         "full","disabled","lpb","label drive","label time","last w drive","last w time",
         "last r drive","last r time","c.user","c.host","c.time","m.user","m.host","m.time","comment"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->vid);
         currentRow.push_back(it->logicalLibraryName);
         currentRow.push_back(it->tapePoolName);
         currentRow.push_back((bool)it->encryptionKey ? it->encryptionKey.value() : "-");
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->capacityInBytes)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->dataOnTapeInBytes)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->lastFSeq)));
         if(it->full) currentRow.push_back("true"); else currentRow.push_back("false");
         if(it->disabled) currentRow.push_back("true"); else currentRow.push_back("false");
         if(it->lbp) currentRow.push_back("true"); else currentRow.push_back("false");

         if(it->labelLog) {
            currentRow.push_back(it->labelLog.value().drive);
            currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->labelLog.value().time)));
         } else {
            currentRow.push_back("-");
            currentRow.push_back("-");
         }

         if(it->lastWriteLog) {
            currentRow.push_back(it->lastWriteLog.value().drive);
            currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->lastWriteLog.value().time)));
         } else {
            currentRow.push_back("-");
            currentRow.push_back("-");
         }

         if(it->lastReadLog) {
            currentRow.push_back(it->lastReadLog.value().drive);
            currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->lastReadLog.value().time)));
         } else {
            currentRow.push_back("-");
            currentRow.push_back("-");
         }

         addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
         currentRow.push_back(it->comment);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Label(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid   = getRequired(OptionString::VID);
   auto  force = getOptional(OptionBoolean::FORCE);
   auto  lbp   = getOptional(OptionBoolean::LBP);

   m_scheduler.queueLabel(m_cliIdentity, vid,
                          force ? force.value() : false,
                          lbp ? lbp.value() : true);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name      = getRequired(OptionString::TAPE_POOL);
   auto &ptn       = getRequired(OptionUInt64::PARTIAL_TAPES_NUMBER);
   auto &comment   = getRequired(OptionString::COMMENT);
   auto &encrypted = getRequired(OptionBoolean::ENCRYPTED);

   m_catalogue.createTapePool(m_cliIdentity, name, ptn, encrypted, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name      = getRequired(OptionString::TAPE_POOL);
   auto  ptn       = getOptional(OptionUInt64::PARTIAL_TAPES_NUMBER);
   auto  comment   = getOptional(OptionString::COMMENT);
   auto  encrypted = getOptional(OptionBoolean::ENCRYPTED);

   if(comment) {
      m_catalogue.modifyTapePoolComment(m_cliIdentity, name, comment.value());
   }
   if(ptn) {
      m_catalogue.modifyTapePoolNbPartialTapes(m_cliIdentity, name, ptn.value());
   }
   if(encrypted) {
      m_catalogue.setTapePoolEncryption(m_cliIdentity, name, encrypted.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name = getRequired(OptionString::TAPE_POOL);

   m_catalogue.deleteTapePool(name);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   const std::list<cta::catalogue::TapePool> list= m_catalogue.getTapePools();

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "name","# partial tapes","encrypt","c.user","c.host","c.time","m.user","m.host","m.time","comment"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->name);
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->nbPartialTapes)));
         if(it->encryption) currentRow.push_back("true"); else currentRow.push_back("false");
         addLogInfoToResponseRow(currentRow, it->creationLog, it->lastModificationLog);
         currentRow.push_back(it->comment);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTest_Read(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   auto &drive     = getRequired(OptionString::DRIVE);
   auto &vid       = getRequired(OptionString::VID);
   auto &output    = getRequired(OptionString::OUTPUT);
   auto &firstfseq = getRequired(OptionUInt64::FIRST_FSEQ);
   auto &lastfseq  = getRequired(OptionUInt64::LAST_FSEQ);

   bool checkchecksum = has_flag(OptionBoolean::CHECK_CHECKSUM);

   cta::common::dataStructures::ReadTestResult res = m_scheduler.readTest(m_cliIdentity, drive, vid,
      firstfseq, lastfseq, checkchecksum, output);

   std::vector<std::vector<std::string>> responseTable;
   std::vector<std::string> header = { "fseq","checksum type","checksum value","error" };
   responseTable.push_back(header);

   for(auto it = res.checksums.cbegin(); it != res.checksums.cend(); it++)
   {
      std::vector<std::string> currentRow;
      currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->first)));
      currentRow.push_back(it->second.first);
      currentRow.push_back(it->second.second);
      if(res.errors.find(it->first) != res.errors.cend()) {
        currentRow.push_back(res.errors.at(it->first));
      } else {
        currentRow.push_back("-");
      }
      responseTable.push_back(currentRow);
   }

   m_option_bool[OptionBoolean::SHOW_HEADER] = true;
   cmdlineOutput << formatResponse(responseTable) << std::endl
                 <<  "Drive: "      << res.driveName
                 << " Vid: "        << res.vid
                 << " #Files: "     << res.totalFilesRead
                 << " #Bytes: "     << res.totalBytesRead
                 << " Time: "       << res.totalTimeInSeconds << " s"
                 << " Speed(avg): " << static_cast<long double>(res.totalBytesRead) /
                                       static_cast<long double>(res.totalTimeInSeconds) << " B/s"
                 << std::endl;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTest_Write(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   auto &drive = getRequired(OptionString::DRIVE);
   auto &vid   = getRequired(OptionString::VID);
   auto &file  = getRequired(OptionString::FILENAME);

   cta::common::dataStructures::WriteTestResult res = m_scheduler.writeTest(m_cliIdentity, drive, vid, file);

   std::vector<std::vector<std::string>> responseTable;
   std::vector<std::string> header = { "fseq","checksum type","checksum value","error" };
   responseTable.push_back(header);

   for(auto it = res.checksums.cbegin(); it != res.checksums.cend(); it++)
   {
      std::vector<std::string> currentRow;
      currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->first)));
      currentRow.push_back(it->second.first);
      currentRow.push_back(it->second.second);
      if(res.errors.find(it->first) != res.errors.cend()) {
        currentRow.push_back(res.errors.at(it->first));
      } else {
        currentRow.push_back("-");
      }
      responseTable.push_back(currentRow);
   }

   m_option_bool[OptionBoolean::SHOW_HEADER] = true;
   cmdlineOutput << formatResponse(responseTable) << std::endl
                 <<  "Drive: "      << res.driveName
                 << " Vid: "        << res.vid
                 << " #Files: "     << res.totalFilesWritten
                 << " #Bytes: "     << res.totalBytesWritten
                 << " Time: "       << res.totalTimeInSeconds << " s"
                 << " Speed(avg): " << static_cast<long double>(res.totalBytesWritten) /
                                       static_cast<long double>(res.totalTimeInSeconds) << " B/s"
                 << std::endl;

   response.set_message_txt(cmdlineOutput.str());

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTest_WriteAuto(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   auto &drive  = getRequired(OptionString::DRIVE);
   auto &vid    = getRequired(OptionString::VID);
   auto &number = getRequired(OptionUInt64::NUMBER_OF_FILES);
   auto &size   = getRequired(OptionUInt64::FILE_SIZE);
   auto &input  = getRequired(OptionString::INPUT);

   cta::common::dataStructures::TestSourceType type;

   if(input == "zero") {
      type = cta::common::dataStructures::TestSourceType::devzero;
   } else if(input == "urandom") {
      type = cta::common::dataStructures::TestSourceType::devurandom;
   } else {
      throw cta::exception::UserError("--input value must be either \"zero\" or \"urandom\"");
   }

   cta::common::dataStructures::WriteTestResult res = m_scheduler.write_autoTest(m_cliIdentity, drive,
      vid, number, size, type);

   std::vector<std::vector<std::string>> responseTable;
   std::vector<std::string> header = { "fseq","checksum type","checksum value","error" };
   responseTable.push_back(header);
   for(auto it = res.checksums.cbegin(); it != res.checksums.cend(); it++)
   {
      std::vector<std::string> currentRow;
      currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->first)));
      currentRow.push_back(it->second.first);
      currentRow.push_back(it->second.second);
      if(res.errors.find(it->first) != res.errors.cend()) {
         currentRow.push_back(res.errors.at(it->first));
      } else {
         currentRow.push_back("-");
      }
      responseTable.push_back(currentRow);
   }

   m_option_bool[OptionBoolean::SHOW_HEADER] = true;
   cmdlineOutput << formatResponse(responseTable) << std::endl
                 <<  "Drive: "      << res.driveName
                 << " Vid: "        << res.vid
                 << " #Files: "     << res.totalFilesWritten
                 << " #Bytes: "     << res.totalBytesWritten
                 << " Time: "       << res.totalTimeInSeconds << " s"
                 << " Speed(avg): " << static_cast<long double>(res.totalBytesWritten) /
                                       static_cast<long double>(res.totalTimeInSeconds) << " B/s"
                 << std::endl;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processVerify_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid    = getRequired(OptionString::VID);
   auto  number = getOptional(OptionUInt64::NUMBER_OF_FILES);

   m_scheduler.queueVerify(m_cliIdentity, vid, number);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processVerify_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = getRequired(OptionString::VID);

   m_scheduler.cancelVerify(m_cliIdentity, vid);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processVerify_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   auto vid = getOptional(OptionString::VID);

   std::list<cta::common::dataStructures::VerifyInfo> list;

   if(vid) {
      list.push_back(m_scheduler.getVerify(m_cliIdentity, vid.value()));
   } else {
      list = m_scheduler.getVerifys(m_cliIdentity);
   }

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "vid","files","size","to verify","failed","verified","status","name","host","time"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(it->vid);
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->totalFiles)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->totalSize)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->filesToVerify)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->filesFailed)));
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->filesVerified)));
         currentRow.push_back(it->verifyStatus);
         currentRow.push_back(it->creationLog.username);
         currentRow.push_back(it->creationLog.host);       
         currentRow.push_back(timeToString(it->creationLog.time));
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processVerify_Err(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   auto &vid = getRequired(OptionString::VID);

   cta::common::dataStructures::VerifyInfo info = m_scheduler.getVerify(m_cliIdentity, vid);

   if(!info.errors.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = { "fseq","error message" };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = info.errors.cbegin(); it != info.errors.cend(); it++) {
         std::vector<std::string> currentRow;
         currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->first)));
         currentRow.push_back(it->second);
         responseTable.push_back(currentRow);
      }
      cmdlineOutput << formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
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



std::string RequestMessage::formatResponse(const std::vector<std::vector<std::string>> &responseTable) const
{
   bool has_header = has_flag(cta::admin::OptionBoolean::SHOW_HEADER);

   if(responseTable.empty() || responseTable.at(0).empty()) return "";

   std::vector<int> columnSizes;

   for(uint j = 0; j < responseTable.at(0).size(); j++) { //for each column j
      uint columnSize = 0;
      for(uint i = 0; i<responseTable.size(); i++) { //for each row i
         if(responseTable.at(i).at(j).size() > columnSize) {
            columnSize = responseTable.at(i).at(j).size();
         }
      }
      columnSizes.push_back(columnSize);//loops here
   }
   std::stringstream responseSS;
   for(auto row = responseTable.cbegin(); row != responseTable.cend(); row++) {
      if(has_header && row == responseTable.cbegin()) responseSS << "\x1b[31;1m";
      for(uint i = 0; i<row->size(); i++) {
         responseSS << std::string(i ? "  " : "") << std::setw(columnSizes.at(i)) << row->at(i);
      }
      if(has_header && row == responseTable.cbegin()) responseSS << "\x1b[0m" << std::endl;
      else responseSS << std::endl;
   }
   return responseSS.str();
}



void RequestMessage::addLogInfoToResponseRow(std::vector<std::string> &responseRow,
                                             const cta::common::dataStructures::EntryLog &creationLog,
                                             const cta::common::dataStructures::EntryLog &lastModificationLog) const
{
   responseRow.push_back(creationLog.username);
   responseRow.push_back(creationLog.host);
   responseRow.push_back(timeToString(creationLog.time));
   responseRow.push_back(lastModificationLog.username);
   responseRow.push_back(lastModificationLog.host);
   responseRow.push_back(timeToString(lastModificationLog.time));
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
