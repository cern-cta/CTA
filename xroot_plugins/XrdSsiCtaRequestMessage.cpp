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

#include <XrdSsiPbException.hpp>
using XrdSsiPb::PbException;

#include <cmdline/CtaAdminCmdParse.hpp>
#include "XrdSsiCtaRequestMessage.hpp"



namespace cta { namespace xrd {

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
 * Helper function to convert bytes to Mb and output as a string
 */
static std::string bytesToMbString(uint64_t bytes)
{
   long double mBytes = static_cast<long double>(bytes)/(1000.0*1000.0);

   std::ostringstream oss;
   oss << std::setprecision(2) << std::fixed << mBytes;
   return oss.str();
}



void RequestMessage::process(const cta::xrd::Request &request, cta::xrd::Response &response)
{
   // Branch on the Request payload type

   switch(request.request_case())
   {
      using namespace cta::xrd;

      case Request::kAdmincmd:
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
               processArchiveFile_Ls(request.admincmd(), response);
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
         }
         break;

      case Request::kNotification:
         // Map the Workflow Event to a method
         switch(request.notification().wf().event()) {
            using namespace cta::eos;

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

void RequestMessage::processCLOSEW(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Unpack message

   cta::common::dataStructures::UserIdentity originator;
   originator.name          = notification.cli().user().username();
   originator.group         = notification.cli().user().groupname();

   cta::common::dataStructures::DiskFileInfo diskFileInfo;
   diskFileInfo.owner       = notification.file().owner().username();
   diskFileInfo.group       = notification.file().owner().groupname();
   diskFileInfo.path        = notification.file().lpath();

   cta::common::dataStructures::ArchiveRequest request;
   request.checksumType     = notification.file().cks().name();
   request.checksumValue    = notification.file().cks().value();
   request.diskFileInfo     = diskFileInfo;
   request.diskFileID       = notification.file().fid();
   request.fileSize         = notification.file().size();
   request.requester        = originator;
   request.srcURL           = notification.wf().instance().url();
   request.storageClass     = notification.file().xattr().at("CTA_StorageClass");
   request.archiveReportURL = notification.transport().report_url();

   // Queue the request

   uint64_t archiveFileId = m_scheduler.queueArchive(m_cliIdentity.username, request, m_lc);

   // Set archiveFileId in response (deprecated)

   std::string result_str = "<eos::wfe::path::fxattr:sys.archiveFileId>" + std::to_string(archiveFileId);
#ifdef XRDSSI_DEBUG
   std::cerr << result_str << std::endl;
#endif
   response.set_message_txt(result_str);

   // Set response type

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processPREPARE(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Unpack message

   cta::common::dataStructures::UserIdentity originator;
   originator.name          = notification.cli().user().username();
   originator.group         = notification.cli().user().groupname();

   cta::common::dataStructures::DiskFileInfo diskFileInfo;
   diskFileInfo.owner       = notification.file().owner().username();
   diskFileInfo.group       = notification.file().owner().groupname();
   diskFileInfo.path        = notification.file().lpath();

   cta::common::dataStructures::RetrieveRequest request;
   request.requester        = originator;
   request.dstURL           = notification.transport().dst_url();
   request.diskFileInfo     = diskFileInfo;

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
   // must be converted to a valid uint64_t

   std::string archiveFileIdStr = notification.file().xattr().at("CTA_ArchiveFileId");
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   // Queue the request

   m_scheduler.queueRetrieve(m_cliIdentity.username, request, m_lc);

   // Set response type

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDELETE(const cta::eos::Notification &notification, cta::xrd::Response &response)
{
   // Unpack message

   cta::common::dataStructures::UserIdentity originator;
   originator.name          = notification.cli().user().username();
   originator.group         = notification.cli().user().groupname();

   cta::common::dataStructures::DiskFileInfo diskFileInfo;
   diskFileInfo.owner       = notification.file().owner().username();
   diskFileInfo.group       = notification.file().owner().groupname();
   diskFileInfo.path        = notification.file().lpath();

   cta::common::dataStructures::DeleteArchiveRequest request;
   request.requester        = originator;

   // CTA Archive ID is an EOS extended attribute, i.e. it is stored as a string, which
   // must be converted to a valid uint64_t

   std::string archiveFileIdStr = notification.file().xattr().at("CTA_ArchiveFileId");
   if((request.archiveFileID = strtoul(archiveFileIdStr.c_str(), nullptr, 10)) == 0)
   {
      throw PbException("Invalid archiveFileID " + archiveFileIdStr);
   }

   // Queue the request

   cta::utils::Timer t;
   m_scheduler.deleteArchive(m_cliIdentity.username, request, m_lc);

   // Create a log entry

   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", request.archiveFileID).add("catalogueTime", t.secs());
   m_lc.log(cta::log::INFO, "In processDELETE(): deleted archive file.");

   // Set response type

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



// Admin commands

void RequestMessage::processAdmin_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = m_option_str.at(OptionString::USERNAME);
   auto &comment  = m_option_str.at(OptionString::COMMENT);

   m_catalogue.createAdminUser(m_cliIdentity, username, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = m_option_str.at(OptionString::USERNAME);
   auto &comment  = m_option_str.at(OptionString::COMMENT);

   m_catalogue.modifyAdminUserComment(m_cliIdentity, username, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = m_option_str.at(OptionString::USERNAME);

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

   auto &hostname = m_option_str.at(OptionString::HOSTNAME);
   auto &comment  = m_option_str.at(OptionString::COMMENT);

   m_catalogue.createAdminHost(m_cliIdentity, hostname, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdminHost_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &hostname = m_option_str.at(OptionString::HOSTNAME);
   auto &comment  = m_option_str.at(OptionString::COMMENT);

   m_catalogue.modifyAdminHostComment(m_cliIdentity, hostname, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdminHost_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &hostname = m_option_str.at(OptionString::HOSTNAME);

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



void RequestMessage::processArchiveFile_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;
   cta::catalogue::TapeFileSearchCriteria searchCriteria;

   if(!has_flag(OptionBoolean::ALL))
   {
      bool has_any = false; // set to true if at least one optional option is set

      // Get the search criteria from the optional options

      searchCriteria.archiveFileId  = getOptional(OptionUInt64::ARCHIVE_FILE_ID, m_option_uint64, &has_any);
      searchCriteria.tapeFileCopyNb = getOptional(OptionUInt64::COPY_NUMBER,     m_option_uint64, &has_any);
      searchCriteria.diskFileId     = getOptional(OptionString::DISKID,          m_option_str,    &has_any);
      searchCriteria.vid            = getOptional(OptionString::VID,             m_option_str,    &has_any);
      searchCriteria.tapePool       = getOptional(OptionString::TAPE_POOL,       m_option_str,    &has_any);
      searchCriteria.diskFileUser   = getOptional(OptionString::OWNER,           m_option_str,    &has_any);
      searchCriteria.diskFileGroup  = getOptional(OptionString::GROUP,           m_option_str,    &has_any);
      searchCriteria.storageClass   = getOptional(OptionString::STORAGE_CLASS,   m_option_str,    &has_any);
      searchCriteria.diskFilePath   = getOptional(OptionString::PATH,            m_option_str,    &has_any);
      searchCriteria.diskInstance   = getOptional(OptionString::INSTANCE,        m_option_str,    &has_any);

      if(!has_any) {
         throw cta::exception::UserError("Must specify at least one search option, or --all");
      }
   }

   bool has_header = has_flag(OptionBoolean::SHOW_HEADER);

   if(has_flag(OptionBoolean::SUMMARY)) {
      cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue.getTapeFileSummary(searchCriteria);
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = { "total number of files","total size" };
      std::vector<std::string> row = {std::to_string(static_cast<unsigned long long>(summary.totalFiles)),
                                      std::to_string(static_cast<unsigned long long>(summary.totalBytes))};
      if(has_header) responseTable.push_back(header);
      responseTable.push_back(row);
      cmdlineOutput << formatResponse(responseTable);
   } else {
      auto archiveFileItor = m_catalogue.getArchiveFiles(searchCriteria);
#if 0
      // TO DO: Implement list archive files
      m_listArchiveFilesCmd.reset(new xrootPlugins::ListArchiveFilesCmd(m_log, error, has_header, std::move(archiveFileItor)));
#endif
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in       = m_option_str.at(OptionString::INSTANCE);
   auto &scn      = m_option_str.at(OptionString::STORAGE_CLASS);
   auto &cn       = m_option_uint64.at(OptionUInt64::COPY_NUMBER);
   auto &tapepool = m_option_str.at(OptionString::TAPE_POOL);
   auto &comment  = m_option_str.at(OptionString::COMMENT);

   m_catalogue.createArchiveRoute(m_cliIdentity, in, scn, cn, tapepool, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in  = m_option_str.at(OptionString::INSTANCE);
   auto &scn = m_option_str.at(OptionString::STORAGE_CLASS);
   auto &cn  = m_option_uint64.at(OptionUInt64::COPY_NUMBER);

   auto tapepool = getOptional(OptionString::TAPE_POOL, m_option_str);
   auto comment  = getOptional(OptionString::COMMENT, m_option_str);

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

   auto &in  = m_option_str.at(OptionString::INSTANCE);
   auto &scn = m_option_str.at(OptionString::STORAGE_CLASS);
   auto &cn  = m_option_uint64.at(OptionUInt64::COPY_NUMBER);

   m_catalogue.deleteArchiveRoute(in, scn, cn);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::ArchiveRoute> list= m_catalogue.getArchiveRoutes();

   if(list.size() > 0) {
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

   std::stringstream cmdlineOutput;

#if 0
   auto &drive = m_option_str.at(OptionString::DRIVE);

   changeStateForDrivesByRegex(drive, m_lc, cmdlineOutput, true, false);
#endif

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Down(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

#if 0
   auto &drive = m_option_str.at(OptionString::DRIVE);

   changeStateForDrivesByRegex(drive, m_lc, cmdlineOutput, false, has_flag(OptionBoolean::FORCE));
#endif

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   // Dump all drives unless we specified a drive
   bool singleDrive = false;
   bool driveFound = false;

   auto drive = getOptional(OptionString::DRIVE, m_option_str, &singleDrive);
   auto driveStates = m_scheduler.getDriveStates(m_cliIdentity, m_lc);

   if (!driveStates.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> headers = {
         "library","drive","host","desired","request","status","since","vid","tapepool","files",
         "MBytes","MB/s","session","age"
      };
      responseTable.push_back(headers);
      m_option_bool[OptionBoolean::SHOW_HEADER] = true;

      typedef decltype(*driveStates.begin()) dStateVal_t;
      driveStates.sort([](const dStateVal_t &a, const dStateVal_t &b){ return a.driveName < b.driveName; });

      for (auto ds: driveStates)
      {
         if(singleDrive && drive.value() != ds.driveName) continue;
         driveFound = true;

         std::vector<std::string> currentRow;
         currentRow.push_back(ds.logicalLibrary);
         currentRow.push_back(ds.driveName);
         currentRow.push_back(ds.host);
         currentRow.push_back(ds.desiredDriveState.up ? "Up" : "Down");
         currentRow.push_back(cta::common::dataStructures::toString(ds.mountType));
         currentRow.push_back(cta::common::dataStructures::toString(ds.driveStatus));

         // print the time spent in the current state
         switch(ds.driveStatus) {
            case cta::common::dataStructures::DriveStatus::Probing:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.probeStartTime))));
            case cta::common::dataStructures::DriveStatus::Up:
            case cta::common::dataStructures::DriveStatus::Down:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.downOrUpStartTime))));
               break;
            case cta::common::dataStructures::DriveStatus::Starting:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.startStartTime))));
               break;
            case cta::common::dataStructures::DriveStatus::Mounting:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.mountStartTime))));
               break;
            case cta::common::dataStructures::DriveStatus::Transferring:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.transferStartTime))));
               break;
            case cta::common::dataStructures::DriveStatus::CleaningUp:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.cleanupStartTime))));
               break;
            case cta::common::dataStructures::DriveStatus::Unloading:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.unloadStartTime))));
               break;
            case cta::common::dataStructures::DriveStatus::Unmounting:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.unmountStartTime))));
               break;
            case cta::common::dataStructures::DriveStatus::DrainingToDisk:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.drainingStartTime))));
               break;
            case cta::common::dataStructures::DriveStatus::Shutdown:
               currentRow.push_back(std::to_string(static_cast<unsigned long long>((time(nullptr)-ds.shutdownTime))));
            case cta::common::dataStructures::DriveStatus::Unknown:
               currentRow.push_back("-");
               break;
         }

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

      if (singleDrive && !driveFound) {
         throw cta::exception::UserError(std::string("No such drive: ") + drive.value());
      }

      cmdlineOutput<< formatResponse(responseTable);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &mountpolicy = m_option_str.at(OptionString::MOUNT_POLICY);
   auto &in          = m_option_str.at(OptionString::INSTANCE);
   auto &name        = m_option_str.at(OptionString::USERNAME);
   auto &comment     = m_option_str.at(OptionString::COMMENT);

   m_catalogue.createRequesterGroupMountRule(m_cliIdentity, mountpolicy, in, name, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in          = m_option_str.at(OptionString::INSTANCE);
   auto &name        = m_option_str.at(OptionString::USERNAME);
   auto  mountpolicy = getOptional(OptionString::MOUNT_POLICY, m_option_str);
   auto  comment     = getOptional(OptionString::COMMENT, m_option_str);

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

   auto &in          = m_option_str.at(OptionString::INSTANCE);
   auto &name        = m_option_str.at(OptionString::USERNAME);

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

   auto tapepool = getOptional(OptionString::TAPE_POOL, m_option_str);

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

   auto vid = getOptional(OptionString::VID, m_option_str);

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

   auto &name    = m_option_str.at(OptionString::LOGICAL_LIBRARY);
   auto &comment = m_option_str.at(OptionString::COMMENT);

   m_catalogue.createLogicalLibrary(m_cliIdentity, name, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name    = m_option_str.at(OptionString::LOGICAL_LIBRARY);
   auto &comment = m_option_str.at(OptionString::COMMENT);

   m_catalogue.modifyLogicalLibraryComment(m_cliIdentity, name, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name = m_option_str.at(OptionString::LOGICAL_LIBRARY);

   m_catalogue.deleteLogicalLibrary(name);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::LogicalLibrary> list= m_catalogue.getLogicalLibraries();

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

   auto &group                 = m_option_str.at   (OptionString::MOUNT_POLICY);
   auto &archivepriority       = m_option_uint64.at(OptionUInt64::ARCHIVE_PRIORITY);
   auto &minarchiverequestage  = m_option_uint64.at(OptionUInt64::MIN_ARCHIVE_REQUEST_AGE);
   auto &retrievepriority      = m_option_uint64.at(OptionUInt64::RETRIEVE_PRIORITY);
   auto &minretrieverequestage = m_option_uint64.at(OptionUInt64::MIN_RETRIEVE_REQUEST_AGE);
   auto &maxdrivesallowed      = m_option_uint64.at(OptionUInt64::MAX_DRIVES_ALLOWED);
   auto &comment               = m_option_str.at   (OptionString::COMMENT);

   m_catalogue.createMountPolicy(m_cliIdentity, group, archivepriority, minarchiverequestage, retrievepriority,
                                 minretrieverequestage, maxdrivesallowed, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &group = m_option_str.at(OptionString::MOUNT_POLICY);

   auto archivepriority       = getOptional(OptionUInt64::ARCHIVE_PRIORITY, m_option_uint64);
   auto minarchiverequestage  = getOptional(OptionUInt64::MIN_ARCHIVE_REQUEST_AGE, m_option_uint64);
   auto retrievepriority      = getOptional(OptionUInt64::RETRIEVE_PRIORITY, m_option_uint64);
   auto minretrieverequestage = getOptional(OptionUInt64::MIN_RETRIEVE_REQUEST_AGE, m_option_uint64);
   auto maxdrivesallowed      = getOptional(OptionUInt64::MAX_DRIVES_ALLOWED, m_option_uint64);
   auto comment               = getOptional(OptionString::COMMENT, m_option_str);

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

   auto &group = m_option_str.at(OptionString::MOUNT_POLICY);

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

   std::stringstream cmdlineOutput;

   auto &vid = m_option_str.at(OptionString::VID);
   auto  tag = getOptional(OptionString::TAG, m_option_str);

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

   m_scheduler.queueRepack(m_cliIdentity, vid, tag, type);

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = m_option_str.at(OptionString::VID);

   m_scheduler.cancelRepack(m_cliIdentity, vid);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   std::list<cta::common::dataStructures::RepackInfo> list;

   auto vid = getOptional(OptionString::VID, m_option_str);

   if(!vid) {      
      list = m_scheduler.getRepacks(m_cliIdentity);
   } else {
      list.push_back(m_scheduler.getRepack(m_cliIdentity, vid.value()));
   }

   if(!list.empty())
   {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "vid","files","size","type","tag","to retrieve","to archive","failed","archived","status","name","host","time"
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
         currentRow.push_back(it->tag);
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

   auto &vid = m_option_str.at(OptionString::VID);

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

   auto &mountpolicy = m_option_str.at(OptionString::MOUNT_POLICY);
   auto &in          = m_option_str.at(OptionString::INSTANCE);
   auto &name        = m_option_str.at(OptionString::USERNAME);
   auto &comment     = m_option_str.at(OptionString::COMMENT);

   m_catalogue.createRequesterMountRule(m_cliIdentity, mountpolicy, in, name, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in          = m_option_str.at(OptionString::INSTANCE);
   auto &name        = m_option_str.at(OptionString::USERNAME);
   auto  comment     = getOptional(OptionString::COMMENT, m_option_str);
   auto  mountpolicy = getOptional(OptionString::MOUNT_POLICY, m_option_str);

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

   auto &in          = m_option_str.at(OptionString::INSTANCE);
   auto &name        = m_option_str.at(OptionString::USERNAME);

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

   auto &tapepool = m_option_str.at(OptionString::TAPE_POOL);

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
         "type","tapepool","vid","files queued","MBytes queued","oldest age","priority","min age",
         "max drives","cur. mounts","cur. files","cur. MBytes","MB/s","next mounts","tapes capacity",
         "files on tapes","MBytes on tapes","full tapes","empty tapes","disabled tapes",
         "writables tapes"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);
      for (auto & q: queuesAndMounts)
      {
         const uint64_t MBytes = 1000 * 1000;

         std::vector<std::string> currentRow;
         currentRow.push_back(common::dataStructures::toString(q.mountType));
         currentRow.push_back(q.tapePool);
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

   storageClass.diskInstance = m_option_str.at   (OptionString::INSTANCE);
   storageClass.name         = m_option_str.at   (OptionString::STORAGE_CLASS);
   storageClass.nbCopies     = m_option_uint64.at(OptionUInt64::COPY_NUMBER);
   storageClass.comment      = m_option_str.at   (OptionString::COMMENT);

   m_catalogue.createStorageClass(m_cliIdentity, storageClass);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in  = m_option_str.at(OptionString::INSTANCE);
   auto &scn = m_option_str.at(OptionString::STORAGE_CLASS);

   auto comment = getOptional(OptionString::COMMENT, m_option_str);
   auto cn      = getOptional(OptionUInt64::COPY_NUMBER, m_option_uint64);

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

   auto &in  = m_option_str.at(OptionString::INSTANCE);
   auto &scn = m_option_str.at(OptionString::STORAGE_CLASS);

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

   auto &vid            = m_option_str.at(OptionString::VID);
   auto &logicallibrary = m_option_str.at(OptionString::LOGICAL_LIBRARY);
   auto &tapepool       = m_option_str.at(OptionString::TAPE_POOL);
   auto &capacity       = m_option_uint64.at(OptionUInt64::CAPACITY);
   auto &disabled       = m_option_bool.at(OptionBoolean::DISABLED);
   auto &full           = m_option_bool.at(OptionBoolean::FULL);
   auto  comment        = getOptional(OptionString::COMMENT, m_option_str);

   m_catalogue.createTape(m_cliIdentity, vid, logicallibrary, tapepool, capacity, disabled, full, comment ? comment.value() : "-");

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = m_option_str.at(OptionString::VID);

   auto logicallibrary = getOptional(OptionString::LOGICAL_LIBRARY, m_option_str);
   auto tapepool       = getOptional(OptionString::TAPE_POOL, m_option_str);
   auto capacity       = getOptional(OptionUInt64::CAPACITY, m_option_uint64);
   auto comment        = getOptional(OptionString::COMMENT, m_option_str);
   auto encryptionkey  = getOptional(OptionString::ENCRYPTION_KEY, m_option_str);
   auto disabled       = getOptional(OptionBoolean::DISABLED, m_option_bool);
   auto full           = getOptional(OptionBoolean::FULL, m_option_bool);

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

   auto &vid = m_option_str.at(OptionString::VID);

   m_catalogue.deleteTape(vid);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Reclaim(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = m_option_str.at(OptionString::VID);

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

      searchCriteria.disabled        = getOptional(OptionBoolean::DISABLED,       m_option_bool,   &has_any);
      searchCriteria.full            = getOptional(OptionBoolean::FULL,           m_option_bool,   &has_any);
      searchCriteria.lbp             = getOptional(OptionBoolean::LBP,            m_option_bool,   &has_any);
      searchCriteria.capacityInBytes = getOptional(OptionUInt64::CAPACITY,        m_option_uint64, &has_any);
      searchCriteria.logicalLibrary  = getOptional(OptionString::LOGICAL_LIBRARY, m_option_str,    &has_any);
      searchCriteria.tapePool        = getOptional(OptionString::TAPE_POOL,       m_option_str,    &has_any);
      searchCriteria.vid             = getOptional(OptionString::VID,             m_option_str,    &has_any);

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

   auto &vid   = m_option_str.at(OptionString::VID);
   auto  force = getOptional(OptionBoolean::FORCE, m_option_bool);
   auto  lbp   = getOptional(OptionBoolean::LBP, m_option_bool);
   // If the tag option is not specified, the tag will be set to "-", which means no tagging
   auto  tag   = getOptional(OptionString::TAG, m_option_str);

   m_scheduler.queueLabel(m_cliIdentity, vid,
                          force ? force.value() : false,
                          lbp ? lbp.value() : true,
                          tag);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



#if 0
void RequestMessage::processTapePool_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   optional<std::string> name = getOptionStringValue("-n", "--name", true, false);

      optional<uint64_t> ptn = getOptionUint64Value("-p", "--partialtapesnumber", true, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", true, false);
      optional<bool> encrypted = getOptionBoolValue("-e", "--encrypted", true, false);
      checkOptions(help.str());
      m_catalogue.createTapePool(m_cliIdentity, name.value(), ptn.value(), encrypted.value(), comment.value());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

    else if("ch" == m_requestTokens.at(2)) { //ch
      optional<uint64_t> ptn = getOptionUint64Value("-p", "--partialtapesnumber", false, false);
      optional<std::string> comment = getOptionStringValue("-m", "--comment", false, false);
      optional<bool> encrypted = getOptionBoolValue("-e", "--encrypted", false, false);
      checkOptions(help.str());
      if(comment) {
        m_catalogue.modifyTapePoolComment(m_cliIdentity, name.value(), comment.value());
      }
      if(ptn) {
        m_catalogue.modifyTapePoolNbPartialTapes(m_cliIdentity, name.value(), ptn.value());
      }
      if(encrypted) {
        m_catalogue.setTapePoolEncryption(m_cliIdentity, name.value(), encrypted.value());
      }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;


      m_catalogue.deleteTapePool(name.value());

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

    std::list<cta::common::dataStructures::TapePool> list= m_catalogue.getTapePools();
    if(list.size()>0) {
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

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTest_Read(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

  optional<std::string> drive = getOptionStringValue("-d", "--drive", true, false);
  optional<std::string> vid = getOptionStringValue("-v", "--vid", true, false);
  if("read" == m_requestTokens.at(2)) {
    optional<uint64_t> firstfseq = getOptionUint64Value("-f", "--firstfseq", true, false);
    optional<uint64_t> lastfseq = getOptionUint64Value("-l", "--lastfseq", true, false);
    optional<std::string> output = getOptionStringValue("-o", "--output", true, false);        
    bool checkchecksum = hasOption("-c", "--checkchecksum");
    checkOptions(help.str());
    optional<std::string> tag = getOptionStringValue("-t", "--tag", false, false);
    std::string tag_value = tag? tag.value():"-";
    cta::common::dataStructures::ReadTestResult res = m_scheduler.readTest(m_cliIdentity, drive.value(), vid.value(), firstfseq.value(), lastfseq.value(), checkchecksum, output.value(), tag_value);   
    std::vector<std::vector<std::string>> responseTable;
    std::vector<std::string> header = { "fseq","checksum type","checksum value","error" };
    responseTable.push_back(header);
    for(auto it = res.checksums.cbegin(); it != res.checksums.cend(); it++) {
      std::vector<std::string> currentRow;
      currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->first)));
      currentRow.push_back(it->second.first);
      currentRow.push_back(it->second.second);
      if(res.errors.find(it->first) != res.errors.cend()) {
        currentRow.push_back(res.errors.at(it->first));
      }
      else {
        currentRow.push_back("-");
      }
      responseTable.push_back(currentRow);
    }
    cmdlineOutput << formatResponse(responseTable, true)
           << std::endl << "Drive: " << res.driveName << " Vid: " << res.vid << " #Files: " << res.totalFilesRead << " #Bytes: " << res.totalBytesRead 
           << " Time: " << res.totalTimeInSeconds << " s Speed(avg): " << (long double)res.totalBytesRead/(long double)res.totalTimeInSeconds << " B/s" <<std::endl;
  }
  else if("write" == m_requestTokens.at(2) || "write_auto" == m_requestTokens.at(2)) {
    cta::common::dataStructures::WriteTestResult res;
    if("write" == m_requestTokens.at(2)) { //write
      optional<std::string> file = getOptionStringValue("-f", "--file", true, false);
      checkOptions(help.str());
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false, false);
      std::string tag_value = tag? tag.value():"-";
      res = m_scheduler.writeTest(m_cliIdentity, drive.value(), vid.value(), file.value(), tag_value);
    }
    else { //write_auto
      optional<uint64_t> number = getOptionUint64Value("-n", "--number", true, false);
      optional<uint64_t> size = getOptionUint64Value("-s", "--size", true, false);
      optional<std::string> input = getOptionStringValue("-i", "--input", true, false);
      if(input.value()!="zero"&&input.value()!="urandom") {
        m_missingRequiredOptions.push_back("--input value must be either \"zero\" or \"urandom\"");
      }
      checkOptions(help.str());
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false, false);
      std::string tag_value = tag? tag.value():"-";
      cta::common::dataStructures::TestSourceType type;
      if(input.value()=="zero") { //zero
        type = cta::common::dataStructures::TestSourceType::devzero;
      }
      else { //urandom
        type = cta::common::dataStructures::TestSourceType::devurandom;
      }
      res = m_scheduler.write_autoTest(m_cliIdentity, drive.value(), vid.value(), number.value(), size.value(), type, tag_value);
    }
    std::vector<std::vector<std::string>> responseTable;
    std::vector<std::string> header = { "fseq","checksum type","checksum value","error" };
    responseTable.push_back(header);
    for(auto it = res.checksums.cbegin(); it != res.checksums.cend(); it++) {
      std::vector<std::string> currentRow;
      currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->first)));
      currentRow.push_back(it->second.first);
      currentRow.push_back(it->second.second);
      if(res.errors.find(it->first) != res.errors.cend()) {
        currentRow.push_back(res.errors.at(it->first));
      }
      else {
        currentRow.push_back("-");
      }
      responseTable.push_back(currentRow);
    }
    cmdlineOutput << formatResponse(responseTable, true)
           << std::endl << "Drive: " << res.driveName << " Vid: " << res.vid << " #Files: " << res.totalFilesWritten << " #Bytes: " << res.totalBytesWritten 
           << " Time: " << res.totalTimeInSeconds << " s Speed(avg): " << (long double)res.totalBytesWritten/(long double)res.totalTimeInSeconds << " B/s" <<std::endl;
  }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTest_Write(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTest_WriteAuto(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processVerify_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

  if("add" == m_requestTokens.at(2) || "err" == m_requestTokens.at(2) || "rm" == m_requestTokens.at(2)) {
    optional<std::string> vid = getOptionStringValue("-v", "--vid", true, false);
    if("add" == m_requestTokens.at(2)) { //add
      optional<std::string> tag = getOptionStringValue("-t", "--tag", false, false);
      optional<uint64_t> numberOfFiles = getOptionUint64Value("-p", "--partial", false, false); //nullopt means do a complete verification      
      checkOptions(help.str());
      m_scheduler.queueVerify(m_cliIdentity, vid.value(), tag, numberOfFiles);
    }
    else if("err" == m_requestTokens.at(2)) { //err
      checkOptions(help.str());
      cta::common::dataStructures::VerifyInfo info = m_scheduler.getVerify(m_cliIdentity, vid.value());
      if(info.errors.size()>0) {
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
    }
    else { //rm
      checkOptions(help.str());
      m_scheduler.cancelVerify(m_cliIdentity, vid.value());
    }
  }
  else if("ls" == m_requestTokens.at(2)) { //ls
    std::list<cta::common::dataStructures::VerifyInfo> list;
    optional<std::string> vid = getOptionStringValue("-v", "--vid", true, false);
    if(!vid) {      
      list = m_scheduler.getVerifys(m_cliIdentity);
    }
    else {
      list.push_back(m_scheduler.getVerify(m_cliIdentity, vid.value()));
    }
    if(list.size()>0) {
      std::vector<std::vector<std::string>> responseTable;
      std::vector<std::string> header = {
         "vid","files","size","tag","to verify","failed","verified","status","name","host","time"
      };
      if(has_flag(OptionBoolean::SHOW_HEADER)) responseTable.push_back(header);    
      for(auto it = list.cbegin(); it != list.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->vid);
        currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->totalFiles)));
        currentRow.push_back(std::to_string(static_cast<unsigned long long>(it->totalSize)));
        currentRow.push_back(it->tag);
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
  }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processVerify_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processVerify_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processVerify_Err(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}
#endif



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
}



std::string RequestMessage::formatResponse(const std::vector<std::vector<std::string>> &responseTable)
{
   bool has_header = has_flag(cta::admin::OptionBoolean::SHOW_HEADER);

   if(responseTable.empty() || responseTable.at(0).empty()) return "";

   std::vector<int> columnSizes;

   for(uint j = 0; j < responseTable.at(0).size(); j++) { //for each column j
      uint columnSize = 0;
      for(uint i = 0; i<responseTable.size(); i++) { //for each row i
         if(responseTable.at(i).at(j).size()>columnSize) {
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
                                             const cta::common::dataStructures::EntryLog &lastModificationLog)
{
   responseRow.push_back(creationLog.username);
   responseRow.push_back(creationLog.host);
   responseRow.push_back(timeToString(creationLog.time));
   responseRow.push_back(lastModificationLog.username);
   responseRow.push_back(lastModificationLog.host);
   responseRow.push_back(timeToString(lastModificationLog.time));
}

}} // namespace cta::xrd

