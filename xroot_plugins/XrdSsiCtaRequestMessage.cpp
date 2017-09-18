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

// Convert AdminCmd <Cmd, SubCmd> pair to an integer so that it can be used in a switch statement
constexpr unsigned int cmd_pair(cta::admin::AdminCmd::Cmd cmd, cta::admin::AdminCmd::SubCmd subcmd) {
   return (cmd << 16) + subcmd;
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

   uint64_t archiveFileId = m_scheduler.queueArchive(m_instance_name, request, m_lc);

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

   m_scheduler.queueRetrieve(m_instance_name, request, m_lc);

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
   m_scheduler.deleteArchive(m_instance_name, request, m_lc);

   // Create a log entry

   cta::log::ScopedParamContainer params(m_lc);
   params.add("fileId", request.archiveFileID).add("catalogueTime", t.secs());
   m_lc.log(cta::log::INFO, "In processDELETE(): deleted archive file.");

   // Set response type

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdminHost_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdminHost_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdminHost_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdminHost_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveFile_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Up(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Down(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processDrive_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processListPendingArchives(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processListPendingRetrieves(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   auto vid_it       = m_option_str.find(OptionString::VID);
   bool has_extended = m_option_bool.find(OptionBoolean::EXTENDED) != m_option_bool.end();
   bool has_header   = m_option_bool.find(OptionBoolean::SHOW_HEADER) != m_option_bool.end();

   std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > result;

   if(vid_it == m_option_str.end()) {
      result = m_scheduler.getPendingRetrieveJobs(m_lc);
   } else {
      std::list<cta::common::dataStructures::RetrieveJob> list = m_scheduler.getPendingRetrieveJobs(vid_it->second, m_lc);

      if(list.size() > 0) {
         result[vid_it->second] = list;
      }
   }

   if(result.size() > 0)
   {
      std::vector<std::vector<std::string>> responseTable;

      if(has_extended)
      {
         std::vector<std::string> header = {"vid","id","copy no.","fseq","block id","size","user","group","path"};
         if(has_header) responseTable.push_back(header);    
         for(auto it = result.cbegin(); it != result.cend(); it++)
         {
            for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++)
            {
               std::vector<std::string> currentRow;
               currentRow.push_back(it->first);
               currentRow.push_back(std::to_string((unsigned long long)jt->request.archiveFileID));
               cta::common::dataStructures::ArchiveFile file = m_catalogue.getArchiveFileById(jt->request.archiveFileID);
               currentRow.push_back(std::to_string((unsigned long long)(jt->tapeCopies.at(it->first).first)));
               currentRow.push_back(std::to_string((unsigned long long)(jt->tapeCopies.at(it->first).second.fSeq)));
               currentRow.push_back(std::to_string((unsigned long long)(jt->tapeCopies.at(it->first).second.blockId)));
               currentRow.push_back(std::to_string((unsigned long long)file.fileSize));
               currentRow.push_back(jt->request.requester.name);
               currentRow.push_back(jt->request.requester.group);
               currentRow.push_back(jt->request.diskFileInfo.path);
               responseTable.push_back(currentRow);
            }
         }
      }
      else
      {
         std::vector<std::string> header = {"vid","total files","total size"};
         if(has_header) responseTable.push_back(header);    
         for(auto it = result.cbegin(); it != result.cend(); it++)
         {
            std::vector<std::string> currentRow;
            currentRow.push_back(it->first);
            currentRow.push_back(std::to_string((unsigned long long)it->second.size()));
            uint64_t size=0;
            for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++)
            {
               cta::common::dataStructures::ArchiveFile file = m_catalogue.getArchiveFileById(jt->request.archiveFileID);
               size += file.fileSize;
            }
            currentRow.push_back(std::to_string((unsigned long long)size));
            responseTable.push_back(currentRow);
         }
      }
      cmdlineOutput << formatResponse(responseTable, has_header);
   }

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRepack_Err(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processShrink(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processShowQueues(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Reclaim(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Label(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Add(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Ch(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Rm(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

   response.set_message_txt(cmdlineOutput.str());
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTest_Read(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
   using namespace cta::admin;

   std::stringstream cmdlineOutput;

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



std::string RequestMessage::formatResponse(const std::vector<std::vector<std::string>> &responseTable, const bool has_header)
{
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

}} // namespace cta::xrd

