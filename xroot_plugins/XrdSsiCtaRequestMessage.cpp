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
            case cmd_pair(AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_CH): 
            case cmd_pair(AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_ADMINHOST, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_ADMINHOST, AdminCmd::SUBCMD_CH): 
            case cmd_pair(AdminCmd::CMD_ADMINHOST, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_ADMINHOST, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_ARCHIVEFILE, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_CH): 
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_UP): 
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_DOWN): 
            case cmd_pair(AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_CH): 
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_LS):
            case cmd_pair(AdminCmd::CMD_LISTPENDINGARCHIVES, AdminCmd::SUBCMD_NONE): 
goto the_end;
            case cmd_pair(AdminCmd::CMD_LISTPENDINGRETRIEVES, AdminCmd::SUBCMD_NONE): processListPendingRetrieves(request.admincmd(), response); break;
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_CH): 
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_CH): 
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ERR): 
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_CH): 
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_SHRINK, AdminCmd::SUBCMD_NONE): 
            case cmd_pair(AdminCmd::CMD_SHOWQUEUES, AdminCmd::SUBCMD_NONE): 
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_CH): 
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_CH): 
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_RECLAIM): 
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LABEL): 
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_CH): 
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_TEST, AdminCmd::SUBCMD_READ): 
            case cmd_pair(AdminCmd::CMD_TEST, AdminCmd::SUBCMD_WRITE): 
            case cmd_pair(AdminCmd::CMD_TEST, AdminCmd::SUBCMD_WRITE_AUTO): 
            case cmd_pair(AdminCmd::CMD_VERIFY, AdminCmd::SUBCMD_ADD): 
            case cmd_pair(AdminCmd::CMD_VERIFY, AdminCmd::SUBCMD_RM): 
            case cmd_pair(AdminCmd::CMD_VERIFY, AdminCmd::SUBCMD_LS): 
            case cmd_pair(AdminCmd::CMD_VERIFY, AdminCmd::SUBCMD_ERR): 

            the_end:
            default:
               throw PbException("Admin command pair <" +
                     AdminCmd_Cmd_Name(request.admincmd().cmd()) + ", " +
                     AdminCmd_SubCmd_Name(request.admincmd().subcmd()) +
                     "> is not implemented.");
         }
         break;

      case Request::kNotification:
         switch(request.notification().wf().event()) {
            using namespace cta::eos;

            case Workflow::CLOSEW:  processCLOSEW (request.notification(), response); break;
            case Workflow::PREPARE: processPREPARE(request.notification(), response); break;
            case Workflow::DELETE:  processDELETE (request.notification(), response); break;

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



void RequestMessage::processListPendingRetrieves(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response)
{
#if 0
  std::stringstream cmdlineOutput;
  std::stringstream help;
  log::LogContext lc(m_log);
  help << m_requestTokens.at(0) << " lpr/listpendingretrieves [--header/-h] [--vid/-v <vid>] [--extended/-x]" << std::endl;
  if (hasOption("-?", "--help")) {
    return help.str();
  }
#endif
   using namespace cta::admin;

   auto vid      = m_option_str.find(OptionString::VID);
   //bool extended = m_option_bool.find(OptionBoolean::EXTENDED) != m_option_bool.end();

   std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > result;

   if(vid == m_option_str.end()) {
      result = m_scheduler.getPendingRetrieveJobs(m_lc);
   } else {
      std::list<cta::common::dataStructures::RetrieveJob> list = m_scheduler.getPendingRetrieveJobs(vid->second, m_lc);

      if(list.size() > 0) {
         result[vid->second] = list;
      }
   }

#if 0
  if(result.size()>0)
  {
    std::vector<std::vector<std::string>> responseTable;

    if(extended) {
      std::vector<std::string> header = {"vid","id","copy no.","fseq","block id","size","user","group","path"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = result.cbegin(); it != result.cend(); it++) {
        for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++) {
          std::vector<std::string> currentRow;
          currentRow.push_back(it->first);
          currentRow.push_back(std::to_string((unsigned long long)jt->request.archiveFileID));
          cta::common::dataStructures::ArchiveFile file = m_catalogue->getArchiveFileById(jt->request.archiveFileID);
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
    } else {
      std::vector<std::string> header = {"vid","total files","total size"};
      if(hasOption("-h", "--header")) responseTable.push_back(header);    
      for(auto it = result.cbegin(); it != result.cend(); it++) {
        std::vector<std::string> currentRow;
        currentRow.push_back(it->first);
        currentRow.push_back(std::to_string((unsigned long long)it->second.size()));
        uint64_t size=0;
        for(auto jt = it->second.cbegin(); jt != it->second.cend(); jt++) {
          cta::common::dataStructures::ArchiveFile file = m_catalogue->getArchiveFileById(jt->request.archiveFileID);
          size += file.fileSize;
        }
        currentRow.push_back(std::to_string((unsigned long long)size));
        responseTable.push_back(currentRow);
      }
    }
    cmdlineOutput << formatResponse(responseTable, hasOption("-h", "--header"));
  }

  //return cmdlineOutput.str();
#endif
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

}} // namespace cta::xrd

