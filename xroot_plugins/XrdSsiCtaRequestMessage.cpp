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

#include "catalogue/CreateMountPolicyAttributes.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "cmdline/CtaAdminCmdParse.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/utils/Regex.hpp"
#include "XrdCtaActivityMountRuleLs.hpp"
#include "XrdCtaAdminLs.hpp"
#include "XrdCtaArchiveRouteLs.hpp"
#include "XrdCtaChangeStorageClass.hpp"
#include "XrdCtaDiskInstanceLs.hpp"
#include "XrdCtaDiskInstanceSpaceLs.hpp"
#include "XrdCtaDiskSystemLs.hpp"
#include "XrdCtaDriveLs.hpp"
#include "XrdCtaFailedRequestLs.hpp"
#include "XrdCtaGroupMountRuleLs.hpp"
#include "XrdCtaLogicalLibraryLs.hpp"
#include "XrdCtaMediaTypeLs.hpp"
#include "XrdCtaMountPolicyLs.hpp"
#include "XrdCtaRecycleTapeFileLs.hpp"
#include "XrdCtaRepackLs.hpp"
#include "XrdCtaRequesterMountRuleLs.hpp"
#include "XrdCtaShowQueues.hpp"
#include "XrdCtaStorageClassLs.hpp"
#include "XrdCtaTapeFileLs.hpp"
#include "XrdCtaTapeLs.hpp"
#include "XrdCtaTapePoolLs.hpp"
#include "XrdCtaVersion.hpp"
#include "XrdCtaVirtualOrganizationLs.hpp"
#include "XrdSsiCtaRequestMessage.hpp"

#include "frontend/common/PbException.hpp"
#include "frontend/common/AdminCmd.hpp"
#include "frontend/common/WorkflowEvent.hpp"

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

         m_client_versions.ctaVersion = request.admincmd().client_version();
         m_client_versions.protobufTag = request.admincmd().protobuf_tag();

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
                  processModifyArchiveFile(response);
                  break;

               default:
                  throw exception::PbException("Admin command pair <" +
                        AdminCmd_Cmd_Name(request.admincmd().cmd()) + ", " +
                        AdminCmd_SubCmd_Name(request.admincmd().subcmd()) +
                        "> is not implemented.");
            } // end switch
            
            // Log the admin command
            logAdminCmd(__FUNCTION__, "success", "", request.admincmd(), t);
         } catch(exception::PbException &ex) {
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

      case Request::kNotification: {
        frontend::WorkflowEvent wfe(m_service.getFrontendService(), m_cliIdentity, request.notification());
        response = wfe.process();
        break;
      } // end case Request::kNotification

      case Request::REQUEST_NOT_SET:
         throw exception::PbException("Request message has not been set.");

      default:
         throw exception::PbException("Unrecognized Request message. "
                           "Possible Protocol Buffer version mismatch between client and server.");
   }
}

// Admin commands

void RequestMessage::logAdminCmd(const std::string &function, const std::string &status, const std::string &reason,
const cta::admin::AdminCmd &admincmd, cta::utils::Timer &t)
{
   using namespace cta::admin;

   cta::log::ScopedParamContainer params(m_lc);

   std::string log_msg = "In RequestMessage::" + function + "(): Admin command succeeded.";

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

   m_catalogue.AdminUser()->createAdminUser(m_cliIdentity, username, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = getRequired(OptionString::USERNAME);
   auto &comment  = getRequired(OptionString::COMMENT);

   m_catalogue.AdminUser()->modifyAdminUserComment(m_cliIdentity, username, comment);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processAdmin_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &username = getRequired(OptionString::USERNAME);

   m_catalogue.AdminUser()->deleteAdminUser(username);

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

   m_catalogue.ArchiveRoute()->createArchiveRoute(m_cliIdentity, scn, cn, tapepool, comment);

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
      m_catalogue.ArchiveRoute()->modifyArchiveRouteComment(m_cliIdentity, scn, cn, comment.value());
   }
   if(tapepool) {
      m_catalogue.ArchiveRoute()->modifyArchiveRouteTapePoolName(m_cliIdentity, scn, cn, tapepool.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processArchiveRoute_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &scn = getRequired(OptionString::STORAGE_CLASS);
   auto &cn  = getRequired(OptionUInt64::COPY_NUMBER);

   m_catalogue.ArchiveRoute()->deleteArchiveRoute(scn, cn);

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

  const auto tapeDriveNames = m_catalogue.DriveState()->getTapeDriveNames();
  bool drivesFound = false;

  for (auto tapeDriveName : tapeDriveNames)
  {
    const auto regexResult = driveNameRegex.exec(tapeDriveName);
    if (!regexResult.empty())
    {
      const auto tapeDrive = m_catalogue.DriveState()->getTapeDrive(tapeDriveName).value();

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

  stream = new FailedRequestLsStream(*this, m_catalogue, m_scheduler, m_service.getFrontendService().getSchedDb(), m_lc);

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

   m_catalogue.RequesterGroupMountRule()->createRequesterGroupMountRule(m_cliIdentity, mountpolicy, in, name, comment);

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
      m_catalogue.RequesterGroupMountRule()->modifyRequesterGroupMountRuleComment(m_cliIdentity, in, name, comment.value());
   }
   if(mountpolicy) {
      m_catalogue.RequesterGroupMountRule()->modifyRequesterGroupMountRulePolicy(m_cliIdentity, in, name, mountpolicy.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processGroupMountRule_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in   = getRequired(OptionString::INSTANCE);
   auto &name = getRequired(OptionString::USERNAME);

   m_catalogue.RequesterGroupMountRule()->deleteRequesterGroupMountRule(in, name);

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

   m_catalogue.LogicalLibrary()->createLogicalLibrary(m_cliIdentity, name, isDisabled ? isDisabled.value() : false, comment);

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
      m_catalogue.LogicalLibrary()->setLogicalLibraryDisabled(m_cliIdentity, name, disabled.value());
      if ((!disabled.value()) && (!disabledReason)) {
         //if enabling the tape and the reason is not specified in the command, erase the reason
         m_catalogue.LogicalLibrary()->modifyLogicalLibraryDisabledReason(m_cliIdentity, name, "");
      }
   }
   if(comment) {
      m_catalogue.LogicalLibrary()->modifyLogicalLibraryComment(m_cliIdentity, name, comment.value());
   }
   if (disabledReason) {
      m_catalogue.LogicalLibrary()->modifyLogicalLibraryDisabledReason(m_cliIdentity, name, disabledReason.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processLogicalLibrary_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name = getRequired(OptionString::LOGICAL_LIBRARY);

   m_catalogue.LogicalLibrary()->deleteLogicalLibrary(name);

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
   m_catalogue.MediaType()->createMediaType(m_cliIdentity, mediaType);

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
    m_catalogue.MediaType()->modifyMediaTypeCartridge(m_cliIdentity,mediaTypeName,cartridge.value());
  }
  if(primaryDensityCode){
    m_catalogue.MediaType()->modifyMediaTypePrimaryDensityCode(m_cliIdentity,mediaTypeName,primaryDensityCode.value());
  }
  if(secondaryDensityCode){
    m_catalogue.MediaType()->modifyMediaTypeSecondaryDensityCode(m_cliIdentity,mediaTypeName,secondaryDensityCode.value());
  }
  if(nbWraps){
    std::optional<uint32_t> newNbWraps = nbWraps.value();
    m_catalogue.MediaType()->modifyMediaTypeNbWraps(m_cliIdentity,mediaTypeName,newNbWraps);
  }
  if(minlpos){
    m_catalogue.MediaType()->modifyMediaTypeMinLPos(m_cliIdentity, mediaTypeName, minlpos);
  }
  if(maxlpos){
    m_catalogue.MediaType()->modifyMediaTypeMaxLPos(m_cliIdentity,mediaTypeName,maxlpos);
  }
  if(comment){
    m_catalogue.MediaType()->modifyMediaTypeComment(m_cliIdentity,mediaTypeName,comment.value());
  }
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMediaType_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   const auto &mtn = getRequired(OptionString::MEDIA_TYPE);

   m_catalogue.MediaType()->deleteMediaType(mtn);

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

   m_catalogue.MountPolicy()->createMountPolicy(m_cliIdentity, mountPolicy);

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
      m_catalogue.MountPolicy()->modifyMountPolicyArchivePriority(m_cliIdentity, group, archivepriority.value());
   }
   if(minarchiverequestage) {
      m_catalogue.MountPolicy()->modifyMountPolicyArchiveMinRequestAge(m_cliIdentity, group, minarchiverequestage.value());
   }
   if(retrievepriority) {
      m_catalogue.MountPolicy()->modifyMountPolicyRetrievePriority(m_cliIdentity, group, retrievepriority.value());
   }
   if(minretrieverequestage) {
      m_catalogue.MountPolicy()->modifyMountPolicyRetrieveMinRequestAge(m_cliIdentity, group, minretrieverequestage.value());
   }
   if(comment) {
      m_catalogue.MountPolicy()->modifyMountPolicyComment(m_cliIdentity, group, comment.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processMountPolicy_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &group = getRequired(OptionString::MOUNT_POLICY);

   m_catalogue.MountPolicy()->deleteMountPolicy(group);

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
  MountPolicyList mountPolicies = m_catalogue.MountPolicy()->getMountPolicies();
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

  bool noRecall = has_flag(OptionBoolean::NO_RECALL);

  // Process each item in the list
  for(auto it = vid_list.begin(); it != vid_list.end(); ++it) {
    SchedulerDatabase::QueueRepackRequest repackRequest(*it,bufferURL,type,mountPolicy, noRecall);
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

   m_catalogue.RequesterMountRule()->createRequesterMountRule(m_cliIdentity, mountpolicy, in, name, comment);

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
      m_catalogue.RequesterMountRule()->modifyRequesteMountRuleComment(m_cliIdentity, in, name, comment.value());
   }
   if(mountpolicy) {
      m_catalogue.RequesterMountRule()->modifyRequesterMountRulePolicy(m_cliIdentity, in, name, mountpolicy.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processRequesterMountRule_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in   = getRequired(OptionString::INSTANCE);
   auto &name = getRequired(OptionString::USERNAME);

   m_catalogue.RequesterMountRule()->deleteRequesterMountRule(in, name);

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

   m_catalogue.RequesterActivityMountRule()->createRequesterActivityMountRule(m_cliIdentity, mountpolicy, in, name,
      activityRegex, comment);

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
      m_catalogue.RequesterActivityMountRule()->modifyRequesterActivityMountRuleComment(m_cliIdentity, in, name,
         activityRegex, comment.value());
   }
   if(mountpolicy) {
      m_catalogue.RequesterActivityMountRule()->modifyRequesterActivityMountRulePolicy(m_cliIdentity, in, name,
         activityRegex, mountpolicy.value());
   }
   
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processActivityMountRule_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &in   = getRequired(OptionString::INSTANCE);
   auto &name = getRequired(OptionString::USERNAME);
   auto &activityRegex = getRequired(OptionString::ACTIVITY_REGEX);

   m_catalogue.RequesterActivityMountRule()->deleteRequesterActivityMountRule(in, name, activityRegex);

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

   m_catalogue.StorageClass()->createStorageClass(m_cliIdentity, storageClass);

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
      m_catalogue.StorageClass()->modifyStorageClassComment(m_cliIdentity, scn, comment.value());
   }
   if(cn) {
      m_catalogue.StorageClass()->modifyStorageClassNbCopies(m_cliIdentity, scn, cn.value());
   }
   if(vo){
     m_catalogue.StorageClass()->modifyStorageClassVo(m_cliIdentity,scn,vo.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &scn = getRequired(OptionString::STORAGE_CLASS);

   m_catalogue.StorageClass()->deleteStorageClass(scn);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processStorageClass_Ls(cta::xrd::Response &response, XrdSsiStream* &stream)
{
  using namespace cta::admin;

  std::optional<std::string> scn = getOptional(OptionString::STORAGE_CLASS);

  // Create a XrdSsi stream object to return the results
  stream = new StorageClassLsStream(*this, m_catalogue, m_scheduler, scn);

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
     tape.state = common::dataStructures::Tape::stringToState(state.value(), true);
   }
   tape.stateReason = stateReason;
   m_catalogue.Tape()->createTape(m_cliIdentity, tape);

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
      if (m_catalogue.Tape()->getNbFilesOnTape(vid) != 0) {
         response.set_type(cta::xrd::Response::RSP_ERR_CTA);
         return;
      }
      m_catalogue.Tape()->modifyTapeMediaType(m_cliIdentity, vid, mediaType.value());
   }
   if(vendor) {
      m_catalogue.Tape()->modifyTapeVendor(m_cliIdentity, vid, vendor.value());
   }
   if(logicallibrary) {
      m_catalogue.Tape()->modifyTapeLogicalLibraryName(m_cliIdentity, vid, logicallibrary.value());
   }
   if(tapepool) {
      m_catalogue.Tape()->modifyTapeTapePoolName(m_cliIdentity, vid, tapepool.value());
   }
   if(comment) {
      if(comment.value().empty()){
        //If the comment is an empty string, the user meant to delete it
        comment = std::nullopt;
      }
      m_catalogue.Tape()->modifyTapeComment(m_cliIdentity, vid, comment);
   }
   if(encryptionkeyName) {
      m_catalogue.Tape()->modifyTapeEncryptionKeyName(m_cliIdentity, vid, encryptionkeyName.value());
   }
   if(full) {
      m_catalogue.Tape()->setTapeFull(m_cliIdentity, vid, full.value());
   }
   if(state){
     auto stateEnumValue = common::dataStructures::Tape::stringToState(state.value(), true);
     m_scheduler.triggerTapeStateChange(m_cliIdentity,vid,stateEnumValue,stateReason, m_lc);
   }
   if (dirty) {
      m_catalogue.Tape()->setTapeDirty(m_cliIdentity, vid, dirty.value());
   }
   if (verificationStatus) {
      m_catalogue.Tape()->modifyTapeVerificationStatus(m_cliIdentity, vid, verificationStatus.value());
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
   m_catalogue.Tape()->deleteTape(vid);

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTape_Reclaim(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &vid = getRequired(OptionString::VID);

   m_catalogue.Tape()->reclaimTape(m_cliIdentity, vid, m_lc);

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
  
  auto archiveFile = m_catalogue.ArchiveFile()->getArchiveFileForDeletion(searchCriteria);
  grpc::EndpointMap endpoints(m_namespaceMap);
  auto diskFilePath = endpoints.getPath(archiveFile.diskInstance, archiveFile.diskFileId);
  archiveFile.diskFileInfo.path = diskFilePath;

  m_catalogue.TapeFile()->deleteTapeFileCopy(archiveFile, reason);
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

   m_catalogue.TapePool()->createTapePool(m_cliIdentity, name, vo, ptn, encrypted, supply, comment);

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
      m_catalogue.TapePool()->modifyTapePoolComment(m_cliIdentity, name, comment.value());
   }
   if(vo) {
      m_catalogue.TapePool()->modifyTapePoolVo(m_cliIdentity, name, vo.value());
   }
   if(ptn) {
      m_catalogue.TapePool()->modifyTapePoolNbPartialTapes(m_cliIdentity, name, ptn.value());
   }
   if(encrypted) {
      m_catalogue.TapePool()->setTapePoolEncryption(m_cliIdentity, name, encrypted.value());
   }
   if(supply) {
      m_catalogue.TapePool()->modifyTapePoolSupply(m_cliIdentity, name, supply.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}



void RequestMessage::processTapePool_Rm(cta::xrd::Response &response)
{
   using namespace cta::admin;

   auto &name = getRequired(OptionString::TAPE_POOL);

   m_catalogue.TapePool()->deleteTapePool(name);

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

  m_catalogue.DiskSystem()->createDiskSystem(m_cliIdentity, name,diskInstance, diskInstanceSpace, fileRegexp,
   targetedFreeSpace, sleepTime, comment);

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
      m_catalogue.DiskSystem()->modifyDiskSystemComment(m_cliIdentity, name, comment.value());
   }
   if(fileRegexp) {
      m_catalogue.DiskSystem()->modifyDiskSystemFileRegexp(m_cliIdentity, name, fileRegexp.value());
   }
   if (sleepTime) {
     m_catalogue.DiskSystem()->modifyDiskSystemSleepTime(m_cliIdentity, name, sleepTime.value());
   }
   if(targetedFreeSpace) {
      m_catalogue.DiskSystem()->modifyDiskSystemTargetedFreeSpace(m_cliIdentity, name, targetedFreeSpace.value());
   }
   if (diskInstanceName) {
      m_catalogue.DiskSystem()->modifyDiskSystemDiskInstanceName(m_cliIdentity, name, diskInstanceName.value());
   }
   if (diskInstanceSpaceName) {
      m_catalogue.DiskSystem()->modifyDiskSystemDiskInstanceSpaceName(m_cliIdentity, name, diskInstanceSpaceName.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskSystem_Rm(cta::xrd::Response &response)
{
  using namespace cta::admin;

  const auto &name = getRequired(OptionString::DISK_SYSTEM);

  m_catalogue.DiskSystem()->deleteDiskSystem(name);

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

  m_catalogue.DiskInstance()->createDiskInstance(m_cliIdentity, name, comment);

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskInstance_Ch(cta::xrd::Response &response)
{
   using namespace cta::admin;

   const auto &name              = getRequired(OptionString::DISK_INSTANCE);
   const auto comment            = getOptional(OptionString::COMMENT);

   if(comment) {
      m_catalogue.DiskInstance()->modifyDiskInstanceComment(m_cliIdentity, name, comment.value());
   }

   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskInstance_Rm(cta::xrd::Response &response)
{
  using namespace cta::admin;

  const auto &name = getRequired(OptionString::DISK_INSTANCE);

  m_catalogue.DiskInstance()->deleteDiskInstance(name);

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
   
  m_catalogue.DiskInstanceSpace()->createDiskInstanceSpace(m_cliIdentity, name, diskInstance, freeSpaceQueryURL,
   refreshInterval, comment);

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
      m_catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceComment(m_cliIdentity, name, diskInstance,
         comment.value());
   }
   if(freeSpaceQueryURL) {
      m_catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceQueryURL(m_cliIdentity, name, diskInstance,
         freeSpaceQueryURL.value());
   }
   if(refreshInterval) {
      m_catalogue.DiskInstanceSpace()->modifyDiskInstanceSpaceRefreshInterval(m_cliIdentity, name, diskInstance,
         refreshInterval.value());
   }
   
   response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processDiskInstanceSpace_Rm(cta::xrd::Response &response)
{
  using namespace cta::admin;

  const auto &name         = getRequired(OptionString::DISK_INSTANCE_SPACE);
  const auto &diskInstance = getRequired(OptionString::DISK_INSTANCE);

  m_catalogue.DiskInstanceSpace()->deleteDiskInstanceSpace(name, diskInstance);

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

  m_catalogue.VO()->createVirtualOrganization(m_cliIdentity,vo);

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
    m_catalogue.VO()->modifyVirtualOrganizationComment(m_cliIdentity,name,comment.value());

  if(readMaxDrives)
    m_catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(m_cliIdentity,name,readMaxDrives.value());

  if(writeMaxDrives)
    m_catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(m_cliIdentity,name,writeMaxDrives.value());

  if(maxFileSize)
    m_catalogue.VO()->modifyVirtualOrganizationMaxFileSize(m_cliIdentity,name,maxFileSize.value());

  if(diskInstanceName)
    m_catalogue.VO()->modifyVirtualOrganizationDiskInstanceName(m_cliIdentity, name, diskInstanceName.value());

  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processVirtualOrganization_Rm(cta::xrd::Response& response) {
  using namespace cta::admin;

  const auto &name = getRequired(OptionString::VO);

  m_catalogue.VO()->deleteVirtualOrganization(name);

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

  const auto tapeDriveNames = m_catalogue.DriveState()->getTapeDriveNames();
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
  m_catalogue.FileRecycleLog()->restoreFileInRecycleLog(searchCriteria, std::to_string(fid));
  response.set_type(cta::xrd::Response::RSP_SUCCESS);
}

void RequestMessage::processModifyArchiveFile(cta::xrd::Response& response) {
   try {
      using namespace cta::admin;

      std::optional<std::string> newStorageClassName = getOptional(OptionString::STORAGE_CLASS_NAME);

      std::optional<std::string> fxId = getOptional(OptionString::FXID);
      std::optional<std::string> diskInstance = getOptional(OptionString::DISK_INSTANCE);

      auto archiveFileIds = getRequired(OptionStrList::FILE_ID);

      // call is from cta-change-storageclass
      if(newStorageClassName) {
         XrdCtaChangeStorageClass xrdCtaChangeStorageClass(m_catalogue, m_lc);
         xrdCtaChangeStorageClass.updateCatalogue(archiveFileIds, newStorageClassName.value());
      }
      // call is from cta-eos-namespace-inject
      if(fxId && diskInstance) {
         m_catalogue.ArchiveFile()->modifyArchiveFileFxIdAndDiskInstance(cta::utils::toUint64(archiveFileIds[0]),
            fxId.value(), diskInstance.value());
      }
      response.set_type(cta::xrd::Response::RSP_SUCCESS);
   } catch(exception::UserError &ue) {
      response.set_message_txt(ue.getMessage().str());
      response.set_type(Response::RSP_ERR_USER);
   }
}

}} // namespace cta::xrd
