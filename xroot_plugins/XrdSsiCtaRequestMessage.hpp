/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend Message handler
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

#pragma once

#include <XrdSsi/XrdSsiEntity.hh>

#include "XrdSsiCtaServiceProvider.hpp"
#include "xroot_plugins/messages/cta_frontend.pb.h"

namespace cta { namespace xrd {

/*!
 * CTA Frontend Request Message class
 */
class RequestMessage
{
public:
   RequestMessage(const XrdSsiEntity &client, const XrdSsiCtaServiceProvider *service) :
      m_catalogue(service->getCatalogue()),
      m_scheduler(service->getScheduler()),
      m_lc       (service->getLogContext()) {
         m_cliIdentity.username = client.name;
         m_cliIdentity.host     = client.host;
      }

   /*!
    * Process the Notification request
    *
    * @param[in]     request
    * @param[out]    response        Response message to return to EOS
    */
   void process(const cta::xrd::Request &request, cta::xrd::Response &response);

private:
   /*!
    * Process Notification events
    *
    * @param[in]     notification    Notification request message from EOS WFE
    * @param[out]    response        Response message to return to EOS
    */
   typedef void notification_event_t(const cta::eos::Notification &notification, cta::xrd::Response &response);

   notification_event_t processCLOSEW;       //!< Archive file event
   notification_event_t processPREPARE;      //!< Retrieve file event
   notification_event_t processDELETE;       //!< Delete file event

   /*!
    * Process AdminCmd events
    *
    * @param[in]     admincmd        CTA Admin command request message
    * @param[out]    response        CTA Admin command response message
    */
   typedef void admincmd_t(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response);

   admincmd_t processAdmin_Add;
   admincmd_t processAdmin_Ch;
   admincmd_t processAdmin_Rm;
   admincmd_t processAdmin_Ls;
   admincmd_t processAdminHost_Add;
   admincmd_t processAdminHost_Ch;
   admincmd_t processAdminHost_Rm;
   admincmd_t processAdminHost_Ls;
   admincmd_t processArchiveFile_Ls;
   admincmd_t processArchiveRoute_Add;
   admincmd_t processArchiveRoute_Ch;
   admincmd_t processArchiveRoute_Rm;
   admincmd_t processArchiveRoute_Ls;
   admincmd_t processDrive_Up;
   admincmd_t processDrive_Down;
   admincmd_t processDrive_Ls;
   admincmd_t processGroupMountRule_Add;
   admincmd_t processGroupMountRule_Ch;
   admincmd_t processGroupMountRule_Rm;
   admincmd_t processGroupMountRule_Ls;
   admincmd_t processListPendingArchives;
   admincmd_t processListPendingRetrieves;
   admincmd_t processLogicalLibrary_Add;
   admincmd_t processLogicalLibrary_Ch;
   admincmd_t processLogicalLibrary_Rm;
   admincmd_t processLogicalLibrary_Ls;
   admincmd_t processMountPolicy_Add;
   admincmd_t processMountPolicy_Ch;
   admincmd_t processMountPolicy_Rm;
   admincmd_t processMountPolicy_Ls;
   admincmd_t processRepack_Add;
   admincmd_t processRepack_Rm;
   admincmd_t processRepack_Ls;
   admincmd_t processRepack_Err;
   admincmd_t processRequesterMountRule_Add;
   admincmd_t processRequesterMountRule_Ch;
   admincmd_t processRequesterMountRule_Rm;
   admincmd_t processRequesterMountRule_Ls;
   admincmd_t processShrink;
   admincmd_t processShowQueues;
   admincmd_t processStorageClass_Add;
   admincmd_t processStorageClass_Ch;
   admincmd_t processStorageClass_Rm;
   admincmd_t processStorageClass_Ls;
   admincmd_t processTape_Add;
   admincmd_t processTape_Ch;
   admincmd_t processTape_Rm;
   admincmd_t processTape_Reclaim;
   admincmd_t processTape_Ls;
   admincmd_t processTape_Label;
   admincmd_t processTapePool_Add;
   admincmd_t processTapePool_Ch;
   admincmd_t processTapePool_Rm;
   admincmd_t processTapePool_Ls;
   admincmd_t processTest_Read;
   admincmd_t processTest_Write;
   admincmd_t processTest_WriteAuto;
   admincmd_t processVerify_Add;
   admincmd_t processVerify_Rm;
   admincmd_t processVerify_Ls;
   admincmd_t processVerify_Err;

   /*!
    * Convert time to string
    */
   std::string timeToString(const time_t &time) {
      std::string timeString(ctime(&time));
      timeString.resize(timeString.size()-1); //remove newline
      return timeString;
   }

   /*!
    * Import Google Protobuf option fields into maps
    *
    * @param[in]     admincmd        CTA Admin command request message
    */
   void importOptions(const cta::admin::AdminCmd &admincmd);

   /*!
    * Returns the response string for admin commands in a tabular format
    * 
    * @param[in]     responseTable   The response 2D matrix
    *
    * @returns       the response string properly formatted in a table
    */
   std::string formatResponse(const std::vector<std::vector<std::string>> &responseTable, bool has_header);

   /*!
    * Adds the creation log and the last modification log to the current response row
    * 
    * @param[in,out] responseRow            The current response row to modify
    * @param[in]     creationLog            the creation log
    * @param[in]     lastModificationLog    the last modification log
    */
   void addLogInfoToResponseRow(std::vector<std::string> &responseRow,
                                const cta::common::dataStructures::EntryLog &creationLog,
                                const cta::common::dataStructures::EntryLog &lastModificationLog);

   // Member variables

   cta::catalogue::Catalogue                            &m_catalogue;        //!< Reference to CTA Catalogue
   cta::Scheduler                                       &m_scheduler;        //!< Reference to CTA Scheduler
   cta::log::LogContext                                  m_lc;               //!< CTA Log Context
   cta::common::dataStructures::SecurityIdentity         m_cliIdentity;      //!< The client identity info: username and host
   std::map<cta::admin::OptionBoolean::Key, bool>        m_option_bool;      //!< Boolean options
   std::map<cta::admin::OptionUInt64::Key,  uint64_t>    m_option_uint64;    //!< UInt64 options
   std::map<cta::admin::OptionString::Key,  std::string> m_option_str;       //!< String options
};

}} // namespace cta::xrd
