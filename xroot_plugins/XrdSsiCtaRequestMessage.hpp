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
#include "cta_frontend.pb.h"



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

         // Map the client protcol string to an enum value
         auto proto_it = m_protomap.find(client.prot);
         m_protocol = proto_it != m_protomap.end() ? proto_it->second : Protocol::OTHER;
      }

   /*!
    * Process a Notification request or an Admin command request
    *
    * @param[in]     request
    * @param[out]    response        Response protocol buffer
    * @param[out]    stream          Reference to Response stream pointer
    */
   void process(const cta::xrd::Request &request, cta::xrd::Response &response, XrdSsiStream* &stream);

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
   admincmd_t processArchiveRoute_Add;
   admincmd_t processArchiveRoute_Ch;
   admincmd_t processArchiveRoute_Rm;
   admincmd_t processArchiveRoute_Ls;
   admincmd_t processDrive_Up;
   admincmd_t processDrive_Down;
   admincmd_t processDrive_Ls;
   admincmd_t processDrive_Rm;
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
    * "af ls" command
    *
    * This is a special case as it can return a protocol buffer (for the summary) or a stream (for
    * the full listing)
    *
    * @param[in]     admincmd        CTA Admin command request message
    * @param[out]    response        Response protocol buffer message
    * @param[out]    stream          Reference to Response stream message pointer
    */
   void processArchiveFile_Ls(const cta::admin::AdminCmd &admincmd, cta::xrd::Response &response, XrdSsiStream* &stream);

   /*!
    * Drive state enum
    */
   enum DriveState { Up, Down };

   /*!
    * Changes state for the drives by a given regex.
    *
    * @param[in]     regex          The regex to match drive name(s) to change
    * @param[in]     drive_state    The desired state for the drives (Up or Down)
    *
    * @returns       The result of the operation, to return to the client
    */
   std::string setDriveState(const std::string &regex, DriveState drive_state);

   /*!
    * Returns the response string for admin commands in a tabular format
    * 
    * @param[in]     responseTable   The response 2D matrix
    *
    * @returns       the response string properly formatted in a table
    */
   std::string formatResponse(const std::vector<std::vector<std::string>> &responseTable) const;

   /*!
    * Adds the creation log and the last modification log to the current response row
    * 
    * @param[in,out] responseRow            The current response row to modify
    * @param[in]     creationLog            the creation log
    * @param[in]     lastModificationLog    the last modification log
    */
   void addLogInfoToResponseRow(std::vector<std::string> &responseRow,
                                const cta::common::dataStructures::EntryLog &creationLog,
                                const cta::common::dataStructures::EntryLog &lastModificationLog) const;

   /*!
    * Import Google Protobuf option fields into maps
    *
    * @param[in]     admincmd        CTA Admin command request message
    */
   void importOptions(const cta::admin::AdminCmd &admincmd);

   /*!
    * Get a required option
    */
   const std::string &getRequired(cta::admin::OptionString::Key key) const {
      return m_option_str.at(key);
   }
   const uint64_t &getRequired(cta::admin::OptionUInt64::Key key) const {
      return m_option_uint64.at(key);
   }
   const bool &getRequired(cta::admin::OptionBoolean::Key key) const {
      return m_option_bool.at(key);
   }

   /*!
    * Get an optional option
    *
    * The has_option parameter is set if the option exists and left unmodified if the option does not
    * exist. This is provided as a convenience to monitor whether at least one option was set from a
    * list of optional options.
    *
    * @param[in]     key           option key to look up in options
    * @param[out]    has_option    Set to true if the option exists, unmodified if it does not
    *
    * @returns       value of the option if it exists, an object of type nullopt_t if it does not
    */
   template<typename K, typename V>
   optional<V> getOptional(K key, const std::map<K,V> &options, bool *has_option) const
   {
      auto it = options.find(key);

      if(it != options.end()) {
         if(has_option != nullptr) *has_option = true;
         return it->second;
      } else {
         return cta::nullopt;
      }
   }

   /*!
    * Overloaded versions of getOptional
    *
    * These map the key type to the template specialization of <key,value> pairs
    */
   optional<std::string> getOptional(cta::admin::OptionString::Key key, bool *has_option = nullptr) const {
      return getOptional(key, m_option_str, has_option);
   }
   optional<uint64_t> getOptional(cta::admin::OptionUInt64::Key key, bool *has_option = nullptr) const {
      return getOptional(key, m_option_uint64, has_option);
   }
   optional<bool> getOptional(cta::admin::OptionBoolean::Key key, bool *has_option = nullptr) const {
      return getOptional(key, m_option_bool, has_option);
   }

   /*!
    * Check if an optional flag has been set
    *
    * This is a simpler version of getOptional for checking flags which are either present
    * or not. In the case of flags, they should always have the value true if the flag is
    * present, but we do a redundant check anyway.
    *
    * @param[in] option    Optional command line option
    *
    * @retval    true      The flag is present in the options map, and its value is true
    * @retval    false     The flag is either not present or is present and set to false
    */
   bool has_flag(cta::admin::OptionBoolean::Key option) const {
      auto opt_it = m_option_bool.find(option);
      return opt_it != m_option_bool.end() && opt_it->second;
   }

   // Security protocol used to connect

   enum class Protocol { SSS, KRB5, OTHER };

   const std::map<std::string, Protocol> m_protomap = {
      { "sss",  Protocol::SSS  },
      { "krb5", Protocol::KRB5 },
   };

   // Member variables

   Protocol                                                m_protocol;               //!< The protocol the client used to connect
   cta::common::dataStructures::SecurityIdentity           m_cliIdentity;            //!< Client identity: username/host
   cta::catalogue::Catalogue                              &m_catalogue;              //!< Reference to CTA Catalogue
   cta::Scheduler                                         &m_scheduler;              //!< Reference to CTA Scheduler
   cta::log::LogContext                                    m_lc;                     //!< CTA Log Context
   std::map<cta::admin::OptionBoolean::Key, bool>          m_option_bool;            //!< Boolean options
   std::map<cta::admin::OptionUInt64::Key,  uint64_t>      m_option_uint64;          //!< UInt64 options
   std::map<cta::admin::OptionString::Key,  std::string>   m_option_str;             //!< String options
};

}} // namespace cta::xrd

