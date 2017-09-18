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
      m_lc(service->getLogContext()),
      m_instance_name(client.name) {}

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

   admincmd_t processListPendingRetrieves;

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

   // Member variables

   cta::catalogue::Catalogue                            &m_catalogue;        //!< Reference to CTA Catalogue
   cta::Scheduler                                       &m_scheduler;        //!< Reference to CTA Scheduler
   cta::log::LogContext                                  m_lc;               //!< CTA Log Context
   const char * const                                    m_instance_name;    //!< Instance name = XRootD client name
   std::map<cta::admin::OptionBoolean::Key, bool>        m_option_bool;      //!< Boolean options
   std::map<cta::admin::OptionUInt64::Key,  uint64_t>    m_option_uint64;    //!< UInt64 options
   std::map<cta::admin::OptionString::Key,  std::string> m_option_str;       //!< String options
};

}} // namespace cta::xrd
