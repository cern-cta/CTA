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

   // Member variables

   cta::Scheduler       &m_scheduler;        //!< Reference to CTA Scheduler
   cta::log::LogContext  m_lc;               //!< CTA Log Context
   const char * const    m_instance_name;    //!< Instance name = XRootD client name
};

}} // namespace cta::xrd
