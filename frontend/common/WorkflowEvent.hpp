/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2023 CERN
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

#pragma once

#include "cta_frontend.pb.h"
#include "frontend/common/FrontendService.hpp"

namespace cta::frontend {

class WorkflowEvent {
public:
  WorkflowEvent(const frontend::FrontendService& frontendService,
    const common::dataStructures::SecurityIdentity& clientIdentity,
    const eos::Notification& event);

  ~WorkflowEvent() = default;

  /*!
   * Process the Workflow event
   *
   * @return Protobuf to return to the client
   */
  xrd::Response process();

private:
  /*!
   * Handlers for each Workflow event type
   *
   * Note: The OPENW event is not handled by CTA, as files destined for tape are immutable.
   *
   * @param[in]     event      Workflow event and metadata received from client
   * @param[out]    response   Response protobuf to return to client
   */
  void processOPENW         (xrd::Response& response);    //!< Open for write event
  void processCREATE        (xrd::Response& response);    //!< New archive file ID event
  void processCLOSEW        (xrd::Response& response);    //!< Archive file event
  void processPREPARE       (xrd::Response& response);    //!< Retrieve file event
  void processABORT_PREPARE (xrd::Response& response);    //!< Abort retrieve file event
  void processDELETE        (xrd::Response& response);    //!< Delete file event
  void processUPDATE_FID    (xrd::Response& response);    //!< Update disk file ID event

  /*!
   * Throw an exception for empty protocol buffer strings
   */
  void checkIsNotEmptyString(const std::string& value, const std::string& name) const {
    if(value.empty()) {
      throw exception::PbException("Protocol buffer field " + name + " is an empty string.");
    }
  }

  const eos::Notification                     m_event;                      //!< Workflow Event protocol buffer
  common::dataStructures::SecurityIdentity    m_cliIdentity;                //!< Client identity: username, host, authentication
  catalogue::Catalogue                       &m_catalogue;                  //!< Reference to CTA Catalogue
  cta::Scheduler                             &m_scheduler;                  //!< Reference to CTA Scheduler
  log::LogContext                             m_lc;                         //!< CTA Log Context
  std::string                                 m_verificationMountPolicy;    //!< Verification mount policy
};

} // namespace cta::frontend
