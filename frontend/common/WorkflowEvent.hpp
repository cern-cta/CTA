/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "frontend/common/FrontendService.hpp"
#include "frontend/common/PbException.hpp"

#include "cta_frontend.pb.h"

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
  void processOPENW(xrd::Response& response);          //!< Open for write event
  void processCREATE(xrd::Response& response);         //!< New archive file ID event
  void processCLOSEW(xrd::Response& response);         //!< Archive file event
  void processPREPARE(xrd::Response& response);        //!< Retrieve file event
  void processABORT_PREPARE(xrd::Response& response);  //!< Abort retrieve file event
  void processDELETE(xrd::Response& response);         //!< Delete file event
  void processUPDATE_FID(xrd::Response& response);     //!< Update disk file ID event

  /*!
   * Throw an exception for empty protocol buffer strings
   */
  void checkIsNotEmptyString(const std::string& value, const std::string& name) const {
    if (value.empty()) {
      throw exception::PbException("Protocol buffer field " + name + " is an empty string.");
    }
  }

  const eos::Notification m_event;                         //!< Workflow Event protocol buffer
  common::dataStructures::SecurityIdentity m_cliIdentity;  //!< Client identity: username, host, authentication
  catalogue::Catalogue& m_catalogue;                       //!< Reference to CTA Catalogue
  cta::Scheduler& m_scheduler;                             //!< Reference to CTA Scheduler
  log::LogContext m_lc;                                    //!< CTA Log Context
  std::string m_verificationMountPolicy;                   //!< Verification mount policy

  bool m_zeroLengthFilesDisallowed;  //!< Do not allow 0-length files to be archived
  std::set<std::string>
    m_zeroLengthFilesDisallowedExceptions;  //!< Virtual Organizations (VOs) exempted from the 0-length file rule (if enabled)
};

}  // namespace cta::frontend
