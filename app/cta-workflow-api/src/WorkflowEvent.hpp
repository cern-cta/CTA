/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "WorkflowApiConfig.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "scheduler/batching/FrontendBatchingLayer.hpp"
#include "service-resources/ServiceResources.hpp"

#include <optional>
#include <string>

#include "cta_frontend.pb.h"

namespace cta::frontend {

/*!
 * Helpers extracting fields from a workflow event. Exposed for unit testing only.
 */
namespace proto_notifications {

/*!
 * Validate and extract the identity of the user who triggered the event
 *
 * @throws exception::PbException if the username or groupname is empty
 */
common::dataStructures::RequesterIdentity getRequester(const eos::Notification& event);

/*!
 * Extract the archive file ID: the first-class `archive_file_id` field if non-zero, otherwise the
 * `sys.archive.file_id` extended attribute, otherwise the legacy `CTA_ArchiveFileId` extended attribute.
 * The extended attribute must be a positive decimal integer with no leading or trailing characters.
 *
 * @throws exception::PbException if no ID is present or the extended attribute is not a valid ID
 */
uint64_t getArchiveFileId(const eos::Notification& event);

/*!
 * Extract the scheduler request ID. The first-class attribute takes priority; if it is empty, fall back to the given
 * extended attribute
 *
 * @return std::nullopt if neither is present
 */
std::optional<std::string> getRequestId(const eos::Notification& event, const std::string& xattrKey);

}  // namespace proto_notifications

class WorkflowEvent {
public:
  WorkflowEvent(const service_resources::ServiceResources& serviceResources,
                const workflow_api::WorkflowApiConfig& config,
                const common::dataStructures::SecurityIdentity& clientIdentity,
#ifdef CTA_PGSCHED
                FrontendBatchingLayer& batchingLayer,
#endif
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

  const eos::Notification m_event;                         //!< Workflow Event protocol buffer
  common::dataStructures::SecurityIdentity m_cliIdentity;  //!< Client identity: username, host, authentication
  catalogue::Catalogue& m_catalogue;                       //!< Reference to CTA Catalogue
  cta::Scheduler& m_scheduler;                             //!< Reference to CTA Scheduler
#ifdef CTA_PGSCHED
  cta::FrontendBatchingLayer*
    m_batchingLayer;  //!< Opportunistic-batching layer for archive/retrieve queueing (pgsched only)
#endif
  log::LogContext m_lc;                   //!< CTA Log Context
  std::string m_verificationMountPolicy;  //!< Verification mount policy

  bool m_zeroLengthFilesDisallowed;  //!< Do not allow 0-length files to be archived
  std::set<std::string, std::less<>>
    m_zeroLengthFilesDisallowedExceptions;  //!< Virtual Organizations (VOs) exempted from the 0-length file rule (if enabled)
};

}  // namespace cta::frontend
