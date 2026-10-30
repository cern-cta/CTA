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

namespace cta::workflow_api {

/// Helpers extracting fields from a workflow event. Exposed for unit testing only.
namespace proto_notifications {

/// Validate and extract the identity of the user who triggered the event
///
/// @throws exception::PbException if the username or groupname is empty
common::dataStructures::RequesterIdentity getRequester(const eos::Notification& event);

/// Extract the archive file ID: the first-class `archive_file_id` field if non-zero, otherwise the
/// `sys.archive.file_id` extended attribute, otherwise the legacy `CTA_ArchiveFileId` extended attribute.
/// The extended attribute must be a positive decimal integer with no leading or trailing characters.
///
/// @throws exception::PbException if no ID is present or the extended attribute is not a valid ID
uint64_t getArchiveFileId(const eos::Notification& event);

/// Extract the scheduler request ID. The first-class attribute takes priority; if it is empty, fall back to the given
/// extended attribute
///
/// @return std::nullopt if neither is present
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

  /// Process the Workflow event
  ///
  /// @return Protobuf to return to the client
  xrd::Response process();

private:
  /// Open for write event. Not handled by CTA, as files destined for tape are immutable: only acknowledged.
  void processOPENW(xrd::Response& response);
  /// New archive file ID event
  void processCREATE(xrd::Response& response);
  /// Archive file event
  void processCLOSEW(xrd::Response& response);
  /// Retrieve file event
  void processPREPARE(xrd::Response& response);
  /// Abort retrieve file event
  void processABORT_PREPARE(xrd::Response& response);
  /// Delete file event
  void processDELETE(xrd::Response& response);
  /// Update disk file ID event
  void processUPDATE_FID(xrd::Response& response);

  /// Workflow Event protocol buffer
  const eos::Notification m_event;
  /// Client identity: username, host, authentication
  common::dataStructures::SecurityIdentity m_cliIdentity;
  /// Reference to CTA Catalogue
  catalogue::Catalogue& m_catalogue;
  /// Reference to CTA Scheduler
  cta::Scheduler& m_scheduler;
#ifdef CTA_PGSCHED
  /// Opportunistic-batching layer for archive/retrieve queueing (pgsched only)
  cta::FrontendBatchingLayer* m_batchingLayer;
#endif
  /// CTA Log Context
  log::LogContext m_lc;
  /// Verification mount policy
  std::string m_verificationMountPolicy;

  /// Do not allow 0-length files to be archived
  bool m_zeroLengthFilesDisallowed;
  /// Virtual Organizations (VOs) exempted from the 0-length file rule (if enabled)
  std::set<std::string, std::less<>> m_zeroLengthFilesDisallowedExceptions;
};

}  // namespace cta::workflow_api
