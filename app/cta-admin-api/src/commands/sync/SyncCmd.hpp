/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AdminApiConfig.hpp"
#include "commands/AdminCmdOptions.hpp"
#include "commands/sync/SyncCmdContext.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/Timer.hpp"
#include "service-resources/ServiceResources.hpp"

#include <string>

#include "cta_frontend.pb.h"

namespace cta::admin_api::commands::sync {

enum class SyncCmdStatus { SUCCESS, USER_ERROR, EXCEPTION };

/// @brief Processes one non-streamed admin command: authorizes the admin, runs the handler of the command (see
/// SyncCmdRegistry and handlers/Handlers.hpp) and logs the outcome
class SyncCmd {
public:
  /// @throws cta::exception::UserError if the client is not an admin
  SyncCmd(const service_resources::ServiceResources& serviceResources,
          const admin_api::AdminApiConfig& config,
          const common::dataStructures::SecurityIdentity& clientIdentity,
          const admin::AdminCmd& adminCmd);

  /// m_context refers to the other members
  SyncCmd(const SyncCmd&) = delete;
  SyncCmd& operator=(const SyncCmd&) = delete;

  /// Process the admin command
  ///
  /// @return Protobuf to return to the client
  xrd::Response process();

private:
  /// Log an admin command
  ///
  /// @param[in]    status      Status of the executed admin command
  /// @param[in]    reason      Reason for this log message (error message)
  /// @param[in]    t           Timer
  void logSyncCmd(const SyncCmdStatus status, const std::string& reason, utils::Timer& t);

  /// Administrator Command protocol buffer
  const admin::AdminCmd m_adminCmd;
  /// Options of the command
  const AdminCmdOptions m_options;
  /// CTA Log Context
  log::LogContext m_lc;
  /// Client identity: username, host, authentication
  common::dataStructures::SecurityIdentity m_cliIdentity;
  /// What the handlers need, refers to the members above
  const SyncCmdContext m_context;
};

}  // namespace cta::admin_api::commands::sync
