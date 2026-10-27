/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/sync/SyncCmd.hpp"

#include "commands/sync/SyncCmdRegistry.hpp"
#include "common/exception/UserError.hpp"
#include "common/semconv/Attributes.hpp"
#include "grpc-server/PbException.hpp"
#include "grpc-server/server/RequestTracker.hpp"

namespace cta::admin_api::commands::sync {

SyncCmd::SyncCmd(const service_resources::ServiceResources& serviceResources,
                 const admin_api::AdminApiConfig& config,
                 const log::LogContext& lc,
                 const common::dataStructures::SecurityIdentity& clientIdentity,
                 const admin::AdminCmd& adminCmd)
    : m_adminCmd(adminCmd),
      m_options(adminCmd),
      m_lc(lc),
      m_cliIdentity(clientIdentity),
      m_context {serviceResources.getCatalogue(),
                 serviceResources.getScheduler(),
                 m_lc,
                 m_cliIdentity,
                 config.policies,
                 config.defaults} {
  m_context.scheduler.authorizeAdmin(m_cliIdentity, m_lc);
}

xrd::Response SyncCmd::process() {
  cta::grpc_server::RequestTracker requestTracker("ADMIN", "admin");
  xrd::Response response;

  try {
    // Check if admin command is explicitly disabled
    if (isCommandDisabled(m_context.policies.admin_mode, m_adminCmd)) {
      throw DisabledAdminCmd(m_adminCmd);
    }

    const SyncCmdHandler handler = findHandler(m_adminCmd);
    if (handler == nullptr) {
      throw exception::PbException("Admin command pair <" + AdminCmd_Cmd_Name(m_adminCmd.cmd()) + ", "
                                   + AdminCmd_SubCmd_Name(m_adminCmd.subcmd()) + "> is not implemented.");
    }
    // call the actual command handler
    const std::optional<std::string> message = handler(m_context, m_options);

    response.set_type(xrd::Response::RSP_SUCCESS);
    if (message.has_value()) {
      response.set_message_txt(message.value());
    }
  } catch (exception::PbException&) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    throw;
  } catch (exception::UserError&) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kUserError);
    throw;
  } catch (exception::Exception&) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    throw;
  } catch (std::runtime_error&) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    throw;
  }
  return response;
}

}  // namespace cta::admin_api::commands::sync
