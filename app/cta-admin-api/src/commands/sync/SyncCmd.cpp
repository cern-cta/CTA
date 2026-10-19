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
#include "tools/CtaAdminCmdParser.hpp"

namespace cta::admin_api::commands::sync {

SyncCmd::SyncCmd(const service_resources::ServiceResources& serviceResources,
                 const admin_api::AdminApiConfig& config,
                 const common::dataStructures::SecurityIdentity& clientIdentity,
                 const admin::AdminCmd& adminCmd)
    : m_adminCmd(adminCmd),
      m_options(adminCmd),
      m_lc(serviceResources.getLogContext()),
      m_cliIdentity(clientIdentity),
      m_context {serviceResources.getCatalogue(),
                 serviceResources.getScheduler(),
                 m_lc,
                 m_cliIdentity,
                 serviceResources.getScheduler().getSchedulerBackendName(),
                 config.admin.mode,
                 config.archive.max_file_size_gb * 1000 * 1000 * 1000,
                 config.repack.buffer_url,
                 config.repack.max_files_to_select,
                 serviceResources.getRecycleLogQuarantineSecs()} {
  m_lc.push({"user", m_cliIdentity.username});

  m_context.scheduler.authorizeAdmin(m_cliIdentity, m_lc);
}

xrd::Response SyncCmd::process() {
  cta::grpc_server::RequestTracker requestTracker("ADMIN", "admin");
  xrd::Response response;
  utils::Timer t;

  try {
    // Check if admin command is explicitly disabled
    if (isCommandDisabled(m_context.adminCommandMode, m_adminCmd)) {
      throw cta::exception::UserError(DISABLED_ADMIN_CMD_MSG);
    }

    const SyncCmdHandler handler = findHandler(m_adminCmd);
    if (handler == nullptr) {
      throw exception::PbException("Admin command pair <" + AdminCmd_Cmd_Name(m_adminCmd.cmd()) + ", "
                                   + AdminCmd_SubCmd_Name(m_adminCmd.subcmd()) + "> is not implemented.");
    }
    // A handler only sets the message, or reports a user error, when it needs to
    response.set_type(xrd::Response::RSP_SUCCESS);
    handler(m_context, m_options, response);

    // Log the admin command
    logSyncCmd(SyncCmdStatus::SUCCESS, "", t);
  } catch (exception::PbException& ex) {
    logSyncCmd(SyncCmdStatus::EXCEPTION, ex.what(), t);
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    throw ex;
  } catch (exception::UserError& ex) {
    logSyncCmd(SyncCmdStatus::USER_ERROR, ex.getMessageValue(), t);
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kUserError);
    throw ex;
  } catch (exception::Exception& ex) {
    logSyncCmd(SyncCmdStatus::EXCEPTION, ex.what(), t);
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    throw ex;
  } catch (std::runtime_error& ex) {
    logSyncCmd(SyncCmdStatus::EXCEPTION, ex.what(), t);
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    throw ex;
  }
  return response;
}

void SyncCmd::logSyncCmd(const SyncCmdStatus status, const std::string& reason, utils::Timer& t) {
  // We do the metric recording here to prevent repetition in the catch statements
  std::string statusStr;
  switch (status) {
    case SyncCmdStatus::SUCCESS: {
      statusStr = "success";
      break;
    }
    case SyncCmdStatus::USER_ERROR: {
      statusStr = "user_error";
      break;
    }
    case SyncCmdStatus::EXCEPTION: {
      statusStr = "failure";
      break;
    }
  }

  log::ScopedParamContainer params(m_lc);

  // Reverse lookup of strings corresponding to <command,subcommand> pair
  for (const auto& [cmd_name, cmd_value] : admin::cmdLookup) {
    // Return the matching long string (length > 3)
    if (m_adminCmd.cmd() == cmd_value && cmd_name.length() > 3) {
      params.add("command", cmd_name);
      break;
    }
  }
  for (const auto& [subcmd_name, subcmd_value] : admin::subcmdLookup) {
    if (m_adminCmd.subcmd() == subcmd_value) {
      params.add("subcommand", subcmd_name);
      break;
    }
  }

  params.add("status", statusStr);

  if (!reason.empty()) {
    params.add("reason", reason);
  }

  // Log options passed from the command line
  std::pair<admin::AdminCmd::Cmd, admin::AdminCmd::SubCmd> cmd_key(m_adminCmd.cmd(), m_adminCmd.subcmd());
  std::vector<admin::Option> cmd_options = admin::cmdOptions.at(cmd_key);

  for (const auto& cmd_option : cmd_options) {
    bool has_option = false;
    const auto& lookup_key = cmd_option.get_key();
    // Lookup if command line option was used in the command
    switch (cmd_option.get_type()) {
      case admin::Option::option_t::OPT_FLAG:  // Treat flag options as bool options
      case admin::Option::option_t::OPT_BOOL: {
        auto bool_key = admin::boolOptions.at(lookup_key);
        auto opt_value = m_options.getOptional(bool_key, &has_option);
        if (has_option) {
          auto descriptor = admin::OptionBoolean::Key_descriptor();
          auto bool_key_name = descriptor->FindValueByNumber(bool_key)->name();
          params.add(bool_key_name, opt_value.value() ? "true" : "false");
        }
      } break;
      case admin::Option::option_t::OPT_UINT: {
        auto int_key = admin::uint64Options.at(lookup_key);
        auto opt_value = m_options.getOptional(int_key, &has_option);
        if (has_option) {
          auto descriptor = admin::OptionUInt64::Key_descriptor();
          auto int_key_name = descriptor->FindValueByNumber(int_key)->name();
          params.add(int_key_name, opt_value.value());
        }
      } break;
      case admin::Option::option_t::OPT_CMD:  // Treat command options as string options
      case admin::Option::option_t::OPT_STR: {
        auto string_key = admin::strOptions.at(lookup_key);
        auto opt_value = m_options.getOptional(string_key, &has_option);
        if (has_option) {
          auto descriptor = admin::OptionString::Key_descriptor();
          auto string_key_name = descriptor->FindValueByNumber(string_key)->name();
          params.add(string_key_name, opt_value.value());
        }
      } break;
      case admin::Option::option_t::OPT_STR_LIST: {
        auto string_list_key = admin::strListOptions.at(lookup_key);
        auto opt_value = m_options.getOptional(string_list_key, &has_option);
        if (has_option) {
          auto descriptor = admin::OptionStrList::Key_descriptor();
          auto string_list_key_name = descriptor->FindValueByNumber(string_list_key)->name();
          params.add(string_list_key_name, cmd_option.get_help_text());
        }
      } break;
    }  // end switch
  }  // end for
  params.add("adminTime", t.secs());
  params.add("user", m_cliIdentity.username);
  m_lc.log(cta::log::INFO, "Admin command was processed");
}

}  // namespace cta::admin_api::commands::sync
