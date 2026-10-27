/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/AdminCmdLogParams.hpp"

#include "tools/CtaAdminCmdParser.hpp"

namespace cta::admin_api::commands {

void addAdminCmdLogParams(log::ScopedParamContainer& params, const admin::AdminCmd& adminCmd) {
  // Reverse lookup of strings corresponding to <command,subcommand> pair
  for (const auto& [cmd_name, cmd_value] : admin::cmdLookup) {
    // Return the matching long string (length > 3)
    if (adminCmd.cmd() == cmd_value && cmd_name.length() > 3) {
      params.add("command", cmd_name);
      break;
    }
  }
  for (const auto& [subcmd_name, subcmd_value] : admin::subcmdLookup) {
    if (adminCmd.subcmd() == subcmd_value) {
      params.add("subcommand", subcmd_name);
      break;
    }
  }

  // Options passed on the command line
  for (const auto& opt : adminCmd.option_bool()) {
    params.add(admin::OptionBoolean::Key_Name(opt.key()), opt.value() ? "true" : "false");
  }
  for (const auto& opt : adminCmd.option_uint64()) {
    params.add(admin::OptionUInt64::Key_Name(opt.key()), opt.value());
  }
  for (const auto& opt : adminCmd.option_str()) {
    params.add(admin::OptionString::Key_Name(opt.key()), opt.value());
  }
  for (const auto& opt : adminCmd.option_str_list()) {
    params.add(admin::OptionStrList::Key_Name(opt.key()), opt.item_size());
  }
}

}  // namespace cta::admin_api::commands
