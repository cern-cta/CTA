/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/AdminCmdOptions.hpp"

#include "common/exception/UserError.hpp"
#include "common/utils/utils.hpp"
#include "tools/CtaAdminCmdParser.hpp"

#include <ranges>

namespace cta::admin_api::commands {

AdminCmdOptions::AdminCmdOptions(const admin::AdminCmd& adminCmd) {
  // Validate the Protocol Buffer
  validateCmd(adminCmd);

  auto keyValue = [](const auto& opt) { return std::pair {opt.key(), opt.value()}; };

  // Import Boolean options
  std::ranges::transform(adminCmd.option_bool(), std::inserter(m_option_bool, m_option_bool.end()), keyValue);

  // Import UInt64 options
  std::ranges::transform(adminCmd.option_uint64(), std::inserter(m_option_uint64, m_option_uint64.end()), keyValue);

  // Import String options
  std::ranges::transform(adminCmd.option_str(), std::inserter(m_option_str, m_option_str.end()), keyValue);

  // Import String List options
  std::ranges::transform(adminCmd.option_str_list(),
                         std::inserter(m_option_str_list, m_option_str_list.end()),
                         [](const auto& opt) {
                           return std::pair {opt.key(), std::vector<std::string>(opt.item().begin(), opt.item().end())};
                         });
}

std::optional<std::string> AdminCmdOptions::getAndValidateDiskFileIdOptional(bool* has_any) const {
  using namespace cta::admin;
  auto diskFileIdHex = getOptional(OptionString::FXID, has_any);
  auto diskFileIdStr = getOptional(OptionString::DISK_FILE_ID, has_any);

  if (diskFileIdHex && diskFileIdStr) {
    throw exception::UserError("File ID can't be received in both string (" + diskFileIdStr.value()
                               + ") and hexadecimal (" + diskFileIdHex.value() + ") formats");
  }

  if (diskFileIdHex) {
    // If provided, convert FXID (hexadecimal) to DISK_FILE_ID (decimal)
    if (!cta::utils::isValidHex(diskFileIdHex.value())) {
      throw cta::exception::UserError(diskFileIdHex.value() + " is not a valid hexadecimal file ID value");
    }
    return cta::utils::hexadecimalToDecimal(diskFileIdHex.value());
  }

  return diskFileIdStr;
}

}  // namespace cta::admin_api::commands
