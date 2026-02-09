/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "AdminCmdOptions.hpp"

#include "common/exception/UserError.hpp"
#include "common/utils/utils.hpp"
#include "tools/CtaAdminCmdParser.hpp"

namespace cta::frontend {

AdminCmdOptions::AdminCmdOptions(const admin::AdminCmd& adminCmd) {
  importOptions(adminCmd);
}

void AdminCmdOptions::importOptions(const admin::AdminCmd& adminCmd) {
  // Validate the Protocol Buffer
  validateCmd(adminCmd);

  // Import Boolean options
  std::for_each(adminCmd.option_bool().begin(), adminCmd.option_bool().end(), [this](auto opt) {
    m_option_bool.insert(std::make_pair(opt.key(), opt.value()));
  });

  // Import UInt64 options
  std::for_each(adminCmd.option_uint64().begin(), adminCmd.option_uint64().end(), [this](auto opt) {
    m_option_uint64.insert(std::make_pair(opt.key(), opt.value()));
  });

  // Import String options
  std::for_each(adminCmd.option_str().begin(), adminCmd.option_str().end(), [this](auto opt) {
    m_option_str.insert(std::make_pair(opt.key(), opt.value()));
  });

  // Import String List options
  for (auto opt_it = adminCmd.option_str_list().begin(); opt_it != adminCmd.option_str_list().end(); ++opt_it) {
    std::vector<std::string> items;
    for (auto item_it = opt_it->item().begin(); item_it != opt_it->item().end(); ++item_it) {
      items.push_back(*item_it);
    }
    m_option_str_list.try_emplace(opt_it->key(), items);
  }
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

}  // namespace cta::frontend
