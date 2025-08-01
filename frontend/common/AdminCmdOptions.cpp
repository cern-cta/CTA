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

#include "AdminCmdOptions.hpp"
#include "cmdline/CtaAdminCmdParser.hpp"

namespace cta::frontend {

void AdminCmdOptions::importOptions(const admin::AdminCmd& adminCmd) {
  // Validate the Protocol Buffer
  validateCmd(adminCmd);

  // Import Boolean options
  std::for_each(adminCmd.option_bool().begin(),
                adminCmd.option_bool().end(),
                [this](auto opt){
                  m_option_bool.insert(std::make_pair(opt.key(), opt.value()));
                });

  // Import UInt64 options
  std::for_each(adminCmd.option_uint64().begin(),
                adminCmd.option_uint64().end(),
                [this](auto opt){
                  m_option_uint64.insert(std::make_pair(opt.key(), opt.value()));
                });

  // Import String options
  std::for_each(adminCmd.option_str().begin(),
                adminCmd.option_str().end(),
                [this](auto opt){
                  m_option_str.insert(std::make_pair(opt.key(), opt.value()));
                });

  // Import String List options
  for(auto opt_it = adminCmd.option_str_list().begin(); opt_it != adminCmd.option_str_list().end(); ++opt_it) {
    std::vector<std::string> items;
    for(auto item_it = opt_it->item().begin(); item_it != opt_it->item().end(); ++item_it) {
      items.push_back(*item_it);
    }
    m_option_str_list.try_emplace(opt_it->key(), items);
  }
}

} // namespace cta::frontend