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

#include "cmdline/CtaAdminCmdParse.hpp"
#include "frontend/common/AdminCmd.hpp"

namespace cta {
namespace frontend {

AdminCmd::AdminCmd(const frontend::FrontendService& frontendService,
  const common::dataStructures::SecurityIdentity& clientIdentity,
  const admin::AdminCmd& adminCmd) :
  m_cliIdentity(clientIdentity),
  m_catalogue(frontendService.getCatalogue()),
  m_scheduler(frontendService.getScheduler()),
  m_lc(frontendService.getLogContext())
{
  // Check that the user is authorized
  m_scheduler.authorizeAdmin(m_cliIdentity, m_lc);

  // Validate the Protocol Buffer and import options into maps
  importOptions(adminCmd);
  m_clientVersion.ctaVersion  = adminCmd.client_version();
  m_clientVersion.protobufTag = adminCmd.protobuf_tag();
}

xrd::Response AdminCmd::process() {
  xrd::Response response;

  return response;
}

void AdminCmd::importOptions(const admin::AdminCmd& adminCmd)
{
  // Validate the Protocol Buffer
  validateCmd(adminCmd);

  // Import Boolean options
  for(auto opt_it = adminCmd.option_bool().begin(); opt_it != adminCmd.option_bool().end(); ++opt_it) {
    m_option_bool.insert(std::make_pair(opt_it->key(), opt_it->value()));
  }

  // Import UInt64 options
  for(auto opt_it = adminCmd.option_uint64().begin(); opt_it != adminCmd.option_uint64().end(); ++opt_it) {
    m_option_uint64.insert(std::make_pair(opt_it->key(), opt_it->value()));
  }

  // Import String options
  for(auto opt_it = adminCmd.option_str().begin(); opt_it != adminCmd.option_str().end(); ++opt_it) {
    m_option_str.insert(std::make_pair(opt_it->key(), opt_it->value()));
  }

  // Import String List options
  for(auto opt_it = adminCmd.option_str_list().begin(); opt_it != adminCmd.option_str_list().end(); ++opt_it) {
    std::vector<std::string> items;
    for(auto item_it = opt_it->item().begin(); item_it != opt_it->item().end(); ++item_it) {
      items.push_back(*item_it);
    }
    m_option_str_list.insert(std::make_pair(opt_it->key(), items));
  }
}

}} // namespace cta::frontend
