/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include "LogicalLibraryLsResponseStream.hpp"
#include "frontend/common/AdminCmdOptions.hpp"

namespace cta::frontend {

LogicalLibraryLsResponseStream::LogicalLibraryLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                               cta::Scheduler& scheduler,
                                                               const std::string& instanceName,
                                                               const admin::AdminCmd& adminCmd)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName) {
  cta::frontend::AdminCmdOptions request(adminCmd);

  std::optional<bool> disabled = request.getOptional(cta::admin::OptionBoolean::DISABLED);
  m_logicalLibraries = m_catalogue.LogicalLibrary()->getLogicalLibraries();

  if (disabled.has_value()) {
    std::list<cta::common::dataStructures::LogicalLibrary>::iterator next_ll = m_logicalLibraries.begin();
    while (next_ll != m_logicalLibraries.end()) {
      if (disabled.value() != (*next_ll).isDisabled) {
        next_ll = m_logicalLibraries.erase(next_ll);
      } else {
        ++next_ll;
      }
    }
  }
}

bool LogicalLibraryLsResponseStream::isDone() {
  return m_logicalLibraries.empty();
}

cta::xrd::Data LogicalLibraryLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto ll = m_logicalLibraries.front();
  m_logicalLibraries.pop_front();

  cta::xrd::Data data;
  auto ll_item = data.mutable_llls_item();

  ll_item->set_name(ll.name);
  ll_item->set_is_disabled(ll.isDisabled);
  if (ll.physicalLibraryName) {
    ll_item->set_physical_library(ll.physicalLibraryName.value());
  }
  if (ll.disabledReason) {
    ll_item->set_disabled_reason(ll.disabledReason.value());
  }
  ll_item->mutable_creation_log()->set_username(ll.creationLog.username);
  ll_item->mutable_creation_log()->set_host(ll.creationLog.host);
  ll_item->mutable_creation_log()->set_time(ll.creationLog.time);
  ll_item->mutable_last_modification_log()->set_username(ll.lastModificationLog.username);
  ll_item->mutable_last_modification_log()->set_host(ll.lastModificationLog.host);
  ll_item->mutable_last_modification_log()->set_time(ll.lastModificationLog.time);
  ll_item->set_comment(ll.comment);
  ll_item->set_instance_name(m_instanceName);

  return data;
}

}  // namespace cta::frontend
