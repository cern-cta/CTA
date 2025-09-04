#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/log/LogContext.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include <list>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class GroupMountRuleLsResponseStream : public CtaAdminResponseStream {
public:
  GroupMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                 cta::Scheduler& scheduler,
                                 const frontend::AdminCmdStream& requestMsg);
  bool isDone() override;
  cta::xrd::Data next() override;
  

private:
  std::list<cta::common::dataStructures::RequesterGroupMountRule> m_groupMountRules;
};

GroupMountRuleLsResponseStream::GroupMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                               cta::Scheduler& scheduler,
                                                               const frontend::AdminCmdStream& requestMsg)
    : CtaAdminResponseStream(catalogue, scheduler, requestMsg.getInstanceName()),
      m_groupMountRules(catalogue.RequesterGroupMountRule()->getRequesterGroupMountRules()) {}


bool GroupMountRuleLsResponseStream::isDone() {
  return m_groupMountRules.empty();
}

cta::xrd::Data GroupMountRuleLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto gmr = m_groupMountRules.front();
  m_groupMountRules.pop_front();

  cta::xrd::Data data;
  auto gmr_item = data.mutable_gmrls_item();

  gmr_item->set_instance_name(m_instanceName);
  gmr_item->set_disk_instance(gmr.diskInstance);
  gmr_item->set_group_mount_rule(gmr.name);
  gmr_item->set_mount_policy(gmr.mountPolicy);
  gmr_item->mutable_creation_log()->set_username(gmr.creationLog.username);
  gmr_item->mutable_creation_log()->set_host(gmr.creationLog.host);
  gmr_item->mutable_creation_log()->set_time(gmr.creationLog.time);
  gmr_item->mutable_last_modification_log()->set_username(gmr.lastModificationLog.username);
  gmr_item->mutable_last_modification_log()->set_host(gmr.lastModificationLog.host);
  gmr_item->mutable_last_modification_log()->set_time(gmr.lastModificationLog.time);
  gmr_item->set_comment(gmr.comment);

  return data;
}

}  // namespace cta::cmdline