#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/log/LogContext.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include <list>
#include "common/checksum/ChecksumBlobSerDeser.hpp"

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class ActivityMountRuleLsResponseStream : public CtaAdminResponseStream {
public:
  ActivityMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                    cta::Scheduler& scheduler,
                                    const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;
  void init(const admin::AdminCmd& admincmd) override;

private:
  std::list<cta::common::dataStructures::RequesterActivityMountRule> m_activityMountRules;
};

ActivityMountRuleLsResponseStream::ActivityMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                                     cta::Scheduler& scheduler,
                                                                     const std::string& instanceName)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName) {}

void ActivityMountRuleLsResponseStream::init(const admin::AdminCmd& admincmd) {
  m_activityMountRules = m_catalogue.RequesterActivityMountRule()->getRequesterActivityMountRules();
}

bool ActivityMountRuleLsResponseStream::isDone() {
  return m_activityMountRules.empty();
}

cta::xrd::Data ActivityMountRuleLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto amr = m_activityMountRules.front();
  m_activityMountRules.pop_front();

  cta::xrd::Data data;
  auto amr_item = data.mutable_amrls_item();

  amr_item->set_disk_instance(amr.diskInstance);
  amr_item->set_activity_mount_rule(amr.name);
  amr_item->set_mount_policy(amr.mountPolicy);
  amr_item->set_activity_regex(amr.activityRegex);
  amr_item->mutable_creation_log()->set_username(amr.creationLog.username);
  amr_item->mutable_creation_log()->set_host(amr.creationLog.host);
  amr_item->mutable_creation_log()->set_time(amr.creationLog.time);
  amr_item->mutable_last_modification_log()->set_username(amr.lastModificationLog.username);
  amr_item->mutable_last_modification_log()->set_host(amr.lastModificationLog.host);
  amr_item->mutable_last_modification_log()->set_time(amr.lastModificationLog.time);
  amr_item->set_comment(amr.comment), amr_item->set_instance_name(m_instanceName);

  return data;
}

}  // namespace cta::cmdline