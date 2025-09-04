#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/log/LogContext.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include <list>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class AdminLsResponseStream : public CtaAdminResponseStream {
public:
  AdminLsResponseStream(cta::catalogue::Catalogue& catalogue,
                        cta::Scheduler& scheduler,
                        const frontend::AdminCmdStream& requestMsg);

  bool isDone() override;
  cta::xrd::Data next() override;
  

private:
  std::list<cta::common::dataStructures::AdminUser> m_adminUsers;
};

AdminLsResponseStream::AdminLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                             cta::Scheduler& scheduler,
                                             const frontend::AdminCmdStream& requestMsg)
    : CtaAdminResponseStream(catalogue, scheduler, requestMsg.getInstanceName()),
      m_adminUsers(catalogue.AdminUser()->getAdminUsers()) {}


bool AdminLsResponseStream::isDone() {
  return m_adminUsers.empty();
}

cta::xrd::Data AdminLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto ad = m_adminUsers.front();
  m_adminUsers.pop_front();

  cta::xrd::Data data;
  auto ad_item = data.mutable_adls_item();

  ad_item->set_user(ad.name);
  ad_item->mutable_creation_log()->set_username(ad.creationLog.username);
  ad_item->mutable_creation_log()->set_host(ad.creationLog.host);
  ad_item->mutable_creation_log()->set_time(ad.creationLog.time);
  ad_item->mutable_last_modification_log()->set_username(ad.lastModificationLog.username);
  ad_item->mutable_last_modification_log()->set_host(ad.lastModificationLog.host);
  ad_item->mutable_last_modification_log()->set_time(ad.lastModificationLog.time);
  ad_item->set_comment(ad.comment);
  ad_item->set_instance_name(m_instanceName);

  return data;
}

}  // namespace cta::cmdline