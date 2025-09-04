#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include <list>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class LogicalLibraryLsResponseStream : public CtaAdminResponseStream {
public:
  LogicalLibraryLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                 cta::Scheduler& scheduler,
                                 const frontend::AdminCmdStream& admincmd);

  bool isDone() override;
  cta::xrd::Data next() override;
  

private:
  std::list<cta::common::dataStructures::LogicalLibrary> m_logicalLibraries;
};

LogicalLibraryLsResponseStream::LogicalLibraryLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                               cta::Scheduler& scheduler,
                                                               const frontend::AdminCmdStream& requestMsg)
    : CtaAdminResponseStream(catalogue, scheduler, requestMsg.getInstanceName()) {
      cta::frontend::AdminCmdOptions request(requestMsg.getAdminCmd());

      std::optional<bool> disabled = request.getOptional(cta::admin::OptionBoolean::DISABLED);
      m_logicalLibraries = m_catalogue.LogicalLibrary()->getLogicalLibraries();

      if (disabled) {
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

}  // namespace cta::cmdline