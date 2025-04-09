#pragma once

#include "CtaAdminResponseStream.hpp"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>
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
                                   const std::string& instanceName);        
    bool isDone() override;
    cta::xrd::Data next() override;
    void init(const admin::AdminCmd& admincmd) override;

private:
    cta::catalogue::Catalogue& m_catalogue;
    cta::Scheduler& m_scheduler;
    const std::string m_instanceName;
    
    std::list<cta::common::dataStructures::RequesterGroupMountRule> m_groupMountRules;
};

GroupMountRuleLsResponseStream::GroupMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                                               cta::Scheduler& scheduler, 
                                                               const std::string& instanceName)
    : m_catalogue(catalogue), 
      m_scheduler(scheduler), 
      m_instanceName(instanceName) {
}

void GroupMountRuleLsResponseStream::init(const admin::AdminCmd& admincmd) {
    // GroupMountRuleLs command doesn't take any parameters, just get all group mount rules
    auto groupMountRules = m_catalogue.RequesterGroupMountRule()->getRequesterGroupMountRules();
    
    // Convert list to our internal list
    m_groupMountRules = std::move(groupMountRules);
}

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

} // namespace cta::cmdline