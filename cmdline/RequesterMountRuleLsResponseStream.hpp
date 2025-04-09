#pragma once

#include "CtaAdminResponseStream.hpp"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/log/LogContext.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include <list>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class RequesterMountRuleLsResponseStream : public CtaAdminResponseStream {
public:
    RequesterMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                       cta::Scheduler& scheduler, 
                                       const std::string& instanceName);        
    bool isDone() override;
    cta::xrd::Data next() override;
    void init(const admin::AdminCmd& admincmd) override;

private:
    cta::catalogue::Catalogue& m_catalogue;
    cta::Scheduler& m_scheduler;
    const std::string m_instanceName;
    
    std::list<cta::common::dataStructures::RequesterMountRule> m_requesterMountRules;
};

RequesterMountRuleLsResponseStream::RequesterMountRuleLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                                                       cta::Scheduler& scheduler, 
                                                                       const std::string& instanceName)
    : m_catalogue(catalogue), 
      m_scheduler(scheduler), 
      m_instanceName(instanceName) {
}

void RequesterMountRuleLsResponseStream::init(const admin::AdminCmd& admincmd) {
    // RequesterMountRuleLs command doesn't take any parameters, just get all requester mount rules
    m_requesterMountRules = m_catalogue.RequesterMountRule()->getRequesterMountRules();
}

bool RequesterMountRuleLsResponseStream::isDone() {
    return m_requesterMountRules.empty();
}

cta::xrd::Data RequesterMountRuleLsResponseStream::next() {
    if (isDone()) {
        throw std::runtime_error("Stream is exhausted");
    }
    
    const auto rmr = m_requesterMountRules.front();
    m_requesterMountRules.pop_front();
    
    cta::xrd::Data data;
    auto rmr_item = data.mutable_rmrls_item();
    
    rmr_item->set_instance_name(m_instanceName);
    rmr_item->set_disk_instance(rmr.diskInstance);
    rmr_item->set_requester_mount_rule(rmr.name);
    rmr_item->set_mount_policy(rmr.mountPolicy);
    rmr_item->mutable_creation_log()->set_username(rmr.creationLog.username);
    rmr_item->mutable_creation_log()->set_host(rmr.creationLog.host);
    rmr_item->mutable_creation_log()->set_time(rmr.creationLog.time);
    rmr_item->mutable_last_modification_log()->set_username(rmr.lastModificationLog.username);
    rmr_item->mutable_last_modification_log()->set_host(rmr.lastModificationLog.host);
    rmr_item->mutable_last_modification_log()->set_time(rmr.lastModificationLog.time);
    rmr_item->set_comment(rmr.comment);
   
    return data;
}

} // namespace cta::cmdline