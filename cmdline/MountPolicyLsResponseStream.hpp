#pragma once

#include "CtaAdminResponseStream.hpp"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>
#include "common/dataStructures/MountPolicy.hpp"
#include <list>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class MountPolicyLsResponseStream : public CtaAdminResponseStream {
public:
    MountPolicyLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                               cta::Scheduler& scheduler, 
                               const std::string& instanceName);
        
    bool isDone() override;
    cta::xrd::Data next() override;
    void init(const admin::AdminCmd& admincmd) override;

private:
    cta::catalogue::Catalogue& m_catalogue;
    cta::Scheduler& m_scheduler;
    const std::string m_instanceName;
    
    std::list<common::dataStructures::MountPolicy> m_mountPolicies;
};

MountPolicyLsResponseStream::MountPolicyLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                                        cta::Scheduler& scheduler, 
                                                        const std::string& instanceName)
    : m_catalogue(catalogue), 
      m_scheduler(scheduler), 
      m_instanceName(instanceName) {
}

void MountPolicyLsResponseStream::init(const admin::AdminCmd& admincmd) {
    // Mount policy ls doesn't have specific search criteria - it just gets all policies
    m_mountPolicies = m_catalogue.MountPolicy()->getMountPolicies();
}

bool MountPolicyLsResponseStream::isDone() {
    return m_mountPolicies.empty();
}

cta::xrd::Data MountPolicyLsResponseStream::next() {
    if (isDone()) {
        throw std::runtime_error("Stream is exhausted");
    }
    
    const auto mp = m_mountPolicies.front();
    m_mountPolicies.pop_front();
    
    cta::xrd::Data data;
    auto mpItem = data.mutable_mpls_item();
    
    mpItem->set_name(mp.name);
    mpItem->set_instance_name(m_instanceName);
    mpItem->set_archive_priority(mp.archivePriority);
    mpItem->set_archive_min_request_age(mp.archiveMinRequestAge);
    mpItem->set_retrieve_priority(mp.retrievePriority);
    mpItem->set_retrieve_min_request_age(mp.retrieveMinRequestAge);
    mpItem->mutable_creation_log()->set_username(mp.creationLog.username);
    mpItem->mutable_creation_log()->set_host(mp.creationLog.host);
    mpItem->mutable_creation_log()->set_time(mp.creationLog.time);
    mpItem->mutable_last_modification_log()->set_username(mp.lastModificationLog.username);
    mpItem->mutable_last_modification_log()->set_host(mp.lastModificationLog.host);
    mpItem->mutable_last_modification_log()->set_time(mp.lastModificationLog.time);
    mpItem->set_comment(mp.comment);
    
    return data;
}

} // namespace cta::cmdline