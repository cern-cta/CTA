#pragma once

#include "CtaAdminResponseStream.hpp"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>
#include "common/dataStructures/DiskInstance.hpp"
#include <list>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class DiskInstanceLsResponseStream : public CtaAdminResponseStream {
public:
    DiskInstanceLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                cta::Scheduler& scheduler, 
                                const std::string& instanceName);
        
    bool isDone() override;
    cta::xrd::Data next() override;
    void init(const admin::AdminCmd& admincmd) override;

private:
    cta::catalogue::Catalogue& m_catalogue;
    cta::Scheduler& m_scheduler;
    const std::string m_instanceName;
    
    std::list<cta::common::dataStructures::DiskInstance> m_diskInstances;
};

DiskInstanceLsResponseStream::DiskInstanceLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                                          cta::Scheduler& scheduler, 
                                                          const std::string& instanceName)
    : m_catalogue(catalogue), 
      m_scheduler(scheduler), 
      m_instanceName(instanceName) {
}

void DiskInstanceLsResponseStream::init(const admin::AdminCmd& admincmd) {
    m_diskInstances = m_catalogue.DiskInstance()->getAllDiskInstances();
}

bool DiskInstanceLsResponseStream::isDone() {
    return m_diskInstances.empty();
}

cta::xrd::Data DiskInstanceLsResponseStream::next() {
    if (isDone()) {
        throw std::runtime_error("Stream is exhausted");
    }
    
    const auto di = m_diskInstances.front();
    m_diskInstances.pop_front();
    
    cta::xrd::Data data;
    auto di_item = data.mutable_dils_item();
    
    di_item->set_name(di.name);
    di_item->set_instance_name(m_instanceName);
    di_item->mutable_creation_log()->set_username(di.creationLog.username);
    di_item->mutable_creation_log()->set_host(di.creationLog.host);
    di_item->mutable_creation_log()->set_time(di.creationLog.time);
    di_item->mutable_last_modification_log()->set_username(di.lastModificationLog.username);
    di_item->mutable_last_modification_log()->set_host(di.lastModificationLog.host);
    di_item->mutable_last_modification_log()->set_time(di.lastModificationLog.time);
    di_item->set_comment(di.comment);
    
    return data;
}

} // namespace cta::cmdline