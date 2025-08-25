#pragma once

#include "CtaAdminResponseStream.hpp"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>
#include "common/dataStructures/VirtualOrganization.hpp"
#include <list>

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class VirtualOrganizationLsResponseStream : public CtaAdminResponseStream {
public:
    VirtualOrganizationLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                       cta::Scheduler& scheduler, 
                                       const std::string& instanceName);
        
    bool isDone() override;
    cta::xrd::Data next() override;
    void init(const admin::AdminCmd& admincmd) override;

private:
    cta::catalogue::Catalogue& m_catalogue;
    cta::Scheduler& m_scheduler;
    const std::string m_instanceName;
    
    std::list<cta::common::dataStructures::VirtualOrganization> m_virtualOrganizations;
};

VirtualOrganizationLsResponseStream::VirtualOrganizationLsResponseStream(cta::catalogue::Catalogue& catalogue, 
                                                                        cta::Scheduler& scheduler, 
                                                                        const std::string& instanceName)
    : m_catalogue(catalogue), 
      m_scheduler(scheduler), 
      m_instanceName(instanceName) {
}

void VirtualOrganizationLsResponseStream::init(const admin::AdminCmd& admincmd) {
    // VirtualOrganization ls doesn't have specific search criteria - it just gets all VOs
    m_virtualOrganizations = m_catalogue.VO()->getVirtualOrganizations();
}

bool VirtualOrganizationLsResponseStream::isDone() {
    return m_virtualOrganizations.empty();
}

cta::xrd::Data VirtualOrganizationLsResponseStream::next() {
    if (isDone()) {
        throw std::runtime_error("Stream is exhausted");
    }
    
    const auto vo = m_virtualOrganizations.front();
    m_virtualOrganizations.pop_front();
    
    cta::xrd::Data data;
    auto vo_item = data.mutable_vols_item();
    
    vo_item->set_name(vo.name);
    vo_item->set_instance_name(m_instanceName);
    vo_item->set_read_max_drives(vo.readMaxDrives);
    vo_item->set_write_max_drives(vo.writeMaxDrives);
    vo_item->set_max_file_size(vo.maxFileSize);
    vo_item->mutable_creation_log()->set_username(vo.creationLog.username);
    vo_item->mutable_creation_log()->set_host(vo.creationLog.host);
    vo_item->mutable_creation_log()->set_time(vo.creationLog.time);
    vo_item->mutable_last_modification_log()->set_username(vo.lastModificationLog.username);
    vo_item->mutable_last_modification_log()->set_host(vo.lastModificationLog.host);
    vo_item->mutable_last_modification_log()->set_time(vo.lastModificationLog.time);
    vo_item->set_comment(vo.comment);
    vo_item->set_diskinstance(vo.diskInstanceName);
    vo_item->set_is_repack_vo(vo.isRepackVo);
    
    return data;
}

} // namespace cta::cmdline