// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "CtaAdminServerWriteReactor.hpp"

namespace cta::frontend::grpc {

class GroupMountRuleLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        GroupMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        std::list<cta::common::dataStructures::RequesterGroupMountRule> m_groupMountRuleList; 
        std::list<cta::common::dataStructures::RequesterGroupMountRule>::const_iterator next_gmr;
};

GroupMountRuleLsWriteReactor::GroupMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName),
      m_groupMountRuleList(catalogue.RequesterGroupMountRule()->getRequesterGroupMountRules()) {
    using namespace cta::admin;

    std::cout << "In GroupMountRuleLsWriteReactor constructor, just entered!" << std::endl;

    next_gmr = m_groupMountRuleList.cbegin();
    NextWrite();
}

void GroupMountRuleLsWriteReactor::NextWrite() {
    std::cout << "In GroupMountRuleLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::GROUPMOUNTRULE_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_gmr != m_groupMountRuleList.cend()) {
            const auto& gmr = *next_gmr;
            ++next_gmr;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::GroupMountRuleLsItem *gmr_item = data->mutable_gmrls_item();
            
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

            std::cout << "Calling StartWrite on the server, with some data this time" << std::endl;
            m_response.set_allocated_data(data);
            StartWrite(&m_response);
            return; // because we will be called in a loop by OnWriteDone()
        } // end while
        std::cout << "Finishing the call on the server side" << std::endl;
        // Finish the call
        Finish(::grpc::Status::OK);
    }
}
} // namespace cta::frontend::grpc