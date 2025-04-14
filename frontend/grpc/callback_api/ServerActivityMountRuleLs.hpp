// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/RequesterActivityMountRule.hpp"

namespace cta::frontend::grpc {

class ActivityMountRuleLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        ActivityMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            if (!ok) {
                std::cout << "Unexpected failure in OnWriteDone" << std::endl;
                Finish(Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
            }
            NextWrite();
        }
        void OnDone() override;
        void NextWrite();
    private:
        std::list<cta::common::dataStructures::RequesterActivityMountRule> m_activityMountRuleList; 
        bool m_isHeaderSent;
        cta::xrd::StreamResponse m_response;
        std::list<cta::common::dataStructures::RequesterActivityMountRule>::const_iterator next_amr;
};

void ActivityMountRuleLsWriteReactor::OnDone() {
    delete this;
}

ActivityMountRuleLsWriteReactor::ActivityMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
    : m_activityMountRuleList(catalogue.RequesterActivityMountRule()->getRequesterActivityMountRules()),
      m_isHeaderSent(false) {
    using namespace cta::admin;

    std::cout << "In ActivityMountRuleLsWriteReactor constructor, just entered!" << std::endl;

    next_amr = m_activityMountRuleList.cbegin();
    NextWrite();
}

void ActivityMountRuleLsWriteReactor::NextWrite() {
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::ACTIVITYMOUNTRULE_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_amr != m_activityMountRuleList.cend()) {
            const auto& amr = *next_amr;
            ++next_amr;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::ActivityMountRuleLsItem *amr_item = data->mutable_amrls_item();
            
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
            amr_item->set_comment(amr.comment);

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