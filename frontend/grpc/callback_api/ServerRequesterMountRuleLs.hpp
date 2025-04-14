// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"

namespace cta::frontend::grpc {

class RequesterMountRuleLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        RequesterMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In RequesterMountRuleLsWriteReactor, we are inside OnWriteDone" << std::endl;
            if (!ok) {
                std::cout << "Unexpected failure in OnWriteDone" << std::endl;
                Finish(Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
            }
            std::cout << "Calling NextWrite inside server's OnWriteDone" << std::endl;
            NextWrite();
        }
        void OnDone() override;
        void NextWrite();
    private:
        std::list<cta::common::dataStructures::RequesterMountRule> m_requesterMountRuleList; 
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
        cta::xrd::StreamResponse m_response;
        std::list<cta::common::dataStructures::RequesterMountRule>::const_iterator next_rmr;
};

void RequesterMountRuleLsWriteReactor::OnDone() {
    std::cout << "In RequesterMountRuleLsWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

RequesterMountRuleLsWriteReactor::RequesterMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
    : m_requesterMountRuleList(catalogue.RequesterMountRule()->getRequesterMountRules()),
      m_isHeaderSent(false) {
    using namespace cta::admin;

    std::cout << "In RequesterMountRuleLsWriteReactor constructor, just entered!" << std::endl;

    next_rmr = m_requesterMountRuleList.cbegin();
    NextWrite();
}

void RequesterMountRuleLsWriteReactor::NextWrite() {
    std::cout << "In RequesterMountRuleLsWriteReactor::NextWrite(), just entered!" << std::endl;
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
        while(next_rmr != m_requesterMountRuleList.cend()) {
            const auto& rmr = *next_rmr;
            ++next_rmr;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::RequesterMountRuleLsItem *rmr_item = data->mutable_rmrls_item();
            
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