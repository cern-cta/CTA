// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "CtaAdminServerWriteReactor.hpp"

namespace cta::frontend::grpc {

class MountPolicyLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        MountPolicyLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        std::list<cta::common::dataStructures::MountPolicy> m_mountPolicyList;
        std::list<cta::common::dataStructures::MountPolicy>::const_iterator next_mp;
};

MountPolicyLsWriteReactor::MountPolicyLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName),
      m_mountPolicyList(catalogue.MountPolicy()->getMountPolicies()) {
    using namespace cta::admin;

    std::cout << "In MountPolicyLsWriteReactor constructor, just entered!" << std::endl;

    next_mp = m_mountPolicyList.cbegin();
    NextWrite();
}

void MountPolicyLsWriteReactor::NextWrite() {
    std::cout << "In MountPolicyLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::MOUNTPOLICY_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_mp != m_mountPolicyList.cend()) {
            const auto& mp = *next_mp;
            ++next_mp;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::MountPolicyLsItem *mp_item = data->mutable_mpls_item();
            
            mp_item->set_name(mp.name);
            mp_item->set_archive_priority(mp.archivePriority);
            mp_item->set_archive_min_request_age(mp.archiveMinRequestAge);
            mp_item->set_retrieve_priority(mp.retrievePriority);
            mp_item->set_retrieve_min_request_age(mp.retrieveMinRequestAge);
            mp_item->mutable_creation_log()->set_username(mp.creationLog.username);
            mp_item->mutable_creation_log()->set_host(mp.creationLog.host);
            mp_item->mutable_creation_log()->set_time(mp.creationLog.time);
            mp_item->mutable_last_modification_log()->set_username(mp.lastModificationLog.username);
            mp_item->mutable_last_modification_log()->set_host(mp.lastModificationLog.host);
            mp_item->mutable_last_modification_log()->set_time(mp.lastModificationLog.time);
            mp_item->set_comment(mp.comment);

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