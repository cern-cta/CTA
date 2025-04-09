#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class ActivityMountRuleLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        ActivityMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        std::list<cta::common::dataStructures::RequesterActivityMountRule> m_activityMountRuleList;
        std::list<cta::common::dataStructures::RequesterActivityMountRule>::const_iterator next_amr;
};

ActivityMountRuleLsWriteReactor::ActivityMountRuleLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName),
      m_activityMountRuleList(catalogue.RequesterActivityMountRule()->getRequesterActivityMountRules()) {
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
            
            fillActivityMountRuleItem(amr, amr_item, m_instanceName);

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