#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class LogicalLibraryLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        LogicalLibraryLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        std::list<cta::common::dataStructures::LogicalLibrary> m_logicalLibraryList;
        std::optional<bool> m_disabled;
        std::list<cta::common::dataStructures::LogicalLibrary>::const_iterator next_ll;
};

LogicalLibraryLsWriteReactor::LogicalLibraryLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName),
      m_logicalLibraryList(catalogue.LogicalLibrary()->getLogicalLibraries()) {
    using namespace cta::admin;

    request::RequestMessage requestMsg(*request);
    
    m_disabled = requestMsg.getOptional(admin::OptionBoolean::DISABLED);

    next_ll = m_logicalLibraryList.cbegin();
    NextWrite();
}

void LogicalLibraryLsWriteReactor::NextWrite() {
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::LOGICALLIBRARY_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_ll != m_logicalLibraryList.cend()) {
            const auto& ll = *next_ll;
            ++next_ll;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::LogicalLibraryLsItem *ll_item = data->mutable_llls_item();
            
            if (m_disabled && m_disabled.value() != ll.isDisabled) {
                continue;
            }

            fillLogicalLibraryItem(ll, ll_item, m_instanceName);

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