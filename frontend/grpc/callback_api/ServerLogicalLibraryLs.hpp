// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"

namespace cta::frontend::grpc {

class LogicalLibraryLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        LogicalLibraryLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
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
        std::list<cta::common::dataStructures::LogicalLibrary> m_logicalLibraryList;
        bool m_isHeaderSent;
        std::optional<bool> m_disabled;
        cta::xrd::StreamResponse m_response;
        std::list<cta::common::dataStructures::LogicalLibrary>::const_iterator next_ll;
};

void LogicalLibraryLsWriteReactor::OnDone() {
    delete this;
}

LogicalLibraryLsWriteReactor::LogicalLibraryLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
    : m_logicalLibraryList(catalogue.LogicalLibrary()->getLogicalLibraries()),
      m_isHeaderSent(false) {
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
            next_ll++;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::LogicalLibraryLsItem *ll_item = data->mutable_llls_item();
            
            if (m_disabled && m_disabled.value() != ll.isDisabled) {
                continue;
            }
        
            ll_item->set_name(ll.name);
            ll_item->set_is_disabled(ll.isDisabled);
            if(ll.physicalLibraryName) {
                ll_item->set_physical_library(ll.physicalLibraryName.value());
            }
            if (ll.disabledReason) {
                ll_item->set_disabled_reason(ll.disabledReason.value());
            }
            ll_item->mutable_creation_log()->set_username(ll.creationLog.username);
            ll_item->mutable_creation_log()->set_host(ll.creationLog.host);
            ll_item->mutable_creation_log()->set_time(ll.creationLog.time);
            ll_item->mutable_last_modification_log()->set_username(ll.lastModificationLog.username);
            ll_item->mutable_last_modification_log()->set_host(ll.lastModificationLog.host);
            ll_item->mutable_last_modification_log()->set_time(ll.lastModificationLog.time);
            ll_item->set_comment(ll.comment);

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