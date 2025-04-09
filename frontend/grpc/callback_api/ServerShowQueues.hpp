// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class ShowQueuesWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        ShowQueuesWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, cta::log::LogContext lc, const cta::xrd::Request* request);
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
        std::list<cta::common::dataStructures::QueueAndMountSummary> m_queuesAndMountsList; 
        std::optional<std::string> m_schedulerBackendName;
        const std::string m_instanceName;
        bool m_isHeaderSent;
        cta::xrd::StreamResponse m_response;
        std::list<cta::common::dataStructures::QueueAndMountSummary>::const_iterator next_sq;
};

void ShowQueuesWriteReactor::OnDone() {
    delete this;
}

ShowQueuesWriteReactor::ShowQueuesWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, cta::log::LogContext lc, const cta::xrd::Request* request)
    : m_queuesAndMountsList(scheduler.getQueuesAndMountSummaries(lc)),
      m_schedulerBackendName(scheduler.getSchedulerBackendName()),
      m_instanceName(instanceName),
      m_isHeaderSent(false) {
    using namespace cta::admin;

    std::cout << "In ShowQueuesWriteReactor constructor, just entered!" << std::endl;

    next_sq = m_queuesAndMountsList.cbegin();
    NextWrite();
}

void ShowQueuesWriteReactor::NextWrite() {
    m_response.Clear();
    using namespace cta::admin;
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::SHOWQUEUES);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_sq != m_queuesAndMountsList.cend()) {
            const auto& sq = *next_sq;
            ++next_sq;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::ShowQueuesItem *sq_item = data->mutable_sq_item();
            
            fillSqItem(sq, sq_item, m_schedulerBackendName, m_instanceName);

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