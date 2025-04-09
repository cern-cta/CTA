// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include <common/dataStructures/ArchiveRouteTypeSerDeser.hpp>
#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::frontend::grpc {

class ArchiveRouteLsWriteReactor : public CtaAdminServerWriteReactor {
    public:
        ArchiveRouteLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request);
        void NextWrite() override;
    private:
        std::list<cta::common::dataStructures::ArchiveRoute> m_archiveRouteList;
        std::list<cta::common::dataStructures::ArchiveRoute>::const_iterator next_ar;
};

ArchiveRouteLsWriteReactor::ArchiveRouteLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& instanceName, const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName),
      m_archiveRouteList(catalogue.ArchiveRoute()->getArchiveRoutes()) {
    using namespace cta::admin;

    std::cout << "In ArchiveRouteLsWriteReactor constructor, just entered!" << std::endl;

    next_ar = m_archiveRouteList.cbegin();
    NextWrite();
}

void ArchiveRouteLsWriteReactor::NextWrite() {
    std::cout << "In ArchiveRouteLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::ARCHIVEROUTE_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_ar != m_archiveRouteList.cend()) {
            const auto& ar = *next_ar;
            ++next_ar;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::ArchiveRouteLsItem *ar_item = data->mutable_arls_item();
            
            fillArchiveRouteItem(ar, ar_item, m_instanceName);

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