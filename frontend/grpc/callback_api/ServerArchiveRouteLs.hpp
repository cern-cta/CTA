// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include <common/dataStructures/ArchiveRouteTypeSerDeser.hpp>

namespace cta::frontend::grpc {

class ArchiveRouteLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        ArchiveRouteLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In ArchiveRouteLsWriteReactor, we are inside OnWriteDone" << std::endl;
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
        std::list<cta::common::dataStructures::ArchiveRoute> m_archiveRouteList; 
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
        cta::xrd::StreamResponse m_response;
        std::list<cta::common::dataStructures::ArchiveRoute>::const_iterator next_ar;
};

void ArchiveRouteLsWriteReactor::OnDone() {
    std::cout << "In ArchiveRouteLsWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

ArchiveRouteLsWriteReactor::ArchiveRouteLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
    : m_archiveRouteList(catalogue.ArchiveRoute()->getArchiveRoutes()),
      m_isHeaderSent(false) {
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
            
            ar_item->set_storage_class(ar.storageClassName);
            ar_item->set_copy_number(ar.copyNb);
            ar_item->set_archive_route_type(cta::admin::ArchiveRouteTypeFormatToProtobuf(ar.type));
            ar_item->set_tapepool(ar.tapePoolName);
            ar_item->mutable_creation_log()->set_username(ar.creationLog.username);
            ar_item->mutable_creation_log()->set_host(ar.creationLog.host);
            ar_item->mutable_creation_log()->set_time(ar.creationLog.time);
            ar_item->mutable_last_modification_log()->set_username(ar.lastModificationLog.username);
            ar_item->mutable_last_modification_log()->set_host(ar.lastModificationLog.host);
            ar_item->mutable_last_modification_log()->set_time(ar.lastModificationLog.time);
            ar_item->set_comment(ar.comment);

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