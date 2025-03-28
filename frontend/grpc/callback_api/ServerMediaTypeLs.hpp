// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "catalogue/MediaTypeWithLogs.hpp"

namespace cta::frontend::grpc {

class MediaTypeLsWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
    public:
        MediaTypeLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In MediaTypeLsWriteReactor, we are inside OnWriteDone" << std::endl;
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
        std::list<cta::catalogue::MediaTypeWithLogs> m_mediaTypeList; 
        bool m_isHeaderSent; // or could be a static variable in the function NextWrite()
        cta::xrd::StreamResponse m_response;
        std::list<cta::catalogue::MediaTypeWithLogs>::const_iterator next_mediatype;
};

void MediaTypeLsWriteReactor::OnDone() {
    std::cout << "In MediaTypeLsWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

MediaTypeLsWriteReactor::MediaTypeLsWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const cta::xrd::Request* request)
    : m_mediaTypeList(catalogue.MediaType()->getMediaTypes()),
      m_isHeaderSent(false) {
    using namespace cta::admin;

    std::cout << "In MediaTypeLsWriteReactor constructor, just entered!" << std::endl;

    next_mediatype = m_mediaTypeList.cbegin();
    NextWrite();
}

void MediaTypeLsWriteReactor::NextWrite() {
    std::cout << "In MediaTypeLsWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::MEDIATYPE_LS);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        while(next_mediatype != m_mediaTypeList.cend()) {
            const auto& mt = *next_mediatype;
            next_mediatype++;
            cta::xrd::Data* data = new cta::xrd::Data();
            cta::admin::MediaTypeLsItem *mt_item = data->mutable_mtls_item();

            mt_item->set_name(mt.name);
            mt_item->set_cartridge(mt.cartridge);
            mt_item->set_capacity(mt.capacityInBytes);
            if (mt.primaryDensityCode) mt_item->set_primary_density_code(mt.primaryDensityCode.value());
            if (mt.secondaryDensityCode) mt_item->set_secondary_density_code(mt.secondaryDensityCode.value());
            if (mt.nbWraps) mt_item->set_number_of_wraps(mt.nbWraps.value());
            if (mt.minLPos) mt_item->set_min_lpos(mt.minLPos.value());
            if (mt.maxLPos) mt_item->set_max_lpos(mt.maxLPos.value());
            mt_item->set_comment(mt.comment);
            mt_item->mutable_creation_log()->set_username(mt.creationLog.username);
            mt_item->mutable_creation_log()->set_host(mt.creationLog.host);
            mt_item->mutable_creation_log()->set_time(mt.creationLog.time);
            mt_item->mutable_last_modification_log()->set_username(mt.lastModificationLog.username);
            mt_item->mutable_last_modification_log()->set_host(mt.lastModificationLog.host);
            mt_item->mutable_last_modification_log()->set_time(mt.lastModificationLog.time);

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