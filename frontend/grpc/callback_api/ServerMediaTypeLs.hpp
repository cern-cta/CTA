#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/MediaTypeLsResponseStream.hpp"
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

class MediaTypeLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    MediaTypeLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                           cta::Scheduler& scheduler,
                           const std::string& instanceName,
                           const cta::xrd::Request* request);
    void NextWrite() override;

private:
    std::unique_ptr<cta::cmdline::MediaTypeLsResponseStream> m_stream;
};

MediaTypeLsWriteReactor::MediaTypeLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                cta::Scheduler& scheduler,
                                                const std::string& instanceName,
                                                const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName) {
    
    try {
        m_stream = std::make_unique<cta::cmdline::MediaTypeLsResponseStream>(
            catalogue, scheduler, instanceName);
        
        m_stream->init(request->admincmd());
        
        NextWrite();
    } catch (const std::exception& ex) {
        Finish(Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.what()));
    }
}

void MediaTypeLsWriteReactor::NextWrite() {
    m_response.Clear();
    
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::MEDIATYPE_LS);
        m_response.set_allocated_header(header);
        m_isHeaderSent = true;
        StartWrite(&m_response);
        return;
    }
    
    if (!m_stream->isDone()) {
        cta::xrd::Data* data = new cta::xrd::Data(m_stream->next());
        m_response.set_allocated_data(data);
        StartWrite(&m_response);
        return;
    }
    
    Finish(::grpc::Status::OK);
}

} // namespace cta::frontend::grpc