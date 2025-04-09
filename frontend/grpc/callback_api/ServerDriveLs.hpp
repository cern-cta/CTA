#pragma once

#include "CtaAdminServerWriteReactor.hpp"
#include "cmdline/DriveLsResponseStream.hpp"
#include "../RequestMessage.hpp"

namespace cta::frontend::grpc {

class DriveLsWriteReactor : public CtaAdminServerWriteReactor {
public:
    DriveLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                       cta::Scheduler& scheduler,
                       const std::string& instanceName,
                       cta::log::LogContext lc,
                       const cta::xrd::Request* request);
    void NextWrite() override;

private:
    std::unique_ptr<cta::cmdline::DriveLsResponseStream> m_stream;
};

DriveLsWriteReactor::DriveLsWriteReactor(cta::catalogue::Catalogue& catalogue,
                                       cta::Scheduler& scheduler,
                                       const std::string& instanceName,
                                       cta::log::LogContext lc,
                                       const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(catalogue, scheduler, instanceName) {
    
    try {
        m_stream = std::make_unique<cta::cmdline::DriveLsResponseStream>(
            catalogue, scheduler, instanceName, lc);
        
        m_stream->init(request->admincmd());
        
        NextWrite();
    } catch (const std::exception& ex) {
        Finish(Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.what()));
    }
}

void DriveLsWriteReactor::NextWrite() {
    m_response.Clear();
    
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::DRIVE_LS);
        m_response.set_allocated_header(header);
        
        m_isHeaderSent = true;
        StartWrite(&m_response);
        return;
    }
    
    if (!m_stream->isDone()) {
        cta::xrd::Data* data = new cta::xrd::Data();
        *data = m_stream->next();
        
        m_response.set_allocated_data(data);
        StartWrite(&m_response);
    } else {
        Finish(::grpc::Status::OK);
    }
}
} // namespace cta::frontend::grpc