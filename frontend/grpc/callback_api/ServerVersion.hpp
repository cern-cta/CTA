// #include "CtaAdminServer.hpp" // need this for the class CtaAdminServerWriteReactor, nothing else
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "frontend/common/Version.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "version.h"

namespace cta::frontend::grpc {

class VersionWriteReactor : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> /* CtaAdminServerWriteReactor */ {
    public:
        VersionWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string &catalogueConnString, const cta::xrd::Request* request);
        void OnWriteDone(bool ok) override {
            std::cout << "In VersionWriteReactor, we are inside OnWriteDone" << std::endl;
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
        bool m_isHeaderSent;
        frontend::Version m_client_versions;
        frontend::Version m_server_versions;
        std::string m_catalogue_conn_string;
        std::string m_catalogue_version;
        std::optional<std::string> m_schedulerBackendName;
        bool m_is_upgrading;
        cta::xrd::StreamResponse m_response;
};

void VersionWriteReactor::OnDone() {
    std::cout << "In VersionWriteReactor::OnDone(), about to delete this object" << std::endl;
    delete this;
}

VersionWriteReactor::VersionWriteReactor(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, const std::string& catalogueConnString, const cta::xrd::Request* request)
    : m_isHeaderSent(false),
      m_catalogue_conn_string(catalogueConnString),
      m_catalogue_version(catalogue.Schema()->getSchemaVersion().getSchemaVersion<std::string>()),
      m_schedulerBackendName(scheduler.getSchedulerBackendName()),
      m_is_upgrading(catalogue.Schema()->getSchemaVersion().getStatus<catalogue::SchemaVersion::Status>() ==
                     catalogue::SchemaVersion::Status::UPGRADING) {

    std::cout << "In VersionWriteReactor constructor, just entered!" << std::endl;

    m_server_versions.ctaVersion = CTA_VERSION;
    m_server_versions.protobufTag = XROOTD_SSI_PROTOBUF_INTERFACE_VERSION;

    m_client_versions.ctaVersion = request->admincmd().client_version();
    m_client_versions.protobufTag = request->admincmd().protobuf_tag();
    
    NextWrite();
}

void VersionWriteReactor::NextWrite() {
    std::cout << "In VersionWriteReactor::NextWrite(), just entered!" << std::endl;
    m_response.Clear();
    // is this the first item? Then write the header
    if (!m_isHeaderSent) {
        cta::xrd::Response *header = new cta::xrd::Response();
        std::cout << "header is not sent, sending the header" << std::endl;
        header->set_type(cta::xrd::Response::RSP_SUCCESS);
        header->set_show_header(cta::admin::HeaderType::VERSION_CMD);
        m_response.set_allocated_header(header); // now the message takes ownership of the allocated object, we don't need to free header

        m_isHeaderSent = true;
        std::cout << "about to call StartWrite on the server side" << std::endl;
        StartWrite(&m_response); // this will trigger the OnWriteDone method
        std::cout << "called StartWrite on the server" << std::endl;
        return; // because we'll be called in a loop by OnWriteDone
    } else {
        cta::xrd::Data* data = new cta::xrd::Data();
        cta::admin::VersionItem *version = data->mutable_version_item();
        
        auto client_version = version->mutable_client_version();
        client_version->set_cta_version(m_client_versions.ctaVersion);
        client_version->set_xrootd_ssi_protobuf_interface_version(m_client_versions.protobufTag);
        auto server_version = version->mutable_server_version();
        server_version->set_cta_version(m_server_versions.ctaVersion);
        server_version->set_xrootd_ssi_protobuf_interface_version(m_server_versions.protobufTag);
        version->set_catalogue_connection_string(m_catalogue_conn_string);
        version->set_catalogue_version(m_catalogue_version);
        version->set_is_upgrading(m_is_upgrading);
        version->set_scheduler_backend_name(m_schedulerBackendName.value());

        std::cout << "Calling StartWrite on the server, with some data this time" << std::endl;
        m_response.set_allocated_data(data);
        StartWrite(&m_response);
        std::cout << "Finishing the call on the server side" << std::endl;
        // Finish the call
        Finish(::grpc::Status::OK);
    }
}
} // namespace cta::frontend::grpc