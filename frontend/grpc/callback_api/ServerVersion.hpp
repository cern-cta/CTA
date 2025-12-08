#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "common/dataStructures/LabelFormatSerDeser.hpp"
#include "frontend/common/Version.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "version.h"
#include "CtaAdminServerWriteReactor.hpp"

namespace cta::frontend::grpc {

class VersionWriteReactor : public CtaAdminServerWriteReactor {
public:
  VersionWriteReactor(cta::catalogue::Catalogue& catalogue,
                      cta::Scheduler& scheduler,
                      const std::string& instanceName,
                      const std::string& catalogueConnString,
                      const cta::xrd::Request* request);
  void NextWrite() override;

private:
  bool m_isVersionSent = false;
  frontend::Version m_client_versions;
  frontend::Version m_server_versions;
  std::string m_catalogue_conn_string;
  std::string m_catalogue_version;
  bool m_is_upgrading;
};

VersionWriteReactor::VersionWriteReactor(cta::catalogue::Catalogue& catalogue,
                                         cta::Scheduler& scheduler,
                                         const std::string& instanceName,
                                         const std::string& catalogueConnString,
                                         const cta::xrd::Request* request)
    : CtaAdminServerWriteReactor(scheduler, instanceName, nullptr, cta::admin::HeaderType::VERSION_CMD),
      m_catalogue_conn_string(catalogueConnString),
      m_catalogue_version(catalogue.Schema()->getSchemaVersion().getSchemaVersion<std::string>()),
      m_is_upgrading(catalogue.Schema()->getSchemaVersion().getStatus<catalogue::SchemaVersion::Status>() ==
                     catalogue::SchemaVersion::Status::UPGRADING) {
  m_server_versions.ctaVersion = CTA_VERSION;
  m_server_versions.protobufTag = XROOTD_SSI_PROTOBUF_INTERFACE_VERSION;

  m_client_versions.ctaVersion = request->admincmd().client_version();
  m_client_versions.protobufTag = request->admincmd().protobuf_tag();

  NextWrite();
}

void VersionWriteReactor::NextWrite() {
  m_response.Clear();
  // is this the first item? Then write the header
  if (!m_isHeaderSent) {
    cta::xrd::Response* header = new cta::xrd::Response();
    header->set_type(cta::xrd::Response::RSP_SUCCESS);
    header->set_show_header(cta::admin::HeaderType::VERSION_CMD);
    m_response.set_allocated_header(
      header);  // now the message takes ownership of the allocated object, we don't need to free header

    m_isHeaderSent = true;
    StartWrite(&m_response);  // this will trigger the OnWriteDone method
    return;                   // because we'll be called in a loop by OnWriteDone
  } else if (!m_isVersionSent) {
    cta::xrd::Data* data = new cta::xrd::Data();
    cta::admin::VersionItem* version = data->mutable_version_item();

    auto client_version = version->mutable_client_version();
    client_version->set_cta_version(m_client_versions.ctaVersion);
    client_version->set_xrootd_ssi_protobuf_interface_version(m_client_versions.protobufTag);
    auto server_version = version->mutable_server_version();
    server_version->set_cta_version(m_server_versions.ctaVersion);
    server_version->set_xrootd_ssi_protobuf_interface_version(m_server_versions.protobufTag);
    version->set_catalogue_connection_string(m_catalogue_conn_string);
    version->set_catalogue_version(m_catalogue_version);
    version->set_is_upgrading(m_is_upgrading);
    version->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
    version->set_instance_name(m_instanceName);

    m_isVersionSent = true;

    m_response.set_allocated_data(data);
    StartWrite(&m_response);
  } else {
    // Finish the call
    Finish(::grpc::Status::OK);
  }
}
}  // namespace cta::frontend::grpc
