/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "version.hpp"

#include "catalogue/SchemaVersion.hpp"
#include "commands/stream/converters/StreamConverters.hpp"

namespace cta::admin_api::commands::stream {

VersionInfo loadVersion(catalogue::Catalogue& catalogue,
                        cta::Scheduler& scheduler,
                        const std::string& catalogueConnString,
                        const admin::AdminCmd& adminCmd) {
  VersionInfo versionInfo;

  versionInfo.client.ctaVersion = adminCmd.client_version();
  versionInfo.client.protobufTag = adminCmd.protobuf_tag();

  versionInfo.server.ctaVersion = CTA_VERSION;
  versionInfo.server.protobufTag = XROOTD_SSI_PROTOBUF_INTERFACE_VERSION;

  const auto schemaVersion = catalogue.Schema()->getSchemaVersion();
  versionInfo.catalogueConnectionString = catalogueConnString;
  versionInfo.catalogueVersion = schemaVersion.getSchemaVersion<std::string>();
  versionInfo.isUpgrading =
    schemaVersion.getStatus<catalogue::SchemaVersion::Status>() == catalogue::SchemaVersion::Status::UPGRADING;
  versionInfo.schedulerBackendName = scheduler.getSchedulerBackendName();

  return versionInfo;
}

void toProto(const VersionInfo& versionInfo, cta::xrd::Data& data, const std::string& instanceName) {
  auto item = data.mutable_version_item();

  auto clientVersion = item->mutable_client_version();
  clientVersion->set_cta_version(versionInfo.client.ctaVersion);
  clientVersion->set_xrootd_ssi_protobuf_interface_version(versionInfo.client.protobufTag);

  auto serverVersion = item->mutable_server_version();
  serverVersion->set_cta_version(versionInfo.server.ctaVersion);
  serverVersion->set_xrootd_ssi_protobuf_interface_version(versionInfo.server.protobufTag);

  item->set_catalogue_connection_string(versionInfo.catalogueConnectionString);
  item->set_catalogue_version(versionInfo.catalogueVersion);
  item->set_is_upgrading(versionInfo.isUpgrading);
  item->set_scheduler_backend_name(versionInfo.schedulerBackendName);
  item->set_instance_name(instanceName);
}

}  // namespace cta::admin_api::commands::stream
