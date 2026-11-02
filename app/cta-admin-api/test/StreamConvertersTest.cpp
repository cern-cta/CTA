/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/stream/converters/StreamConverters.hpp"

#include "commands/stream/VectorResponseStream.hpp"

#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace unitTests {

namespace {

const cta::common::dataStructures::EntryLog creationLog("creator", "creatorHost", 1000);
const cta::common::dataStructures::EntryLog modificationLog("modifier", "modifierHost", 2000);

cta::common::dataStructures::AdminUser adminUserNamed(const std::string& name) {
  cta::common::dataStructures::AdminUser adminUser;
  adminUser.name = name;
  return adminUser;
}

}  // namespace

TEST(StreamConverters, VectorResponseStreamStreamsItemsInOrderConvertedByToProtoThenIsDone) {
  cta::admin_api::commands::stream::VectorResponseStream<cta::common::dataStructures::AdminUser> stream(
    cta::admin::HeaderType::ADMIN_LS,
    "instance",
    {adminUserNamed("first"), adminUserNamed("second")});

  ASSERT_FALSE(stream.isDone());
  const auto first = stream.next();
  EXPECT_EQ(first.adls_item().user(), "first");
  EXPECT_EQ(first.adls_item().instance_name(), "instance");
  ASSERT_FALSE(stream.isDone());
  EXPECT_EQ(stream.next().adls_item().user(), "second");
  EXPECT_TRUE(stream.isDone());
}

TEST(StreamConverters, VectorResponseStreamIsDoneWhenEmptyAndReportsItsHeaderType) {
  cta::admin_api::commands::stream::VectorResponseStream<cta::common::dataStructures::AdminUser> stream(
    cta::admin::HeaderType::TAPE_LS,
    "instance",
    {});
  EXPECT_TRUE(stream.isDone());
  EXPECT_EQ(stream.headerType(), cta::admin::HeaderType::TAPE_LS);
}

TEST(StreamConverters, VectorResponseStreamThrowsWhenExhausted) {
  cta::admin_api::commands::stream::VectorResponseStream<cta::common::dataStructures::AdminUser> stream(
    cta::admin::HeaderType::ADMIN_LS,
    "instance",
    {adminUserNamed("only")});
  stream.next();
  EXPECT_THROW(stream.next(), std::runtime_error);
}

TEST(StreamConverters, EntryLogsAreFilledInCreationAndLastModificationLog) {
  cta::common::dataStructures::AdminUser adminUser;
  adminUser.name = "adminName";
  adminUser.comment = "adminComment";
  adminUser.creationLog = creationLog;
  adminUser.lastModificationLog = modificationLog;

  cta::xrd::Data data;
  cta::admin_api::commands::stream::toProto(adminUser, data, "instance");

  const auto& item = data.adls_item();
  EXPECT_EQ(item.user(), "adminName");
  EXPECT_EQ(item.comment(), "adminComment");
  EXPECT_EQ(item.instance_name(), "instance");
  EXPECT_EQ(item.creation_log().username(), "creator");
  EXPECT_EQ(item.creation_log().host(), "creatorHost");
  EXPECT_EQ(item.creation_log().time(), 1000U);
  EXPECT_EQ(item.last_modification_log().username(), "modifier");
  EXPECT_EQ(item.last_modification_log().host(), "modifierHost");
  EXPECT_EQ(item.last_modification_log().time(), 2000U);
}

TEST(StreamConverters, TapePoolUsesCreatedAndModifiedLogs) {
  cta::catalogue::TapePool tapePool;
  tapePool.name = "pool";
  tapePool.vo.name = "vo";
  tapePool.nbTapes = 3;
  tapePool.creationLog = creationLog;
  tapePool.lastModificationLog = modificationLog;
  tapePool.supply_source_set = {"source"};

  cta::xrd::Data data;
  cta::admin_api::commands::stream::toProto(tapePool, data, "instance");

  const auto& item = data.tpls_item();
  EXPECT_EQ(item.name(), "pool");
  EXPECT_EQ(item.vo(), "vo");
  EXPECT_EQ(item.num_tapes(), 3U);
  EXPECT_EQ(item.created().username(), "creator");
  EXPECT_EQ(item.modified().username(), "modifier");
  ASSERT_EQ(item.supply_source_size(), 1);
  EXPECT_EQ(item.supply_source(0), "source");
}

TEST(StreamConverters, DiskInstanceSpaceReportsItsOwnNameAsInstanceName) {
  cta::common::dataStructures::DiskInstanceSpace space;
  space.name = "spaceName";
  space.diskInstance = "diskInstance";

  cta::xrd::Data data;
  cta::admin_api::commands::stream::toProto(space, data, "instance");

  EXPECT_EQ(data.disls_item().name(), "spaceName");
  EXPECT_EQ(data.disls_item().instance_name(), "spaceName");
  EXPECT_EQ(data.disls_item().disk_instance(), "diskInstance");
}

TEST(StreamConverters, MediaTypeMapsOptionalFields) {
  cta::catalogue::MediaTypeWithLogs mediaType;
  mediaType.name = "LTO8";
  mediaType.primaryDensityCode = 94;
  mediaType.nbWraps = std::nullopt;
  mediaType.creationLog = creationLog;
  mediaType.lastModificationLog = modificationLog;

  cta::xrd::Data data;
  cta::admin_api::commands::stream::toProto(mediaType, data, "instance");

  const auto& item = data.mtls_item();
  EXPECT_EQ(item.name(), "LTO8");
  EXPECT_EQ(item.instance_name(), "instance");
  EXPECT_EQ(item.primary_density_code(), 94U);
  EXPECT_EQ(item.number_of_wraps(), 0U);
  EXPECT_EQ(item.creation_log().username(), "creator");
  EXPECT_EQ(item.last_modification_log().host(), "modifierHost");
}

TEST(StreamConverters, VersionReportsClientServerAndCatalogue) {
  cta::admin_api::commands::stream::VersionInfo versionInfo;
  versionInfo.client = {"clientCta", "clientTag"};
  versionInfo.server = {"serverCta", "serverTag"};
  versionInfo.catalogueConnectionString = "oracle:db";
  versionInfo.catalogueVersion = "15.0";
  versionInfo.isUpgrading = true;
  versionInfo.schedulerBackendName = "backend";

  cta::xrd::Data data;
  cta::admin_api::commands::stream::toProto(versionInfo, data, "instance");

  const auto& item = data.version_item();
  EXPECT_EQ(item.client_version().cta_version(), "clientCta");
  EXPECT_EQ(item.client_version().xrootd_ssi_protobuf_interface_version(), "clientTag");
  EXPECT_EQ(item.server_version().cta_version(), "serverCta");
  EXPECT_EQ(item.server_version().xrootd_ssi_protobuf_interface_version(), "serverTag");
  EXPECT_EQ(item.catalogue_connection_string(), "oracle:db");
  EXPECT_EQ(item.catalogue_version(), "15.0");
  EXPECT_TRUE(item.is_upgrading());
  EXPECT_EQ(item.scheduler_backend_name(), "backend");
  EXPECT_EQ(item.instance_name(), "instance");
}

}  // namespace unitTests
