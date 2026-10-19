/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "WorkflowEvent.hpp"

#include "grpc-server/PbException.hpp"

#include <gtest/gtest.h>
#include <limits>

namespace unitTests {

namespace {

using cta::exception::PbException;
using cta::frontend::proto_notifications::getArchiveFileId;
using cta::frontend::proto_notifications::getRequester;
using cta::frontend::proto_notifications::getRequestId;

cta::eos::Notification makeEvent() {
  cta::eos::Notification event;
  event.mutable_wf()->set_event(cta::eos::Workflow::CLOSEW);
  return event;
}

cta::eos::Notification makeEventWithXattr(const std::string& key, const std::string& value) {
  auto event = makeEvent();
  (*event.mutable_file()->mutable_xattr())[key] = value;
  return event;
}

}  // namespace

// ---------------------------------------------------------------------------------------------------------------------
// getArchiveFileId
// ---------------------------------------------------------------------------------------------------------------------

TEST(WorkflowEventGetArchiveFileId, FirstClassFieldTakesPriority) {
  auto event = makeEventWithXattr("sys.archive.file_id", "456");
  event.mutable_file()->set_archive_file_id(123);
  EXPECT_EQ(getArchiveFileId(event), 123);
}

TEST(WorkflowEventGetArchiveFileId, FallsBackToXattr) {
  EXPECT_EQ(getArchiveFileId(makeEventWithXattr("sys.archive.file_id", "456")), 456);
}

TEST(WorkflowEventGetArchiveFileId, FallsBackToLegacyXattr) {
  EXPECT_EQ(getArchiveFileId(makeEventWithXattr("CTA_ArchiveFileId", "789")), 789);
}

TEST(WorkflowEventGetArchiveFileId, XattrTakesPriorityOverLegacyXattr) {
  auto event = makeEventWithXattr("sys.archive.file_id", "456");
  (*event.mutable_file()->mutable_xattr())["CTA_ArchiveFileId"] = "789";
  EXPECT_EQ(getArchiveFileId(event), 456);
}

TEST(WorkflowEventGetArchiveFileId, AcceptsMaxUint64) {
  EXPECT_EQ(getArchiveFileId(makeEventWithXattr("sys.archive.file_id", "18446744073709551615")),
            std::numeric_limits<uint64_t>::max());
}

TEST(WorkflowEventGetArchiveFileId, MissingIdThrows) {
  EXPECT_THROW(getArchiveFileId(makeEvent()), PbException);
}

TEST(WorkflowEventGetArchiveFileId, ZeroThrows) {
  EXPECT_THROW(getArchiveFileId(makeEventWithXattr("sys.archive.file_id", "0")), PbException);
}

// The legacy frontend parsed the extended attribute with strtoul(), which accepted the values below. They are now
// rejected.
TEST(WorkflowEventGetArchiveFileId, MalformedXattrThrows) {
  for (const std::string value : {"", "abc", "12abc", " 12", "12 ", "+12", "-1", "0x10", "18446744073709551616"}) {
    SCOPED_TRACE("value='" + value + "'");
    EXPECT_THROW(getArchiveFileId(makeEventWithXattr("sys.archive.file_id", value)), PbException);
    EXPECT_THROW(getArchiveFileId(makeEventWithXattr("CTA_ArchiveFileId", value)), PbException);
  }
}

// ---------------------------------------------------------------------------------------------------------------------
// getRequestId
// ---------------------------------------------------------------------------------------------------------------------

TEST(WorkflowEventGetRequestId, FirstClassFieldTakesPriority) {
  auto event = makeEventWithXattr("sys.cta.objectstore.id", "fromXattr");
  event.mutable_file()->set_request_objectstore_id("fromField");
  EXPECT_EQ(getRequestId(event, "sys.cta.objectstore.id"), "fromField");
}

TEST(WorkflowEventGetRequestId, FallsBackToXattr) {
  EXPECT_EQ(getRequestId(makeEventWithXattr("sys.cta.objectstore.id", "fromXattr"), "sys.cta.objectstore.id"),
            "fromXattr");
}

TEST(WorkflowEventGetRequestId, OnlyTheGivenXattrIsUsed) {
  EXPECT_EQ(getRequestId(makeEventWithXattr("sys.cta.objectstore.id", "fromXattr"), "sys.cta.archive.objectstore.id"),
            std::nullopt);
}

TEST(WorkflowEventGetRequestId, MissingIdReturnsNullopt) {
  EXPECT_EQ(getRequestId(makeEvent(), "sys.cta.objectstore.id"), std::nullopt);
}

// ---------------------------------------------------------------------------------------------------------------------
// getRequester
// ---------------------------------------------------------------------------------------------------------------------

TEST(WorkflowEventGetRequester, ReturnsUserAndGroup) {
  auto event = makeEvent();
  event.mutable_cli()->mutable_user()->set_username("alice");
  event.mutable_cli()->mutable_user()->set_groupname("tape");
  const auto requester = getRequester(event);
  EXPECT_EQ(requester.name, "alice");
  EXPECT_EQ(requester.group, "tape");
}

TEST(WorkflowEventGetRequester, EmptyUsernameThrows) {
  auto event = makeEvent();
  event.mutable_cli()->mutable_user()->set_groupname("tape");
  EXPECT_THROW(getRequester(event), PbException);
}

TEST(WorkflowEventGetRequester, EmptyGroupnameThrows) {
  auto event = makeEvent();
  event.mutable_cli()->mutable_user()->set_username("alice");
  EXPECT_THROW(getRequester(event), PbException);
}

}  // namespace unitTests
