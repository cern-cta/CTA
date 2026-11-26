/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/DiskReport.hpp"

#include "common/utils/Base64.hpp"

#include <gtest/gtest.h>

namespace unitTests {

namespace {

const std::string REDACTED(cta::disk_system::REDACTED_QUERY);

}  // namespace

TEST(SanitizeFailureReason, KeepsTextWithoutUrls) {
  EXPECT_EQ("plain failure: no such file", cta::disk_system::sanitizeFailureReason("plain failure: no such file"));
}

TEST(SanitizeFailureReason, RemovesQueryAndKeepsHostAndSurroundingText) {
  EXPECT_EQ("Retrieve failed on root://diskserver//eos/file" + REDACTED + " checksum mismatch",
            cta::disk_system::sanitizeFailureReason(
              "Retrieve failed on root://diskserver//eos/file?cap.msg=secret&cap.sym=key checksum mismatch"));
}

TEST(SanitizeFailureReason, RedactsEveryUrlWithQueryOrFragment) {
  EXPECT_EQ("a root://h1//f1" + REDACTED + " b https://h2/x" + REDACTED + " c eos://h3/y" + REDACTED
              + " d e root://h4//f4",
            cta::disk_system::sanitizeFailureReason(
              "a root://h1//f1?authz=t b https://h2/x#frag c eos://h3/y?q d e root://h4//f4"));
}

TEST(SanitizeFailureReason, ReplacesControlCharactersWithSpaces) {
  EXPECT_EQ("Close() on root://fst//eos/f" + REDACTED + " [ERROR] input/output error  code:400  x",
            cta::disk_system::sanitizeFailureReason(
              "Close() on root://fst//eos/f?cap.msg=secret [ERROR] input/output error\n code:400\r\tx"));
}

TEST(SanitizeFailureReason, RedactsUserInformation) {
  const std::string userinfo(cta::disk_system::REDACTED_USERINFO);
  EXPECT_EQ("open root://" + userinfo + "fst.cern.ch:1095//eos/f failed, then root://" + userinfo + "h//g" + REDACTED,
            cta::disk_system::sanitizeFailureReason(
              "open root://alice:s3cret@fst.cern.ch:1095//eos/f failed, then root://a@b:c@h//g?authz=t"));
}

TEST(SanitizeFailureReason, KeepsTheEndOfLongReasons) {
  const std::string reason = "start" + std::string(2000, 'x') + "end";
  const std::string sanitised = cta::disk_system::sanitizeFailureReason(reason);
  EXPECT_EQ(cta::disk_system::MAX_REPORTED_FAILURE_LENGTH, sanitised.size());
  EXPECT_TRUE(sanitised.starts_with("..."));
  EXPECT_TRUE(sanitised.ends_with("end"));
}

TEST(FailureReportURL, AppendsSanitisedBase64ReasonToEosUrl) {
  const std::string prefix = "eosQuery://mgm//eos/wfe/passwd?mgm.event=sync::retrieve_failed&mgm.errmsg=";
  const std::string url = cta::disk_system::failureReportURL(prefix, "failed on root://ds//f?cap.msg=secret");
  ASSERT_TRUE(url.starts_with(prefix)) << url;
  EXPECT_EQ("failed on root://ds//f" + REDACTED, cta::utils::base64decode(url.substr(prefix.size())));
}

TEST(FailureReportURL, AppendsSanitisedBase64ReasonToDCacheUrl) {
  const std::string prefix = "eosQuery://pool.example.org:1094/error/0000ABCD?error=";
  const std::string url = cta::disk_system::failureReportURL(prefix, "Retrieve failed on root://ds//f?cap.msg=secret");
  ASSERT_TRUE(url.starts_with(prefix)) << url;
  EXPECT_EQ("Retrieve failed on root://ds//f" + REDACTED, cta::utils::base64decode(url.substr(prefix.size())));
}

TEST(FailureReportURL, KeepsUrlsWithoutReasonSlot) {
  const std::string url = "eosQuery://mgm//eos/wfe/passwd?mgm.event=sync::retrieve_failed";
  EXPECT_EQ(url, cta::disk_system::failureReportURL(url, "failed"));
  EXPECT_EQ("null:", cta::disk_system::failureReportURL("null:", "failed"));
  EXPECT_EQ("", cta::disk_system::failureReportURL("", "failed"));
}

TEST(FailureReportURL, KeepsUrlWhenReasonIsEmpty) {
  const std::string url = "eosQuery://mgm//eos/wfe/passwd?mgm.event=sync::archive_failed&mgm.errmsg=";
  EXPECT_EQ(url, cta::disk_system::failureReportURL(url, ""));
}

}  // namespace unitTests
