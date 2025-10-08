/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <optional>
#include <string>
#include <vector>

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"

namespace cta {

namespace common::dataStructures {
struct SecurityIdentity;
}

namespace log {
class Logger;
}

namespace rdbms {
class Conn;
}

namespace catalogue {

CTA_GENERATE_EXCEPTION_CLASS(CommentOrReasonWithMoreSizeThanMaximunAllowed);

class RdbmsCatalogueUtils {
public:
  static bool diskSystemExists(rdbms::Conn &conn, const std::string &name);
  [[nodiscard]] static std::optional<std::string> checkCommentOrReasonMaxLength(const std::optional<std::string>& str,
    log::Logger* log);
  static bool storageClassExists(rdbms::Conn &conn, const std::string &storageClassName);
  static bool virtualOrganizationExists(rdbms::Conn &conn, const std::string &voName);
  static std::optional<std::string> defaultVirtualOrganizationForRepackExists(rdbms::Conn &conn);
  static bool mediaTypeExists(rdbms::Conn &conn, const std::string &name);
  static bool diskInstanceExists(rdbms::Conn &conn, const std::string &name);
  static bool tapePoolExists(rdbms::Conn &conn, const std::string &tapePoolName);
  static bool logicalLibraryExists(rdbms::Conn &conn, const std::string &logicalLibraryName);
  static bool tapeExists(rdbms::Conn &conn, const std::string &vid);
  static bool archiveFileIdExists(rdbms::Conn &conn, const uint64_t archiveFileId);
  static bool mountPolicyExists(rdbms::Conn &conn, const std::string &mountPolicyName);
  static bool requesterActivityMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
    const std::string &requesterName, const std::string &activityRegex);
  static bool diskFileIdExists(rdbms::Conn &conn, const std::string &diskInstanceName, const std::string &diskFileId);
  static bool diskFileUserExists(rdbms::Conn &conn, const std::string &diskInstanceName, uint32_t diskFileOwnerUid);
  static bool diskFileGroupExists(rdbms::Conn &conn, const std::string &diskInstanceName, uint32_t diskFileGid);
  static bool requesterMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
    const std::string &requesterName);
  static bool requesterGroupMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
    const std::string &requesterGroupName);

  static bool isSetAndEmpty(const std::optional<std::string> &optionalStr);
  static bool isSetAndEmpty(const std::optional<std::vector<std::string>> &optionalStrList);

  static std::optional<std::string> nulloptIfEmptyStr(const std::optional<std::string> &optionalStr);

  /**
   * Set the DIRTY flag to true
   * @param conn the database connection
   * @param vid	the vid in which we want to set it as dirty
   */
  static void setTapeDirty(rdbms::Conn &conn, const std::string &vid);
  static void setTapeDirty(rdbms::Conn& conn, const uint64_t & archiveFileId);
  static void updateTape(rdbms::Conn &conn, const std::string &vid, const uint64_t lastFSeq,
    const uint64_t compressedBytesWritten, const uint64_t filesWritten, const std::string &tapeDrive);
  static std::string generateTapeStateModifiedBy(const common::dataStructures::SecurityIdentity & admin);
};

}} // namespace cta::catalogue
