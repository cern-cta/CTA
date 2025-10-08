/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <optional>
#include <string>
#include <vector>

namespace cta::catalogue {

/**
 * The collection of criteria used to select a set of tape files.
 * A tape file is selected if it meets all of the specified criteria.
 * A criterion is only considered specified if it has been set.
 * Please note that no wild cards, for example '*' or '%', are supported.
 */
struct RecycleTapeFileSearchCriteria {
  /**
   * The unique identifier of an archive file.
   */
  std::optional<uint64_t> archiveFileId;

  /**
   * The name of a disk instance.
   */
  std::optional<std::string> diskInstance;

  /**
   * The volume identifier of a tape.
   */
  std::optional<std::string> vid;

  /**
   * List of disk file IDs.
   *
   * These are given as a list of strings in DECIMAL format. EOS provides the fxids in hex format. The parsing and
   * conversion into decimal is done in the cta-admin client, ready to be built into a SQL query string.
   */
  std::optional<std::vector<std::string>> diskFileIds;

  /**
   * The copy number of the deleted tape file.
   */
  std::optional<uint64_t> copynb;

  /**
   * Minimum recycle log time of a file
   */
  std::optional<uint64_t> recycleLogTimeMin;

  /**
   * Maximum recycle log time of a file
   */
  std::optional<uint64_t> recycleLogTimeMax;

  /**
   * The name of the virtual organization
   */
   std::optional<std::string> vo;

}; // struct TapeFileSearchCriteria

} // namespace cta::catalogue
