/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "common/dataStructures/Tape.hpp"

namespace cta::catalogue {

/**
 * The collection of criteria used to select a set of tapes.
 *
 * A tape is selected if it meets all of the specified criteria.
 *
 * A criterion is only considered specified if it has been set.
 *
 * Please note that no wild cards, for example '*' or '%', are supported.
 */
struct TapeSearchCriteria {
  /**
   * The volume identifier of a tape.
   */
  std::optional<std::string> vid;

  /**
   * The media type of a tape.
   */
  std::optional<std::string> mediaType;

  /**
   * The vendor of a tape.
   */
  std::optional<std::string> vendor;

  /**
   * The name of a logical library.
   */
  std::optional<std::string> logicalLibrary;

  /**
   * The name of a tape pool.
   */
  std::optional<std::string> tapePool;

  /**
   * The virtual organisation that owns a tape.
   */
  std::optional<std::string> vo;

  /**
   * The purchase order related to the tape
   */
  std::optional<std::string> purchaseOrder;

  /**
   * The physical library related to the tape
   */
  std::optional<std::string> physicalLibraryName;

  /**
   * The capacity of a tape in bytes
   */
  std::optional<uint64_t> capacityInBytes;

  /**
   * Set to true if searching for full tapes.
   */
  std::optional<bool> full;

  /**
   * Set to true if searching for tapes imported from castor.
   */
  std::optional<bool> fromCastor;

  /**
   * List of disk file IDs to search for.
   */
  std::optional<std::vector<std::string>> diskFileIds;

  /**
   * Check tapes with missing tape file copies
   */
  std::optional<bool> checkMissingFileCopies;

  /**
   * Missing tape file copies minimum age in secs
   */
  uint64_t missingFileCopiesMinAgeSecs;

  /**
   * The state of the tapes to look for
   */
  std::optional<common::dataStructures::Tape::State> state;
};  // struct TapeSearchCriteria

} // namespace cta::catalogue
