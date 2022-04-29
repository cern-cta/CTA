/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <optional>
#include <string>
#include <vector>

namespace cta {
namespace catalogue {

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

}; // struct TapeFileSearchCriteria

} // namespace catalogue
} // namespace cta
