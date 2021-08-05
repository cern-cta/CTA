/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <string>
#include <vector>

#include "common/optional.hpp"

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
  optional<uint64_t> archiveFileId;

  /**
   * The name of a disk instance.
   */
  optional<std::string> diskInstance;
  
  /**
   * The volume identifier of a tape.
   */
  optional<std::string> vid;

  /**
   * List of disk file IDs.
   *
   * These are given as a list of strings in DECIMAL format. EOS provides the fxids in hex format. The parsing and
   * conversion into decimal is done in the cta-admin client, ready to be built into a SQL query string.
   */
  optional<std::vector<std::string>> diskFileIds;

  /**
   * The copy number of the deleted tape file.
   */
  optional<uint64_t> copynb;
  
}; // struct TapeFileSearchCriteria

} // namespace catalogue
} // namespace cta
