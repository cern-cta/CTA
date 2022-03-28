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

#include <string>
#include <vector>

#include <common/optional.hpp>

namespace cta {
namespace catalogue {

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
  optional<std::string> vid;

  /**
   * The media type of a tape.
   */
  optional<std::string> mediaType;

  /**
   * The vendor of a tape.
   */
  optional<std::string> vendor;

  /**
   * The name of a logical library.
   */
  optional<std::string> logicalLibrary;

  /**
   * The name of a tape pool.
   */
  optional<std::string> tapePool;

  /**
   * The virtual organisation that owns a tape.
   */
  optional<std::string> vo;

  /**
   * The capacity of a tape in bytes
   */
  optional<uint64_t> capacityInBytes;

  /**
   * Set to true if searching for full tapes.
   */
  optional<bool> full;

  /**
   * Set to true if searching for tapes imported from castor.
   */
  optional<bool> fromCastor;

  /**
   * List of disk file IDs to search for.
   */
  optional<std::vector<std::string>> diskFileIds;
  
  /**
   * The state of the tapes to look for
   */
  optional<common::dataStructures::Tape::State> state;
  
}; // struct TapeSearchCriteria

} // namespace catalogue
} // namespace cta
