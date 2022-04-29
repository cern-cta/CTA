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

#include "common/dataStructures/Tape.hpp"

#include <optional>
#include <string>

namespace cta {
namespace catalogue {

/**
 * The tape attributes required to create a new tape entry in the CTA catalogue.
 */
struct CreateTapeAttributes {

  /**
   * The volume identifier of the tape.
   */
  std::string vid;

  /**
   * The media type of the tape.
   */
  std::string mediaType;

  /**
   * The vendor of the tape.
   */
  std::string vendor;

  /**
   * The logical library in which the tape is located.
   */
  std::string logicalLibraryName;

  /**
   * The tape pool to which the tape belongs.
   */
  std::string tapePoolName;

  /**
   * True if the tape is full.
   */
  bool full;

  /**
   * Optional comment about the tape.
   */
  std::optional<std::string> comment;

  /**
   * State of the tape
   */
  cta::common::dataStructures::Tape::State state;

  /**
   * Optional reason for the state
   */
  std::optional<std::string> stateReason;

  /**
   * Constructor.
   *
   * Sets the value of all boolean member-variables to false.
   */
  CreateTapeAttributes():
    full(false) {
  }
}; // struct CreateTapeAttributes

} // namespace catalogue
} // namespace cta
