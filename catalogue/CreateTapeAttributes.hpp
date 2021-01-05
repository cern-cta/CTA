/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/optional.hpp"
#include "common/dataStructures/Tape.hpp"

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
   * True if the tape is disabled.
   */
  bool disabled;

  /**
   * True if the tape is read-only.
   */
  bool readOnly;

  /**
   * Optional comment about the tape.
   */
  optional<std::string> comment;

  /**
   * State of the tape
   */
  cta::common::dataStructures::Tape::State state;
  
  /**
   * Optional reason for the state 
   */
  cta::optional<std::string> stateReason;
  
  /**
   * Constructor.
   *
   * Sets the value of all boolean member-variables to false.
   */
  CreateTapeAttributes():
    full(false),
    disabled(false),
    readOnly(false) {
  }
}; // struct CreateTapeAttributes

} // namespace catalogue
} // namespace cta
