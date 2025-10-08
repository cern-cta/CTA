/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/dataStructures/Tape.hpp"

#include <optional>
#include <string>

namespace cta::catalogue {

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
  bool full = false;

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
   * The purchase order related to the tape
   */
  std::optional<std::string> purchaseOrder;

  /**
   * Constructor.
   *
   * Sets the value of all boolean member-variables to false.
   */
  CreateTapeAttributes() = default;
}; // struct CreateTapeAttributes

} // namespace cta::catalogue
