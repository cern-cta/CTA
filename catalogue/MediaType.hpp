/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace cta::catalogue {

/**
 * Structure describing a tape media type.
 */
struct MediaType {

  /**
   * Constructor.
   *
   * Sets the value of all integer member-variables to zero. Optional integer
   * member-variables are left as null.
   */
  MediaType() = default;

  /**
   * The capacity in bytes.
   */
  std::uint64_t capacityInBytes = 0;

  /**
   * The name of the media type.
   */
  std::string name;

  /**
   * The cartridge.
   */
  std::string cartridge;

  /**
   * The primary SCSI density code.
   */
  std::optional<std::uint8_t> primaryDensityCode;

  /**
   * The secondary SCSI density code.
   */
  std::optional<std::uint8_t> secondaryDensityCode;

  /**
   * The number of tape wraps.
   */
  std::optional<std::uint32_t> nbWraps;

  /**
   * The minimum longitudinal tape position.
   */
  std::optional<std::uint64_t> minLPos;

  /**
   * The maximum longitudinal tape position.
   */
  std::optional<std::uint64_t> maxLPos;

  /**
   * The user comment.
   */
  std::string comment;

}; // struct MediaType

} // namespace cta::catalogue
