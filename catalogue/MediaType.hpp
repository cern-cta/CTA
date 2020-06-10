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

#include <cstdint>
#include <string>

namespace cta {
namespace catalogue {

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
  MediaType():
    capacityInBytes(0) {
  }

  /**
   * The name of the media type.
   */
  std::string name;

  /**
   * The cartridge.
   */
  std::string cartridge;

  /**
   * The capacity in bytes.
   */
  std::uint64_t capacityInBytes;

  /**
   * The primary SCSI density code.
   */
  cta::optional<std::uint8_t> primaryDensityCode;

  /**
   * The secondary SCSI density code.
   */
  cta::optional<std::uint8_t> secondaryDensityCode;

  /**
   * The number of tape wraps.
   */
  cta::optional<std::uint32_t> nbWraps;

  /**
   * The minimum longitudinal tape position.
   */
  cta::optional<std::uint64_t> minLPos;

  /**
   * The maximum longitudinal tape position.
   */
  cta::optional<std::uint64_t> maxLPos;

  /**
   * The user comment.
   */
  std::string comment;

}; // struct MediaType

} // namespace catalogue
} // namespace cta
