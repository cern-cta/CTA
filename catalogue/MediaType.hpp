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
