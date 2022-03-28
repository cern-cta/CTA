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

#include "common/dataStructures/OwnerIdentity.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * The status of a remote file or a directory.
 */
struct RemoteFileStatus {
  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   */
  RemoteFileStatus();

  /**
   * Constructor.
   *
   * @param owner The identity of the owner.
   * @param mode The mode bits of the file or directory.
   * @param size The size of the file in bytes.
   */
  RemoteFileStatus(
    const common::dataStructures::OwnerIdentity &owner,
    const mode_t mode,
    const uint64_t size);

  /**
   * The identity of the owner.
   */
  common::dataStructures::OwnerIdentity owner;

  /**
   * The mode bits of the directory entry.
   */
  mode_t mode;

  /**
   * The size of the file in bytes.
   */
  uint64_t size;

}; // class RemoteFileStatus

} // namespace cta
